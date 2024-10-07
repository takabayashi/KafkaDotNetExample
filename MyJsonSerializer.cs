using NJsonSchema;
using NJsonSchema.Generation;
using NJsonSchema.Validation;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;

namespace Local.Serdes
{
    public class MyJsonSerializer<T> : AsyncSerializer<T, JsonSchema> where T : class
    {
        private readonly JsonSchemaGeneratorSettings jsonSchemaGeneratorSettings;
        private readonly List<SchemaReference> ReferenceList = new List<SchemaReference>();
        private JsonSchemaValidator validator = new JsonSchemaValidator();
        private int? schemaId;
        private JsonSchema schema;
        private string schemaText;
        private string schemaFullname;

        public MyJsonSerializer(ISchemaRegistryClient schemaRegistryClient, JsonSerializerConfig? config = null, JsonSchemaGeneratorSettings? jsonSchemaGeneratorSettings = null, IList<IRuleExecutor>? ruleExecutors = null) : base(schemaRegistryClient, config, ruleExecutors)
        {
            this.jsonSchemaGeneratorSettings = jsonSchemaGeneratorSettings;

            this.schema = this.jsonSchemaGeneratorSettings == null
                ? JsonSchema.FromType<T>()
                : JsonSchema.FromType<T>(this.jsonSchemaGeneratorSettings);
            this.schemaText = schema.ToJson();
            this.schemaFullname = schema.Title;

            if (config == null) { return; }

            var nonJsonConfig = config
                .Where(item => !item.Key.StartsWith("json.") && !item.Key.StartsWith("rules."));
            if (nonJsonConfig.Count() > 0)
            {
                throw new ArgumentException($"JsonSerializer: unknown configuration parameter {nonJsonConfig.First().Key}");
            }

            if (config.BufferBytes != null) { this.initialBufferSize = config.BufferBytes.Value; }
            if (config.AutoRegisterSchemas != null) { this.autoRegisterSchema = config.AutoRegisterSchemas.Value; }
            if (config.NormalizeSchemas != null) { this.normalizeSchemas = config.NormalizeSchemas.Value; }
            if (config.UseLatestVersion != null) { this.useLatestVersion = config.UseLatestVersion.Value; }
            if (config.LatestCompatibilityStrict != null) { this.latestCompatibilityStrict = config.LatestCompatibilityStrict.Value; }
            if (config.UseLatestWithMetadata != null) { this.useLatestWithMetadata = config.UseLatestWithMetadata; }
            if (config.SubjectNameStrategy != null) { this.subjectNameStrategy = config.SubjectNameStrategy.Value.ToDelegate(); }

            if (this.useLatestVersion && this.autoRegisterSchema)
            {
                throw new ArgumentException($"JsonSerializer: cannot enable both use.latest.version and auto.register.schemas");
            }
        }

        public MyJsonSerializer(ISchemaRegistryClient schemaRegistryClient, Schema schema, JsonSerializerConfig? config = null, JsonSchemaGeneratorSettings? jsonSchemaGeneratorSettings = null, IList<IRuleExecutor>? ruleExecutors = null) : this(schemaRegistryClient, config, jsonSchemaGeneratorSettings, ruleExecutors)
        {
            foreach (var reference in schema.References)
            {
                ReferenceList.Add(reference);
            }

            Confluent.SchemaRegistry.Serdes.JsonSchemaResolver utils = new Confluent.SchemaRegistry.Serdes.JsonSchemaResolver(schemaRegistryClient, schema, this.jsonSchemaGeneratorSettings);
            JsonSchema jsonSchema = utils.GetResolvedSchema().Result;
            this.schema = jsonSchema;
            this.schemaText = schema.SchemaString;
            this.schemaFullname = jsonSchema.Title;
        }

        public override async Task<byte[]> SerializeAsync(T value, SerializationContext context)
        {
            if (value == null) { return null; }

            try
            {
                string subject;
                RegisteredSchema latestSchema = null;
                await serdeMutex.WaitAsync().ConfigureAwait(continueOnCapturedContext: false);
                try
                {
                    subject = this.subjectNameStrategy != null
                        // use the subject name strategy specified in the serializer config if available.
                        ? this.subjectNameStrategy(context, this.schemaFullname)
                        // else fall back to the deprecated config from (or default as currently supplied by) SchemaRegistry.
                        : context.Component == MessageComponentType.Key
                            ? schemaRegistryClient.ConstructKeySubjectName(context.Topic, this.schemaFullname)
                            : schemaRegistryClient.ConstructValueSubjectName(context.Topic, this.schemaFullname);

                    latestSchema = await GetReaderSchema(subject, new Schema(schemaText, ReferenceList, Confluent.SchemaRegistry.SchemaType.Json))
                        .ConfigureAwait(continueOnCapturedContext: false);

                    if (!subjectsRegistered.Contains(subject))
                    {
                        if (latestSchema != null)
                        {
                            schemaId = latestSchema.Id;
                        }
                        else
                        {
                            // first usage: register/get schema to check compatibility
                            schemaId = autoRegisterSchema
                                ? await schemaRegistryClient.RegisterSchemaAsync(subject,
                                        new Schema(this.schemaText, ReferenceList, Confluent.SchemaRegistry.SchemaType.Json), normalizeSchemas)
                                    .ConfigureAwait(continueOnCapturedContext: false)
                                : await schemaRegistryClient.GetSchemaIdAsync(subject,
                                        new Schema(this.schemaText, ReferenceList, Confluent.SchemaRegistry.SchemaType.Json), normalizeSchemas)
                                    .ConfigureAwait(continueOnCapturedContext: false);
                        }
                        subjectsRegistered.Add(subject);
                    }
                }
                finally
                {
                    serdeMutex.Release();
                }

                if (latestSchema != null)
                {
                    var latestSchemaJson = await GetParsedSchema(latestSchema).ConfigureAwait(false);
                    FieldTransformer fieldTransformer = async (ctx, transform, message) =>
                    {
                        return await JsonUtils.Transform(ctx, latestSchemaJson, "$", message, transform).ConfigureAwait(false);
                    };
                    value = await ExecuteRules(context.Component == MessageComponentType.Key, subject,
                            context.Topic, context.Headers, RuleMode.Write, null,
                            latestSchema, value, fieldTransformer)
                        .ContinueWith(t => (T)t.Result)
                        .ConfigureAwait(continueOnCapturedContext: false);
                }

                var serializedString = Newtonsoft.Json.JsonConvert.SerializeObject(value, this.jsonSchemaGeneratorSettings?.ActualSerializerSettings);
                var validationResult = validator.Validate(serializedString, this.schema);
                if (validationResult.Count > 0)
                {
                    throw new InvalidDataException("Schema validation failed for properties: [" + string.Join(", ", validationResult.Select(r => r.Path)) + "]");
                }

                using (var stream = new MemoryStream(initialBufferSize))
                using (var writer = new BinaryWriter(stream))
                {
                    stream.WriteByte(0);
                    writer.Write(System.Net.IPAddress.HostToNetworkOrder(schemaId.Value));
                    writer.Write(System.Text.Encoding.UTF8.GetBytes(serializedString));
                    return stream.ToArray();
                }
            }
            catch (AggregateException e)
            {
                throw e.InnerException;
            }
        }

        protected override async Task<JsonSchema> ParseSchema(Schema schema)
        {
            Confluent.SchemaRegistry.Serdes.JsonSchemaResolver utils = new Confluent.SchemaRegistry.Serdes.JsonSchemaResolver(
                schemaRegistryClient, schema, jsonSchemaGeneratorSettings);
            return await utils.GetResolvedSchema();
        }
    }
}
