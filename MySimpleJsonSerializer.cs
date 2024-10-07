using NJsonSchema;
using NJsonSchema.Generation;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using NJsonSchema.Validation;
using System.Reflection;
using System.Text.Json;

namespace Local.Serdes
{
    public class MySimpleJsonSerializer<T> : JsonSerializer<T> where T : class
    {
        private readonly List<SchemaReference> ReferenceList = new List<SchemaReference>();

        private JsonSchemaValidator validator = new JsonSchemaValidator();

        private int? schemaId;

        private JsonSchema schema;
        private string schemaText;
        private string schemaFullname;

        private JsonSerializerOptions jsonSerializerOptions;


        public MySimpleJsonSerializer(
            ISchemaRegistryClient schemaRegistryClient,
            JsonSerializerConfig? config = null,
            JsonSchemaGeneratorSettings? jsonSchemaGeneratorSettings = null,
            IList<IRuleExecutor>? ruleExecutors = null) : base(schemaRegistryClient, config, jsonSchemaGeneratorSettings, ruleExecutors)
        {
            Type baseType = typeof(JsonSerializer<T>);
            this.schema = (JsonSchema)baseType.GetField("schema", BindingFlags.NonPublic | BindingFlags.Instance).GetValue(this);
            this.schemaFullname = (string)baseType.GetField("schemaFullname", BindingFlags.NonPublic | BindingFlags.Instance).GetValue(this);
            this.schemaText = (string)baseType.GetField("schemaText", BindingFlags.NonPublic | BindingFlags.Instance).GetValue(this);

            this.jsonSerializerOptions = new JsonSerializerOptions
            {
                //PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
                WriteIndented = false,
                PropertyNameCaseInsensitive = false,
                AllowTrailingCommas = false,
                Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping
            };
        }

        public override async Task<byte[]?> SerializeAsync(T value, SerializationContext context)
        {
            if (value == null) { return null; }

            try
            {
                string subject;
                RegisteredSchema? latestSchema = null;
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
                            schemaId = await schemaRegistryClient.GetSchemaIdAsync(subject, new Schema(this.schemaText, ReferenceList, Confluent.SchemaRegistry.SchemaType.Json), normalizeSchemas).ConfigureAwait(continueOnCapturedContext: false);
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

                //var serializedString = Newtonsoft.Json.JsonConvert.SerializeObject(value, this.jsonSchemaGeneratorSettings?.ActualSerializerSettings);
                var serializedString = System.Text.Json.JsonSerializer.Serialize(value, this.jsonSerializerOptions);
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
    }
}
