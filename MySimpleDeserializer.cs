using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Newtonsoft.Json;
using NJsonSchema;
using NJsonSchema.Generation;
using NJsonSchema.Validation;
using System.Text.Json;


namespace Local.Serdes
{
    public class MySimpleJsonDeserializer<T> : JsonDeserializer<T> where T : class
    {
        private readonly JsonSchemaGeneratorSettings jsonSchemaGeneratorSettings;

        private JsonSchemaValidator validator = new JsonSchemaValidator();

        private JsonSchema schema = null;

        private JsonSerializerOptions jsonSerializerOptions;

        public MySimpleJsonDeserializer(
            IEnumerable<KeyValuePair<string, string>> config = null,
            JsonSchemaGeneratorSettings jsonSchemaGeneratorSettings = null
        ) : base(config, jsonSchemaGeneratorSettings) { }

        public MySimpleJsonDeserializer(
            ISchemaRegistryClient schemaRegistryClient,
            IEnumerable<KeyValuePair<string, string>> config = null,
            JsonSchemaGeneratorSettings jsonSchemaGeneratorSettings = null
        ) : base(schemaRegistryClient, config, jsonSchemaGeneratorSettings) { }

        public MySimpleJsonDeserializer(
            ISchemaRegistryClient schemaRegistryClient,
            JsonDeserializerConfig config,
            JsonSchemaGeneratorSettings jsonSchemaGeneratorSettings = null,
            IList<IRuleExecutor> ruleExecutors = null
        ) : base(schemaRegistryClient, config, jsonSchemaGeneratorSettings, ruleExecutors) { }

        public MySimpleJsonDeserializer(
            ISchemaRegistryClient schemaRegistryClient,
            Schema schema,
            IEnumerable<KeyValuePair<string, string>> config = null,
            JsonSchemaGeneratorSettings jsonSchemaGeneratorSettings = null
        ) : base(schemaRegistryClient, schema, config, jsonSchemaGeneratorSettings)
        {
            this.jsonSerializerOptions = new JsonSerializerOptions
            {
                //PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
                WriteIndented = false,
                PropertyNameCaseInsensitive = false,
                AllowTrailingCommas = false,
                Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping
            };
        }

        public override async Task<T> DeserializeAsync(ReadOnlyMemory<byte> data, bool isNull, SerializationContext context)
        {
            if (isNull) { return null; }

            var array = data.ToArray();
            if (array.Length < 6)
            {
                throw new InvalidDataException($"Expecting data framing of length 6 bytes or more but total data size is {array.Length} bytes");
            }

            bool isKey = context.Component == MessageComponentType.Key;
            string topic = context.Topic;
            string subject = this.subjectNameStrategy != null
                // use the subject name strategy specified in the serializer config if available.
                ? this.subjectNameStrategy(
                    new SerializationContext(isKey ? MessageComponentType.Key : MessageComponentType.Value, topic),
                    null)
                // else fall back to the deprecated config from (or default as currently supplied by) SchemaRegistry.
                : schemaRegistryClient == null
                    ? null
                    : isKey
                        ? schemaRegistryClient.ConstructKeySubjectName(topic)
                        : schemaRegistryClient.ConstructValueSubjectName(topic);

            Schema latestSchema = await GetReaderSchema(subject).ConfigureAwait(continueOnCapturedContext: false);

            try
            {
                Schema writerSchema = null;
                JsonSchema writerSchemaJson = null;
                T value;
                IList<Migration> migrations = new List<Migration>();
                using (var stream = new MemoryStream(array))
                using (var reader = new BinaryReader(stream))
                {
                    var magicByte = reader.ReadByte();
                    if (magicByte != 0)
                    {
                        throw new InvalidDataException($"Expecting message {context.Component.ToString()} with Confluent Schema Registry framing. Magic byte was {array[0]}, expecting {0}");
                    }

                    var writerId = IPAddress.NetworkToHostOrder(reader.ReadInt32());

                    if (schemaRegistryClient != null)
                    {
                        (writerSchema, writerSchemaJson) = await GetSchema(subject, writerId);
                    }

                    if (latestSchema != null)
                    {
                        migrations = await GetMigrations(subject, writerSchema, latestSchema)
                            .ConfigureAwait(continueOnCapturedContext: false);
                    }

                    if (migrations.Count > 0)
                    {
                        using (var jsonStream = new MemoryStream(array, headerSize, array.Length - headerSize))
                        using (var jsonReader = new StreamReader(jsonStream, Encoding.UTF8))
                        {

                            string jsonString = await jsonReader.ReadToEndAsync();
                            using JsonDocument jsonDocument = JsonDocument.Parse(jsonString);
                            JsonElement jsonElement = jsonDocument.RootElement;
                            jsonElement = (JsonElement)await ExecuteMigrations(migrations, isKey, subject, topic, context.Headers, jsonElement)
                                .ContinueWith(t => t.Result)
                                .ConfigureAwait(continueOnCapturedContext: false);

                            //JToken json = Newtonsoft.Json.JsonConvert.DeserializeObject<JToken>(jsonReader.ReadToEnd(), this.jsonSchemaGeneratorSettings?.ActualSerializerSettings);
                            //json = await ExecuteMigrations(migrations, isKey, subject, topic, context.Headers, json)
                            //    .ContinueWith(t => (JToken)t.Result)
                            //    .ConfigureAwait(continueOnCapturedContext: false);

                            if (schema != null)
                            {
                                var validationResult = validator.Validate(jsonElement.GetRawText(), schema);

                                if (validationResult.Count > 0)
                                {
                                    throw new InvalidDataException("Schema validation failed for properties: [" +
                                                                   string.Join(", ", validationResult.Select(r => r.Path)) + "]");
                                }
                            }

                            value = jsonElement.Deserialize<T>(jsonSerializerOptions);
                        }
                    }
                    else
                    {
                        using (var jsonStream = new MemoryStream(array, headerSize, array.Length - headerSize))
                        using (var jsonReader = new StreamReader(jsonStream, Encoding.UTF8))
                        {
                            string serializedString = jsonReader.ReadToEnd();

                            if (schema != null)
                            {
                                var validationResult = validator.Validate(serializedString, schema);

                                if (validationResult.Count > 0)
                                {
                                    throw new InvalidDataException("Schema validation failed for properties: [" +
                                                                   string.Join(", ", validationResult.Select(r => r.Path)) + "]");
                                }
                            }

                            value = System.Text.Json.JsonSerializer.Deserialize<T>(serializedString, this.jsonSerializerOptions);
                        }
                    }
                }
                if (writerSchema != null)
                {
                    FieldTransformer fieldTransformer = async (ctx, transform, message) =>
                    {
                        return await JsonUtils.Transform(ctx, writerSchemaJson, "$", message, transform).ConfigureAwait(false);
                    };
                    value = await ExecuteRules(context.Component == MessageComponentType.Key, subject,
                            context.Topic, context.Headers, RuleMode.Read, null,
                            writerSchema, value, fieldTransformer)
                        .ContinueWith(t => (T)t.Result)
                        .ConfigureAwait(continueOnCapturedContext: false);
                }

                return value;
            }
            catch (AggregateException e)
            {
                throw e.InnerException;
            }
        }

        protected override async Task<JsonSchema> ParseSchema(Schema schema)
        {
            Confluent.SchemaRegistry.Serdes.JsonSchemaResolver utils = new Confluent.SchemaRegistry.Serdes.JsonSchemaResolver(schemaRegistryClient, schema, jsonSchemaGeneratorSettings);
            return await utils.GetResolvedSchema();
        }
    }
}
