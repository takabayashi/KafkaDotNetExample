using NJsonSchema.Generation;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using System.Reflection;
using System.Reflection.Emit;
using Newtonsoft.Json;

namespace Local.Serdes
{
    public class MyInterceptedJsonSerializer<T> : IAsyncSerializer<T> where T : class
    {
        private JsonSerializer<T> confluentSerializer;
        private Func<T, JsonSerializerSettings, string> interceptedMethod;
        private JsonSchemaGeneratorSettings jsonSchemaGeneratorSettings;

        public MyInterceptedJsonSerializer(
            ISchemaRegistryClient schemaRegistryClient,
            JsonSerializerConfig? config = null,
            JsonSchemaGeneratorSettings? jsonSchemaGeneratorSettings = null,
            IList<IRuleExecutor>? ruleExecutors = null)
        {
            this.confluentSerializer = new JsonSerializer<T>(schemaRegistryClient, config, jsonSchemaGeneratorSettings, ruleExecutors);
            this.jsonSchemaGeneratorSettings = jsonSchemaGeneratorSettings;

            var dynamicMethod = new DynamicMethod(
                "InterceptedSerializeObject",
                typeof(string),
                new[] { typeof(T), typeof(JsonSerializerSettings) },
                typeof(MyJsonSerializer<T>).Module
            );

            var il = dynamicMethod.GetILGenerator();
            il.EmitWriteLine("Intercepting call to: JsonConvert.SerializeObject");
            il.Emit(OpCodes.Ldarg_0); // Load 'value' argument
            il.Emit(OpCodes.Ldarg_1); // Load 'settings' argument

            var originalMethod = typeof(JsonConvert).GetMethod("SerializeObject", new[] { typeof(object), typeof(JsonSerializerSettings) });
            il.Emit(OpCodes.Call, originalMethod);

            il.EmitWriteLine("Exiting call to: JsonConvert.SerializeObject");
            this.interceptedMethod = (Func<T, JsonSerializerSettings, string>)dynamicMethod.CreateDelegate(typeof(Func<T, JsonSerializerSettings, string>));

        }

        public async Task<byte[]> SerializeAsync(T value, SerializationContext context)
        {
            return null;
        }
    }
}
