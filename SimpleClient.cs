using Confluent.Kafka;
using System;
using System.Threading.Tasks;
using System.Security.Cryptography;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Confluent.Kafka.SyncOverAsync;
using Local.Serdes;
using System.Net.Sockets;

class SimpleClient
{
    static async Task Main(string[] args)
    {
        // Choose to run Producer or Consumer
        Console.WriteLine("Choose mode: 1 for Producer, 2 for Consumer, 10 for Producer with Schema Registry, 20 for Consumer with Schema Registry, 99 for AES-GCM check");
        var choice = Console.ReadLine();

        if (choice == "1")
        {
            await RunProducer();
        }
        else if (choice == "2")
        {
            RunConsumer();
        }
        else if (choice == "10")
        {
            await RunProducerWithRegistry();
        }
        else if (choice == "20")
        {
            RunConsumerWithRegistry();
        }
        else if (choice == "99")
        {
            try
            {
                // Attempt to create an instance of AesGcm
                using (var aesGcm = new AesGcm(new byte[16])) // Key size is 128 bits (16 bytes)
                {
                    Console.WriteLine("AES-GCM is supported on this platform.");
                }
            }
            catch (PlatformNotSupportedException ex)
            {
                Console.WriteLine("AES-GCM is NOT supported on this platform.");
                Console.WriteLine($"Exception: {ex.Message}");
            }
            catch (Exception ex)
            {
                Console.WriteLine("An unexpected error occurred.");
                Console.WriteLine($"Exception: {ex.Message}");
            }
        }
        else
        {
            Console.WriteLine("Invalid choice");
        }
    }

    static async Task RunProducer()
    {
        var config = new ProducerConfig { BootstrapServers = "kafka:9092" };

        using (var producer = new ProducerBuilder<Null, string>(config).Build())
        {
            try
            {
                var deliveryResult = await producer.ProduceAsync("test-topic", new Message<Null, string> { Value = "Hello Kafka" });
                Console.WriteLine($"Delivered '{deliveryResult.Value}' to '{deliveryResult.TopicPartitionOffset}'");
            }
            catch (ProduceException<Null, string> e)
            {
                Console.WriteLine($"Delivery failed: {e.Error.Reason}");
            }
        }
    }

    static void RunConsumer()
    {
        var topic = "test-topic";
        var config = new ConsumerConfig
        {
            GroupId = "test-consumer-group", //+ Guid.NewGuid(),
            BootstrapServers = "kafka:9092",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        var partitions = new List<TopicPartitionOffset> { new TopicPartitionOffset(new TopicPartition(topic, 0), 0) };

        using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
        {
            consumer.Assign(partitions);
            consumer.Subscribe(topic);

            while (true)
            {
                var consumeResult = consumer.Consume();
                Console.WriteLine($"Consumed message '{consumeResult.Message.Value}' at: '{consumeResult.TopicPartitionOffset}'.");
            }
        }
    }

    static async Task RunProducerWithRegistry()
    {
        var schemaRegistryConfig = new SchemaRegistryConfig { Url = "http://schema-registry:8081" };
        var producerConfig = new ProducerConfig { BootstrapServers = "kafka:9092" };
        var topic = "test-topic-with-schema";

        using var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);
        var serializer = new MySimpleJsonSerializer<MyMessage>(schemaRegistry);

        using var producer = new ProducerBuilder<string, MyMessage>(producerConfig)
            .SetKeySerializer(Serializers.Utf8)
            .SetValueSerializer(serializer)
            .Build();

        var message = new MyMessage { Id = 1, Name = "Daniel" };

        try
        {
            var deliveryResult = await producer.ProduceAsync(topic, new Message<string, MyMessage> { Key = "key1", Value = message });
            Console.WriteLine($"Delivered '{deliveryResult.Value}' to '{deliveryResult.TopicPartitionOffset}'");
        }
        catch (ProduceException<string, MyMessage> e)
        {
            Console.WriteLine($"Delivery failed: {e}");
        }
    }

    static void RunConsumerWithRegistry()
    {
        System.Console.WriteLine("Running consumer with schema registry");

        var schemaRegistryConfig = new SchemaRegistryConfig { Url = "http://schema-registry:8081" };
        var topic = "test-topic-with-schema";

        var consumerConfig = new ConsumerConfig
        {
            GroupId = "my-group",
            BootstrapServers = "kafka:9092", // Replace with your Kafka broker address
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        using var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);
        var deserializer = new MySimpleJsonDeserializer<MyMessageV2>(schemaRegistry).AsSyncOverAsync();

        var partitions = new List<TopicPartitionOffset> { new TopicPartitionOffset(new TopicPartition(topic, 0), 0) };
        using var consumer = new ConsumerBuilder<string, MyMessageV2>(consumerConfig)
            .SetKeyDeserializer(Deserializers.Utf8)
            .SetValueDeserializer(deserializer)
            .Build();

        consumer.Assign(partitions);
        consumer.Subscribe(topic);

        CancellationTokenSource cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            cts.Cancel();
        };

        try
        {
            while (true)
            {
                try
                {
                    var consumeResult = consumer.Consume(cts.Token);
                    Console.WriteLine($"Consumed message '{consumeResult.Message.Value.Id}::{consumeResult.Message.Value.Name}' at: '{consumeResult.TopicPartitionOffset}'.");
                }
                catch (ConsumeException e)
                {
                    Console.WriteLine($"Error occurred: {e}");
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Ensure the consumer leaves the group cleanly and final offsets are committed.
            consumer.Close();
        }
    }

    public class MyMessage
    {
        public int Id { get; set; }
        public required string Name { get; set; }
    }

    public class MyMessageV2
    {
        public int Id { get; set; }
        public required string Name { get; set; }
        public string? LastName { get; set; }
    }
}