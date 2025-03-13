using Confluent.Kafka;

const string topic = "TestTopic";

var config = new ProducerConfig
{
    // User-specific properties that you must set
    BootstrapServers = "<BOOTSTRAP_SERVERS>",
    SaslUsername = "<API_KEY>",
    SaslPassword = "<API_SECRET>",

    // Fixed properties
    SecurityProtocol = SecurityProtocol.SaslSsl,
    SaslMechanism = SaslMechanism.Plain,
    Acks = Acks.All
};

using (var producer = new ProducerBuilder<string, string>(config).Build())
{
    var numProduced = 0;
    Console.WriteLine("\n\n");
    Console.WriteLine("Note: Type N for exit the application\n\n");

    while (true)
    {
        Console.WriteLine("Enter Message to send");
        string message = Console.ReadLine();
        if (message == "N" || message == "n")
        {
            Console.WriteLine("You entered N to exit application");
            producer.Flush(TimeSpan.FromSeconds(10));
            Console.WriteLine($"{numProduced} messages were produced to topic {topic}");
            break;
        }

        producer.Produce(topic, new Message<string, string> { Key = "Test", Value = message },
           (deliveryReport) =>
           {
               if (deliveryReport.Error.Code != ErrorCode.NoError)
               {
                   Console.WriteLine($"Failed to deliver message: {deliveryReport.Error.Reason}");
               }
               else
               {
                   Console.WriteLine($"Produced event to topic {topic}: key = Test value = {message}");
                   numProduced += 1;
               }
           });
    }
}