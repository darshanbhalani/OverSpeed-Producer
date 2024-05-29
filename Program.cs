using Confluent.Kafka;
using Kafka_Producer;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;

class Program
{
    private static List<string> vehicleNumber = [
    "GJ18AB1245", "GJ18WR5535", "GJ18KZ1147", "GJ18WW0193", "GJ18UL1925", "GJ18DQ1845", "GJ18RS9505", "GJ18GP0853", "GJ18AB2305", "GJ18KK1234",
    "GJ18PL7852", "GJ18MR6548", "GJ18YN4379", "GJ18QR7261", "GJ18KT3190", "GJ18FS5017", "GJ18LS2834", "GJ18GH9912", "GJ18VC8745", "GJ18TN2103",
    "GJ18BP6490", "GJ18DZ1402", "GJ18RM3627", "GJ18YT5348", "GJ18HJ9821", "GJ18AB6453", "GJ18NM2084", "GJ18FW9172", "GJ18ZU1839", "GJ18CL7062",
    "GJ18MP7384", "GJ18JH5942", "GJ18NC8237", "GJ18PQ4951", "GJ18VR7206", "GJ18BW8491", "GJ18FK2876", "GJ18DC3748", "GJ18MK8231", "GJ18TS9873"
    //"GJ18WL6378", "GJ18HR4502", "GJ18KS1098", "GJ18QZ5637", "GJ18XT2493", "GJ18LY1945", "GJ18OP3846", "GJ18EV2759", "GJ18MN5041", "GJ18JK6347"
];
    
    public static async Task Main(string[] args)
    {
        var configuration = new ConfigurationBuilder()
            .SetBasePath(AppContext.BaseDirectory)
            .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
            .Build();

        var config = new ProducerConfig
        {
            BootstrapServers = $"{configuration["BootstrapService:Server"]}:{configuration["BootstrapService:Port"]}"
        };

        using (var producer = new ProducerBuilder<Null, string>(config).Build())
        {
            while (true)
            {
                List<DataModel> dataModels = new List<DataModel>();

                foreach (var v in vehicleNumber)
                {
                    dataModels.Add(new DataModel
                    {
                        VehicleNumber = v,
                        VehicleSpeed = randomSpeed(),
                        TimeStamp = DateTime.Now,
                    });
                }

                try
                {
                    var deliveryResult = await producer.ProduceAsync(configuration["BootstrapService:Topic"], new Message<Null, string> { Value = JsonConvert.SerializeObject(dataModels) });
                    await Console.Out.WriteLineAsync(JsonConvert.SerializeObject(dataModels));
                    await Console.Out.WriteLineAsync();
                }
                catch (ProduceException<Null, string> e)
                {
                    Console.WriteLine($"Delivery failed: {e.Error.Reason}");
                }
                Thread.Sleep(1000);
            }
        }

       
    }
    private static int randomSpeed()
    {
        int max = 80;
        Random random = new Random();
        return random.Next(0, max + 1);
    }
}
