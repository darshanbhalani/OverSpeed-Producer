using Confluent.Kafka;
using Kafka_Producer;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using Npgsql;

class Program
{
    private static List<DataModel> vehicles = new List<DataModel>();
    private static DateTime lastExecutionTime = DateTime.MinValue;
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
            await Console.Out.WriteLineAsync("--------------------------------------------------------------------");
            await Console.Out.WriteLineAsync("Collecting Vehicles Informations...");
            await Console.Out.WriteLineAsync("--------------------------------------------------------------------");
            getVehiclesList(configuration);
            await Console.Out.WriteLineAsync("Information Collected...");
            await Console.Out.WriteLineAsync("--------------------------------------------------------------------");
            while (true)
            {
                List<DataModel> dataModels = vehicles;
                foreach (var v in dataModels)
                {
                    v.VehicleSpeed = randomSpeed();
                    v.TimeStamp = DateTime.Now;
                }

                try
                {
                    await producer.ProduceAsync(configuration["BootstrapService:Topic"], new Message<Null, string> { Value = JsonConvert.SerializeObject(dataModels) });
                    await Console.Out.WriteLineAsync(JsonConvert.SerializeObject(dataModels));
                    await Console.Out.WriteLineAsync();
                }
                catch (ProduceException<Null, string> e)
                {
                    Console.WriteLine($"Delivery failed: {e.Error.Reason}");
                }
                if ((DateTime.Now - lastExecutionTime).TotalSeconds >= 600)
                {
                    await getVehiclesList(configuration);
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

    private static async Task getVehiclesList(IConfiguration configuration)
    {
        List<DataModel> tempData = new List<DataModel>();
        var connectionString = $"Host={configuration["DBConfiguration:Host"]};Port={configuration["DBConfiguration:Port"]};Username={configuration["DBConfiguration:Username"]};Password={configuration["DBConfiguration:Password"]};Database={configuration["DBConfiguration:Database"]}";
        using (var connection = new NpgsqlConnection(connectionString))
        {
            connection.Open();
                using (var cmd = new NpgsqlCommand("SELECT * FROM GETALLVEHICLES()", connection)) {
                    using(var reader = cmd.ExecuteReader())
                    {
                        if(reader.Read())
                        {
                            while(reader.Read())
                            {
                                tempData.Add(new DataModel
                                {
                                    VehicleNumber=reader.GetString(0),
                                    VehicleAuthority=reader.GetString(1),
                                    VehicleCity= reader.GetString(2),
                                    VehicleState=reader.GetString(3),
                                });
                                vehicles = tempData;
                            }
                    }
                }

            }
        }
    }
}
