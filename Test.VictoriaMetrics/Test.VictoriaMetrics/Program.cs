using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;

namespace Test.VictoriaMetrics;

internal class Program
{
    private const string Measurement = "Performance";

    private static async Task Main(string[] args)
    {
        var influxItems = new ConcurrentQueue<InfluxItem>(
            Enumerable.Range(1, 10_000).Select(x => new InfluxItem(x)));

        var influxGroups = new List<InfluxGroup>()
        {
            new(InfluxSpeed.Fast, Measurement),
            new(InfluxSpeed.Medium, Measurement),
            new(InfluxSpeed.Slow, Measurement),
            new(InfluxSpeed.VerySlow, Measurement),
        };

        foreach (var influxGroup in influxGroups)
        {
            await influxGroup.PopulateItemsAsync(influxItems);
            _ = influxGroup.ReadAsync();
        }

        while (true)
        {
            // Write count item in each group
            //Console.WriteLine($"Fast: {influxGroups[0].Items.Count}");
            //Console.WriteLine($"Medium: {influxGroups[1].Items.Count}");
            //Console.WriteLine($"Slow: {influxGroups[2].Items.Count}");
            //Console.WriteLine($"VerySlow: {influxGroups[3].Items.Count}");
            await Task.Delay(1000);
        }
    }
}

public class InfluxGroup
{
    private readonly string _measurement;
    private readonly HttpClient _client = new HttpClient();
    private readonly Random _random = new Random();

    public InfluxGroup(InfluxSpeed influxSpeed, string measurement)
    {
        _measurement = measurement;
        InfluxSpeed = influxSpeed;
    }

    public InfluxSpeed InfluxSpeed { get; }
    public List<InfluxItem> Items { get; } = new List<InfluxItem>();

    public async Task PopulateItemsAsync(ConcurrentQueue<InfluxItem> influxItems)
    {
        int count = GetItemCount();
        for (int i = 0; i < count; i++)
        {
            if (influxItems.TryDequeue(out var item))
            {
                Items.Add(item);
            }
        }
    }

    public async Task ReadAsync()
    {
        var stopwatch = new Stopwatch();
        var itemsCount = Items.Count;
        const int chunkSize = 1000;
        var chunks = (int)Math.Ceiling((double)itemsCount / chunkSize);

        while (true)
        {
            stopwatch.Restart();
            var tasks = new List<Task>();
            for (int chunkIndex = 0; chunkIndex < chunks; chunkIndex++)
            {
                int startIndex = chunkIndex * chunkSize;
                int endIndex = Math.Min(startIndex + chunkSize, itemsCount);
                var chunk = Items.GetRange(startIndex, endIndex - startIndex);

                tasks.Add(ReadChunkAsync(chunk));
            }

            await Task.WhenAll(tasks);

            stopwatch.Stop();
            if (InfluxSpeed == InfluxSpeed.Fast)
                Console.WriteLine($"{InfluxSpeed}: {stopwatch.Elapsed.TotalMilliseconds:0.00}");
            await DelayBasedOnSpeed();
        }
    }

    private async Task ReadChunkAsync(List<InfluxItem> chunk)
    {
        var lineBody = new StringBuilder();
        foreach (var item in chunk)
        {
            lineBody.Append($"{item.Read()},");
        }
        lineBody.Length--; // Remove trailing comma

        var unixTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var line = $"{_measurement} {lineBody} {unixTimestamp}";
        await InsertAsync(line);
    }

    private async Task InsertAsync(string payload)
    {
        string victoriaMetricsUrl = "http://localhost:8428"; // VictoriaMetrics server URL
        try
        {
            // Prepare the API endpoint URL for remote write
            string apiUrl = $"{victoriaMetricsUrl}/influx/api/v2/write";

            // Prepare the request content with the payload
            StringContent content = new StringContent(payload, Encoding.UTF8, "application/x-www-form-urlencoded");

            // Send POST request to VictoriaMetrics remote write API endpoint
            HttpResponseMessage response = await _client.PostAsync(apiUrl, content);

            // Handle the response
            if (!response.IsSuccessStatusCode)
            {
                Console.WriteLine($"Error: {response.StatusCode} - {response.ReasonPhrase}");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error: {ex.Message}");
        }
    }

    private async Task DelayBasedOnSpeed()
    {
        int delayMilliseconds = GetDelayMilliseconds();
        await Task.Delay(delayMilliseconds);
    }

    private int GetItemCount()
    {
        return InfluxSpeed switch
        {
            InfluxSpeed.Fast => 2000,
            InfluxSpeed.Medium => 2000,
            InfluxSpeed.Slow => 2000,
            InfluxSpeed.VerySlow => 50000,
            _ => 0,
        };
    }

    private int GetDelayMilliseconds()
    {
        return InfluxSpeed switch
        {
            InfluxSpeed.Fast => 1,
            InfluxSpeed.Medium => 50,
            InfluxSpeed.Slow => 500,
            InfluxSpeed.VerySlow => 1000,
            _ => 0,
        };
    }
}

public class InfluxItem
{
    private readonly Random _random = new Random();
    public string Name { get; }
    public int Value { get; private set; }

    public InfluxItem(int id)
    {
        Name = $"Flux{id:00000}";
    }

    public string Read()
    {
        Value = _random.Next(-1000, 1000);
        Task.Delay(TimeSpan.FromMicroseconds(1)).Wait();
        return $"{Name}={Value}";
    }
}

public enum InfluxSpeed
{
    Fast,
    Medium,
    Slow,
    VerySlow,
}