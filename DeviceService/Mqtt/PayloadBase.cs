using Newtonsoft.Json;

namespace DeviceService.Mqtt;

public class PayloadBase<T>
{
    [JsonProperty("Seq")]
    public long? Seq { get; set; }

    [JsonProperty("Data")]
    public T? Data { get; set; }
}
