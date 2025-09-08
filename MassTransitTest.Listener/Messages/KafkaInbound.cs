namespace MassTransitTest.ApiService.Messages;

public record KafkaInbound
{
    public string SomeId { get; init; }
    public string Data { get; init; } = string.Empty;
}