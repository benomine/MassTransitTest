namespace MassTransitTest.ApiService.Messages;

public record KafkaInbound
{
    public Guid CorrelationId { get; init; }
    public string Data { get; init; } = string.Empty;
}