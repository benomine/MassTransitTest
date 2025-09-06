namespace MassTransitTest.ApiService.Messages;

public record MessageProcessing
{
    public Guid CorrelationId { get; init; }
    public string Data { get; init; } = string.Empty;
}