namespace MassTransitTest.ApiService.Messages;

public record MessageReceived
{
    public Guid CorrelationId { get; init; }
    public string Data { get; init; } = string.Empty;
}