namespace MassTransitTest.ApiService.Messages;

public record ProcessingFailedMessage
{
    public Guid CorrelationId { get; init; }
    public string Error { get; init; } = string.Empty;
}