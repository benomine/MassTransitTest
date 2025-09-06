namespace MassTransitTest.ApiService.Messages;

public record ProcessingCompletedMessage
{
    public Guid CorrelationId { get; init; }
}