namespace MassTransitTest.ApiService.Messages;

public interface ProcessingCompletedMessage
{
    public Guid CorrelationId { get; }
}