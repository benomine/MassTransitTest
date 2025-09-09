namespace MassTransitTest.ApiService.Messages;

public interface ProcessingFailedMessage
{
    public Guid CorrelationId { get; }
    public string Error { get; }
}