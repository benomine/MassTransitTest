using MassTransit;

namespace MassTransitTest.ApiService.Messages;

public class MessageState: SagaStateMachineInstance
{
    public Guid CorrelationId { get; set; }
    public int CurrentState { get; set; }
    public string? Data { get; set; }
    public string? Error { get; set; }
}
