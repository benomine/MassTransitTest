using MassTransit;
using MassTransitTest.ApiService.Messages;

namespace MassTransitTest.ApiService.StateMachines;

public sealed class MessageStateMachine : MassTransitStateMachine<MessageState>
{
    private readonly ILogger<MessageStateMachine> _logger;

    public State Received { get; private set; }
    public State Processing { get; private set; }
    public State Completed { get; private set; }
    public State Failed { get; private set; }

    public Event<MessageReceived> ReceivedEvent { get; private set; }
    public Event<ProcessingCompletedMessage> ProcessingCompleted { get; private set; }
    public Event<ProcessingFailedMessage> ProcessingFailed { get; private set; }

    public MessageStateMachine(ILogger<MessageStateMachine> logger)
    {
        _logger = logger;

        InstanceState(x => x.CurrentState, Received, Processing, Completed, Failed);

        Event(() => ReceivedEvent, x =>
        {
            x.CorrelateById(m => m.Message.CorrelationId);
            x.InsertOnInitial = true;
        });
        Event(() => ProcessingCompleted, x => x.CorrelateById(m => m.Message.CorrelationId));
        Event(() => ProcessingFailed, x => x.CorrelateById(m => m.Message.CorrelationId));

        Initially(
            When(ReceivedEvent)
                .Then(ctx =>
                {
                    _logger.LogInformation("Received Message {CorrelationId}", ctx.Message.CorrelationId);
                    ctx.Saga.CorrelationId = ctx.Message.CorrelationId;
                    ctx.Saga.Data = ctx.Message.Data;
                })
                .Publish(ctx => ctx.Init<MessageProcessing>(new { ctx.Saga.CorrelationId, ctx.Saga.Data }))
                .TransitionTo(Processing)
        );

        During(Processing,
            When(ProcessingCompleted)
                .Then(context =>
                {
                    _logger.LogInformation("Processing Completed {CorrelationId}", context.Message.CorrelationId);
                })
                .TransitionTo(Completed)
                .Finalize(),
            When(ProcessingFailed)
                .Then(ctx =>
                {
                    _logger.LogWarning("Processing Failed {CorrelationId}", ctx.Message.CorrelationId);
                    ctx.Saga.Error = ctx.Message.Error;
                })
                .TransitionTo(Failed)
        );

        SetCompletedWhenFinalized();
    }
}