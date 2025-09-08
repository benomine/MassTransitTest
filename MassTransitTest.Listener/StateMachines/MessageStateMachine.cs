using System.Security.Cryptography;
using MassTransit;
using MassTransitTest.ApiService.Messages;

namespace MassTransitTest.ApiService.StateMachines;

public sealed class MessageStateMachine : MassTransitStateMachine<MessageState>
{
    private readonly ILogger<MessageStateMachine> _logger;

    // ReSharper disable UnassignedGetOnlyAutoProperty
    public State Completed { get; }
    public State Failed { get; }

    public Event<KafkaInbound> ReceivedEvent { get; }

    public MessageStateMachine(ILogger<MessageStateMachine> logger)
    {
        _logger = logger;

        InstanceState(x => x.CurrentState);

        Event(() => ReceivedEvent, x =>
        {
            x.CorrelateById(m => ComputeCorrelationId(m.Message.SomeId));
            x.InsertOnInitial = true;
        });

        Initially(
            When(ReceivedEvent)
                .Then(ctx =>
                {
                    ctx.Saga.CorrelationId = ComputeCorrelationId(ctx.Message.SomeId);
                    _logger.LogInformation("Received Message {CorrelationId}", ctx.Saga.CorrelationId);
                    ctx.Saga.Data = ctx.Message.Data;
                })
                .Then(ProcessMessage)
                .IfElse(ctx => ctx.Saga.Error is null,
                    binder => binder.TransitionTo(Completed).Finalize(),
                    binder => binder.TransitionTo(Failed))
        );

        SetCompletedWhenFinalized();
    }

    private Guid ComputeCorrelationId(string id)
    {
        var hash = MD5.HashData(System.Text.Encoding.UTF8.GetBytes(id));
        return new Guid(hash);
    }

    private void ProcessMessage(BehaviorContext<MessageState, KafkaInbound> context)
    {
        try
        {
            _logger.LogInformation("Traitement {CorrelationId}", context.Saga.CorrelationId);

            if (string.IsNullOrWhiteSpace(context.Message.Data))
                throw new InvalidOperationException("Données invalides");

        }
        catch (Exception ex)
        {
            context.Saga.Error = ex.Message;
        }
    }

    private async Task ProcessMessageAsync(BehaviorContext<MessageState, KafkaInbound> context)
    {
        try
        {
            _logger.LogInformation("Traitement {CorrelationId}", context.Saga.CorrelationId);

            if (string.IsNullOrWhiteSpace(context.Message.Data))
                throw new InvalidOperationException("Données invalides");

        }
        catch (Exception ex)
        {
            context.Saga.Error = ex.Message;
        }
    }
}