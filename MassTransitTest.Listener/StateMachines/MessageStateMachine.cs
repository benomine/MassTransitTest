using System.Security.Cryptography;
using MassTransit;
using MassTransitTest.ApiService.Messages;

namespace MassTransitTest.ApiService.StateMachines;

public sealed class MessageStateMachine : MassTransitStateMachine<MessageState>
{
    private readonly ILogger<MessageStateMachine> _logger;

    // ReSharper disable UnassignedGetOnlyAutoProperty
    public State Completed { get; }

    public Event<KafkaInbound> ReceivedEvent { get; }
    public Event FailedEvent { get; }
    public Event CompletedEvent { get; }
    
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
                .ThenAsync(ProcessMessageAsync)
        );
        
        During(Initial,
            When(CompletedEvent)
                .Then(context =>
                {
                    _logger.LogInformation("Processing completed for {CorrelationId}", context.Saga.CorrelationId);
                })
                .TransitionTo(Completed)
                .Finalize());
        
        During(Initial,
            When(FailedEvent)
                .Then(context =>
                {
                    _logger.LogError("Processing failed for {CorrelationId}", context.Saga.CorrelationId);
                })
            );
        
        SetCompletedWhenFinalized();
    }
    
    public static Guid ComputeCorrelationId(string id)
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

            context.Raise(CompletedEvent);
        }
        catch (Exception ex)
        {
            context.Saga.Error = ex.Message;
            context.Raise(FailedEvent);
        }
    }

    private async Task ProcessMessageAsync(BehaviorContext<MessageState, KafkaInbound> context)
    {
        try
        {
            _logger.LogInformation("Traitement {CorrelationId}", context.Saga.CorrelationId);
            await Task.Delay(200);
            
            if (string.IsNullOrWhiteSpace(context.Message.Data))
                throw new InvalidOperationException("Données invalides");

            await context.Raise(CompletedEvent);
        }
        catch (Exception ex)
        {
            context.Saga.Error = ex.Message;
            await context.Raise(FailedEvent);
        }
    }
}