using MassTransit;
using MassTransitTest.ApiService.Messages;

namespace MassTransitTest.ApiService.Consumers;

public class MessageProcessingConsumer : IConsumer<MessageProcessing>
{
    private readonly ILogger<MessageProcessingConsumer> _logger;
    public MessageProcessingConsumer(ILogger<MessageProcessingConsumer> logger) => _logger = logger;

    public async Task Consume(ConsumeContext<MessageProcessing> context)
    {
        try
        {
            _logger.LogInformation("Traitement {CorrelationId}", context.Message.CorrelationId);

            if (string.IsNullOrWhiteSpace(context.Message.Data))
                throw new InvalidOperationException("Données invalides");

            await context.Publish(new ProcessingCompletedMessage { CorrelationId = context.Message.CorrelationId });
        }
        catch (Exception ex)
        {
            await context.Publish(new ProcessingFailedMessage
            {
                CorrelationId = context.Message.CorrelationId,
                Error = ex.Message
            });
        }
    }
}