using MassTransit;
using MassTransitTest.ApiService.Messages;

namespace MassTransitTest.ApiService.Consumers;

public class InitialConsumer : IConsumer<KafkaInbound>
{
    public async Task Consume(ConsumeContext<KafkaInbound> context)
    {
        await context.Publish(new MessageReceived
        {
            CorrelationId = context.Message.CorrelationId,
            Data = context.Message.Data
        });
    }
}