using MassTransit;
using MassTransit.EntityFrameworkCoreIntegration;
using MassTransitTest.ApiService.Messages;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace MassTransitTest.ApiService.Data;

public class MessageDbContext : SagaDbContext
{
    public DbSet<MessageState> MessageStates { get; set; }
    
    public MessageDbContext(DbContextOptions options) : base(options)
    {
        
    }

    protected override IEnumerable<ISagaClassMap> Configurations
    {
        get
        {
            yield return new MessageStateMap();
        }
    }
}

public class MessageStateMap : SagaClassMap<MessageState>
{
    protected override void Configure(EntityTypeBuilder<MessageState> entity, ModelBuilder model)
    {
        entity.ToTable("message_states");
        entity.HasKey(x => x.CorrelationId);
        entity.Property(x => x.CorrelationId).HasColumnName("correlationid");
        entity.Property(x => x.CurrentState).HasColumnName("currentstate").IsRequired();
        entity.Property(x => x.Data).HasColumnName("data");
        entity.Property(x => x.Error).HasColumnName("error");
        entity.Property(x => x.Version).HasColumnName("version");
    }
}