#nullable enable
namespace MassTransit.EntityFrameworkCoreIntegration
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Context;
    using Microsoft.EntityFrameworkCore;
    using Microsoft.EntityFrameworkCore.Storage;
    using Middleware;
    using Middleware.Outbox;


    public class DbContextOutboxCompensateContext<TDbContext, TLog> :
        CompensateContextProxy<TLog>,
        OutboxCompensateContext<TLog>,
        DbTransactionContext
        where TDbContext : DbContext
        where TLog : class
    {
        readonly TDbContext _dbContext;
        readonly InboxState _inboxState;
        readonly OutboxConsumeOptions _options;
        readonly DbSet<OutboxMessage> _outboxMessageSet;
        readonly IServiceProvider _provider;
        readonly IDbContextTransaction _transaction;

        public DbContextOutboxCompensateContext(CompensateContext<TLog> context, OutboxConsumeOptions options, IServiceProvider provider,
            TDbContext dbContext,
            IDbContextTransaction transaction, InboxState inboxState)
            : base(context)
        {
            _dbContext = dbContext;
            _transaction = transaction;
            _inboxState = inboxState;
            _options = options;
            _provider = provider;

            _outboxMessageSet = dbContext.Set<OutboxMessage>();

            CapturedContext = context;

            var outboxReceiveContext = new OutboxReceiveContext(this, context.ReceiveContext);

            ReceiveContext = outboxReceiveContext;
            PublishEndpointProvider = outboxReceiveContext.PublishEndpointProvider;

            if (context.TryGetPayload(out MessageSchedulerContext schedulerContext))
            {
                context.AddOrUpdatePayload<MessageSchedulerContext>(
                    () => new ConsumeMessageSchedulerContext(this, schedulerContext.SchedulerFactory),
                    existing => new ConsumeMessageSchedulerContext(this, existing.SchedulerFactory));
            }
        }

        protected Guid ConsumerId => _options.ConsumerId;

        public ConsumeContext CapturedContext { get; }

        public override Guid? MessageId => _inboxState.MessageId;

        public bool ContinueProcessing { get; set; } = true;

        public bool IsMessageConsumed => _inboxState.Consumed.HasValue;
        public bool IsOutboxDelivered => _inboxState.Delivered.HasValue;
        public int ReceiveCount => _inboxState.ReceiveCount;
        public long? LastSequenceNumber => _inboxState.LastSequenceNumber;

        public Guid TransactionId => _transaction.TransactionId;

        public async Task SetConsumed()
        {
            _inboxState.Consumed = DateTime.UtcNow;
            _dbContext.Update(_inboxState);

            await _dbContext.SaveChangesAsync(CancellationToken).ConfigureAwait(false);

            LogContext.Debug?.Log("Outbox Consumed: {MessageId} {Consumed}", MessageId, _inboxState.Consumed);
        }

        public async Task SetDelivered()
        {
            _inboxState.Delivered = DateTime.UtcNow;
            _dbContext.Update(_inboxState);

            await _dbContext.SaveChangesAsync(CancellationToken).ConfigureAwait(false);

            LogContext.Debug?.Log("Outbox Delivered: {MessageId} {Delivered}", MessageId, _inboxState.Delivered);
        }

        public async Task<List<OutboxMessageContext>> LoadOutboxMessages()
        {
            var lastSequenceNumber = LastSequenceNumber ?? 0;

            List<OutboxMessage> messages = await _dbContext.Set<OutboxMessage>()
                .Where(x => x.InboxMessageId == MessageId && x.InboxConsumerId == ConsumerId && x.SequenceNumber > lastSequenceNumber)
                .OrderBy(x => x.SequenceNumber)
                .Take(_options.MessageDeliveryLimit + 1)
                .AsNoTracking()
                .ToListAsync(CancellationToken).ConfigureAwait(false);

            for (var i = 0; i < messages.Count; i++)
                messages[i].Deserialize(SerializerContext);

            return messages.Cast<OutboxMessageContext>().ToList();
        }

        public Task NotifyOutboxMessageDelivered(OutboxMessageContext message)
        {
            _inboxState.LastSequenceNumber = message.SequenceNumber;
            _dbContext.Update(_inboxState);

            return Task.CompletedTask;
        }

        public async Task RemoveOutboxMessages()
        {
            var count = await _dbContext.Set<OutboxMessage>()
                .Where(x => x.InboxMessageId == MessageId && x.InboxConsumerId == ConsumerId)
                .ExecuteDeleteAsync(CancellationToken).ConfigureAwait(false);

            if (count > 0)
                LogContext.Debug?.Log("Outbox removed {Count} messages: {MessageId}", count, MessageId);
        }

        public Task AddSend<T>(SendContext<T> context)
            where T : class
        {
            return _outboxMessageSet.AddSend(context, SerializerContext, MessageId, ConsumerId);
        }

        public object GetService(Type serviceType)
        {
            return _provider.GetService(serviceType);
        }
    }
}
