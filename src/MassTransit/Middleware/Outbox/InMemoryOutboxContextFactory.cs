namespace MassTransit.Middleware.Outbox
{
    using System;
    using System.Threading.Tasks;


    public class InMemoryOutboxContextFactory :
        IOutboxContextFactory<InMemoryOutboxMessageRepository>
    {
        readonly InMemoryOutboxMessageRepository _messageRepository;
        readonly IServiceProvider _provider;

        public InMemoryOutboxContextFactory(InMemoryOutboxMessageRepository messageRepository, IServiceProvider provider)
        {
            _messageRepository = messageRepository;
            _provider = provider;
        }

        public async Task Send<T>(ConsumeContext<T> context, OutboxConsumeOptions options, IPipe<OutboxConsumeContext<T>> next)
            where T : class
        {
            var updateDeliveryCount = true;
            var continueProcessing = true;

            var messageId = context.GetOriginalMessageId() ?? throw new MessageException(typeof(T), "MessageId required to use the outbox");

            while (continueProcessing)
            {
                await _messageRepository.MarkInUse(context.CancellationToken).ConfigureAwait(false);

                InMemoryInboxMessage inboxMessage = null;
                try
                {
                    inboxMessage = await _messageRepository.Lock(messageId, options.ConsumerId, context.CancellationToken).ConfigureAwait(false);
                }
                finally
                {
                    _messageRepository.Release();
                }

                try
                {
                    if (updateDeliveryCount)
                        inboxMessage.ReceiveCount++;

                    updateDeliveryCount = false;

                    var outboxContext = new InMemoryOutboxConsumeContext<T>(context, options, _provider, inboxMessage);

                    await next.Send(outboxContext).ConfigureAwait(false);

                    continueProcessing = outboxContext.ContinueProcessing;
                }
                finally
                {
                    inboxMessage.Release();
                }
            }
        }

        public Task Send<TArguments>(ExecuteContext<TArguments> context, OutboxConsumeOptions options, IPipe<OutboxExecuteContext<TArguments>> next)
            where TArguments : class
        {
            throw new NotSupportedException("InMemory outbox for activities is configured through InMemoryOutboxConfigurationObserver");
        }

        public Task Send<TLog>(CompensateContext<TLog> context, OutboxConsumeOptions options, IPipe<OutboxCompensateContext<TLog>> next)
            where TLog : class
        {
            throw new NotSupportedException("InMemory outbox for activities is configured through InMemoryOutboxConfigurationObserver");
        }

        public void Probe(ProbeContext context)
        {
            context.CreateFilterScope("inMemoryOutboxContextFactory");
        }
    }
}
