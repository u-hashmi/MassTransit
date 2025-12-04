namespace MassTransit.Middleware.InMemoryOutbox
{
    using System.Threading.Tasks;
    using Courier.Contracts;


    /// <summary>
    /// Adapts the next pipe from CompensateContext to ConsumeContext for use with OutboxMessagePipe.
    /// When OutboxMessagePipe calls Send with OutboxConsumeContext, this adapter creates
    /// an OutboxCompensateContextProxy and forwards to the actual next pipe.
    /// </summary>
    public class OutboxCompensatePipeAdapter<TLog> :
        IPipe<ConsumeContext<RoutingSlip>>
        where TLog : class
    {
        readonly CompensateContext<TLog> _originalContext;
        readonly IPipe<CompensateContext<TLog>> _next;

        public OutboxCompensatePipeAdapter(CompensateContext<TLog> originalContext, IPipe<CompensateContext<TLog>> next)
        {
            _originalContext = originalContext;
            _next = next;
        }

        public async Task Send(ConsumeContext<RoutingSlip> context)
        {
            // The context is actually an OutboxConsumeContext<RoutingSlip>
            var outboxContext = context as OutboxConsumeContext<RoutingSlip>;
            if (outboxContext == null)
                throw new ConfigurationException("Expected OutboxConsumeContext<RoutingSlip> but received " + context.GetType().Name);

            // Create a CompensateContext wrapper that uses the outbox's ReceiveContext for Send/Publish interception
            var outboxCompensateContext = new OutboxCompensateContextProxy<TLog>(_originalContext, outboxContext);

            await _next.Send(outboxCompensateContext).ConfigureAwait(false);
        }

        public void Probe(ProbeContext context)
        {
            _next.Probe(context);
        }
    }
}
