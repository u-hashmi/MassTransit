namespace MassTransit.Middleware.InMemoryOutbox
{
    using System.Threading.Tasks;
    using Courier.Contracts;


    /// <summary>
    /// Adapts the next pipe from ExecuteContext to ConsumeContext for use with OutboxMessagePipe.
    /// When OutboxMessagePipe calls Send with OutboxConsumeContext, this adapter creates
    /// an OutboxExecuteContextProxy and forwards to the actual next pipe.
    /// </summary>
    public class OutboxExecutePipeAdapter<TArguments> :
        IPipe<ConsumeContext<RoutingSlip>>
        where TArguments : class
    {
        readonly ExecuteContext<TArguments> _originalContext;
        readonly IPipe<ExecuteContext<TArguments>> _next;

        public OutboxExecutePipeAdapter(ExecuteContext<TArguments> originalContext, IPipe<ExecuteContext<TArguments>> next)
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

            // Create an ExecuteContext wrapper that uses the outbox's ReceiveContext for Send/Publish interception
            var outboxExecuteContext = new OutboxExecuteContextProxy<TArguments>(_originalContext, outboxContext);

            await _next.Send(outboxExecuteContext).ConfigureAwait(false);
        }

        public void Probe(ProbeContext context)
        {
            _next.Probe(context);
        }
    }
}
