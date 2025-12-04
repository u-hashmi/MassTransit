namespace MassTransit.Middleware.InMemoryOutbox
{
    using Context;
    using Courier.Contracts;


    /// <summary>
    /// Wraps a CompensateContext to provide outbox Send/Publish interception
    /// </summary>
    public class OutboxCompensateContextProxy<TLog> :
        CompensateContextProxy<TLog>
        where TLog : class
    {
        public OutboxCompensateContextProxy(CompensateContext<TLog> context, OutboxConsumeContext<RoutingSlip> outboxContext)
            : base(context)
        {
            // Override ReceiveContext with the outbox's ReceiveContext
            // This ensures all Send/Publish calls go through the outbox
            ReceiveContext = outboxContext.ReceiveContext;
        }
    }
}
