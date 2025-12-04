namespace MassTransit.Middleware.InMemoryOutbox
{
    using Context;
    using Courier.Contracts;


    /// <summary>
    /// Wraps an ExecuteContext to provide outbox Send/Publish interception
    /// </summary>
    public class OutboxExecuteContextProxy<TArguments> :
        ExecuteContextProxy<TArguments>
        where TArguments : class
    {
        public OutboxExecuteContextProxy(ExecuteContext<TArguments> context, OutboxConsumeContext<RoutingSlip> outboxContext)
            : base(context)
        {
            // Override ReceiveContext with the outbox's ReceiveContext
            // This ensures all Send/Publish calls go through the outbox
            ReceiveContext = outboxContext.ReceiveContext;
        }
    }
}
