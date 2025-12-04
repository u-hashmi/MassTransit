namespace MassTransit.Middleware
{
    using System.Threading.Tasks;


    /// <summary>
    /// Factory interface for creating outbox contexts. Activities use the same Send method as consumers
    /// via the adapter pipe pattern - no activity-specific methods needed.
    /// </summary>
    /// <typeparam name="TContext">The outbox context type (e.g., DbContext for EF)</typeparam>
    public interface IOutboxContextFactory<TContext> :
        IProbeSite
        where TContext : class
    {
        /// <summary>
        /// Sends a message through the outbox. This single method handles consumers, sagas, and activities.
        /// Activities use adapter pipes to bridge OutboxConsumeContext&lt;RoutingSlip&gt; to the appropriate
        /// OutboxExecuteContext or OutboxCompensateContext.
        /// </summary>
        Task Send<T>(ConsumeContext<T> context, OutboxConsumeOptions options, IPipe<OutboxConsumeContext<T>> next)
            where T : class;
    }
}
