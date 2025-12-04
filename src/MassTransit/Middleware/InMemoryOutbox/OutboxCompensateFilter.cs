namespace MassTransit.Middleware.InMemoryOutbox
{
    using System;
    using System.Threading.Tasks;
    using Courier.Contracts;
    using DependencyInjection;


    /// <summary>
    /// Outbox filter for compensate activities. Operates at Log level so that fault events
    /// published by CompensateActivityHost are OUTSIDE the outbox transaction.
    /// </summary>
    public class OutboxCompensateFilter<TContext, TActivity, TLog> :
        IFilter<CompensateContext<TLog>>
        where TContext : class
        where TActivity : class, ICompensateActivity<TLog>
        where TLog : class
    {
        readonly OutboxConsumeOptions _options;
        readonly ICompensateActivityScopeProvider<TActivity, TLog> _scopeProvider;
        readonly IServiceProvider _serviceProvider;

        public OutboxCompensateFilter(ICompensateActivityScopeProvider<TActivity, TLog> scopeProvider,
            IServiceProvider serviceProvider, OutboxConsumeOptions options)
        {
            _scopeProvider = scopeProvider;
            _serviceProvider = serviceProvider;
            _options = options;
        }

        public void Probe(ProbeContext context)
        {
            context.CreateFilterScope("outbox-compensate");
        }

        public async Task Send(CompensateContext<TLog> context, IPipe<CompensateContext<TLog>> next)
        {
            // Get the compensate scope for DI
            await using ICompensateScopeContext<TLog> scope = await _scopeProvider.GetScope(context).ConfigureAwait(false);

            // Get the outbox factory from the scope
            var contextFactory = scope.GetService<IOutboxContextFactory<TContext>>();
            if (contextFactory == null)
                throw new ConsumerException($"Unable to resolve outbox context factory for type '{TypeCache<TContext>.ShortName}'.");

            // CompensateContext IS a ConsumeContext<RoutingSlip>
            var routingSlipContext = context as ConsumeContext<RoutingSlip>;
            if (routingSlipContext == null)
                throw new ConfigurationException("CompensateContext must be a ConsumeContext<RoutingSlip>");

            // Create adapters to bridge CompensateContext to the OutboxMessagePipe infrastructure
            var scopeAdapter = new OutboxCompensateScopeAdapter<TLog>(scope, routingSlipContext, _serviceProvider);
            var pipeAdapter = new OutboxCompensatePipeAdapter<TLog>(context, next);

            // Reuse OutboxMessagePipe for all the delivery logic
            var pipe = new OutboxMessagePipe<RoutingSlip>(_options, scopeAdapter, pipeAdapter);

            // Call the factory which creates OutboxConsumeContext and calls pipe.Send()
            await contextFactory.Send(routingSlipContext, _options, pipe).ConfigureAwait(false);
        }
    }
}
