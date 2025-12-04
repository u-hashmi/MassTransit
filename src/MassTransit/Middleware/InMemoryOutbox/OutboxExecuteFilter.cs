namespace MassTransit.Middleware.InMemoryOutbox
{
    using System;
    using System.Threading.Tasks;
    using Courier.Contracts;
    using DependencyInjection;


    /// <summary>
    /// Outbox filter for execute activities. Operates at Arguments level so that fault events
    /// published by ExecuteActivityHost are OUTSIDE the outbox transaction.
    /// </summary>
    public class OutboxExecuteFilter<TContext, TActivity, TArguments> :
        IFilter<ExecuteContext<TArguments>>
        where TContext : class
        where TActivity : class, IExecuteActivity<TArguments>
        where TArguments : class
    {
        readonly OutboxConsumeOptions _options;
        readonly IExecuteActivityScopeProvider<TActivity, TArguments> _scopeProvider;
        readonly IServiceProvider _serviceProvider;

        public OutboxExecuteFilter(IExecuteActivityScopeProvider<TActivity, TArguments> scopeProvider,
            IServiceProvider serviceProvider, OutboxConsumeOptions options)
        {
            _scopeProvider = scopeProvider;
            _serviceProvider = serviceProvider;
            _options = options;
        }

        public void Probe(ProbeContext context)
        {
            context.CreateFilterScope("outbox-execute");
        }

        public async Task Send(ExecuteContext<TArguments> context, IPipe<ExecuteContext<TArguments>> next)
        {
            // Get the execute scope for DI
            await using IExecuteScopeContext<TArguments> scope = await _scopeProvider.GetScope(context).ConfigureAwait(false);

            // Get the outbox factory from the scope
            var contextFactory = scope.GetService<IOutboxContextFactory<TContext>>();
            if (contextFactory == null)
                throw new ConsumerException($"Unable to resolve outbox context factory for type '{TypeCache<TContext>.ShortName}'.");

            // ExecuteContext IS a ConsumeContext<RoutingSlip>
            var routingSlipContext = context as ConsumeContext<RoutingSlip>;
            if (routingSlipContext == null)
                throw new ConfigurationException("ExecuteContext must be a ConsumeContext<RoutingSlip>");

            // Create adapters to bridge ExecuteContext to the OutboxMessagePipe infrastructure
            var scopeAdapter = new OutboxExecuteScopeAdapter<TArguments>(scope, routingSlipContext, _serviceProvider);
            var pipeAdapter = new OutboxExecutePipeAdapter<TArguments>(context, next);

            // Reuse OutboxMessagePipe for all the delivery logic
            var pipe = new OutboxMessagePipe<RoutingSlip>(_options, scopeAdapter, pipeAdapter);

            // Call the factory which creates OutboxConsumeContext and calls pipe.Send()
            await contextFactory.Send(routingSlipContext, _options, pipe).ConfigureAwait(false);
        }
    }
}
