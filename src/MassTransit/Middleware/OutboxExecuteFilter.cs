namespace MassTransit.Middleware
{
    using System.Threading.Tasks;
    using DependencyInjection;


    /// <summary>
    /// Sends messages through the outbox for execute activities
    /// </summary>
    /// <typeparam name="TContext">The outbox context type</typeparam>
    /// <typeparam name="TActivity">The activity type</typeparam>
    /// <typeparam name="TArguments">The arguments type</typeparam>
    public class OutboxExecuteFilter<TContext, TActivity, TArguments> :
        IFilter<ExecuteContext<TArguments>>
        where TContext : class
        where TActivity : class, IExecuteActivity<TArguments>
        where TArguments : class
    {
        readonly OutboxConsumeOptions _options;
        readonly IExecuteActivityScopeProvider<TActivity, TArguments> _scopeProvider;

        public OutboxExecuteFilter(IExecuteActivityScopeProvider<TActivity, TArguments> scopeProvider, OutboxConsumeOptions options)
        {
            _scopeProvider = scopeProvider;
            _options = options;
        }

        public void Probe(ProbeContext context)
        {
            context.CreateFilterScope("outbox");
        }

        public async Task Send(ExecuteContext<TArguments> context, IPipe<ExecuteContext<TArguments>> next)
        {
            await using IExecuteScopeContext<TArguments> scope = await _scopeProvider.GetScope(context).ConfigureAwait(false);

            var contextFactory = scope.GetService<IOutboxContextFactory<TContext>>();
            if (contextFactory == null)
                throw new ConsumerException($"Unable to resolve outbox context factory for type '{TypeCache<TContext>.ShortName}'.");

            var pipe = new OutboxExecutePipe<TArguments>(_options, scope, next);

            await contextFactory.Send(scope.Context, _options, pipe).ConfigureAwait(false);
        }
    }
}
