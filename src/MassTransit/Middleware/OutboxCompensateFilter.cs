namespace MassTransit.Middleware
{
    using System.Threading.Tasks;
    using DependencyInjection;


    /// <summary>
    /// Sends messages through the outbox for compensate activities
    /// </summary>
    /// <typeparam name="TContext">The outbox context type</typeparam>
    /// <typeparam name="TActivity">The activity type</typeparam>
    /// <typeparam name="TLog">The log type</typeparam>
    public class OutboxCompensateFilter<TContext, TActivity, TLog> :
        IFilter<CompensateContext<TLog>>
        where TContext : class
        where TActivity : class, ICompensateActivity<TLog>
        where TLog : class
    {
        readonly OutboxConsumeOptions _options;
        readonly ICompensateActivityScopeProvider<TActivity, TLog> _scopeProvider;

        public OutboxCompensateFilter(ICompensateActivityScopeProvider<TActivity, TLog> scopeProvider, OutboxConsumeOptions options)
        {
            _scopeProvider = scopeProvider;
            _options = options;
        }

        public void Probe(ProbeContext context)
        {
            context.CreateFilterScope("outbox");
        }

        public async Task Send(CompensateContext<TLog> context, IPipe<CompensateContext<TLog>> next)
        {
            await using ICompensateScopeContext<TLog> scope = await _scopeProvider.GetScope(context).ConfigureAwait(false);

            var contextFactory = scope.GetService<IOutboxContextFactory<TContext>>();
            if (contextFactory == null)
                throw new ConsumerException($"Unable to resolve outbox context factory for type '{TypeCache<TContext>.ShortName}'.");

            var pipe = new OutboxCompensatePipe<TLog>(_options, scope, next);

            await contextFactory.Send(scope.Context, _options, pipe).ConfigureAwait(false);
        }
    }
}
