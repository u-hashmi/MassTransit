namespace MassTransit.Middleware.InMemoryOutbox
{
    using System;
    using System.Threading.Tasks;
    using Courier.Contracts;
    using DependencyInjection;
    using Microsoft.Extensions.DependencyInjection;


    /// <summary>
    /// Adapts an ICompensateScopeContext to IConsumeScopeContext for use with OutboxMessagePipe.
    /// Activities don't support PushConsumeContext, so it's a no-op.
    /// </summary>
    public class OutboxCompensateScopeAdapter<TLog> :
        IConsumeScopeContext<RoutingSlip>
        where TLog : class
    {
        readonly ICompensateScopeContext<TLog> _compensateScope;
        readonly ConsumeContext<RoutingSlip> _context;
        readonly IServiceProvider _serviceProvider;

        public OutboxCompensateScopeAdapter(ICompensateScopeContext<TLog> compensateScope, ConsumeContext<RoutingSlip> context,
            IServiceProvider serviceProvider)
        {
            _compensateScope = compensateScope;
            _context = context;
            _serviceProvider = serviceProvider;
        }

        public ConsumeContext<RoutingSlip> Context => _context;

        public T GetService<T>()
            where T : class
        {
            return _compensateScope.GetService<T>();
        }

        public T CreateInstance<T>(params object[] arguments)
            where T : class
        {
            return ActivatorUtilities.CreateInstance<T>(_serviceProvider, arguments);
        }

        public IDisposable PushConsumeContext(ConsumeContext context)
        {
            // Activity scopes don't support PushConsumeContext - return no-op
            return NoopDisposable.Instance;
        }

        public ValueTask DisposeAsync()
        {
            return _compensateScope.DisposeAsync();
        }


        sealed class NoopDisposable : IDisposable
        {
            public static readonly NoopDisposable Instance = new NoopDisposable();

            public void Dispose()
            {
            }
        }
    }
}
