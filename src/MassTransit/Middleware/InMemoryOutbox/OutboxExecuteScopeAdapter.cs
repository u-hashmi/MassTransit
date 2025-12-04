namespace MassTransit.Middleware.InMemoryOutbox
{
    using System;
    using System.Threading.Tasks;
    using Courier.Contracts;
    using DependencyInjection;
    using Microsoft.Extensions.DependencyInjection;


    /// <summary>
    /// Adapts an IExecuteScopeContext to IConsumeScopeContext for use with OutboxMessagePipe.
    /// Activities don't support PushConsumeContext, so it's a no-op.
    /// </summary>
    public class OutboxExecuteScopeAdapter<TArguments> :
        IConsumeScopeContext<RoutingSlip>
        where TArguments : class
    {
        readonly IExecuteScopeContext<TArguments> _executeScope;
        readonly ConsumeContext<RoutingSlip> _context;
        readonly IServiceProvider _serviceProvider;

        public OutboxExecuteScopeAdapter(IExecuteScopeContext<TArguments> executeScope, ConsumeContext<RoutingSlip> context,
            IServiceProvider serviceProvider)
        {
            _executeScope = executeScope;
            _context = context;
            _serviceProvider = serviceProvider;
        }

        public ConsumeContext<RoutingSlip> Context => _context;

        public T GetService<T>()
            where T : class
        {
            return _executeScope.GetService<T>();
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
            return _executeScope.DisposeAsync();
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
