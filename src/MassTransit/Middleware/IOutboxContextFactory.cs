namespace MassTransit.Middleware
{
    using System;
    using System.Threading.Tasks;


    public interface IOutboxContextFactory<TContext> :
        IProbeSite
        where TContext : class
    {
        Task Send<T>(ConsumeContext<T> context, OutboxConsumeOptions options, IPipe<OutboxConsumeContext<T>> next)
            where T : class;

        Task Send<TArguments>(ExecuteContext<TArguments> context, OutboxConsumeOptions options, IPipe<OutboxExecuteContext<TArguments>> next)
            where TArguments : class;

        Task Send<TLog>(CompensateContext<TLog> context, OutboxConsumeOptions options, IPipe<OutboxCompensateContext<TLog>> next)
            where TLog : class;
    }
}
