namespace MassTransit.Middleware
{
    public interface OutboxExecuteContext<out TArguments> :
        OutboxConsumeContext,
        ExecuteContext<TArguments>
        where TArguments : class
    {
    }
}
