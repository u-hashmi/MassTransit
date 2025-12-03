namespace MassTransit.Middleware
{
    public interface OutboxCompensateContext<out TLog> :
        OutboxConsumeContext,
        CompensateContext<TLog>
        where TLog : class
    {
    }
}
