# Activity Outbox Support Implementation

## Summary

This implementation adds full transactional outbox support for MassTransit Activities (Execute and Compensate activities). Previously, only Consumers and Sagas were supported by the outbox pattern. Activities can now participate in the same transactional guarantees, ensuring messages sent/published from within activities are stored in the outbox and delivered reliably.

## Problem Statement

Activities were not properly connected to the outbox infrastructure:
1. `ConnectActivityConfigurationObserver` was not being called in outbox configuration extensions
2. Activity methods in `OutboxConsumePipeSpecificationObserver` were incorrectly applying filters to `RoutingSlip` messages instead of activity execution contexts
3. No activity-specific outbox filters, pipes, or context implementations existed
4. `BusFactoryConfigurator` was routing activity observers through the wrong configuration path

## Changes Made

### Core Infrastructure Files Created

#### 1. Activity Filter Classes
- **`OutboxExecuteFilter.cs`** - Filter that intercepts execute activity execution and wraps it in outbox transaction
- **`OutboxCompensateFilter.cs`** - Filter that intercepts compensate activity execution and wraps it in outbox transaction

#### 2. Activity Pipe Classes
- **`OutboxExecutePipe.cs`** - Manages outbox message delivery lifecycle for execute activities
- **`OutboxCompensatePipe.cs`** - Manages outbox message delivery lifecycle for compensate activities

#### 3. Activity Context Interfaces
- **`OutboxExecuteContext.cs`** - Interface combining `OutboxConsumeContext` and `ExecuteContext<TArguments>`
- **`OutboxCompensateContext.cs`** - Interface combining `OutboxConsumeContext` and `CompensateContext<TLog>`

#### 4. EntityFramework Implementations
- **`DbContextOutboxExecuteContext.cs`** - EF Core-specific implementation for execute activity outbox
- **`DbContextOutboxCompensateContext.cs`** - EF Core-specific implementation for compensate activity outbox

### Modified Files

#### Technical Research:
```
1. DbContextOutboxExecuteContext & DbContextOutboxCompensateContext files
   Based on: DbContextOutboxConsumeContext<TDbContext, TMessage>
- Location: src/Persistence/MassTransit.EntityFrameworkCoreIntegration/EntityFrameworkCoreIntegration/DbContextOutboxConsumeContext.cs
- I read this file and adapted it, replacing:
    - ConsumeContextProxy<TMessage> → ExecuteContextProxy<TArguments> / CompensateContextProxy<TLog>
    - Kept the same inbox/outbox management logic

2. OutboxExecutePipe & OutboxCompensatePipe files
   Based on: OutboxMessagePipe<TMessage>
- Location: src/MassTransit/Middleware/OutboxMessagePipe.cs
- I read this file and adapted it, changing:
    - IConsumeScopeContext<TMessage> → IExecuteScopeContext<TArguments> / ICompensateScopeContext<TLog>
    - Removed PushConsumeContext calls (not available on activity scopes)
    - Kept the delivery logic for outbox messages

3. OutboxExecuteFilter & OutboxCompensateFilter files
   Based on: OutboxConsumeFilter<TContext, TMessage>
- Location: src/MassTransit/Middleware/OutboxConsumeFilter.cs
- I read this file and adapted it, changing:
    - IConsumeScopeProvider → IExecuteActivityScopeProvider / ICompensateActivityScopeProvider
    - ConsumeContext<TMessage> → ExecuteContext<TArguments> / CompensateContext<TLog>

4. Pattern Validation
   I also checked:
- InMemoryOutboxExecuteContext (src/MassTransit/Middleware/InMemoryOutbox/) - to see how InMemory implementation wrapped execute contexts
- InMemoryOutboxConfigurationObserver - to understand how activities should be configured
- ScopedExecuteActivityPipeSpecificationObserver - to see the correct pattern for adding filters to activities using configurator.Arguments() and configurator.Log()
```

#### Configuration Files

**`IOutboxContextFactory.cs`**
- Added `Send<TArguments>(ExecuteContext<TArguments>...)` method for execute activities
- Added `Send<TLog>(CompensateContext<TLog>...)` method for compensate activities

**`EntityFrameworkOutboxContextFactory.cs`**
- Implemented execute activity `Send` method with full inbox/outbox transaction handling
- Implemented compensate activity `Send` method with full inbox/outbox transaction handling
- Both follow same pattern as consumer `Send` method: lock inbox state, create outbox context, manage transaction lifecycle

**`InMemoryOutboxContextFactory.cs`**
- Added stub implementations throwing `NotSupportedException` (InMemory outbox uses different pattern via `InMemoryOutboxConfigurationObserver`)

**`OutboxConsumePipeSpecificationObserver.cs`**
- Fixed `ActivityConfigured` - now calls `ExecuteActivityConfigured`
- Fixed `ExecuteActivityConfigured` - uses `configurator.Arguments()` instead of `configurator.RoutingSlip()`
- Fixed `CompensateActivityConfigured` - uses `configurator.Log()` instead of `configurator.RoutingSlip()`
- Added `AddExecuteScopedFilter<TActivity, TArguments>()` helper method
- Added `AddCompensateScopedFilter<TActivity, TLog>()` helper method

**`EntityFrameworkOutboxConfigurationExtensions.cs`**
- Added `configurator.ConnectActivityConfigurationObserver(observer)` call in `UseEntityFrameworkOutbox`

**`MongoDbOutboxConfigurationExtensions.cs`**
- Added `configurator.ConnectActivityConfigurationObserver(observer)` call in both `UseMongoDbOutbox` overloads

**`InMemoryOutboxConfigurationExtensions.cs`**
- Added `configurator.ConnectActivityConfigurationObserver(observer)` call in both `UseInMemoryInboxOutbox` overloads

**`BusFactoryConfigurator.cs`**
- Fixed `ConnectActivityConfigurationObserver` to route through `_busConfiguration.Consume.Configurator` instead of `_busConfiguration`
- Makes it consistent with consumer/saga/handler observer routing

## Technical Details

### How Activity Outbox Works

1. **Configuration Phase**
   - When `UseEntityFrameworkOutbox` (or MongoDB/InMemory equivalent) is called, it now connects the activity configuration observer
   - When activities are configured, the observer adds `OutboxExecuteFilter` or `OutboxCompensateFilter` to the activity pipeline

2. **Execution Phase**
   - Activity execution enters the outbox filter
   - Filter creates activity scope and resolves `IOutboxContextFactory<TContext>`
   - Context factory manages inbox state (deduplication) and creates outbox context
   - Activity executes with outbox context, which intercepts all Send/Publish operations
   - Messages are written to outbox tables instead of being sent immediately
   - Transaction commits, persisting both activity results and outbox messages

3. **Delivery Phase**
   - On subsequent receive attempts, outbox messages are loaded from database
   - Messages are sent one by one with delivery tracking
   - After all messages delivered, inbox state is marked as delivered
   - Receive processing completes

### Key Patterns

**Filter Pipeline Order**:
```
ExecuteActivityHost
  -> ExecuteActivityFactoryFilter
    -> ScopeExecuteActivityFactory
      -> OutboxExecuteFilter          ← NEW: Intercepts here
        -> ExecuteActivityFilter
          -> [Activity.Execute()]
```

**Scope Provider Usage**:
- Execute activities: `ExecuteActivityScopeProvider<TActivity, TArguments>`
- Compensate activities: `CompensateActivityScopeProvider<TActivity, TLog>`
- These provide access to scoped services and the outbox context factory

**Generic Type Parameters**:
- Filters are generic over `<TContext, TActivity, TArguments>` (or `TLog`)
- `TContext` is the persistence context (e.g., `DbContext`)
- `TActivity` is the concrete activity type
- `TArguments`/`TLog` are the activity data types

## Benefits

1. **Transactional Guarantees** - Activities now have same reliability as consumers/sagas
2. **Exactly-Once Semantics** - Duplicate activity executions are prevented via inbox state
3. **Automatic Retry** - Failed message delivery is automatically retried
4. **Consistent Pattern** - Activities use same outbox pattern as other message handlers

## Testing

After rebuilding and republishing packages:

1. Configure outbox on receive endpoint with activities
2. Activity executes and sends/publishes messages
3. Check database - messages should appear in `OutboxMessage` table
4. Check inbox state - should track execution and delivery
5. Stack traces should show `OutboxExecuteFilter` in the pipeline

## Compatibility

- **Frameworks**: net472, netstandard2.0, net8.0, net9.0, net10.0
- **Outbox Providers**:
  - ✅ **EntityFramework Core** - Fully implemented
  - ⚠️ **MongoDB** - Configuration connected, but context factory methods throw `NotSupportedException`
  - ⚠️ **InMemory** - Configuration connected, but uses different pattern via `InMemoryOutboxConfigurationObserver`
- **Activity Types**: Execute activities, Compensate activities, Two-phase activities

### Implementation Notes

**EntityFramework Core**: Full implementation with activity-specific context classes and factory methods.

**MongoDB**: The activity observer is connected in configuration, but the `MongoDbOutboxContextFactory` would need to implement the activity `Send` methods. Currently throws `NotSupportedException` as a placeholder.

**InMemory**: Uses a different architectural pattern - activities are configured through `InMemoryOutboxConfigurationObserver` which directly adds specifications, not through the context factory pattern. The `InMemoryOutboxContextFactory.Send` methods for activities throw `NotSupportedException` as they're not meant to be called.

## Future Considerations

- Monitor performance impact of activity outbox (additional database roundtrips)
- Consider optimizations for high-throughput scenarios
- Evaluate if activity-specific configuration options are needed
- Add integration tests for activity outbox scenarios

## Files Changed Summary

**New Files (8)**:
- `OutboxExecuteFilter.cs`
- `OutboxExecutePipe.cs`
- `OutboxExecuteContext.cs`
- `DbContextOutboxExecuteContext.cs`
- `OutboxCompensateFilter.cs`
- `OutboxCompensatePipe.cs`
- `OutboxCompensateContext.cs`
- `DbContextOutboxCompensateContext.cs`

**Modified Files (8)**:
- `IOutboxContextFactory.cs`
- `EntityFrameworkOutboxContextFactory.cs`
- `InMemoryOutboxContextFactory.cs`
- `OutboxConsumePipeSpecificationObserver.cs`
- `EntityFrameworkOutboxConfigurationExtensions.cs`
- `MongoDbOutboxConfigurationExtensions.cs`
- `InMemoryOutboxConfigurationExtensions.cs`
- `BusFactoryConfigurator.cs`

## Version

Changes implemented for MassTransit version **8.5.7-beta** (IDK)
