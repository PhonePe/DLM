# Usage

## Builder parameters

| Parameter  | Type       | Required | Description |
|------------|------------|----------|-------------|
| `clientId` | `String`   | Yes      | Unique identifier for the calling service. Lock IDs are scoped as `clientId#lockId`, so different clients can lock the same entity independently. |
| `farmId`   | `String`   | Yes      | Data center / farm identifier. Used in key construction for `DC`-level locks. |
| `lockBase` | `LockBase` | Yes      | The lock engine — wraps the storage backend and lock mode. |

## Initialize lock manager

=== "Aerospike"

    ```java
    DistributedLockManager lockManager = DistributedLockManager.builder()
        .clientId("CLIENT_ID")
        .farmId("FA1")
        .lockBase(LockBase.builder()
            .mode(LockMode.EXCLUSIVE)
            .lockStore(AerospikeStore.builder()
                .aerospikeClient(aerospikeClient)
                .namespace("NAMESPACE")
                .setSuffix("distributed_lock")
                .build())
            .build())
        .build();

    lockManager.initialize();
    ```

=== "HBase"

    ```java
    DistributedLockManager lockManager = DistributedLockManager.builder()
        .clientId("CLIENT_ID")
        .farmId("FA1")
        .lockBase(LockBase.builder()
            .mode(LockMode.EXCLUSIVE)
            .lockStore(HBaseStore.builder()
                .connection(connection)
                .tableName("table_name")
                .build())
            .build())
        .build();

    lockManager.initialize();
    ```

!!! warning
    Always call `lockManager.initialize()` before any lock operations.
    For HBase, this creates the table if it does not exist.

## Get a lock instance

```java
Lock lock = lockManager.getLockInstance("order-123", LockLevel.DC);
```

The returned `Lock` object contains:

- **lockId** — composed as `clientId#order-123`.
- **lockLevel** — `DC` or `XDC`.
- **farmId** — inherited from the manager.
- **acquiredStatus** — an `AtomicBoolean` tracking whether this instance currently holds the lock.

!!! info
    The `Lock` object is lightweight and does not perform any I/O on creation.
    Actual storage interaction happens only on `acquire` / `release`.

## Acquiring locks

### Non-blocking — `tryAcquireLock`

Attempts to acquire immediately. Throws `DLMException` with `ErrorCode.LOCK_UNAVAILABLE` if the lock is already held.

```java
// Default TTL (90 seconds)
lockManager.tryAcquireLock(lock);

// Custom TTL
lockManager.tryAcquireLock(lock, Duration.ofSeconds(120));
```

### Blocking — `acquireLock`

Retries in a loop (1-second intervals) until the lock is acquired or the timeout expires.

```java
// Default TTL (90s) and default timeout (90s)
lockManager.acquireLock(lock);

// Custom TTL, default timeout (90s)
lockManager.acquireLock(lock, Duration.ofSeconds(30));

// Custom TTL and custom timeout
lockManager.acquireLock(lock, Duration.ofSeconds(30), Duration.ofSeconds(10));
```

!!! note "Default values"
    | Constant                         | Default  |
    |----------------------------------|----------|
    | `DEFAULT_LOCK_TTL_SECONDS`       | 90 seconds |
    | `DEFAULT_WAIT_FOR_LOCK_IN_SECONDS` | 90 seconds |
    | `WAIT_TIME_FOR_NEXT_RETRY`       | 1 second |

## Releasing locks

```java
boolean released = lockManager.releaseLock(lock);
```

- Returns `true` if the lock was held and successfully released.
- Returns `false` if the lock was not held by this instance (i.e. `acquiredStatus` was already `false`).

!!! warning
    Always release in a `finally` block to avoid lock leaks.

## Error handling

All lock operations throw `DLMException`. Use `getErrorCode()` to distinguish failure reasons:

```java
Lock lock = lockManager.getLockInstance("order-123", LockLevel.DC);
try {
    lockManager.tryAcquireLock(lock, Duration.ofSeconds(60));
    // critical section
} catch (DLMException e) {
    switch (e.getErrorCode()) {
        case LOCK_UNAVAILABLE -> log.warn("Lock held by another holder");
        case CONNECTION_ERROR -> log.error("Storage backend unreachable", e);
        case RETRIES_EXHAUSTED -> log.error("All retry attempts failed", e);
        default -> log.error("Unexpected error", e);
    }
} finally {
    lockManager.releaseLock(lock);
}
```

See [Error Codes](locking.md#error-codes) for the full list.

## Complete lifecycle example

```java
// ── Setup (application startup) ──
DistributedLockManager lockManager = DistributedLockManager.builder()
    .clientId("payment-service")
    .farmId("dc1")
    .lockBase(LockBase.builder()
        .mode(LockMode.EXCLUSIVE)
        .lockStore(AerospikeStore.builder()
            .aerospikeClient(aerospikeClient)
            .namespace("locks")
            .setSuffix("distributed_lock")
            .build())
        .build())
    .build();
lockManager.initialize();

// ── Use (request handling) ──
Lock lock = lockManager.getLockInstance("txn-456", LockLevel.DC);
try {
    lockManager.acquireLock(lock, Duration.ofSeconds(30), Duration.ofSeconds(10));
    processPayment("txn-456");
} catch (DLMException e) {
    if (e.getErrorCode() == ErrorCode.LOCK_UNAVAILABLE) {
        // another instance is processing this transaction
    }
} finally {
    lockManager.releaseLock(lock);
}

// ── Teardown (application shutdown) ──
lockManager.destroy();
```

## Cleanup

When the application shuts down, call `destroy()` to close the underlying store connection and release resources:

```java
lockManager.destroy();
```

- For **Aerospike**, this calls `aerospikeClient.close()`.
- For **HBase**, this calls `connection.close()`.

!!! danger
    Failing to call `destroy()` may leave dangling connections to the storage backend.
    Invoke it in a shutdown hook or your framework's lifecycle callback.
