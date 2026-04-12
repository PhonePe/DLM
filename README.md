# Distributed Lock Manager (DLM)

[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=PhonePe_DLM&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=PhonePe_DLM)

Locking is a very common expectation in SoA, where a vulnerable entity needs to be protected for a certain duration.
And the definition of vulnerable entity changes from one client to another depending on the use-cases at hand.

Distributed Lock Manager is an easy-to-use library to achieve various modes of locking, be it - Exclusive or Limited Protected(LP).
In current version, Exclusive locking mode is supported with Aerospike as the underlying storage base by leveraging
its MVCC (MultiVersion Concurrency Control) capability.

## Add Maven Dependency

```xml
<dependency>
  <groupId>com.phonepe</groupId>
  <artifactId>DLM</artifactId>
  <version>1.0.0</version>
</dependency>
```


### Usage

#### Initializing Distributed Lock Manager

##### With Aerospike as lock base

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

##### With HBase as lock base

```java
DistributedLockManager lockManager = DistributedLockManager.builder()
                .clientId("CLIENT_ID")
                .farmId("FA1")
                .lockBase(LockBase.builder()
                        .mode(LockMode.EXCLUSIVE)
                        .lockStore(HBaseStore.builder()
                                .connection(connection) // HBase connection reference
                                .tableName("table_name")
                                .build())
                        .build())
                .build();
lockManager.initialize();
```

PS : For optimum performance, DO NOT pre-create the HBase table. Library will do it for you.


# Entity Lock Management

This library offers various methods for acquiring and releasing locks on critical entities. Below are the available methods:

1. **`tryAcquireLock(lock)`**
    - Attempts immediate lock acquisition. Throws an exception if the lock is unavailable. Does not wait if the lock is currently held by another thread. The default lock duration is 90 seconds.

2. **`tryAcquireLock(lock, duration)`**
    - Immediate lock acquisition attempt with a specified duration. Throws an exception if the lock is unavailable. Does not wait if the lock is held by another thread.

3. **`acquireLock(lock)`**
    - Tries to acquire the lock. If the lock is held by another thread, it waits until the lock becomes available, blocking the thread. The default timeout is 90 seconds, and the lock duration defaults to 90 seconds.

4. **`acquireLock(lock, duration)`**
    - Similar to `acquireLock(lock)` but allows a specified duration for the lock.

5. **`acquireLock(lock, duration, timeout)`**
    - Attempts to acquire the lock and waits for a limited time for it to become available. If the lock is not acquired within the given time, it returns with a failure indication. This method blocks the thread until the lock is acquired.

## Example Usage

```java
// Representing a vulnerable entity by LOCK_ID
final Lock lock = lockManager.getLockInstance("LOCK_ID", LockLevel.DC);
try {
    lockManager.tryAcquireLock(lock); // Attempts to acquire the lock for the default duration of 90 seconds
    // OR lockManager.tryAcquireLock(lock, Duration.ofSeconds(120)); // Tries to acquire the lock for 120 seconds

    // Perform actions once the lock is successfully acquired.

} catch (DLMException e) {
    if (ErrorCode.LOCK_UNAVAILABLE.equals(e.getErrorCode())) {
        // Actions to take if the lock can't be acquired.
    }
} finally {
    // Verify if the lock was released successfully.
    boolean released = lockManager.releaseLock(lock);
}
```

```java
// Representing a vulnerable entity by LOCK_ID
final Lock lock = lockManager.getLockInstance("LOCK_ID", LockLevel.DC);
try {
    lockManager.acquireLock(lock); // Attempts to acquire the lock for the default duration of 90 seconds and waits for 90 seconds
    // OR lockManager.acquireLock(lock, Duration.ofSeconds(30)); // Tries to acquire the lock for 30 seconds, waiting for 90 seconds
    // OR lockManager.acquireLock(lock, Duration.ofSeconds(30), Duration.ofSeconds(30)); // Tries to acquire the lock for 30 seconds, waiting for 30 seconds

    // Perform actions once the lock is successfully acquired.
} catch (DLMException e) {
    if (ErrorCode.LOCK_UNAVAILABLE.equals(e.getErrorCode())) {
        // Actions to take if the lock can't be acquired.
    }
} finally {
    // Verify if the lock was released successfully.
    boolean released = lockManager.releaseLock(lock);
}
```

#### Cleanup

When the application shuts down, call `destroy()` to close the underlying store connection:

```java
lockManager.destroy();
```

#### Lock Levels
* DC - Acquiring/releasing lock within a DC
* XDC - Acquiring/releasing lock across DCs.


**Caution**: Concurrently obtaining a lock on the same entity using XDC across multiple data centers may result in unexpected behavior due to the replication of data between centers. Therefore, it is recommended to utilize DC locks whenever feasible.

For XDC locks requiring strong consistency, opt for a multi-site Aerospike cluster.

#### Notes

A lock exists only within the scope of a Client represented by `CLIENT_ID`.

## Documentation Site (Zensical)

This repository now includes Zensical-based docs under `docs/`.

- Config: `docs/zensical.toml`
- Content: `docs/docs/`
- Python dependencies: `docs/requirements.txt`
- GitHub Pages workflow: `.github/workflows/docs.yml`

Build docs locally:

```bash
cd docs
pip install -r requirements.txt
zensical build --clean
```

Generated site output is available at `docs/site`.
