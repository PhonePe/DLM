# Getting Started

## Requirements

- **Java 17** or later.
- One of the supported storage backends:
    - An **Aerospike** cluster reachable from application nodes, or
    - An **HBase** cluster reachable from application nodes.

## Add dependency

Choose the module for the storage backend used by your application. The selected module includes `dlm-core` transitively and does not include dependencies from the other backend.

=== "Aerospike"

    ```xml
    <dependency>
      <groupId>com.phonepe</groupId>
      <artifactId>dlm-aerospike</artifactId>
      <version>${dlm.version}</version>
    </dependency>
    ```

=== "HBase"

    ```xml
    <dependency>
      <groupId>com.phonepe</groupId>
      <artifactId>dlm-hbase</artifactId>
      <version>${dlm.version}</version>
    </dependency>
    ```

Use `dlm-core` directly only when implementing a custom `ILockStore`. Replace `${dlm.version}` with the latest version from [Maven Central](https://central.sonatype.com/namespace/com.phonepe) or [GitHub Releases](https://github.com/PhonePe/DLM/releases).

## Build locally

```bash
git clone https://github.com/PhonePe/DLM.git
cd DLM
mvn clean install
```

To run the tests:

```bash
mvn clean test
```

!!! note
    Some integration tests require Docker (via Testcontainers) for Aerospike.
    Make sure the Docker daemon is running before executing the full test suite.

## Minimal example

```java
// 1. Build the lock manager
DistributedLockManager lockManager = DistributedLockManager.builder()
    .clientId("order-service")
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

// 2. Initialize (creates tables / validates connectivity)
lockManager.initialize();

// 3. Acquire → work → release
Lock lock = lockManager.getLockInstance("order-123", LockLevel.DC);
try {
    lockManager.acquireLock(lock);
    // critical section
} finally {
    lockManager.releaseLock(lock);
}

// 4. Shutdown
lockManager.destroy();
```

!!! tip
    The example above uses all default timing values (90s TTL, 90s wait, 1s retry).
    To customise these, pass a `LockConfiguration` to the `LockBase` builder.
    See [Configuring lock timing](usage.md#configuring-lock-timing).

## What's next

- [Usage](usage.md) — initialization, lock timing configuration, API overloads, error handling.
- [Locking Semantics](locking.md) — defaults, retry behavior, lock levels.
- [Storage Backends](storages/aerospike.md) — Aerospike and HBase details.
