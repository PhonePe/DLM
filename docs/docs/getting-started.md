# Getting Started

## Requirements

- **Java 17** or later.
- One of the supported storage backends:
    - An **Aerospike** cluster reachable from application nodes, or
    - An **HBase** cluster reachable from application nodes.

## Add dependency

```xml
<dependency>
  <groupId>com.phonepe</groupId>
  <artifactId>DLM</artifactId>
  <version>1.0.1</version>
</dependency>
```

If you are consuming DLM in another project, choose the version published for your environment.

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

## What's next

- [Usage](usage.md) — all initialization options, API overloads, error handling.
- [Locking Semantics](locking.md) — defaults, retry behavior, lock levels.
- [Storage Backends](storages/aerospike.md) — Aerospike and HBase details.
