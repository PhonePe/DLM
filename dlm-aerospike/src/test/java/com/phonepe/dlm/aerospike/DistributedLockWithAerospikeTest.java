/**
 * Copyright (c) 2024 Original Author(s), PhonePe India Pvt. Ltd.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.phonepe.dlm.aerospike;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Host;
import com.aerospike.client.policy.ClientPolicy;
import com.phonepe.dlm.DistributedLockManager;
import com.phonepe.dlm.aerospike.storage.AerospikeStore;
import com.phonepe.dlm.aerospike.utils.TestUtils;
import com.phonepe.dlm.exception.DLMException;
import com.phonepe.dlm.exception.ErrorCode;
import com.phonepe.dlm.lock.Lock;
import com.phonepe.dlm.lock.base.LockBase;
import com.phonepe.dlm.lock.level.LockLevel;
import com.phonepe.dlm.lock.mode.LockMode;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.AbstractWaitStrategy;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author shantanu.tiwari
 */
@Slf4j
public class DistributedLockWithAerospikeTest {
    public static final String AEROSPIKE_HOST = "localhost";
    public static final String AEROSPIKE_DOCKER_IMAGE = "aerospike/aerospike-server:6.4.0.23";
    public static final String AEROSPIKE_NAMESPACE = "DLM";
    public static final int AEROSPIKE_PORT = 3000;
    private static final GenericContainer<?> AEROSPIKE_DOCKER_CONTAINER;

    private DistributedLockManager lockManager;
    public AerospikeClient aerospikeClient;

    static {
        AEROSPIKE_DOCKER_CONTAINER = new GenericContainer<>(DockerImageName.parse(AEROSPIKE_DOCKER_IMAGE))
                .withExposedPorts(AEROSPIKE_PORT)
                .withEnv("NAMESPACE", AEROSPIKE_NAMESPACE)
                .waitingFor(new AbstractWaitStrategy() {
                    @Override
                    protected void waitUntilReady() {
                        Awaitility.await()
                                .atMost(startupTimeout)
                                .pollInterval(Duration.ofMillis(250))
                                .ignoreException(AerospikeException.Connection.class)
                                .until(() -> {
                                    try (AerospikeClient client = new AerospikeClient(
                                            waitStrategyTarget.getHost(),
                                            waitStrategyTarget.getMappedPort(AEROSPIKE_PORT))) {
                                        return client.isConnected();
                                    }
                                });
                    }
                }.withStartupTimeout(Duration.ofSeconds(300)));
        AEROSPIKE_DOCKER_CONTAINER.start();
    }

    @BeforeEach
    public void setUp() {
        aerospikeClient = new AerospikeClient(new ClientPolicy(),
                new Host(AEROSPIKE_DOCKER_CONTAINER.getHost(), AEROSPIKE_DOCKER_CONTAINER.getMappedPort(AEROSPIKE_PORT)));
        aerospikeClient.truncate(aerospikeClient.getInfoPolicyDefault(), AEROSPIKE_NAMESPACE, null, null);

        lockManager = getLockManager(LockMode.EXCLUSIVE);
        lockManager.initialize();
    }

    @Test
    public void lockPositiveSiloDCTest() {
        final Lock lock = lockManager.getLockInstance("LOCK_ID", LockLevel.DC);
        lockManager.tryAcquireLock(lock);
        Assertions.assertTrue(lock.getAcquiredStatus()
                .get());

        boolean released = lockManager.releaseLock(lock);
        Assertions.assertTrue(released);
        Assertions.assertFalse(lock.getAcquiredStatus()
                .get());

        // Attempt to release it again
        released = lockManager.releaseLock(lock);
        Assertions.assertFalse(released);
    }

    @Test
    public void lockPositiveXDCTest() {
        final Lock lock = lockManager.getLockInstance("LOCK_ID", LockLevel.XDC);
        lockManager.tryAcquireLock(lock, Duration.ofSeconds(90));
        Assertions.assertTrue(lock.getAcquiredStatus()
                .get());

        boolean released = lockManager.releaseLock(lock);
        Assertions.assertTrue(released);
        Assertions.assertFalse(lock.getAcquiredStatus()
                .get());

        // Attempt to release it again
        released = lockManager.releaseLock(lock);
        Assertions.assertFalse(released);

    }

    @Test
    public void lockUnavailableForAcquireLockTest() {
        final Lock lock = lockManager.getLockInstance("NEW_LOCK_ID", LockLevel.DC);
        lockManager.acquireLock(lock, Duration.ofSeconds(30)); // Wait and try acquiring the lock.

        final DLMException exception = assertThrows(DLMException.class,
                () -> lockManager.acquireLock(lock, Duration.ofSeconds(2), Duration.ofSeconds(2)));
        assertEquals(ErrorCode.LOCK_UNAVAILABLE, exception.getErrorCode());
    }

    @Test
    public void lockUnavailableForTryAcquireLockWithSameLockInstanceTest() {
        final Lock lock = lockManager.getLockInstance("LOCK_ID", LockLevel.DC);
        lockManager.acquireLock(lock, Duration.ofSeconds(30), Duration.ofSeconds(5));
        Assertions.assertTrue(lock.getAcquiredStatus().get());

        final DLMException exception = assertThrows(DLMException.class, () -> lockManager.tryAcquireLock(lock));
        assertEquals(ErrorCode.LOCK_UNAVAILABLE, exception.getErrorCode());
    }

    @Test
    public void lockUnavailableForTryAcquireLockWithDifferentLockInstanceTest() {
        Lock lock = lockManager.getLockInstance("LOCK_ID", LockLevel.DC);
        lockManager.tryAcquireLock(lock);
        Assertions.assertTrue(lock.getAcquiredStatus().get());

        final Lock contendingLock = lockManager.getLockInstance("LOCK_ID", LockLevel.DC);
        final DLMException exception = assertThrows(DLMException.class,
                () -> lockManager.tryAcquireLock(contendingLock));
        assertEquals(ErrorCode.LOCK_UNAVAILABLE, exception.getErrorCode());
    }

    @Test
    public void concurrentLockAttemptTest() {
        final int attempts = Runtime.getRuntime()
                .availableProcessors();
        final Map<String, AtomicInteger> trackers = new ConcurrentHashMap<>();
        final ExecutorService service = Executors.newFixedThreadPool(attempts);
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicInteger counter = new AtomicInteger(attempts);

        final List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < attempts; i++) {
            TestUtils.waitSometime(100, TimeUnit.MILLISECONDS);
            futures.add(service.submit(() -> {
                Lock lock = null;
                try {
                    lock = lockManager.getLockInstance("LOCK_ID", LockLevel.DC);
                    lockManager.tryAcquireLock(lock);
                    if (lock.getAcquiredStatus()
                            .get()) {
                        trackers.computeIfAbsent("SUCCESSFUL_ACQUIRES", x -> new AtomicInteger(0))
                                .getAndIncrement();
                    }
                    latch.await();
                } catch (DLMException e) {
                    trackers.computeIfAbsent("FAILED_ACQUIRES", x -> new AtomicInteger(0))
                            .getAndIncrement();
                } catch (Exception e) {
                    log.warn("Gracefully ignoring exception", e);
                } finally {
                    boolean result = lockManager.releaseLock(lock);
                    Assertions.assertFalse(lock.getAcquiredStatus()
                            .get());
                    if (result) {
                        trackers.computeIfAbsent("SUCCESSFUL_RELEASES", x -> new AtomicInteger(0))
                                .getAndIncrement();
                    } else {
                        trackers.computeIfAbsent("FAILED_RELEASES", x -> new AtomicInteger(0))
                                .getAndIncrement();
                    }
                }
            }));
        }
        futures.parallelStream()
                .forEach(future -> {
                    try {
                        future.get();
                        if (counter.decrementAndGet() <= 1) {
                            latch.countDown();
                        }
                    } catch (InterruptedException | ExecutionException e) {
                        log.warn("Gracefully ignoring exception", e);
                    }
                });

        // Only one successful acquire / release of locks should take place
        assertEquals(1,
                trackers.getOrDefault("SUCCESSFUL_ACQUIRES", new AtomicInteger(0))
                        .get());
        assertEquals(1,
                trackers.getOrDefault("SUCCESSFUL_RELEASES", new AtomicInteger(0))
                        .get());
        assertEquals(attempts - 1,
                trackers.getOrDefault("FAILED_ACQUIRES", new AtomicInteger(0))
                        .get());
        assertEquals(attempts - 1,
                trackers.getOrDefault("FAILED_RELEASES", new AtomicInteger(0))
                        .get());
    }

    @Test
    public void exceptionInLockTest() {
        final Lock lock = lockManager.getLockInstance("LOCK_ID", LockLevel.DC);
        assertThrows(DLMException.class, () -> lockManager.acquireLock(lock, Duration.ofDays(7300)));
    }

    @Test
    public void interruptedExceptionInLockTest() {
        try {
            final Lock lock = lockManager.getLockInstance("LOCK_ID", LockLevel.DC);
            lockManager.acquireLock(lock);
            Thread.currentThread().interrupt();
            final DLMException exception = assertThrows(DLMException.class,
                    () -> lockManager.acquireLock(lock, Duration.ofSeconds(30)));
            assertEquals(ErrorCode.INTERNAL_ERROR, exception.getErrorCode());
        } finally {
            Thread.interrupted(); // clearing thread interrupted status for isolation
        }
    }

    @AfterEach
    public void tearDown() {
        if (aerospikeClient != null) {
            aerospikeClient.truncate(aerospikeClient.getInfoPolicyDefault(), AEROSPIKE_NAMESPACE, null, null);
        }
        if (lockManager != null) {
            lockManager.destroy();
        }
    }

    public DistributedLockManager getLockManager(final LockMode lockMode) {
        return lockMode.accept(() -> DistributedLockManager.builder()
                .clientId("CLIENT_ID")
                .farmId("FA1")
                .lockBase(LockBase.builder()
                        .mode(lockMode)
                        .lockStore(AerospikeStore.builder()
                                .aerospikeClient(aerospikeClient)
                                .namespace(AEROSPIKE_NAMESPACE)
                                .setSuffix("distributed_lock")
                                .build())
                        .build())
                .build());
    }
}
