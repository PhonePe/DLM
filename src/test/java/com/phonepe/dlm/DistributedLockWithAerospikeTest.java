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

package com.phonepe.dlm;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.policy.ClientPolicy;
import com.google.common.collect.Maps;
import com.phonepe.dlm.lock.Lock;
import com.phonepe.dlm.lock.base.LockBase;
import com.phonepe.dlm.lock.level.LockLevel;
import com.phonepe.dlm.lock.mode.LockMode;
import com.phonepe.dlm.lock.storage.aerospike.AerospikeStore;
import com.phonepe.dlm.util.TestUtils;
import com.phonepe.dlm.exception.DLMException;
import com.phonepe.dlm.exception.ErrorCode;
import io.appform.testcontainers.aerospike.AerospikeContainerConfiguration;
import io.appform.testcontainers.aerospike.container.AerospikeContainer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

/**
 * @author shantanu.tiwari
 */
public class DistributedLockWithAerospikeTest {
    public static final String AEROSPIKE_HOST = "localhost";
    public static final String AEROSPIKE_DOCKER_IMAGE = "aerospike/aerospike-server:latest";
    public static final String AEROSPIKE_NAMESPACE = "DLM";
    public static final int AEROSPIKE_PORT = 3000;
    private static final AerospikeContainer AEROSPIKE_DOCKER_CONTAINER;

    private DistributedLockManager lockManager;
    public AerospikeClient aerospikeClient;

    static {
        AerospikeContainerConfiguration aerospikeContainerConfig = new AerospikeContainerConfiguration(
                true, AEROSPIKE_DOCKER_IMAGE, AEROSPIKE_NAMESPACE, AEROSPIKE_HOST, AEROSPIKE_PORT);
        aerospikeContainerConfig.setWaitTimeoutInSeconds(300L);
        AEROSPIKE_DOCKER_CONTAINER = new AerospikeContainer(aerospikeContainerConfig);
        AEROSPIKE_DOCKER_CONTAINER.start();
    }

    @Before
    public void setUp() {
        aerospikeClient = new AerospikeClient(new ClientPolicy(),
                new Host(AEROSPIKE_DOCKER_CONTAINER.getContainerIpAddress(), AEROSPIKE_DOCKER_CONTAINER.getConnectionPort()));

        lockManager = DistributedLockManager.builder()
                .clientId("CLIENT_ID")
                .farmId("FA1")
                .lockBase(LockBase.builder()
                        .mode(LockMode.EXCLUSIVE)
                        .lockStore(AerospikeStore.builder()
                                .aerospikeClient(aerospikeClient)
                                .namespace(AEROSPIKE_NAMESPACE)
                                .setSuffix("distributed_lock")
                                .build())
                        .build())
                .build();
        lockManager.initialize();
    }

    @After
    public void destroy() {
        lockManager.destroy();
    }

    @Test
    public void lockTestPositiveSiloDC() {
        final Lock lock = lockManager.getLockInstance("LOCK_ID", LockLevel.DC);
        lockManager.tryAcquireLock(lock);
        Assert.assertTrue(lock.getAcquiredStatus()
                .get());

        boolean released = lockManager.releaseLock(lock);
        Assert.assertTrue(released);
        Assert.assertFalse(lock.getAcquiredStatus()
                .get());

        // Attempt to release it again
        released = lockManager.releaseLock(lock);
        Assert.assertFalse(released);
    }

    @Test
    public void lockTestPositiveXDC() {
        final Lock lock = lockManager.getLockInstance("LOCK_ID", LockLevel.XDC);
        lockManager.tryAcquireLock(lock, 90);
        Assert.assertTrue(lock.getAcquiredStatus()
                .get());

        boolean released = lockManager.releaseLock(lock);
        Assert.assertTrue(released);
        Assert.assertFalse(lock.getAcquiredStatus()
                .get());

        // Attempt to release it again
        released = lockManager.releaseLock(lock);
        Assert.assertFalse(released);

    }

    @Test
    public void testAcquireLockWithWait() {
        final Lock lock = lockManager.getLockInstance("NEW_LOCK_ID", LockLevel.DC);
        lockManager.acquireLock(lock, 2); // Lock acquired for 1 seconds
        Assert.assertTrue(lock.getAcquiredStatus().get());

        try {
            lockManager.tryAcquireLock(lock); // Try acquiring a lock and fail if lock is already acquired
        } catch (DLMException e) {
            Assert.assertEquals(ErrorCode.LOCK_UNAVAILABLE, e.getErrorCode());
        }

        lockManager.acquireLock(lock); // Wait and try acquiring the lock.
        Assert.assertTrue(lock.getAcquiredStatus().get());

        try {
            lockManager.acquireLock(lock, 2, 2); // Wait for 2 seconds only for acquiring the lock
            Assert.fail("Flow should not have reached here");
        } catch (DLMException e) {
            Assert.assertEquals(ErrorCode.LOCK_UNAVAILABLE, e.getErrorCode()); // As it won't be released for next 90 secs default
        }
    }

    @Test(expected = DLMException.class)
    public void lockTestNegative1() {
        final Lock lock = lockManager.getLockInstance("LOCK_ID", LockLevel.DC);
        lockManager.tryAcquireLock(lock);
        Assert.assertTrue(lock.getAcquiredStatus()
                .get());
        lockManager.tryAcquireLock(lock);
    }

    @Test(expected = DLMException.class)
    public void lockTestNegative2() {
        Lock lock = lockManager.getLockInstance("LOCK_ID", LockLevel.DC);
        lockManager.tryAcquireLock(lock);
        Assert.assertTrue(lock.getAcquiredStatus()
                .get());
        lock = lockManager.getLockInstance("LOCK_ID", LockLevel.DC);
        lockManager.tryAcquireLock(lock);
    }

    @Test(expected = DLMException.class)
    public void lockTestNegative3() {
        final Lock lock = lockManager.getLockInstance("LOCK_ID", LockLevel.DC);
        lockManager.tryAcquireLock(lock);
        Assert.assertTrue(lock.getAcquiredStatus()
                .get());
        final Lock lock1 = lockManager.getLockInstance("LOCK_ID", LockLevel.DC);
        lockManager.tryAcquireLock(lock1);
    }

    @Test
    public void concurrentLockAttempt() throws InterruptedException {
        final int attempts = Runtime.getRuntime()
                .availableProcessors();
        final Map<String, AtomicInteger> trackers = Maps.newConcurrentMap();
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
                    // ignore;
                } finally {
                    boolean result = lockManager.releaseLock(lock);
                    Assert.assertFalse(lock.getAcquiredStatus()
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
                    } catch (InterruptedException | ExecutionException e1) {
                        // ignore;
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

    @After
    public void tearDown() {
        aerospikeClient.truncate(aerospikeClient.getInfoPolicyDefault(), AEROSPIKE_NAMESPACE, null, null);
    }
}