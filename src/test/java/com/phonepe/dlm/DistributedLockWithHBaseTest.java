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

import com.google.common.collect.Maps;
import com.phonepe.dlm.exception.DLMException;
import com.phonepe.dlm.exception.ErrorCode;
import com.phonepe.dlm.lock.Lock;
import com.phonepe.dlm.lock.base.LockBase;
import com.phonepe.dlm.lock.level.LockLevel;
import com.phonepe.dlm.lock.mode.LockMode;
import com.phonepe.dlm.lock.storage.hbase.HBaseStore;
import com.phonepe.dlm.util.DLMExceptionMatcher;
import com.phonepe.dlm.util.HBaseConnectionStub;
import com.phonepe.dlm.util.TestUtils;
import lombok.extern.slf4j.Slf4j;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

@Slf4j
@RunWith(MockitoJUnitRunner.class)
public class DistributedLockWithHBaseTest {
    @Rule
    public ExpectedException exceptionThrown = ExpectedException.none();

    private DistributedLockManager lockManager;
    private HBaseConnectionStub connection;

    @Before
    public void setUp() {
        connection = new HBaseConnectionStub();
        lockManager = DistributedLockManager.builder()
                .clientId("CLIENT_ID")
                .farmId("FA1")
                .lockBase(LockBase.builder()
                        .mode(LockMode.EXCLUSIVE)
                        .lockStore(HBaseStore.builder()
                                .connection(connection)
                                .tableName("dlm_locks")
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
        Assert.assertTrue(lock.getAcquiredStatus().get());

        boolean released = lockManager.releaseLock(lock);
        Assert.assertTrue(released);
        Assert.assertFalse(lock.getAcquiredStatus().get());

        // Attempt to release it again
        released = lockManager.releaseLock(lock);
        Assert.assertFalse(released);
    }

    @Test
    public void lockTestPositiveXDC() {
        final Lock lock = lockManager.getLockInstance("LOCK_ID", LockLevel.XDC);
        lockManager.tryAcquireLock(lock);
        Assert.assertTrue(lock.getAcquiredStatus().get());

        boolean released = lockManager.releaseLock(lock);
        Assert.assertTrue(released);
        Assert.assertFalse(lock.getAcquiredStatus().get());

        // Attempt to release it again
        released = lockManager.releaseLock(lock);
        Assert.assertFalse(released);
    }

    @Test
    public void lockTestNegative1() {
        final Lock lock = lockManager.getLockInstance("LOCK_ID", LockLevel.DC);
        lockManager.tryAcquireLock(lock);
        Assert.assertTrue(lock.getAcquiredStatus().get());

        exceptionThrown.expect(DLMExceptionMatcher.hasCode(ErrorCode.LOCK_UNAVAILABLE));
        lockManager.tryAcquireLock(lock);
    }

    @Test
    public void lockTestNegative2() {
        Lock lock = lockManager.getLockInstance("LOCK_ID", LockLevel.DC);
        lockManager.tryAcquireLock(lock);

        exceptionThrown.expect(DLMExceptionMatcher.hasCode(ErrorCode.LOCK_UNAVAILABLE));
        lock = lockManager.getLockInstance("LOCK_ID", LockLevel.DC);
        lockManager.tryAcquireLock(lock);
    }

    @Test
    public void lockTestNegative3() {
        final Lock lock = lockManager.getLockInstance("LOCK_ID", LockLevel.DC);
        lockManager.tryAcquireLock(lock);

        exceptionThrown.expect(DLMExceptionMatcher.hasCode(ErrorCode.LOCK_UNAVAILABLE));
        final Lock lock1 = lockManager.getLockInstance("LOCK_ID", LockLevel.DC);
        lockManager.tryAcquireLock(lock1);
    }

    @Test
    public void testInitializeWithException() {
        final HBaseConnectionStub connectionSpy = Mockito.spy(connection);
        Mockito.doThrow(RuntimeException.class)
                .when(connectionSpy)
                .getAdmin();
        lockManager = DistributedLockManager.builder()
                .clientId("CLIENT_ID")
                .lockBase(LockBase.builder()
                        .mode(LockMode.EXCLUSIVE)
                        .lockStore(HBaseStore.builder()
                                .connection(connectionSpy)
                                .tableName("dlm_locks")
                                .build())
                        .build())
                .build();
        exceptionThrown.expect(DLMExceptionMatcher.hasCode(ErrorCode.TABLE_CREATION_ERROR));
        lockManager.initialize();
    }

    @Test
    public void testCloseWithException() {
        final HBaseConnectionStub connectionSpy = Mockito.spy(connection);
        Mockito.doThrow(RuntimeException.class)
                .when(connectionSpy)
                .close();
        lockManager = DistributedLockManager.builder()
                .clientId("CLIENT_ID")
                .lockBase(LockBase.builder()
                        .mode(LockMode.EXCLUSIVE)
                        .lockStore(HBaseStore.builder()
                                .connection(connectionSpy)
                                .tableName("dlm_locks")
                                .build())
                        .build())
                .build();

        try {
            lockManager.destroy();
            Assert.fail("Should have thrown an exception");
        } catch (DLMException e) {
            Assert.assertEquals(ErrorCode.INTERNAL_ERROR, e.getErrorCode());
            // revert the behavior
            Mockito.doNothing()
                    .when(connectionSpy)
                    .close();
        }
    }

    @Test
    public void concurrentLockAttempt() {
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
                    log.warn("Gracefully ignoring exception", e);
                } finally {
                    boolean result = lockManager.releaseLock(lock);
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

}
