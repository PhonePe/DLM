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

package com.phonepe.dlm.hbase;

import com.phonepe.dlm.DistributedLockManager;
import com.phonepe.dlm.exception.DLMException;
import com.phonepe.dlm.exception.ErrorCode;
import com.phonepe.dlm.lock.Lock;
import com.phonepe.dlm.lock.base.LockBase;
import com.phonepe.dlm.lock.level.LockLevel;
import com.phonepe.dlm.lock.mode.LockMode;
import com.phonepe.dlm.hbase.storage.HBaseStore;
import com.phonepe.dlm.hbase.storage.HBaseConnectionStub;
import com.phonepe.dlm.hbase.utils.TestUtils;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

@Slf4j
class DistributedLockWithHBaseTest {
    private DistributedLockManager lockManager;
    private HBaseConnectionStub connection;

    @BeforeEach
    void setUp() {
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

    @AfterEach
    void destroy() {
        lockManager.destroy();
    }

    @Test
    void lockTestPositiveSiloDC() {
        final Lock lock = lockManager.getLockInstance("LOCK_ID", LockLevel.DC);
        lockManager.tryAcquireLock(lock);
        Assertions.assertTrue(lock.getAcquiredStatus().get());

        boolean released = lockManager.releaseLock(lock);
        Assertions.assertTrue(released);
        Assertions.assertFalse(lock.getAcquiredStatus().get());

        // Attempt to release it again
        released = lockManager.releaseLock(lock);
        Assertions.assertFalse(released);
    }

    @Test
    void lockTestPositiveXDC() {
        final Lock lock = lockManager.getLockInstance("LOCK_ID", LockLevel.XDC);
        lockManager.tryAcquireLock(lock);
        Assertions.assertTrue(lock.getAcquiredStatus().get());

        boolean released = lockManager.releaseLock(lock);
        Assertions.assertTrue(released);
        Assertions.assertFalse(lock.getAcquiredStatus().get());

        // Attempt to release it again
        released = lockManager.releaseLock(lock);
        Assertions.assertFalse(released);
    }

    @Test
    void lockTestNegative1() {
        final Lock lock = lockManager.getLockInstance("LOCK_ID", LockLevel.DC);
        lockManager.tryAcquireLock(lock);
        Assertions.assertTrue(lock.getAcquiredStatus().get());

        final DLMException exception = assertThrows(DLMException.class, () -> lockManager.tryAcquireLock(lock));
        assertEquals(ErrorCode.LOCK_UNAVAILABLE, exception.getErrorCode());
    }

    @Test
    void lockTestNegative2() {
        Lock lock = lockManager.getLockInstance("LOCK_ID", LockLevel.DC);
        lockManager.tryAcquireLock(lock);

        final Lock contendingLock = lockManager.getLockInstance("LOCK_ID", LockLevel.DC);
        final DLMException exception = assertThrows(DLMException.class,
                () -> lockManager.tryAcquireLock(contendingLock));
        assertEquals(ErrorCode.LOCK_UNAVAILABLE, exception.getErrorCode());
    }

    @Test
    void lockTestNegative3() {
        final Lock lock = lockManager.getLockInstance("LOCK_ID", LockLevel.DC);
        lockManager.tryAcquireLock(lock);

        final Lock lock1 = lockManager.getLockInstance("LOCK_ID", LockLevel.DC);
        final DLMException exception = assertThrows(DLMException.class, () -> lockManager.tryAcquireLock(lock1));
        assertEquals(ErrorCode.LOCK_UNAVAILABLE, exception.getErrorCode());
    }

    @Test
    void testInitializeWithException() {
        final HBaseConnectionStub connectionSpy = spy(connection);
        doThrow(RuntimeException.class)
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
        final DLMException exception = assertThrows(DLMException.class, lockManager::initialize);
        assertEquals(ErrorCode.TABLE_CREATION_ERROR, exception.getErrorCode());
    }

    @Test
    void testCloseWithException() {
        final HBaseConnectionStub connectionSpy = spy(connection);
        doThrow(RuntimeException.class)
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

        final DLMException exception = assertThrows(DLMException.class, lockManager::destroy);
        assertEquals(ErrorCode.INTERNAL_ERROR, exception.getErrorCode());
        doNothing().when(connectionSpy).close();
    }

    @Test
    void concurrentLockAttempt() {
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
