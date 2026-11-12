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

package com.phonepe.dlm.lock.base;

import com.phonepe.dlm.exception.DLMException;
import com.phonepe.dlm.exception.ErrorCode;
import com.phonepe.dlm.lock.Lock;
import com.phonepe.dlm.lock.level.LockLevel;
import com.phonepe.dlm.lock.mode.LockMode;
import com.phonepe.dlm.lock.storage.ILockStore;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link LockBase} acquisition stamping and ownership-checked release.
 */
class LockBaseTest {
    private static final String LOCK_ID = "LOCK_ID";
    private static final String FARM_ID = "FA1";
    private static final Duration TTL = Duration.ofSeconds(30);

    private final ILockStore store = mock(ILockStore.class);
    private final LockBase lockBase = LockBase.builder()
            .mode(LockMode.EXCLUSIVE)
            .lockStore(store)
            .build();

    @Test
    void acquireStampsLockWithTheTimestampWrittenToTheStore() {
        final Lock lock = newLock();
        final long before = System.currentTimeMillis();

        lockBase.tryAcquireLock(lock, TTL);

        final ArgumentCaptor<Long> acquiredAt = ArgumentCaptor.forClass(Long.class);
        verify(store).write(eq(LOCK_ID), eq(LockLevel.DC), eq(FARM_ID), eq(TTL), acquiredAt.capture());
        assertTrue(lock.getAcquiredStatus().get());
        assertEquals(acquiredAt.getValue(), lock.getAcquiredAt().get());
        assertTrue(acquiredAt.getValue() >= before);
    }

    @Test
    void failedAcquireLeavesLockUnacquired() {
        final Lock lock = newLock();
        doThrow(DLMException.builder().errorCode(ErrorCode.LOCK_UNAVAILABLE).build())
                .when(store).write(any(), any(), any(), any(), anyLong());

        assertThrows(DLMException.class, () -> lockBase.tryAcquireLock(lock, TTL));

        assertFalse(lock.getAcquiredStatus().get());
        assertEquals(0L, lock.getAcquiredAt().get());
    }

    @Test
    void releasePassesAcquisitionTimestampAndReturnsTrueWhenOwned() {
        final Lock lock = newLock();
        lockBase.tryAcquireLock(lock, TTL);
        final long acquiredAt = lock.getAcquiredAt().get();
        when(store.remove(LOCK_ID, LockLevel.DC, FARM_ID, acquiredAt)).thenReturn(true);

        assertTrue(lockBase.releaseLock(lock));

        verify(store).remove(LOCK_ID, LockLevel.DC, FARM_ID, acquiredAt);
        assertFalse(lock.getAcquiredStatus().get());
    }

    @Test
    void releaseReturnsFalseAndClearsStatusWhenLockNoLongerOwned() {
        final Lock lock = newLock();
        lockBase.tryAcquireLock(lock, TTL);
        // Lock expired and was re-acquired by another holder: the store refuses to remove it.
        when(store.remove(any(), any(), any(), anyLong())).thenReturn(false);

        assertFalse(lockBase.releaseLock(lock));

        assertFalse(lock.getAcquiredStatus().get());
    }

    @Test
    void releaseOfNeverAcquiredLockDoesNotTouchTheStore() {
        assertFalse(lockBase.releaseLock(newLock()));

        verifyNoInteractions(store);
    }

    @Test
    void releaseFailureKeepsLockAcquiredSoReleaseCanBeRetried() {
        final Lock lock = newLock();
        lockBase.tryAcquireLock(lock, TTL);
        final long acquiredAt = lock.getAcquiredAt().get();
        when(store.remove(LOCK_ID, LockLevel.DC, FARM_ID, acquiredAt))
                .thenThrow(DLMException.builder().errorCode(ErrorCode.CONNECTION_ERROR).build())
                .thenReturn(true);

        assertThrows(DLMException.class, () -> lockBase.releaseLock(lock));
        assertTrue(lock.getAcquiredStatus().get());

        assertTrue(lockBase.releaseLock(lock));
        assertFalse(lock.getAcquiredStatus().get());
    }

    @Test
    void reacquiringSameInstanceStampsANewTimestamp() throws InterruptedException {
        final Lock lock = newLock();
        when(store.remove(any(), any(), any(), anyLong())).thenReturn(true);
        lockBase.tryAcquireLock(lock, TTL);
        final long firstAcquiredAt = lock.getAcquiredAt().get();
        lockBase.releaseLock(lock);

        Thread.sleep(2);
        lockBase.tryAcquireLock(lock, TTL);

        assertTrue(lock.getAcquiredAt().get() > firstAcquiredAt);
        lockBase.releaseLock(lock);
        verify(store).remove(LOCK_ID, LockLevel.DC, FARM_ID, lock.getAcquiredAt().get());
    }

    private static Lock newLock() {
        return Lock.builder()
                .lockId(LOCK_ID)
                .lockLevel(LockLevel.DC)
                .farmId(FARM_ID)
                .build();
    }
}
