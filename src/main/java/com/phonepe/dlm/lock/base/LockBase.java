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
import com.phonepe.dlm.lock.ILockable;
import com.phonepe.dlm.lock.Lock;
import com.phonepe.dlm.lock.mode.LockMode;
import com.phonepe.dlm.lock.storage.ILockStore;
import com.phonepe.dlm.utils.Timer;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Core implementation of the distributed locking contract defined by {@link ILockable}.
 *
 * <p>{@code LockBase} orchestrates lock acquisition and release by delegating storage operations
 * to the configured {@link ILockStore}. Timing behaviour — TTL, wait timeout, and retry interval —
 * is governed by the {@link LockConfiguration} supplied at construction time. When no configuration
 * is provided the library falls back to {@link LockConfiguration}'s built-in defaults
 * ({@link LockConfiguration#DEFAULT_LOCK_TTL},
 * {@link LockConfiguration#DEFAULT_WAIT_FOR_LOCK},
 * {@link LockConfiguration#DEFAULT_SLEEP_BETWEEN_RETRIES}), preserving full backward
 * compatibility.
 *
 * <h2>Typical construction</h2>
 * <pre>{@code
 * // Backward-compatible: omitting lockConfiguration uses library defaults.
 * LockBase lockBase = LockBase.builder()
 *         .mode(LockMode.EXCLUSIVE)
 *         .lockStore(aerospikeStore)
 *         .build();
 *
 * // Custom timing for a service with tighter SLOs.
 * LockBase lockBase = LockBase.builder()
 *         .mode(LockMode.EXCLUSIVE)
 *         .lockStore(aerospikeStore)
 *         .lockConfiguration(LockConfiguration.builder()
 *         .lockTtl(Duration.ofSeconds(30))
 *         .waitForLock(Duration.ofSeconds(10))
 *         .sleepBetweenRetries(Duration.ofMillis(500))
 *         .build())
 *         .build();
 * }</pre>
 *
 * <p>This class is thread-safe provided the supplied {@link ILockStore} is also thread-safe.
 */
@Slf4j
@Getter
@Builder
@AllArgsConstructor
public class LockBase implements ILockable {

    /**
     * The storage backend used to persist and remove lock records.
     */
    private final ILockStore lockStore;

    /**
     * The locking mode (e.g. {@link LockMode#EXCLUSIVE}).
     * Not actively enforced today but reserved for future multi-mode support.
     */
    private final LockMode mode;

    /**
     * Timing configuration for this lock base instance.
     * <p>
     * When not set via the builder, defaults to {@link LockConfiguration#builder() build()},
     * which applies the library-standard defaults (90 s TTL, 90 s wait, 1 000 ms retry).
     */
    @Builder.Default
    private final LockConfiguration lockConfiguration = LockConfiguration.builder().build();

    @Override
    public void tryAcquireLock(final Lock lock) {
        tryAcquireLock(lock, lockConfiguration.getLockTtl());
    }

    @Override
    public void tryAcquireLock(final Lock lock, final Duration duration) {
        writeToStore(lock, duration);
    }

    @Override
    public void acquireLock(final Lock lock) {
        acquireLock(lock, lockConfiguration.getLockTtl(), lockConfiguration.getWaitForLock());
    }

    @Override
    public void acquireLock(final Lock lock, final Duration duration) {
        acquireLock(lock, duration, lockConfiguration.getWaitForLock());
    }

    @Override
    public void acquireLock(final Lock lock, final Duration duration, final Duration timeout) {
        final Timer timer = new Timer(System.currentTimeMillis(), timeout.getSeconds());
        final AtomicBoolean success = new AtomicBoolean(false);
        do {
            try {
                writeToStore(lock, duration);
                success.set(true);
            } catch (DLMException e) {
                if (timer.isExpired()) {
                    log.error("Lock wait time {}secs is over, lock is still not available", timeout);
                    throw e;
                }
                if (e.getErrorCode() == ErrorCode.LOCK_UNAVAILABLE) {
                    sleep(lockConfiguration.getSleepBetweenRetries());
                    continue;
                }
                throw e;
            }
        } while (!success.get());
    }

    @Override
    public boolean releaseLock(final Lock lock) {
        if (lock.getAcquiredStatus().get()) {
            lockStore.remove(lock.getLockId(), lock.getLockLevel(), lock.getFarmId());
            lock.getAcquiredStatus().compareAndSet(true, false);
            return !lock.getAcquiredStatus().get();
        }
        return false;
    }

    private void writeToStore(final Lock lock, final Duration ttlSeconds) {
        lockStore.write(lock.getLockId(), lock.getLockLevel(), lock.getFarmId(), ttlSeconds);
        lock.getAcquiredStatus().compareAndSet(false, true);
    }

    /**
     * Sleeps for the configured retry interval before the next acquisition attempt.
     *
     * @param sleepBetweenRetries the duration to sleep
     * @throws DLMException wrapping {@link InterruptedException} if the thread is interrupted
     *                      while sleeping, with the interrupt status restored on the current thread
     */
    private static void sleep(final Duration sleepBetweenRetries) {
        try {
            Thread.sleep(sleepBetweenRetries.toMillis());
        } catch (InterruptedException e) {
            log.error("Error sleeping the thread", e);
            Thread.currentThread().interrupt();
            throw DLMException.propagate(e);
        }
    }
}
