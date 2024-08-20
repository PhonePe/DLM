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
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@AllArgsConstructor
@Builder
@Getter
public class LockBase implements ILockable {
    public static final Duration DEFAULT_LOCK_TTL_SECONDS = Duration.ofSeconds(90);
    public static final Duration DEFAULT_WAIT_FOR_LOCK_IN_SECONDS = Duration.ofSeconds(90);
    public static final int WAIT_TIME_FOR_NEXT_RETRY = 1000; // 1 second

    private final ILockStore lockStore;
    private final LockMode mode; // Not implemented now, but can be leveraged in the future.

    @Override
    public void tryAcquireLock(final Lock lock) {
        tryAcquireLock(lock, DEFAULT_LOCK_TTL_SECONDS);
    }

    @Override
    public void tryAcquireLock(final Lock lock, final Duration duration) {
        writeToStore(lock, duration);
    }

    @Override
    public void acquireLock(final Lock lock) {
        acquireLock(lock, DEFAULT_LOCK_TTL_SECONDS, DEFAULT_WAIT_FOR_LOCK_IN_SECONDS);
    }

    @Override
    public void acquireLock(final Lock lock, final Duration duration) {
        acquireLock(lock, duration, DEFAULT_WAIT_FOR_LOCK_IN_SECONDS);
    }

    @SneakyThrows
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
                    Thread.sleep(WAIT_TIME_FOR_NEXT_RETRY);
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
}
