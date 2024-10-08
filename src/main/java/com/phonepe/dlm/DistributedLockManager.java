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

import com.phonepe.dlm.exception.ErrorCode;
import com.phonepe.dlm.lock.Lock;
import com.phonepe.dlm.lock.base.LockBase;
import com.phonepe.dlm.lock.level.LockLevel;

import java.time.Duration;

import com.phonepe.dlm.exception.DLMException;
import lombok.AllArgsConstructor;
import lombok.Builder;

@Builder
@AllArgsConstructor
public class DistributedLockManager {
    private final String clientId;
    private final String farmId;
    private final LockBase lockBase;

    public void initialize() {
        lockBase.getLockStore().initialize();
    }

    /**
     * This method attempts to acquire the lock immediately and throws exception if lock is unavailable
     * It does not wait if the lock is currently held by another thread.
     * <p>
     * The lock will be acquired for default time period {@link LockBase#DEFAULT_LOCK_TTL_SECONDS}
     *
     * @param lock The lock to be acquired.
     * @throws DLMException with {@link ErrorCode#LOCK_UNAVAILABLE} if lock is already acquired
     */
    public void tryAcquireLock(final Lock lock) {
        lockBase.tryAcquireLock(lock);
    }

    /**
     * This method attempts to acquire the lock immediately and throws exception if lock is unavailable
     * It does not wait if the lock is currently held by another thread.
     *
     * @param lock     The lock to be acquired
     * @param duration The lock duration in seconds for which lock will be held
     * @throws DLMException with {@link ErrorCode#LOCK_UNAVAILABLE} if lock is already acquired
     */
    public void tryAcquireLock(final Lock lock, final Duration duration) {
        lockBase.tryAcquireLock(lock, duration);
    }

    /**
     * This method tries to acquire the lock, and if the lock is currently held by another thread,
     * it will wait until the lock becomes available.
     * It blocks the thread until the lock is acquired.
     * <p>
     * By default, timeout is {@link LockBase#DEFAULT_WAIT_FOR_LOCK_IN_SECONDS}
     * The lock will be acquired for default time period {@link LockBase#DEFAULT_LOCK_TTL_SECONDS}
     *
     * @param lock The lock to be acquired.
     * @throws DLMException with {@link ErrorCode#LOCK_UNAVAILABLE} if lock is not available even after the timeout
     */
    public void acquireLock(final Lock lock) {
        lockBase.acquireLock(lock);
    }

    /**
     * This method tries to acquire the lock, and if the lock is currently held by another thread,
     * it will wait until the lock becomes available.
     * It blocks the thread until the lock is acquired.
     * <p>
     * By default, timeout is {@link LockBase#DEFAULT_WAIT_FOR_LOCK_IN_SECONDS}
     *
     * @param lock     The lock to be acquired.
     * @param duration The lock duration in seconds for which lock will be held
     * @throws DLMException with {@link ErrorCode#LOCK_UNAVAILABLE} if lock is not available even after the timeout
     */
    public void acquireLock(final Lock lock, final Duration duration) {
        lockBase.acquireLock(lock, duration);
    }

    /**
     * This method attempts to acquire the lock and waits for a limited time for the lock to become available.
     * If the lock is not acquired within the given time, it returns with a failure indication.
     * <p>
     * It blocks the thread until the lock is acquired.
     *
     * @param lock     The lock to be acquired.
     * @param duration The lock duration in seconds for which lock will be held
     * @param timeout  The timeout(wait duration in seconds) for a lock to become available
     * @throws DLMException with {@link ErrorCode#LOCK_UNAVAILABLE} if lock is not available even after the timeout
     */
    public void acquireLock(final Lock lock, final Duration duration, final Duration timeout) {
        lockBase.acquireLock(lock, duration, timeout);
    }

    /**
     * This method releases the acquired lock, allowing other threads to acquire it.
     *
     * @param lock The lock which is acquired
     * @return true if the lock is successfully released, otherwise false
     */
    public boolean releaseLock(final Lock lock) {
        return lockBase.releaseLock(lock);
    }

    /**
     * Method to get lock instance
     *
     * @param lockId    - Identifier on which lock needs to be acquired/released
     * @param lockLevel - DC or XDC (Cross data center)
     * @return LockInstance
     */
    public Lock getLockInstance(final String lockId, final LockLevel lockLevel) {
        return Lock.builder()
                .lockId(String.format("%s#%s", clientId, lockId))
                .lockLevel(lockLevel)
                .farmId(farmId)
                .build();
    }

    public void destroy() {
        lockBase.getLockStore().close();
    }
}
