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

package com.phonepe.dlm.lock.storage;

import java.time.Duration;

import com.phonepe.dlm.lock.level.LockLevel;

/**
 * Storage backend for lock records. A lock is held while its record exists; records expire after their TTL.
 * <p>
 * Implementations must make {@link #write(String, LockLevel, String, Duration, long)} create-only (fail with
 * {@code LOCK_UNAVAILABLE} if the record already exists) and {@link #remove(String, LockLevel, String, long)} an
 * atomic "delete only if it still carries {@code acquiredAt}", so that a holder whose lock expired cannot delete a lock
 * re-acquired by another holder.
 */
public interface ILockStore {
    /**
     * Prepares the backend (e.g. creates tables) before first use.
     */
    void initialize();

    /**
     * Writes the lock record, stamped with {@code acquiredAt}, only if it does not already exist (create-only).
     *
     * @param lockId     the lock identifier
     * @param lockLevel  the lock level ({@code DC} or {@code XDC}), which determines where the record is stored
     * @param farmId     the farm (data centre) of the caller
     * @param ttlSeconds how long the lock record lives before the store expires it
     * @param acquiredAt epoch millis identifying this acquisition; must be passed back to
     *                   {@link #remove(String, LockLevel, String, long)}
     * @throws com.phonepe.dlm.exception.DLMException with {@code LOCK_UNAVAILABLE} if the lock is already held
     */
    void write(String lockId, LockLevel lockLevel, String farmId, Duration ttlSeconds, long acquiredAt);

    /**
     * Atomically removes the lock record only if it still carries {@code acquiredAt}, i.e. it is still held by the
     * caller and has not expired and been re-acquired by someone else.
     *
     * @param lockId     the lock identifier
     * @param lockLevel  the lock level ({@code DC} or {@code XDC}), which determines where the record is stored
     * @param farmId     the farm (data centre) of the caller
     * @param acquiredAt the value passed to {@link #write(String, LockLevel, String, Duration, long)} on acquisition
     * @return {@code true} if the caller's lock record was removed; {@code false} if it no longer exists or belongs to
     * another holder. A store that retains expired records until a later sweep may return {@code true} for the
     * caller's own expired, not-yet-swept record; it must never remove a record carrying a different stamp.
     */
    boolean remove(String lockId, LockLevel lockLevel, String farmId, long acquiredAt);

    /**
     * Releases backend resources such as connections.
     */
    void close();
}
