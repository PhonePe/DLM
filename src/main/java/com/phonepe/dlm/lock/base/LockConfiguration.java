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

import lombok.Builder;
import lombok.Getter;

import java.time.Duration;

/**
 * Immutable configuration encapsulating all timing parameters that govern lock-acquisition
 * behaviour in the Distributed Lock Manager.
 *
 * <p>All parameters are expressed as {@link Duration} values. Any parameter left unset in the
 * builder falls back to its corresponding library default via {@code @Builder.Default}, preserving
 * full backward compatibility for callers that do not supply a {@code LockConfiguration}.
 *
 * <h2>Default values</h2>
 * <ul>
 *   <li><b>lockTtl</b> ({@link #DEFAULT_LOCK_TTL}) — how long the lock is held before the
 *       storage layer expires it automatically.</li>
 *   <li><b>waitForLock</b> ({@link #DEFAULT_WAIT_FOR_LOCK}) — maximum time a caller blocks
 *       waiting for a contended lock.</li>
 *   <li><b>retryInterval</b> ({@link #DEFAULT_RETRY_INTERVAL}) — polling interval between
 *       successive acquisition attempts when a lock is unavailable.</li>
 * </ul>
 *
 * <h2>Usage</h2>
 * <pre>{@code
 * // Use library defaults — identical behaviour to omitting lockConfiguration from the builder.
 * LockConfiguration defaults = LockConfiguration.builder().build();
 *
 * // Custom configuration for a service with tighter SLOs.
 * LockConfiguration custom = LockConfiguration.builder()
 *         .lockTtl(Duration.ofSeconds(30))
 *         .waitForLock(Duration.ofSeconds(10))
 *         .retryInterval(Duration.ofMillis(500))
 *         .build();
 *
 * DistributedLockManager lockManager = DistributedLockManager.builder()
 *         .clientId("MY_SERVICE")
 *         .farmId("MHX")
 *         .lockBase(LockBase.builder()
 *                 .mode(LockMode.EXCLUSIVE)
 *                 .lockConfiguration(custom)
 *                 .lockStore(aerospikeStore)
 *                 .build())
 *         .build();
 * }</pre>
 *
 * <p>Instances of this class are <em>immutable</em> and therefore safe for concurrent use without
 * external synchronisation.
 */
@Getter
@Builder
public final class LockConfiguration {

    /**
     * Default lock time-to-live: 90 seconds.
     * <p>
     * The lock is held for this duration before the storage layer expires it automatically,
     * protecting against deadlocks caused by holders that crash or fail to release the lock.
     */
    public static final Duration DEFAULT_LOCK_TTL = Duration.ofSeconds(90);

    /**
     * Default maximum wait time for lock acquisition: 90 seconds.
     * <p>
     * When a caller invokes a blocking {@code acquireLock} variant without specifying a timeout,
     * the library retries for at most this duration before throwing a
     * {@link com.phonepe.dlm.exception.DLMException} with
     * {@link com.phonepe.dlm.exception.ErrorCode#LOCK_UNAVAILABLE}.
     */
    public static final Duration DEFAULT_WAIT_FOR_LOCK = Duration.ofSeconds(90);

    /**
     * Default polling interval between successive lock-acquisition retries: 1 second.
     * <p>
     * When a lock is unavailable, the library sleeps for this duration before the next attempt.
     * Tuning this value trades off CPU/network overhead against acquisition latency under
     * contention.
     */
    public static final Duration DEFAULT_RETRY_INTERVAL = Duration.ofMillis(1_000);

    /**
     * The duration for which a successfully acquired lock is held before automatic expiry.
     */
    @Builder.Default
    private final Duration lockTtl = DEFAULT_LOCK_TTL;

    /**
     * The maximum duration a blocking {@code acquireLock} call will wait for a contended lock.
     */
    @Builder.Default
    private final Duration waitForLock = DEFAULT_WAIT_FOR_LOCK;

    /**
     * The sleep duration between successive acquisition attempts when a lock is unavailable.
     */
    @Builder.Default
    private final Duration retryInterval = DEFAULT_RETRY_INTERVAL;
}
