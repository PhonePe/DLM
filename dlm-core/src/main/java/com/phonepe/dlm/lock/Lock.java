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

package com.phonepe.dlm.lock;

import com.phonepe.dlm.lock.level.LockLevel;
import lombok.Builder;
import lombok.Getter;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@Getter
public class Lock {
    private final String lockId;
    private final LockLevel lockLevel;
    private final String farmId;
    private final AtomicBoolean acquiredStatus = new AtomicBoolean();
    /**
     * Epoch millis at which this lock instance last acquired the lock. It is written to the store on acquisition and
     * used on release to verify that the stored lock still belongs to this instance (i.e. it has not expired and been
     * re-acquired by another holder). Meaningful only while {@link #acquiredStatus} is {@code true}.
     */
    private final AtomicLong acquiredAt = new AtomicLong();

    @Builder
    public Lock(final String lockId, final LockLevel lockLevel, final String farmId) {
        this.lockId = lockId;
        this.lockLevel = lockLevel;
        this.farmId = farmId;
    }
}
