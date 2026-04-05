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

import com.phonepe.dlm.DistributedLockManager;
import com.phonepe.dlm.lock.mode.LockMode;
import com.phonepe.dlm.lock.storage.ILockStore;
import org.junit.Test;
import org.mockito.Mockito;

import java.time.Duration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Unit tests for {@link LockConfiguration}.
 *
 * <p>Covers:
 * <ul>
 *   <li>Default values match the documented library constants</li>
 *   <li>Custom {@link Duration} values are applied correctly</li>
 *   <li>Partial configuration uses defaults for unset fields</li>
 *   <li>{@link LockBase} builder remains backward compatible when no configuration is provided</li>
 *   <li>{@link LockBase} honours a supplied {@link LockConfiguration}</li>
 * </ul>
 */
public class LockConfigurationTest {

    // -------------------------------------------------------------------------
    // Default value tests
    // -------------------------------------------------------------------------

    @Test
    public void defaultBuildYieldsLibraryDefaults() {
        final LockConfiguration config = LockConfiguration.builder().build();

        assertEquals(LockConfiguration.DEFAULT_LOCK_TTL, config.getLockTtl());
        assertEquals(LockConfiguration.DEFAULT_WAIT_FOR_LOCK, config.getWaitForLock());
        assertEquals(LockConfiguration.DEFAULT_RETRY_INTERVAL, config.getRetryInterval());
    }

    @Test
    public void defaultConstantsHaveExpectedValues() {
        // Guard against accidental drift of the library defaults.
        assertEquals(Duration.ofSeconds(90), LockConfiguration.DEFAULT_LOCK_TTL);
        assertEquals(Duration.ofSeconds(90), LockConfiguration.DEFAULT_WAIT_FOR_LOCK);
        assertEquals(Duration.ofMillis(1_000), LockConfiguration.DEFAULT_RETRY_INTERVAL);
    }

    // -------------------------------------------------------------------------
    // Custom value tests
    // -------------------------------------------------------------------------

    @Test
    public void customDurationValuesAreApplied() {
        final LockConfiguration config = LockConfiguration.builder()
                .lockTtl(Duration.ofSeconds(30))
                .waitForLock(Duration.ofSeconds(15))
                .retryInterval(Duration.ofMillis(500))
                .build();

        assertEquals(Duration.ofSeconds(30), config.getLockTtl());
        assertEquals(Duration.ofSeconds(15), config.getWaitForLock());
        assertEquals(Duration.ofMillis(500), config.getRetryInterval());
    }

    @Test
    public void partialConfigFallsBackToDefaultsForUnsetFields() {
        // Only lockTtl is set; the other two should fall back to library defaults.
        final LockConfiguration config = LockConfiguration.builder()
                .lockTtl(Duration.ofSeconds(45))
                .build();

        assertEquals(Duration.ofSeconds(45), config.getLockTtl());
        assertEquals(LockConfiguration.DEFAULT_WAIT_FOR_LOCK, config.getWaitForLock());
        assertEquals(LockConfiguration.DEFAULT_RETRY_INTERVAL, config.getRetryInterval());
    }

    // -------------------------------------------------------------------------
    // Backward-compatibility: LockBase builder without lockConfiguration
    // -------------------------------------------------------------------------

    @Test
    public void lockBaseBuilderBackwardCompatibleWithoutConfiguration() {
        final ILockStore mockStore = Mockito.mock(ILockStore.class);

        final LockBase lockBase = LockBase.builder()
                .mode(LockMode.EXCLUSIVE)
                .lockStore(mockStore)
                .build();

        assertNotNull(lockBase.getLockConfiguration());
        assertEquals(LockConfiguration.DEFAULT_LOCK_TTL, lockBase.getLockConfiguration().getLockTtl());
        assertEquals(LockConfiguration.DEFAULT_WAIT_FOR_LOCK, lockBase.getLockConfiguration().getWaitForLock());
        assertEquals(LockConfiguration.DEFAULT_RETRY_INTERVAL, lockBase.getLockConfiguration().getRetryInterval());
    }

    @Test
    public void lockBaseHonoursCustomConfiguration() {
        final ILockStore mockStore = Mockito.mock(ILockStore.class);
        final LockConfiguration custom = LockConfiguration.builder()
                .lockTtl(Duration.ofSeconds(20))
                .waitForLock(Duration.ofSeconds(5))
                .retryInterval(Duration.ofMillis(250))
                .build();

        final LockBase lockBase = LockBase.builder()
                .mode(LockMode.EXCLUSIVE)
                .lockStore(mockStore)
                .lockConfiguration(custom)
                .build();

        assertEquals(Duration.ofSeconds(20), lockBase.getLockConfiguration().getLockTtl());
        assertEquals(Duration.ofSeconds(5), lockBase.getLockConfiguration().getWaitForLock());
        assertEquals(Duration.ofMillis(250), lockBase.getLockConfiguration().getRetryInterval());
    }

    // -------------------------------------------------------------------------
    // DistributedLockManager builder backward compatibility
    // -------------------------------------------------------------------------

    @Test
    public void distributedLockManagerBuilderBackwardCompatible() {
        final ILockStore mockStore = Mockito.mock(ILockStore.class);

        final DistributedLockManager manager = DistributedLockManager.builder()
                .clientId("CLIENT_ID")
                .farmId("FA1")
                .lockBase(LockBase.builder()
                        .mode(LockMode.EXCLUSIVE)
                        .lockStore(mockStore)
                        .build())
                .build();

        assertNotNull(manager);
    }
}
