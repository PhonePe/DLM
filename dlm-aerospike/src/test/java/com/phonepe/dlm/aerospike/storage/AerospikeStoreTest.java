/**
 * Copyright (c) 2024 Original Author(s), PhonePe India Pvt. Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package com.phonepe.dlm.aerospike.storage;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.ResultCode;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.policy.WritePolicy;
import com.phonepe.dlm.aerospike.utils.AerospikeUtils;
import com.phonepe.dlm.exception.DLMException;
import com.phonepe.dlm.exception.ErrorCode;
import com.phonepe.dlm.lock.level.LockLevel;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AerospikeStoreTest {
    private static final long ACQUIRED_AT = 1_700_000_000_123L;
    private final IAerospikeClient client = mock(IAerospikeClient.class);
    private final AerospikeStore store = AerospikeStore.builder()
            .aerospikeClient(client)
            .namespace("namespace")
            .setSuffix("locks")
            .build();

    @Test
    void wrapsUnexpectedClientFailure() {
        when(client.getWritePolicyDefault()).thenThrow(new IllegalStateException("client unavailable"));

        final DLMException exception = assertThrows(DLMException.class,
                () -> store.write("lock", LockLevel.DC, "farm", Duration.ofSeconds(30), ACQUIRED_AT));

        assertEquals(ErrorCode.CONNECTION_ERROR, exception.getErrorCode());
    }

    @Test
    void wrapsInterruptedRetryAndRestoresInterruptStatus() {
        Thread.currentThread().interrupt();
        try {
            final DLMException exception = assertThrows(DLMException.class,
                    () -> AerospikeUtils.retry(() -> {
                        throw new AerospikeException("retry");
                    }));

            assertEquals(ErrorCode.INTERNAL_ERROR, exception.getErrorCode());
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void writeStoresAcquisitionTimestampInModifiedAtBin() {
        when(client.getWritePolicyDefault()).thenReturn(new WritePolicy());

        store.write("lock", LockLevel.DC, "farm", Duration.ofSeconds(30), ACQUIRED_AT);

        final ArgumentCaptor<Bin> bins = ArgumentCaptor.forClass(Bin.class);
        verify(client).put(any(WritePolicy.class), any(Key.class), bins.capture());
        assertTrue(bins.getAllValues().contains(new Bin("farm##uat", ACQUIRED_AT)));
    }

    @Test
    void ownerCheckedRemoveFiltersOnAcquisitionTimestampAndReturnsTrueWhenDeleted() {
        when(client.getWritePolicyDefault()).thenReturn(new WritePolicy());
        when(client.delete(any(WritePolicy.class), any(Key.class))).thenReturn(true);

        assertTrue(store.remove("lock", LockLevel.DC, "farm", ACQUIRED_AT));

        final ArgumentCaptor<WritePolicy> policy = ArgumentCaptor.forClass(WritePolicy.class);
        verify(client).delete(policy.capture(), any(Key.class));
        assertEquals(Exp.build(Exp.eq(Exp.intBin("farm##uat"), Exp.val(ACQUIRED_AT))), policy.getValue().filterExp);
        assertTrue(policy.getValue().failOnFilteredOut);
    }

    @Test
    void ownerCheckedRemoveReturnsFalseWithoutRetryWhenLockBelongsToAnotherHolder() {
        when(client.getWritePolicyDefault()).thenReturn(new WritePolicy());
        when(client.delete(any(WritePolicy.class), any(Key.class)))
                .thenThrow(new AerospikeException(ResultCode.FILTERED_OUT));

        assertFalse(store.remove("lock", LockLevel.DC, "farm", ACQUIRED_AT));

        verify(client, times(1)).delete(any(WritePolicy.class), any(Key.class));
    }

    @Test
    void ownerCheckedRemoveReturnsFalseWhenRecordNotFound() {
        when(client.getWritePolicyDefault()).thenReturn(new WritePolicy());
        when(client.delete(any(WritePolicy.class), any(Key.class))).thenReturn(false);

        assertFalse(store.remove("lock", LockLevel.DC, "farm", ACQUIRED_AT));
    }

    @Test
    void ownerCheckedRemoveWrapsUnexpectedClientFailure() {
        when(client.getWritePolicyDefault()).thenThrow(new IllegalStateException("client unavailable"));

        final DLMException exception = assertThrows(DLMException.class,
                () -> store.remove("lock", LockLevel.DC, "farm", ACQUIRED_AT));

        assertEquals(ErrorCode.CONNECTION_ERROR, exception.getErrorCode());
    }

    @Test
    void ownerCheckedRemoveRetriesOtherFailuresThenFails() {
        when(client.getWritePolicyDefault()).thenReturn(new WritePolicy());
        when(client.delete(any(WritePolicy.class), any(Key.class)))
                .thenThrow(new AerospikeException(ResultCode.TIMEOUT));

        final DLMException exception = assertThrows(DLMException.class,
                () -> store.remove("lock", LockLevel.DC, "farm", ACQUIRED_AT));

        assertEquals(ErrorCode.RETRIES_EXHAUSTED, exception.getErrorCode());
        verify(client, times(5)).delete(any(WritePolicy.class), any(Key.class));
    }

    @Test
    void writeWrapsNonLockUnavailableFailureAsConnectionError() {
        when(client.getWritePolicyDefault()).thenReturn(new WritePolicy());
        org.mockito.Mockito.doThrow(new AerospikeException(ResultCode.TIMEOUT))
                .when(client).put(any(WritePolicy.class), any(Key.class), any(Bin.class), any(Bin.class));
        Thread.currentThread().interrupt(); // retry sleep is interrupted -> DLMException(INTERNAL_ERROR) from retry
        try {
            final DLMException exception = assertThrows(DLMException.class,
                    () -> store.write("lock", LockLevel.DC, "farm", Duration.ofSeconds(30), ACQUIRED_AT));
            assertEquals(ErrorCode.CONNECTION_ERROR, exception.getErrorCode());
        } finally {
            Thread.interrupted();
        }
    }
}
