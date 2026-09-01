/**
 * Copyright (c) 2024 Original Author(s), PhonePe India Pvt. Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package com.phonepe.dlm.aerospike.storage;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.AerospikeException;
import com.phonepe.dlm.aerospike.utils.AerospikeUtils;
import com.phonepe.dlm.exception.DLMException;
import com.phonepe.dlm.exception.ErrorCode;
import com.phonepe.dlm.lock.level.LockLevel;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AerospikeStoreTest {
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
                () -> store.write("lock", LockLevel.DC, "farm", Duration.ofSeconds(30)));

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
}
