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

package com.phonepe.dlm.lock.storage.aerospike;

import com.aerospike.client.*;
import com.aerospike.client.policy.CommitLevel;
import com.aerospike.client.policy.GenerationPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.github.rholder.retry.RetryException;
import com.phonepe.dlm.common.Constants;
import com.phonepe.dlm.exception.DLMException;
import com.phonepe.dlm.exception.ErrorCode;
import com.phonepe.dlm.lock.level.LockLevel;
import com.phonepe.dlm.lock.storage.ILockStore;
import com.phonepe.dlm.utils.AerospikeUtils;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

@Data
@Builder
@AllArgsConstructor
public class AerospikeStore implements ILockStore {
    private static final String DATA_BIN = "data";
    private static final String MODIFIED_AT_BIN = "uat";

    private final IAerospikeClient aerospikeClient;
    private final String namespace;
    private final String setSuffix;

    @Override
    public void initialize() {
        // Nothing to initialise
    }

    @Override
    public void write(String lockId, LockLevel lockLevel, String farmId, Duration ttlSeconds) {
        final WritePolicy writePolicy = new WritePolicy(aerospikeClient.getWritePolicyDefault());
        writePolicy.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
        writePolicy.generation = 0;
        writePolicy.expiration = Long.valueOf(ttlSeconds.getSeconds()).intValue(); // as only int is supported
        writePolicy.commitLevel = CommitLevel.COMMIT_MASTER; // Committing to master only, as there is no read required so there is no chance of dirty reads.
        try {
            final List<Bin> binList = new ArrayList<>();
            binList.add(new Bin(AerospikeUtils.getBin(DATA_BIN, farmId), 1));
            binList.add(new Bin(AerospikeUtils.getBin(MODIFIED_AT_BIN, farmId), System.currentTimeMillis()));

            AerospikeUtils.retryer.call(() -> {
                write(lockId, lockLevel, farmId, writePolicy, binList);
                return null;
            });
        } catch (ExecutionException | RetryException e) {
            if (e.getCause() instanceof DLMException) {
                throw DLMException.propagate(e);
            }
            throw DLMException.builder()
                    .cause(e.getCause())
                    .errorCode(ErrorCode.CONNECTION_ERROR)
                    .message(String.format("Error writing lock in aerospike [id = %s]", lockId))
                    .build();
        }
    }

    @Override
    public void remove(String lockId, LockLevel lockLevel, String farmId) {
        try {
            AerospikeUtils.retryer.call(() ->
                    aerospikeClient.delete(aerospikeClient.getWritePolicyDefault(),
                            new Key(namespace, getSetName(lockLevel, farmId), lockId))
            );
        } catch (RetryException e) {
            throw DLMException.builder()
                    .cause(e)
                    .errorCode(ErrorCode.RETRIES_EXHAUSTED)
                    .message(String.format("Error removing lock in aerospike [id = %s]", lockId))
                    .build();
        } catch (ExecutionException e) {
            throw DLMException.builder()
                    .cause(e)
                    .errorCode(ErrorCode.CONNECTION_ERROR)
                    .message(String.format("Error removing lock in aerospike [id = %s]", lockId))
                    .build();
        }
    }

    @Override
    public void close() {
        aerospikeClient.close();
    }

    private void write(final String lockId, final LockLevel lockLevel, final String farmId,
                       final WritePolicy writePolicy, final List<Bin> binList) {
        try {
            aerospikeClient.put(writePolicy,
                    new Key(namespace, getSetName(lockLevel, farmId), lockId),
                    binList.toArray(new Bin[0]));
        } catch (AerospikeException ae) {
            if (ae.getResultCode() == ResultCode.GENERATION_ERROR) {
                throw DLMException.builder()
                        .cause(ae.getCause())
                        .errorCode(ErrorCode.LOCK_UNAVAILABLE)
                        .message(String.format("Error acquiring lock in aerospike [id = %s]", lockId))
                        .build();
            }
            throw ae;
        }
    }

    private String getSetName(final LockLevel lockLevel, final String farmId) {
        return lockLevel.accept(new LockLevel.Visitor<>() {
            @Override
            public String visitDC() {
                return String.join(Constants.DELIMITER, lockLevel.getValue(), farmId, setSuffix);
            }

            @Override
            public String visitXDC() {
                return String.join(Constants.DELIMITER, lockLevel.getValue(), setSuffix);
            }
        });
    }
}
