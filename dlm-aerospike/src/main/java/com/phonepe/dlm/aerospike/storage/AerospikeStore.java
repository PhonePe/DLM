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

package com.phonepe.dlm.aerospike.storage;

import com.aerospike.client.*;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.policy.CommitLevel;
import com.aerospike.client.policy.GenerationPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.phonepe.dlm.common.Constants;
import com.phonepe.dlm.exception.DLMException;
import com.phonepe.dlm.exception.ErrorCode;
import com.phonepe.dlm.lock.level.LockLevel;
import com.phonepe.dlm.lock.storage.ILockStore;
import com.phonepe.dlm.aerospike.utils.AerospikeUtils;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

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

    /**
     * Creates the lock record (create-only, via generation check) with {@code acquiredAt} stored in the
     * {@code <farm>##uat} bin, which {@link #remove(String, LockLevel, String, long)} uses to verify ownership.
     */
    @Override
    public void write(String lockId, LockLevel lockLevel, String farmId, Duration ttlSeconds, long acquiredAt) {
        try {
            final WritePolicy writePolicy = new WritePolicy(aerospikeClient.getWritePolicyDefault());
            writePolicy.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
            writePolicy.generation = 0;
            writePolicy.expiration = (int) ttlSeconds.getSeconds(); // as only int is supported
            writePolicy.commitLevel = CommitLevel.COMMIT_MASTER; // Committing to master only, as there is no read required so there is no chance of dirty reads.
            final List<Bin> binList = new ArrayList<>();
            binList.add(new Bin(AerospikeUtils.getBin(DATA_BIN, farmId), 1));
            binList.add(new Bin(AerospikeUtils.getBin(MODIFIED_AT_BIN, farmId), acquiredAt));

            AerospikeUtils.retry(() -> write(lockId, lockLevel, farmId, writePolicy, binList));
        } catch (DLMException e) {
            if (e.getErrorCode() == ErrorCode.LOCK_UNAVAILABLE) {
                throw e;
            }
            throw connectionError(lockId, e);
        } catch (RuntimeException e) {
            throw connectionError(lockId, e);
        }
    }

    /**
     * Deletes the lock record only if its {@code <farm>##uat} bin still equals {@code acquiredAt}, using a server-side
     * filter expression (Aerospike server 5.2+), so the check and the delete are atomic.
     *
     * Aerospike hides an expired record from reads immediately but keeps it until the namespace's expiry sweep
     * ({@code nsup-period}). Until then, a holder whose lock expired and was not re-acquired still matches its own
     * stamp, so its release deletes that stale record and returns {@code true}. A record re-created by another holder
     * carries a different stamp and is never deleted.
     *
     * @return {@code true} if this holder's record (live, or expired but not yet swept) was deleted; {@code false} if
     * the record is gone or now belongs to another holder (filtered out). Neither of these is retried.
     */
    @Override
    public boolean remove(String lockId, LockLevel lockLevel, String farmId, long acquiredAt) {
        final AtomicBoolean removed = new AtomicBoolean();
        try {
            final WritePolicy deletePolicy = new WritePolicy(aerospikeClient.getWritePolicyDefault());
            deletePolicy.filterExp = Exp.build(
                    Exp.eq(Exp.intBin(AerospikeUtils.getBin(MODIFIED_AT_BIN, farmId)), Exp.val(acquiredAt)));
            deletePolicy.failOnFilteredOut = true;
            AerospikeUtils.retry(() -> {
                try {
                    removed.set(aerospikeClient.delete(deletePolicy,
                            new Key(namespace, getSetName(lockLevel, farmId), lockId)));
                } catch (AerospikeException e) {
                    if (e.getResultCode() != ResultCode.FILTERED_OUT) {
                        throw e;
                    }
                    removed.set(false);
                }
            });
            return removed.get();
        } catch (AerospikeException e) {
            throw removalError(lockId, e);
        } catch (RuntimeException e) {
            throw connectionError(lockId, e);
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

    private DLMException removalError(final String lockId, final AerospikeException cause) {
        return DLMException.builder()
                .cause(cause)
                .errorCode(ErrorCode.RETRIES_EXHAUSTED)
                .message(String.format("Error removing lock in aerospike [id = %s]", lockId))
                .build();
    }

    private DLMException connectionError(final String lockId, final RuntimeException cause) {
        return DLMException.builder()
                .cause(cause)
                .errorCode(ErrorCode.CONNECTION_ERROR)
                .message(String.format("Error accessing aerospike [id = %s]", lockId))
                .build();
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
