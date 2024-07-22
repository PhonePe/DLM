/**
 * Copyright (c) 2023 Original Author(s), PhonePe India Pvt. Ltd.
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

package com.phonepe.dlm.lock.storage.hbase;

import com.phonepe.dlm.common.Constants;
import com.phonepe.dlm.exception.DLMException;
import com.phonepe.dlm.exception.ErrorCode;
import com.phonepe.dlm.lock.level.LockLevel;
import com.phonepe.dlm.lock.storage.ILockStore;
import com.sematext.hbase.ds.AbstractRowKeyDistributor;
import com.sematext.hbase.ds.RowKeyDistributorByHashPrefix;
import com.sematext.hbase.ds.RowKeyDistributorByHashPrefix.Hasher;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

@Data
@Builder
@AllArgsConstructor
public class HBaseStore implements ILockStore {
    private static final Hasher ONE_BYTE_HASHER = new RowKeyDistributorByHashPrefix.OneByteSimpleHash(256);
    private static final AbstractRowKeyDistributor ROW_KEY_DISTRIBUTOR = new RowKeyDistributorByHashPrefix(
            ONE_BYTE_HASHER);
    private static final byte[] COLUMN_FAMILY = Bytes.toBytes("D");
    private static final byte[] COLUMN_NAME = Bytes.toBytes("L");
    private static final byte[] COLUMN_DATA = Bytes.toBytes("M");
    private Connection connection;
    private String tableName;

    @Override
    public void initialize() {
        final TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName))
                .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(COLUMN_FAMILY)
                        .setCompressionType(Compression.Algorithm.GZ)
                        .setMaxVersions(1)
                        .build())
                .build();
        try {
            if (!connection.getAdmin().tableExists(TableName.valueOf(Bytes.toBytes(tableName)))) {
                connection.getAdmin()
                        .createTable(tableDescriptor, ONE_BYTE_HASHER.getAllPossiblePrefixes());
            }
        } catch (Exception e) {
            throw DLMException.builder()
                    .cause(e)
                    .errorCode(ErrorCode.TABLE_CREATION_ERROR)
                    .message(String.format("Could not create table: %s", tableName))
                    .build();
        }
    }

    @Override
    public void write(String lockId, LockLevel lockLevel, String farmId, int ttlSeconds) {
        final byte[] normalisedRowKey = getNormalisedRowKey(lockId, lockLevel, farmId);

        try (final Table table = getTable()) {
            final boolean result = table.checkAndMutate(normalisedRowKey, COLUMN_FAMILY)
                    .qualifier(COLUMN_NAME)
                    .ifNotExists()
                    .thenPut(new Put(normalisedRowKey, System.currentTimeMillis()).setTTL(ttlSeconds * 1_000L)
                            .addColumn(COLUMN_FAMILY, COLUMN_NAME, COLUMN_DATA));
            if (!result) {
                throw DLMException.builder()
                        .errorCode(ErrorCode.LOCK_UNAVAILABLE)
                        .message(String.format("Error acquiring lock in HBase [id = %s]", lockId))
                        .build();
            }
        } catch (IOException e) {
            throw DLMException.builder()
                    .cause(e)
                    .errorCode(ErrorCode.CONNECTION_ERROR)
                    .message(String.format("Error writing lock in HBase [id = %s]", lockId))
                    .build();
        }
    }

    @Override
    public void remove(String lockId, LockLevel lockLevel, String farmId) {
        try (final Table table = getTable()) {
            table.delete(new Delete(getNormalisedRowKey(lockId, lockLevel, farmId)));
        } catch (IOException e) {
            throw DLMException.propagate(e);
        }
    }

    @Override
    public void close() {
        try {
            connection.close();
        } catch (Exception e) {
            throw DLMException.propagate(e);
        }
    }

    private Table getTable() throws IOException {
        return connection.getTable(TableName.valueOf(tableName));
    }

    private byte[] getNormalisedRowKey(final String lockId, final LockLevel lockLevel, final String farmId) {
        final String rowKey = lockLevel.accept(new LockLevel.Visitor<>() {
            @Override
            public String visitDC() {
                return String.join(Constants.DELIMITER, lockLevel.getValue(), farmId, lockId);
            }

            @Override
            public String visitXDC() {
                return String.join(Constants.DELIMITER, lockLevel.getValue(), lockId);
            }
        });
        return ROW_KEY_DISTRIBUTOR.getDistributedKey(Bytes.toBytes(rowKey));
    }

}
