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

package com.phonepe.dlm.hbase.storage;

import com.phonepe.dlm.exception.DLMException;
import com.phonepe.dlm.exception.ErrorCode;
import com.phonepe.dlm.lock.level.LockLevel;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Store-level tests for {@link HBaseStore}'s ownership-checked release.
 */
class HBaseStoreTest {
    private static final String TABLE = "dlm_locks";
    private static final String LOCK_ID = "LOCK_ID";
    private static final String FARM_ID = "FA1";
    private static final Duration TTL = Duration.ofSeconds(30);
    private static final long ACQUIRED_AT = 1_700_000_000_123L;

    private HBaseConnectionStub connection;
    private HBaseStore store;

    @BeforeEach
    void setUp() {
        connection = new HBaseConnectionStub();
        store = HBaseStore.builder()
                .connection(connection)
                .tableName(TABLE)
                .build();
    }

    @Test
    void ownerCheckedRemoveDeletesOwnLock() {
        store.write(LOCK_ID, LockLevel.DC, FARM_ID, TTL, ACQUIRED_AT);

        assertTrue(store.remove(LOCK_ID, LockLevel.DC, FARM_ID, ACQUIRED_AT));
        assertTrue(connection.getTable(TABLE).getData().isEmpty());
    }

    @Test
    void ownerCheckedRemoveReturnsFalseWhenLockAlreadyExpired() {
        store.write(LOCK_ID, LockLevel.DC, FARM_ID, TTL, ACQUIRED_AT);
        connection.getTable(TABLE).getData().clear(); // simulate TTL expiry

        assertFalse(store.remove(LOCK_ID, LockLevel.DC, FARM_ID, ACQUIRED_AT));
    }

    @Test
    void ownerCheckedRemoveLeavesAnotherHoldersLockIntact() {
        store.write(LOCK_ID, LockLevel.DC, FARM_ID, TTL, ACQUIRED_AT + 1_000);

        assertFalse(store.remove(LOCK_ID, LockLevel.DC, FARM_ID, ACQUIRED_AT));
        assertFalse(connection.getTable(TABLE).getData().isEmpty());
    }

    @Test
    void ownerCheckedRemoveWrapsIOException() throws IOException {
        final Connection failingConnection = mock(Connection.class);
        when(failingConnection.getTable(any(TableName.class))).thenThrow(new IOException("unreachable"));
        final HBaseStore failingStore = HBaseStore.builder()
                .connection(failingConnection)
                .tableName(TABLE)
                .build();

        final DLMException exception = assertThrows(DLMException.class,
                () -> failingStore.remove(LOCK_ID, LockLevel.DC, FARM_ID, ACQUIRED_AT));

        assertEquals(ErrorCode.INTERNAL_ERROR, exception.getErrorCode());
    }

    @Test
    void writeStampsCellTimestampWithAcquisitionTime() throws IOException {
        final Table.CheckAndMutateBuilder builder = selfReturningBuilder();
        when(builder.thenPut(any(Put.class))).thenReturn(true);

        storeWith(builder).write(LOCK_ID, LockLevel.DC, FARM_ID, TTL, ACQUIRED_AT);

        final ArgumentCaptor<Put> put = ArgumentCaptor.forClass(Put.class);
        verify(builder).thenPut(put.capture());
        final Cell cell = put.getValue().getFamilyCellMap().get(Bytes.toBytes("D")).get(0);
        assertEquals(ACQUIRED_AT, cell.getTimestamp());
        assertArrayEquals(Bytes.toBytes(ACQUIRED_AT), CellUtil.cloneValue(cell));
    }

    /**
     * The owner-checked release must delete only the cell version written at {@code acquiredAt}. A whole-row delete
     * leaves a tombstone with the region server's clock that hides the next holder's (client-clock stamped) cell when
     * the lock is re-acquired within the clock skew, breaking mutual exclusion.
     */
    @Test
    void ownerCheckedRemoveDeletesOnlyTheHoldersCellVersion() throws IOException {
        final Table.CheckAndMutateBuilder builder = selfReturningBuilder();
        when(builder.thenDelete(any(Delete.class))).thenReturn(true);

        assertTrue(storeWith(builder).remove(LOCK_ID, LockLevel.DC, FARM_ID, ACQUIRED_AT));

        verify(builder).ifEquals(Bytes.toBytes(ACQUIRED_AT));
        final ArgumentCaptor<Delete> delete = ArgumentCaptor.forClass(Delete.class);
        verify(builder).thenDelete(delete.capture());
        final List<Cell> markers = delete.getValue().getFamilyCellMap().get(Bytes.toBytes("D"));
        assertEquals(1, markers.size());
        final Cell marker = markers.get(0);
        assertArrayEquals(Bytes.toBytes("L"), CellUtil.cloneQualifier(marker));
        assertEquals(ACQUIRED_AT, marker.getTimestamp());
        // Version delete (Cell.Type.Delete), not a column/family/row tombstone that would also hide newer cells.
        assertEquals(Cell.Type.Delete, marker.getType());
    }

    private static Table.CheckAndMutateBuilder selfReturningBuilder() {
        final Table.CheckAndMutateBuilder builder = mock(Table.CheckAndMutateBuilder.class);
        when(builder.qualifier(any())).thenReturn(builder);
        when(builder.ifNotExists()).thenReturn(builder);
        when(builder.ifEquals(any())).thenReturn(builder);
        return builder;
    }

    private HBaseStore storeWith(final Table.CheckAndMutateBuilder builder) throws IOException {
        final Table table = mock(Table.class);
        when(table.checkAndMutate(any(byte[].class), any(byte[].class))).thenReturn(builder);
        final Connection mockedConnection = mock(Connection.class);
        when(mockedConnection.getTable(any(TableName.class))).thenReturn(table);
        return HBaseStore.builder().connection(mockedConnection).tableName(TABLE).build();
    }

    @Test
    void writeWrapsIOExceptionAsConnectionError() throws IOException {
        final DLMException exception = assertThrows(DLMException.class,
                () -> failingStore().write(LOCK_ID, LockLevel.DC, FARM_ID, TTL, ACQUIRED_AT));

        assertEquals(ErrorCode.CONNECTION_ERROR, exception.getErrorCode());
    }

    private HBaseStore failingStore() throws IOException {
        final Connection failingConnection = mock(Connection.class);
        when(failingConnection.getTable(any(TableName.class))).thenThrow(new IOException("unreachable"));
        return HBaseStore.builder().connection(failingConnection).tableName(TABLE).build();
    }
}
