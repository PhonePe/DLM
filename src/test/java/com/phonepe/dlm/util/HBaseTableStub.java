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

package com.phonepe.dlm.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.util.Bytes;


@SuppressWarnings("deprecation")
public class HBaseTableStub implements Table {
    private final String tableName;
    private final List<String> columnFamilies = new ArrayList<>();

    private NavigableMap<byte[], NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>>> data = new TreeMap<>(
            Bytes.BYTES_COMPARATOR);

    private static List<Cell> toCell(byte[] row,
                                     NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> rowdata,
                                     int maxVersions) {
        return toCell(row, rowdata, 0, Long.MAX_VALUE, maxVersions);
    }

    public HBaseTableStub(String tableName) {
        this.tableName = tableName;
    }

    public HBaseTableStub(String tableName, String... columnFamilies) {
        this.tableName = tableName;
        this.columnFamilies.addAll(Arrays.asList(columnFamilies));
    }

    public void addColumnFamily(String columnFamily) {
        this.columnFamilies.add(columnFamily);
    }

    public NavigableMap<byte[], NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>>> getData() {
        return data;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TableName getName() {
        return TableName.valueOf(tableName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Configuration getConfiguration() {
        throw new RuntimeException(this.getClass() + " does NOT implement this method.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public HTableDescriptor getTableDescriptor() {
        HTableDescriptor table = new HTableDescriptor(getName());
        for (String columnFamily : columnFamilies) {
            table.addFamily(new HColumnDescriptor(columnFamily));
        }
        return table;
    }

    @Override
    public TableDescriptor getDescriptor() throws IOException {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void mutateRow(RowMutations rm) {
        // currently only support Put and Delete
        for (Mutation mutation : rm.getMutations()) {
            if (mutation instanceof Put) {
                put((Put) mutation);
            } else if (mutation instanceof Delete) {
                delete((Delete) mutation);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Result append(Append append) {
        throw new RuntimeException(this.getClass() + " does NOT implement this method.");
    }

    private static List<Cell> toCell(byte[] row,
                                     NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> rowdata,
                                     long timestampStart,
                                     long timestampEnd,
                                     int maxVersions) {
        List<Cell> ret = new ArrayList<>();
        byte putType = KeyValue.Type.Put.getCode();
        for (byte[] family : rowdata.keySet())
            for (byte[] qualifier : rowdata.get(family)
                    .keySet()) {
                int versionsAdded = 0;
                for (Map.Entry<Long, byte[]> tsToVal : rowdata.get(family)
                        .get(qualifier)
                        .descendingMap()
                        .entrySet()) {
                    if (versionsAdded++ == maxVersions)
                        break;
                    Long timestamp = tsToVal.getKey();
                    if (timestamp < timestampStart)
                        continue;
                    if (timestamp > timestampEnd)
                        continue;
                    byte[] value = tsToVal.getValue();
                    ret.add(CellUtil.createCell(row, family, qualifier, timestamp, putType, value));
                }
            }
        return ret;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean exists(Get get) throws IOException {
        Result result = get(get);
        return result != null && !result.isEmpty();
    }

    @Override
    public boolean[] exists(List<Get> list) {
        return new boolean[0];
    }

    @Override
    public boolean[] existsAll(List<Get> gets) throws IOException {
        boolean[] result = new boolean[gets.size()];
        for (int i = 0; i < gets.size(); i++) {
            result[i] = exists(gets.get(i));
        }
        return result;
    }

    @Override
    public void batch(List<? extends Row> list, Object[] objects) {
        // Not implemented
    }

    /**
     * {@inheritDoc}
     */
    public void batch(List<? extends Row> actions) {
        batch(actions);
    }

    /**
     * {@inheritDoc}
     */

    @Override
    public <R> void batchCallback(List<? extends Row> actions, Object[] results, Batch.Callback<R> callback) {
        throw new RuntimeException(this.getClass() + " does NOT implement this method.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Result get(Get get) throws IOException {
        if (!data.containsKey(get.getRow()))
            return new Result();
        byte[] row = get.getRow();
        List<Cell> cells = new ArrayList<>();
        if (!get.hasFamilies()) {
            cells = toCell(row, data.get(row), get.getMaxVersions());
        } else {
            for (byte[] family : get.getFamilyMap()
                    .keySet()) {
                if (data.get(row)
                        .get(family) == null)
                    continue;
                NavigableSet<byte[]> qualifiers = get.getFamilyMap()
                        .get(family);
                if (qualifiers == null || qualifiers.isEmpty())
                    qualifiers = data.get(row)
                            .get(family)
                            .navigableKeySet();
                for (byte[] qualifier : qualifiers) {
                    if (qualifier == null)
                        qualifier = "".getBytes();
                    if (!data.get(row)
                            .containsKey(family)
                            || !data.get(row)
                                    .get(family)
                                    .containsKey(qualifier)
                            || data.get(row)
                                    .get(family)
                                    .get(qualifier)
                                    .isEmpty())
                        continue;
                    Map.Entry<Long, byte[]> timestampAndValue = data.get(row)
                            .get(family)
                            .get(qualifier)
                            .lastEntry();
                    cells.add(new KeyValue(row,
                            family,
                            qualifier,
                            timestampAndValue.getKey(),
                            timestampAndValue.getValue()));
                }
            }
        }
        Filter filter = get.getFilter();
        if (filter != null) {
            cells = filter(filter, cells);
        }
        cells.sort(new CellComparator() {
            @Override
            public int compare(Cell cell, Cell cell1) {
                return 0;
            }

            @Override
            public boolean equals(Object obj) {
                return false;
            }

            @Override
            public int compare(Cell cell, Cell cell1, boolean b) {
                return 0;
            }

            @Override
            public int compareRows(Cell cell, Cell cell1) {
                return 0;
            }

            @Override
            public int compareRows(Cell cell, byte[] bytes, int i, int i1) {
                return 0;
            }

            @Override
            public int compareWithoutRow(Cell cell, Cell cell1) {
                return 0;
            }

            @Override
            public int compareFamilies(Cell cell, Cell cell1) {
                return 0;
            }

            @Override
            public int compareQualifiers(Cell cell, Cell cell1) {
                return 0;
            }

            @Override
            public int compareTimestamps(Cell cell, Cell cell1) {
                return 0;
            }

            @Override
            public int compareTimestamps(long l, long l1) {
                return 0;
            }

            @SuppressWarnings("rawtypes")
            @Override
            public Comparator getSimpleComparator() {
                return null;
            }
        });
        return Result.create(cells);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Result[] get(List<Get> gets) throws IOException {
        List<Result> results = new ArrayList<>();
        for (Get g : gets) {
            results.add(get(g));
        }
        return results.toArray(new Result[0]);
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unused")
    @Override
    public ResultScanner getScanner(Scan scan) throws IOException {
        final List<Result> ret = new ArrayList<>();
        byte[] st = scan.getStartRow();
        byte[] sp = scan.getStopRow();
        Filter filter = scan.getFilter();

        for (byte[] row : data.keySet()) {
            // if row is equal to startRow emit it. When startRow (inclusive) and
            // stopRow (exclusive) is the same, it should not be excluded which would
            // happen w/o this control.
            if (st != null && st.length > 0 && Bytes.BYTES_COMPARATOR.compare(st, row) != 0) {
                // if row is before startRow do not emit, pass to next row
                if (Bytes.BYTES_COMPARATOR.compare(st, row) > 0)
                    continue;
                // if row is equal to stopRow or after it do not emit, stop iteration
                if (sp != null && sp.length > 0 && Bytes.BYTES_COMPARATOR.compare(sp, row) < 0)
                    break;
            }

            List<Cell> kvs;
            if (!scan.hasFamilies()) {
                kvs = toCell(row,
                        data.get(row),
                        scan.getTimeRange()
                                .getMin(),
                        scan.getTimeRange()
                                .getMax(),
                        scan.getMaxVersions());
            } else {
                kvs = new ArrayList<>();
                for (byte[] family : scan.getFamilyMap()
                        .keySet()) {
                    if (data.get(row)
                            .get(family) == null)
                        continue;
                    NavigableSet<byte[]> qualifiers = scan.getFamilyMap()
                            .get(family);
                    if (qualifiers == null || qualifiers.isEmpty())
                        qualifiers = data.get(row)
                                .get(family)
                                .navigableKeySet();
                    for (byte[] qualifier : qualifiers) {
                        if (data.get(row)
                                .get(family)
                                .get(qualifier) == null)
                            continue;
                        for (Long timestamp : data.get(row)
                                .get(family)
                                .get(qualifier)
                                .descendingKeySet()) {
                            if (timestamp < scan.getTimeRange()
                                    .getMin())
                                continue;
                            if (timestamp > scan.getTimeRange()
                                    .getMax())
                                continue;
                            byte[] value = data.get(row)
                                    .get(family)
                                    .get(qualifier)
                                    .get(timestamp);
                            kvs.add(new KeyValue(row, family, qualifier, timestamp, value));
                            if (kvs.size() == scan.getMaxVersions()) {
                                break;
                            }
                        }
                    }
                }
            }
            if (!kvs.isEmpty()) {
                kvs.sort(new CellComparator() {
                    @Override
                    public int compare(Cell cell, Cell cell1) {
                        return 0;
                    }

                    @Override
                    public boolean equals(Object obj) {
                        return false;
                    }

                    @Override
                    public int compare(Cell cell, Cell cell1, boolean b) {
                        return 0;
                    }

                    @Override
                    public int compareRows(Cell cell, Cell cell1) {
                        return 0;
                    }

                    @Override
                    public int compareRows(Cell cell, byte[] bytes, int i, int i1) {
                        return 0;
                    }

                    @Override
                    public int compareWithoutRow(Cell cell, Cell cell1) {
                        return 0;
                    }

                    @Override
                    public int compareFamilies(Cell cell, Cell cell1) {
                        return 0;
                    }

                    @Override
                    public int compareQualifiers(Cell cell, Cell cell1) {
                        return 0;
                    }

                    @Override
                    public int compareTimestamps(Cell cell, Cell cell1) {
                        return 0;
                    }

                    @Override
                    public int compareTimestamps(long l, long l1) {
                        return 0;
                    }

                    @SuppressWarnings("rawtypes")
                    @Override
                    public Comparator getSimpleComparator() {
                        return null;
                    }
                });
                ret.add(Result.create(kvs));
            }
        }

        return new ResultScanner() {
            private final Iterator<Result> iterator = ret.iterator();

            @Override
            public Iterator<Result> iterator() {
                return iterator;
            }

            @Override
            public Result[] next(int nbRows) {
                ArrayList<Result> resultSets = new ArrayList<>(nbRows);
                for (int i = 0; i < nbRows; i++) {
                    Result next = next();
                    if (next != null) {
                        resultSets.add(next);
                    } else {
                        break;
                    }
                }
                return resultSets.toArray(new Result[0]);
            }

            @Override
            public Result next() {
                try {
                    return iterator().next();
                } catch (NoSuchElementException e) {
                    return null;
                }
            }

            public void close() {
            }

            @Override
            public boolean renewLease() {
                return false;
            }

            @Override
            public ScanMetrics getScanMetrics() {
                return null;
            }
        };
    }

    /**
     * Follows the logical flow through the filter methods for a single row.
     *
     * @param filter HBase filter.
     * @param cells  List of a row's Cells
     * @return List of Cells that were not filtered.
     */
    private List<Cell> filter(Filter filter, List<Cell> cells) throws IOException {
        filter.reset();

        List<Cell> tmp = new ArrayList<>(cells.size());
        tmp.addAll(cells);

        /*
         * Note. Filter flow for a single row. Adapted from
         * "HBase: The Definitive Guide" (p. 163) by Lars George, 2011. See Figure 4-2
         * on p. 163.
         */
        boolean filteredOnRowKey = false;
        List<Cell> nkvs = new ArrayList<>(tmp.size());
        for (Cell cell : tmp) {
            if (filter.filterRowKey(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength())) {
                filteredOnRowKey = true;
                break;
            }
            Filter.ReturnCode filterResult = filter.filterKeyValue(cell);
            if (filterResult == Filter.ReturnCode.INCLUDE) {
                nkvs.add(cell);
            } else if (filterResult == Filter.ReturnCode.NEXT_ROW) {
                break;
            }
            /*
             * Ignoring next key hint which is a optimization to reduce file system IO
             */
        }
        if (filter.hasFilterRow() && !filteredOnRowKey) {
            filter.filterRowCells(nkvs);
        }
        if (filter.filterRow() || filteredOnRowKey) {
            nkvs.clear();
        }
        return nkvs;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultScanner getScanner(byte[] family) throws IOException {
        Scan scan = new Scan();
        scan.addFamily(family);
        return getScanner(scan);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultScanner getScanner(byte[] family, byte[] qualifier) throws IOException {
        Scan scan = new Scan();
        scan.addColumn(family, qualifier);
        return getScanner(scan);
    }

    private <K, V> V forceFind(NavigableMap<K, V> map, K key, V newObject) {
        return map.computeIfAbsent(key, k -> newObject);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void put(Put put) {
        byte[] row = put.getRow();
        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> rowData = forceFind(data,
                row,
                new TreeMap<>(Bytes.BYTES_COMPARATOR));
        for (byte[] family : put.getFamilyCellMap()
                .keySet()) {
            NavigableMap<byte[], NavigableMap<Long, byte[]>> familyData = forceFind(rowData,
                    family,
                    new TreeMap<>(Bytes.BYTES_COMPARATOR));
            for (Cell cell : put.getFamilyCellMap()
                    .get(family)) {
                byte[] qualifier = CellUtil.cloneQualifier(cell);
                NavigableMap<Long, byte[]> qualifierData = forceFind(familyData, qualifier, new TreeMap<>());
                qualifierData.put(cell.getTimestamp(), CellUtil.cloneValue(cell));
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void put(List<Put> puts) {
        for (Put put : puts) {
            put(put);
        }

    }

    private boolean check(byte[] row, byte[] family, byte[] qualifier, byte[] value) {
        if (value == null || value.length == 0)
            return !data.containsKey(row) || !data.get(row)
                    .containsKey(family)
                    || !data.get(row)
                            .get(family)
                            .containsKey(qualifier);
        else
            return data.containsKey(row) && data.get(row)
                    .containsKey(family)
                    && data.get(row)
                            .get(family)
                            .containsKey(qualifier)
                    && !data.get(row)
                            .get(family)
                            .get(qualifier)
                            .isEmpty()
                    && Arrays.equals(data.get(row)
                            .get(family)
                            .get(qualifier)
                            .lastEntry()
                            .getValue(), value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, byte[] value, Put put) {
        if (check(row, family, qualifier, value)) {
            put(put);
            return true;
        }
        return false;
    }

    @Override
    public boolean checkAndPut(byte[] row,
                               byte[] family,
                               byte[] qualifier,
                               CompareFilter.CompareOp compareOp,
                               byte[] value,
                               Put put) {
        return false;
    }

    @Override
    public boolean checkAndPut(byte[] bytes,
                               byte[] bytes1,
                               byte[] bytes2,
                               CompareOperator compareOperator,
                               byte[] bytes3,
                               Put put) throws IOException {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delete(Delete delete) {
        byte[] row = delete.getRow();
        NavigableMap<byte[], List<Cell>> familyCellMap = delete.getFamilyCellMap();
        if (data.get(row) == null)
            return;
        if (familyCellMap.size() == 0) {
            data.remove(row);
            return;
        }
        for (byte[] family : familyCellMap.keySet()) {
            if (data.get(row)
                    .get(family) == null)
                continue;
            if (familyCellMap.get(family)
                    .isEmpty()) {
                data.get(row)
                        .remove(family);
                continue;
            }
            for (Cell cell : familyCellMap.get(family)) {
                data.get(row)
                        .get(CellUtil.cloneFamily(cell))
                        .remove(CellUtil.cloneQualifier(cell));
            }
            if (data.get(row)
                    .get(family)
                    .isEmpty()) {
                data.get(row)
                        .remove(family);
            }
        }
        if (data.get(row)
                .isEmpty()) {
            data.remove(row);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delete(List<Delete> deletes) {
        for (Delete delete : deletes) {
            delete(delete);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier, byte[] value, Delete delete) {
        if (check(row, family, qualifier, value)) {
            delete(delete);
            return true;
        }
        return false;
    }

    @Override
    public boolean checkAndDelete(byte[] row,
                                  byte[] family,
                                  byte[] qualifier,
                                  CompareFilter.CompareOp compareOp,
                                  byte[] value,
                                  Delete delete) {
        throw new RuntimeException(this.getClass() + " does NOT implement this method.");
    }

    @Override
    public boolean checkAndDelete(byte[] bytes,
                                  byte[] bytes1,
                                  byte[] bytes2,
                                  CompareOperator compareOperator,
                                  byte[] bytes3,
                                  Delete delete) throws IOException {
        return false;
    }

    @Override
    public CheckAndMutateBuilder checkAndMutate(byte[] bytes, byte[] bytes1) {
        return new CheckAndMutateBuilder() {

            @Override
            public CheckAndMutateBuilder ifMatches(CompareOperator arg0, byte[] arg1) {
                return this;
            }

            @Override
            public CheckAndMutateBuilder ifNotExists() {
                return this;
            }

            @Override
            public CheckAndMutateBuilder qualifier(byte[] arg0) {
                return this;
            }

            @Override
            public boolean thenDelete(Delete arg0) throws IOException {
                return false;
            }

            @Override
            public boolean thenMutate(RowMutations arg0) throws IOException {
                return false;
            }

            @Override
            public boolean thenPut(Put arg0) throws IOException {
                if (!check(arg0.getRow(), Bytes.toBytes("D"), Bytes.toBytes("L"), Bytes.toBytes("M"))) {
                    put(arg0);
                    return true;
                }
                return false;
            }

            @Override
            public CheckAndMutateBuilder timeRange(TimeRange arg0) {
                return this;
            }
        };
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Result increment(Increment increment) {
        List<Cell> cells = new ArrayList<>();
        Map<byte[], NavigableMap<byte[], Long>> famToVal = increment.getFamilyMapOfLongs();
        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> rowData = forceFind(data,
                increment.getRow(),
                new TreeMap<>(Bytes.BYTES_COMPARATOR));
        for (Map.Entry<byte[], NavigableMap<byte[], Long>> ef : famToVal.entrySet()) {
            byte[] family = ef.getKey();
            NavigableMap<byte[], Long> qToVal = ef.getValue();
            NavigableMap<byte[], NavigableMap<Long, byte[]>> familyData = forceFind(rowData,
                    family,
                    new TreeMap<>(Bytes.BYTES_COMPARATOR));
            for (Map.Entry<byte[], Long> eq : qToVal.entrySet()) {
                long newValue = incrementColumnValue(increment.getRow(), family, eq.getKey(), eq.getValue());
                Cell cell = CellUtil.createCell(increment.getRow(),
                        family,
                        eq.getKey(),
                        System.currentTimeMillis(),
                        KeyValue.Type.Put.getCode(),
                        Bytes.toBytes(newValue));
                cells.add(cell);
                byte[] qualifier = CellUtil.cloneQualifier(cell);
                NavigableMap<Long, byte[]> qualifierData = forceFind(familyData, qualifier, new TreeMap<>());
                qualifierData.put(cell.getTimestamp(), CellUtil.cloneValue(cell));
            }
        }
        return Result.create(cells);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount) {
        return incrementColumnValue(row, family, qualifier, amount, Durability.USE_DEFAULT);
    }

    @Override
    public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount, Durability durability) {
        try {
            Get get = new Get(row);
            if (get(get).getValue(family, qualifier) == null) {
                return amount;
            } else {
                return Bytes.toLong(get(get).getValue(family, qualifier)) + amount;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return amount;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {

    }

    @Override
    public CoprocessorRpcChannel coprocessorService(byte[] row) {
        throw new RuntimeException(this.getClass() + " does NOT implement this method.");
    }

    @Override
    public <T extends org.apache.hadoop.hbase.shaded.com.google.protobuf.Service, R> Map<byte[], R> coprocessorService(
            Class<T> aClass,
            byte[] bytes,
            byte[] bytes1,
            Batch.Call<T, R> call) {
        return null;
    }

    @Override
    public <T extends org.apache.hadoop.hbase.shaded.com.google.protobuf.Service, R> void coprocessorService(
            Class<T> aClass,
            byte[] bytes,
            byte[] bytes1,
            Batch.Call<T, R> call,
            Batch.Callback<R> callback) {
        // Not implemented
    }

    @Override
    public <R extends org.apache.hadoop.hbase.shaded.com.google.protobuf.Message> Map<byte[], R> batchCoprocessorService(
            org.apache.hadoop.hbase.shaded.com.google.protobuf.Descriptors.MethodDescriptor methodDescriptor,
            org.apache.hadoop.hbase.shaded.com.google.protobuf.Message message,
            byte[] bytes,
            byte[] bytes1,
            R r) {
        return null;
    }

    @Override
    public <R extends org.apache.hadoop.hbase.shaded.com.google.protobuf.Message> void batchCoprocessorService(
            org.apache.hadoop.hbase.shaded.com.google.protobuf.Descriptors.MethodDescriptor methodDescriptor,
            org.apache.hadoop.hbase.shaded.com.google.protobuf.Message message,
            byte[] bytes,
            byte[] bytes1,
            R r,
            Batch.Callback<R> callback) {

    }

    @Override
    public boolean checkAndMutate(byte[] row,
                                  byte[] family,
                                  byte[] qualifier,
                                  CompareFilter.CompareOp compareOp,
                                  byte[] value,
                                  RowMutations mutation) {
        throw new RuntimeException(this.getClass() + " does NOT implement this method.");
    }

    @Override
    public boolean checkAndMutate(byte[] bytes,
                                  byte[] bytes1,
                                  byte[] bytes2,
                                  CompareOperator compareOperator,
                                  byte[] bytes3,
                                  RowMutations rowMutations) {
        return false;
    }

    @Override
    public long getRpcTimeout(TimeUnit timeUnit) {
        return 0;
    }

    @Override
    public void setOperationTimeout(int operationTimeout) {
        throw new RuntimeException(this.getClass() + " does NOT implement this method.");
    }

    @Override
    public int getOperationTimeout() {
        throw new RuntimeException(this.getClass() + " does NOT implement this method.");
    }

    @Override
    public void setRpcTimeout(int rpcTimeout) {
        throw new RuntimeException(this.getClass() + " does NOT implement this method.");
    }

    @Override
    public long getReadRpcTimeout(TimeUnit timeUnit) {
        return 0;
    }

    @Override
    public int getReadRpcTimeout() {
        return 0;
    }

    @Override
    public void setReadRpcTimeout(int i) {
        // Not implemented
    }

    @Override
    public long getWriteRpcTimeout(TimeUnit timeUnit) {
        return 0;
    }

    @Override
    public int getWriteRpcTimeout() {
        return 0;
    }

    @Override
    public void setWriteRpcTimeout(int i) {
        // Not implemented
    }

    @Override
    public long getOperationTimeout(TimeUnit timeUnit) {
        return 0;
    }

    @Override
    public int getRpcTimeout() {
        throw new RuntimeException(this.getClass() + " does NOT implement this method.");
    }

    private static void indent(int level, StringBuilder sb) {
        String indent = new String(new char[level]).replace("\0", "  ");
        sb.append(indent);
    }

    /**
     * Create a MockHBaseTable with some pre-loaded data. Parameter should be a map
     * of column-to-data mappings of rows. It can be created with data like:
     *
     * <pre>
     * rowid:
     *   family1:qualifier1: value1
     *   family2:qualifier2: value2
     * </pre>
     *
     * @param name name of the new table
     * @param data data to initialize the table with
     * @return a new MockHBaseTable loaded with given data
     */
    public static HBaseTableStub with(String name, Map<String, Map<String, String>> data) {
        HBaseTableStub table = new HBaseTableStub(name);
        for (String row : data.keySet()) {
            for (String column : data.get(row)
                    .keySet()) {
                String val = data.get(row)
                        .get(column);
                put(table, row, column, val);
            }
        }
        return table;
    }

    /**
     * Create a MockHBaseTable with some pre-loaded data. Parameter should be an
     * array of string arrays which define every column value individually.
     *
     * <pre>
     * new String[][] {
     *   { "&lt;rowid&gt;", "&lt;column&gt;", "&lt;value&gt;" },
     *   { "id", "family:qualifier1", "data1" },
     *   { "id", "family:qualifier2", "data2" }
     * });
     * </pre>
     *
     * @param name name of the new table
     * @param data data to initialize the table with
     * @return a new MockHBaseTable loaded with given data
     */
    public static HBaseTableStub with(String name, String[][] data) {
        HBaseTableStub ret = new HBaseTableStub(name);
        for (String[] row : data) {
            put(ret, row[0], row[1], row[2]);
        }
        return ret;
    }

    /**
     * Helper method of pre-loaders, adds parameters to data.
     *
     * @param table  table to load data into
     * @param row    row id
     * @param column family:qualifier encoded value
     * @param val    value
     */
    private static void put(HBaseTableStub table, String row, String column, String val) {
        String[] fq = split(column);
        byte[] family = Bytes.toBytes(fq[0]);
        byte[] qualifier = Bytes.toBytes(fq[1]);
        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> families = table
                .forceFind(table.data, Bytes.toBytes(row), new TreeMap<>(Bytes.BYTES_COMPARATOR));
        NavigableMap<byte[], NavigableMap<Long, byte[]>> qualifiers = table
                .forceFind(families, family, new TreeMap<>(Bytes.BYTES_COMPARATOR));
        NavigableMap<Long, byte[]> values = table.forceFind(qualifiers, qualifier, new TreeMap<>());
        values.put(System.currentTimeMillis(), Bytes.toBytes(val));
    }

    /**
     * Column identification helper
     *
     * @param column column name in the format family:qualifier
     * @return <code>{"family", "qualifier"}</code>
     */
    private static String[] split(String column) {
        return new String[]{column.substring(0, column.indexOf(':')), column.substring(column.indexOf(':') + 1)};
    }

    /**
     * Read a value saved in the object. Useful for making assertions in tests.
     *
     * @param rowid  rowid of the data to read
     * @param column family:qualifier of the data to read
     * @return value or null if row or column of the row does not exist
     */
    public byte[] read(String rowid, String column) {
        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> row = data.get(Bytes.toBytes(rowid));
        if (row == null)
            return null;
        String[] fq = split(column);
        byte[] family = Bytes.toBytes(fq[0]);
        byte[] qualifier = Bytes.toBytes(fq[1]);
        if (!row.containsKey(family))
            return null;
        if (!row.get(family)
                .containsKey(qualifier))
            return null;
        return row.get(family)
                .get(qualifier)
                .lastEntry()
                .getValue();
    }

    @Override
    public String toString() {
        String nl = System.getProperty("line.separator");
        int i = 1;
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append(nl);
        for (Map.Entry<byte[], NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>>> row : getData()
                .entrySet()) {
            indent(i, sb);
            sb.append(Bytes.toString(row.getKey()));
            sb.append(":");
            sb.append(nl);
            i++;
            for (Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> family : row.getValue()
                    .entrySet()) {
                indent(i, sb);
                sb.append(Bytes.toString(family.getKey()));
                sb.append(":");
                sb.append(nl);
                i++;
                for (Map.Entry<byte[], NavigableMap<Long, byte[]>> column : family.getValue()
                        .entrySet()) {
                    indent(i, sb);
                    sb.append(Bytes.toString(column.getKey()));
                    sb.append(": ");
                    sb.append(Bytes.toString(column.getValue()
                            .lastEntry()
                            .getValue()));
                    sb.append(nl);
                }
                i--;
            }
            i--;
        }
        sb.append(nl);
        sb.append("}");
        return sb.toString();
    }

}