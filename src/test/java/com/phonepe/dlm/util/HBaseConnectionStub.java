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

package com.phonepe.dlm.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.mockito.Mockito;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

@SuppressWarnings("serial")
public class HBaseConnectionStub implements Connection, Serializable {

    private Map<String, HBaseTableStub> tables = new HashMap<>();

    @Override
    public Configuration getConfiguration() {
        return null;
    }

    @Override
    public HBaseTableStub getTable(TableName tableName) {
        String name = tableName.getNameAsString();
        return tables.computeIfAbsent(name, HBaseTableStub::new);
    }

    @Override
    public HBaseTableStub getTable(TableName tableName, ExecutorService pool) {
        return getTable(tableName);
    }

    public HBaseTableStub getTable(String tableName) {
        return tables.get(tableName);
    }

    public Map<String, HBaseTableStub> getTables() {
        return tables;
    }

    @Override
    public BufferedMutator getBufferedMutator(TableName tableName) {
        throw new RuntimeException(this.getClass() + " does NOT implement this method.");
    }

    @Override
    public BufferedMutator getBufferedMutator(BufferedMutatorParams params) {
        throw new RuntimeException(this.getClass() + " does NOT implement this method.");
    }

    @Override
    public RegionLocator getRegionLocator(TableName tableName) {
        throw new RuntimeException(this.getClass() + " does NOT implement this method.");
    }

    @Override
    public Admin getAdmin() {
        return Mockito.mock(Admin.class);
    }

    @Override
    public void close() {
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public TableBuilder getTableBuilder(TableName tableName, ExecutorService executorService) {
        return null;
    }

    @Override
    public void abort(String why, Throwable e) {
    }

    @Override
    public boolean isAborted() {
        return false;
    }
}