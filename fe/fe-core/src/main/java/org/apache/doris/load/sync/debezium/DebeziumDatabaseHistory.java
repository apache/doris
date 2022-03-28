// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.load.sync.debezium;

import io.debezium.config.Configuration;
import io.debezium.relational.history.AbstractDatabaseHistory;
import io.debezium.relational.history.DatabaseHistoryException;
import io.debezium.relational.history.DatabaseHistoryListener;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.HistoryRecordComparator;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;

public class DebeziumDatabaseHistory extends AbstractDatabaseHistory {
    public static final String DATABASE_HISTORY_INSTANCE_NAME = "database.history.instance.name";

    public static final Map<String, ConcurrentLinkedQueue<HistoryRecord>> ALL_RECORDS = new HashMap<>();

    private ConcurrentLinkedQueue<HistoryRecord> records;
    private String instanceName;

    /**
     * Registers the given HistoryRecords into global variable under the given instance name,
     * in order to be accessed by instance of {@link DebeziumDatabaseHistory}.
     */
    public static void registerHistoryRecords(String instanceName, ConcurrentLinkedQueue<HistoryRecord> historyRecords) {
        synchronized (DebeziumDatabaseHistory.ALL_RECORDS) {
            DebeziumDatabaseHistory.ALL_RECORDS.put(instanceName, historyRecords);
        }
    }

    /**
     * Registers an empty HistoryRecords into global variable under the given instance name,
     * in order to be accessed by instance of {@link DebeziumDatabaseHistory}.
     */
    public static void registerEmptyHistoryRecord(String instanceName) {
        registerHistoryRecords(instanceName, new ConcurrentLinkedQueue<>());
    }

    /**
     * Gets the registered HistoryRecords under the given instance name.
     */
    public static ConcurrentLinkedQueue<HistoryRecord> getRegisteredHistoryRecord(String instanceName) {
        synchronized (ALL_RECORDS) {
            if (ALL_RECORDS.containsKey(instanceName)) {
                return ALL_RECORDS.get(instanceName);
            }
        }
        return null;
    }

    @Override
    public void configure(Configuration config, HistoryRecordComparator comparator, DatabaseHistoryListener listener, boolean useCatalogBeforeSchema) {
        super.configure(config, comparator, listener, useCatalogBeforeSchema);
        this.instanceName = config.getString(DATABASE_HISTORY_INSTANCE_NAME);
        this.records = getRegisteredHistoryRecord(instanceName);
        if (records == null) {
            throw new IllegalStateException(
                    String.format("Couldn't find engine instance %s in the global records.", instanceName));
        }
    }

    @Override
    public void stop() {
        super.stop();
        if (instanceName != null) {
            synchronized (ALL_RECORDS) {
                // clear memory
                ALL_RECORDS.remove(instanceName);
            }
        }
    }

    @Override
    protected void storeRecord(HistoryRecord record) throws DatabaseHistoryException {
        this.records.add(record);
    }

    @Override
    protected void recoverRecords(Consumer<HistoryRecord> records) {
        this.records.forEach(records);
    }

    @Override
    public boolean exists() {
        return true;
    }

    @Override
    public boolean storageExists() {
        return true;
    }

}

