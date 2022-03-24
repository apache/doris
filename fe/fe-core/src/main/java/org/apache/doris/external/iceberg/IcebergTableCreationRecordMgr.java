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

package org.apache.doris.external.iceberg;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.IcebergProperty;
import org.apache.doris.catalog.IcebergTable;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.SystemIdGenerator;
import org.apache.doris.common.property.PropertySchema;
import org.apache.doris.common.util.MasterDaemon;

import com.google.common.collect.Maps;

import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Manager for Iceberg automatic creation table records
 * used to create iceberg tables and show table creation records
 */
public class IcebergTableCreationRecordMgr extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(IcebergTableCreationRecordMgr.class);

    private static final String SUCCESS = "success";
    private static final String FAIL = "fail";

    // Iceberg databases, used to list remote iceberg tables
    // dbId -> database
    private Map<Long, Database> icebergDbs = new ConcurrentHashMap<>();
    // database -> table identifier -> properties
    // used to create table
    private Map<Database, Map<TableIdentifier, IcebergProperty>> dbToTableIdentifiers = Maps.newConcurrentMap();
    // table creation records, used for show stmt
    // dbId -> tableId -> create msg
    private Map<Long, Map<Long, IcebergTableCreationRecord>> dbToTableToCreationRecord = Maps.newConcurrentMap();

    private Queue<IcebergTableCreationRecord> tableCreationRecordQueue = new PriorityQueue<>(new TableCreationComparator());
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();


    public IcebergTableCreationRecordMgr() {
        super("iceberg_table_creation_record_mgr", Config.iceberg_table_creation_interval_second * 1000);
    }

    public void registerDb(Database db) throws DdlException {
        long dbId = db.getId();
        icebergDbs.put(dbId, db);
        LOG.info("Register a new Iceberg database[{}-{}]", dbId, db.getFullName());
    }

    private void registerTable(Database db, TableIdentifier identifier, IcebergProperty icebergProperty) {
        if (dbToTableIdentifiers.containsKey(db)) {
            dbToTableIdentifiers.get(db).put(identifier, icebergProperty);
        } else {
            Map<TableIdentifier, IcebergProperty> identifierToProperties = Maps.newConcurrentMap();
            identifierToProperties.put(identifier, icebergProperty);
            dbToTableIdentifiers.put(db, identifierToProperties);
        }
        LOG.info("Register a new table[{}] to database[{}]", identifier.name(), db.getFullName());
    }

    public void deregisterDb(Database db) {
        icebergDbs.remove(db.getId());
        dbToTableIdentifiers.remove(db);
        dbToTableToCreationRecord.remove(db.getId());
        LOG.info("Deregister database[{}-{}]", db.getFullName(), db.getId());
    }

    public void deregisterTable(Database db, IcebergTable table) {
        if (dbToTableIdentifiers.containsKey(db)) {
            TableIdentifier identifier = TableIdentifier.of(table.getIcebergDb(), table.getIcebergTbl());
            Map<TableIdentifier, IcebergProperty> identifierToProperties = dbToTableIdentifiers.get(db);
            identifierToProperties.remove(identifier);
        }
        if (dbToTableToCreationRecord.containsKey(db.getId())) {
            Map<Long, IcebergTableCreationRecord> recordMap = dbToTableToCreationRecord.get(db.getId());
            recordMap.remove(table.getId());
        }
        LOG.info("Deregister table[{}-{}] from database[{}-{}]", table.getName(),
                table.getId(), db.getFullName(), db.getId());
    }

    // remove already created tables or failed tables
    private void removeDuplicateTables() {
        for (Map.Entry<Long, Map<Long, IcebergTableCreationRecord>> entry : dbToTableToCreationRecord.entrySet()) {
            Catalog.getCurrentCatalog().getDb(entry.getKey()).ifPresent(db -> {
                if (dbToTableIdentifiers.containsKey(db)) {
                    for (Map.Entry<Long, IcebergTableCreationRecord> innerEntry : entry.getValue().entrySet()) {
                        String tableName = innerEntry.getValue().getTable();
                        String icebergDbName = db.getDbProperties().getIcebergProperty().getDatabase();
                        TableIdentifier identifier = TableIdentifier.of(icebergDbName, tableName);
                        dbToTableIdentifiers.get(db).remove(identifier);
                    }
                }
            });
        }
    }

    @Override
    protected void runAfterCatalogReady() {
        PropertySchema.DateProperty prop =
                new PropertySchema.DateProperty("key", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));
        // list iceberg tables in dbs
        // When listing table is done, remove database from icebergDbs.
        for (Iterator<Map.Entry<Long, Database>> it = icebergDbs.entrySet().iterator(); it.hasNext(); it.remove()) {
            Map.Entry<Long, Database> entry = it.next();
            Database db = entry.getValue();
            IcebergProperty icebergProperty = db.getDbProperties().getIcebergProperty();
            IcebergCatalog icebergCatalog = null;
            try {
                icebergCatalog = IcebergCatalogMgr.getCatalog(icebergProperty);
            } catch (DdlException e) {
                addTableCreationRecord(db.getId(), -1, db.getFullName(), "", FAIL,
                        prop.writeTimeFormat(new Date(System.currentTimeMillis())), e.getMessage());
                LOG.warn("Failed get Iceberg catalog, hive.metastore.uris[{}], error: {}",
                        icebergProperty.getHiveMetastoreUris(), e.getMessage());
            }
            List<TableIdentifier> icebergTables = null;
            try {
                icebergTables = icebergCatalog.listTables(icebergProperty.getDatabase());

            } catch (DorisIcebergException e) {
                addTableCreationRecord(db.getId(), -1, db.getFullName(), "", FAIL,
                        prop.writeTimeFormat(new Date(System.currentTimeMillis())), e.getMessage());
                LOG.warn("Failed list remote Iceberg database, hive.metastore.uris[{}], database[{}], error: {}",
                        icebergProperty.getHiveMetastoreUris(), icebergProperty.getDatabase(), e.getMessage());
            }
            for (TableIdentifier identifier : icebergTables) {
                IcebergProperty tableProperties = new IcebergProperty(icebergProperty);
                tableProperties.setTable(identifier.name());
                registerTable(db, identifier, tableProperties);
            }
        }

        // create table in Doris
        for (Map.Entry<Database, Map<TableIdentifier, IcebergProperty>> entry : dbToTableIdentifiers.entrySet()) {
            Database db = entry.getKey();
            for (Map.Entry<TableIdentifier, IcebergProperty> innerEntry : entry.getValue().entrySet()) {
                TableIdentifier identifier = innerEntry.getKey();
                IcebergProperty icebergProperty = innerEntry.getValue();
                long tableId = SystemIdGenerator.getNextId();
                try {
                    // get doris table from iceberg
                    IcebergTable table = IcebergCatalogMgr.getTableFromIceberg(tableId, identifier.name(),
                            icebergProperty, identifier, false);
                    // check iceberg table if exists in doris database
                    if (!db.createTableWithLock(table, false, false).first) {
                        ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_EXISTS_ERROR, table.getName());
                    }
                    addTableCreationRecord(db.getId(), tableId, db.getFullName(), table.getName(), SUCCESS,
                            prop.writeTimeFormat(new Date(System.currentTimeMillis())), "");
                    LOG.info("Successfully create table[{}-{}]", table.getName(), tableId);
                } catch (Exception e) {
                    addTableCreationRecord(db.getId(), tableId, db.getFullName(), identifier.name(), FAIL,
                            prop.writeTimeFormat(new Date(System.currentTimeMillis())), e.getMessage());
                    LOG.warn("Failed create table[{}], error: {}", identifier.name(), e.getMessage());
                }
            }
        }
        removeDuplicateTables();
    }

    private void addTableCreationRecord(long dbId, long tableId, String db, String table, String status,
                                        String createTime, String errorMsg) {
        writeLock();
        try {
            while (isQueueFull()) {
                IcebergTableCreationRecord record = tableCreationRecordQueue.poll();
                if (record != null) {
                    Map<Long, IcebergTableCreationRecord> tableRecords = dbToTableToCreationRecord.get(record.getDbId());
                    Iterator<Map.Entry<Long, IcebergTableCreationRecord>> tableRecordsIterator = tableRecords.entrySet().iterator();
                    while (tableRecordsIterator.hasNext()) {
                        long t = tableRecordsIterator.next().getKey();
                        if (t == record.getTableId()) {
                            tableRecordsIterator.remove();
                            break;
                        }
                    }
                }
            }

            IcebergTableCreationRecord record = new IcebergTableCreationRecord(dbId, tableId, db, table, status,
                    createTime, errorMsg);
            tableCreationRecordQueue.offer(record);

            if (!dbToTableToCreationRecord.containsKey(dbId)) {
                dbToTableToCreationRecord.put(dbId, new ConcurrentHashMap<>());
            }
            Map<Long, IcebergTableCreationRecord> tableToRecord = dbToTableToCreationRecord.get(dbId);
            if (!tableToRecord.containsKey(tableId)) {
                tableToRecord.put(tableId, record);
            }
        } finally {
            writeUnlock();
        }
    }

    public List<IcebergTableCreationRecord> getTableCreationRecordByDbId(long dbId) {
        List<IcebergTableCreationRecord> records = new ArrayList<>();

        readLock();
        try {
            if (!dbToTableToCreationRecord.containsKey(dbId)) {
                return records;
            }
            Map<Long, IcebergTableCreationRecord> tableToRecords = dbToTableToCreationRecord.get(dbId);
            for (Map.Entry<Long, IcebergTableCreationRecord> entry : tableToRecords.entrySet()) {
                records.add(entry.getValue());
            }

            return records;
        } finally {
            readUnlock();
        }
    }

    class TableCreationComparator implements Comparator<IcebergTableCreationRecord> {
        @Override
        public int compare(IcebergTableCreationRecord r1, IcebergTableCreationRecord r2) {
            return r1.getCreateTime().compareTo(r2.getCreateTime());
        }
    }

    public boolean isQueueFull() {
        return tableCreationRecordQueue.size() >= Config.max_iceberg_table_creation_record_size;
    }

    private void readLock() {
        lock.readLock().lock();
    }

    private void readUnlock() {
        lock.readLock().unlock();
    }

    private void writeLock() {
        lock.writeLock().lock();
    }

    private void writeUnlock() {
        lock.writeLock().unlock();
    }

}
