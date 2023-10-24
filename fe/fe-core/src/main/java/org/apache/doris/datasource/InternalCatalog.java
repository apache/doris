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

package org.apache.doris.datasource;

import org.apache.doris.analysis.AddPartitionClause;
import org.apache.doris.analysis.AddPartitionLikeClause;
import org.apache.doris.analysis.AddRollupClause;
import org.apache.doris.analysis.AlterClause;
import org.apache.doris.analysis.AlterDatabasePropertyStmt;
import org.apache.doris.analysis.AlterDatabaseQuotaStmt;
import org.apache.doris.analysis.AlterDatabaseQuotaStmt.QuotaType;
import org.apache.doris.analysis.AlterDatabaseRename;
import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.ColumnDef;
import org.apache.doris.analysis.ColumnDef.DefaultValue;
import org.apache.doris.analysis.CreateDbStmt;
import org.apache.doris.analysis.CreateTableAsSelectStmt;
import org.apache.doris.analysis.CreateTableLikeStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.analysis.DataSortInfo;
import org.apache.doris.analysis.DistributionDesc;
import org.apache.doris.analysis.DropDbStmt;
import org.apache.doris.analysis.DropPartitionClause;
import org.apache.doris.analysis.DropTableStmt;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.HashDistributionDesc;
import org.apache.doris.analysis.KeysDesc;
import org.apache.doris.analysis.PartitionDesc;
import org.apache.doris.analysis.PartitionKeyDesc;
import org.apache.doris.analysis.QueryStmt;
import org.apache.doris.analysis.RecoverDbStmt;
import org.apache.doris.analysis.RecoverPartitionStmt;
import org.apache.doris.analysis.RecoverTableStmt;
import org.apache.doris.analysis.SinglePartitionDesc;
import org.apache.doris.analysis.TableName;
import org.apache.doris.analysis.TableRef;
import org.apache.doris.analysis.TruncateTableStmt;
import org.apache.doris.analysis.TypeDef;
import org.apache.doris.catalog.BinlogConfig;
import org.apache.doris.catalog.BrokerTable;
import org.apache.doris.catalog.ColocateGroupSchema;
import org.apache.doris.catalog.ColocateTableIndex;
import org.apache.doris.catalog.ColocateTableIndex.GroupId;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DataProperty;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Database.DbState;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.DatabaseProperty;
import org.apache.doris.catalog.DistributionInfo;
import org.apache.doris.catalog.DistributionInfo.DistributionInfoType;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.EsTable;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.HiveTable;
import org.apache.doris.catalog.IcebergTable;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.InfoSchemaDb;
import org.apache.doris.catalog.JdbcTable;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.MaterializedIndex.IndexState;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.MaterializedView;
import org.apache.doris.catalog.MetaIdGenerator.IdGeneratorBuffer;
import org.apache.doris.catalog.MysqlCompatibleDatabase;
import org.apache.doris.catalog.MysqlDb;
import org.apache.doris.catalog.MysqlTable;
import org.apache.doris.catalog.OdbcTable;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.OlapTable.OlapTableState;
import org.apache.doris.catalog.OlapTableFactory;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.RandomDistributionInfo;
import org.apache.doris.catalog.RangePartitionItem;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Replica.ReplicaState;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.SinglePartitionInfo;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.catalog.Type;
import org.apache.doris.catalog.View;
import org.apache.doris.clone.DynamicPartitionScheduler;
import org.apache.doris.cluster.Cluster;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.MarkedCountDownLatch;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.CountingDataOutputStream;
import org.apache.doris.common.util.DbUtil;
import org.apache.doris.common.util.DynamicPartitionUtil;
import org.apache.doris.common.util.IdGeneratorUtil;
import org.apache.doris.common.util.MetaLockUtils;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.common.util.QueryableReentrantLock;
import org.apache.doris.common.util.SqlParserUtils;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.hive.PooledHiveMetaStoreClient;
import org.apache.doris.datasource.property.constants.HMSProperties;
import org.apache.doris.external.elasticsearch.EsRepository;
import org.apache.doris.external.iceberg.IcebergCatalogMgr;
import org.apache.doris.external.iceberg.IcebergTableCreationRecordMgr;
import org.apache.doris.mtmv.MTMVJobFactory;
import org.apache.doris.mtmv.metadata.MTMVJob;
import org.apache.doris.persist.AlterDatabasePropertyInfo;
import org.apache.doris.persist.AutoIncrementIdUpdateLog;
import org.apache.doris.persist.ColocatePersistInfo;
import org.apache.doris.persist.DatabaseInfo;
import org.apache.doris.persist.DropDbInfo;
import org.apache.doris.persist.DropInfo;
import org.apache.doris.persist.DropPartitionInfo;
import org.apache.doris.persist.PartitionPersistInfo;
import org.apache.doris.persist.RecoverInfo;
import org.apache.doris.persist.ReplicaPersistInfo;
import org.apache.doris.persist.TruncateTableInfo;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.task.CreateReplicaTask;
import org.apache.doris.thrift.TCompressionType;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TStorageFormat;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.thrift.TStorageType;
import org.apache.doris.thrift.TTabletType;
import org.apache.doris.thrift.TTaskType;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import lombok.Getter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


/**
 * The Internal catalog will manage all self-managed meta object in a Doris cluster.
 * Such as Database, tables, etc.
 * There is only one internal catalog in a cluster. And its id is always 0.
 */
public class InternalCatalog implements CatalogIf<Database> {
    public static final String INTERNAL_CATALOG_NAME = "internal";
    public static final long INTERNAL_CATALOG_ID = 0L;

    private static final Logger LOG = LogManager.getLogger(InternalCatalog.class);

    private QueryableReentrantLock lock = new QueryableReentrantLock(true);
    private ConcurrentHashMap<Long, Database> idToDb = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, Database> fullNameToDb = new ConcurrentHashMap<>();

    // Add transient to fix gson issue.
    @Getter
    private transient EsRepository esRepository = new EsRepository();
    @Getter
    private IcebergTableCreationRecordMgr icebergTableCreationRecordMgr = new IcebergTableCreationRecordMgr();

    public InternalCatalog() {
        // create internal databases
        List<MysqlCompatibleDatabase> mysqlCompatibleDatabases = new ArrayList<>();
        mysqlCompatibleDatabases.add(new InfoSchemaDb(SystemInfoService.DEFAULT_CLUSTER));
        mysqlCompatibleDatabases.add(new MysqlDb(SystemInfoService.DEFAULT_CLUSTER));
        MysqlCompatibleDatabase.COUNT = 2;

        for (MysqlCompatibleDatabase idb : mysqlCompatibleDatabases) {
            // do not call unprotectedCreateDb, because it will cause loop recursive when initializing Env singleton
            idToDb.put(idb.getId(), idb);
            fullNameToDb.put(idb.getFullName(), idb);
        }
    }

    @Override
    public String getType() {
        return "internal";
    }

    @Override
    public long getId() {
        return INTERNAL_CATALOG_ID;
    }

    @Override
    public String getName() {
        return INTERNAL_CATALOG_NAME;
    }

    @Override
    public List<String> getDbNames() {
        return Lists.newArrayList(fullNameToDb.keySet());
    }

    public List<Long> getDbIds() {
        return Lists.newArrayList(idToDb.keySet());
    }

    @Nullable
    @Override
    public Database getDbNullable(String dbName) {
        if (StringUtils.isEmpty(dbName)) {
            return null;
        }
        if (fullNameToDb.containsKey(dbName)) {
            return fullNameToDb.get(dbName);
        } else {
            // This maybe a information_schema db request, and information_schema db name is case insensitive.
            // So, we first extract db name to check if it is information_schema.
            // Then we reassemble the origin cluster name with lower case db name,
            // and finally get information_schema db from the name map.
            String fullName = ClusterNamespace.getNameFromFullName(dbName);
            if (fullName.equalsIgnoreCase(InfoSchemaDb.DATABASE_NAME)) {
                String clusterName = ClusterNamespace.getClusterNameFromFullName(dbName);
                fullName = ClusterNamespace.getFullName(clusterName, fullName.toLowerCase());
                // If the fullname is not valid, ClusterNamespace.getFullName may return null
                // and fullNameToDb.get(fullName) may throw null pointer exception
                if (fullName == null) {
                    return null;
                }
                return fullNameToDb.get(fullName);
            }
        }
        return null;
    }

    @Nullable
    @Override
    public Database getDbNullable(long dbId) {
        return idToDb.get(dbId);
    }

    @Override
    public Map<String, String> getProperties() {
        return Maps.newHashMap();
    }

    @Override
    public void modifyCatalogName(String name) {
        LOG.warn("Ignore the modify catalog name in build-in catalog.");
    }

    @Override
    public void modifyCatalogProps(Map<String, String> props) {
        LOG.warn("Ignore the modify catalog props in build-in catalog.");
    }

    @Override
    public String getComment() {
        return "Doris internal catalog";
    }

    public TableName getTableNameByTableId(Long tableId) {
        for (Database db : fullNameToDb.values()) {
            Table table = db.getTableNullable(tableId);
            if (table != null) {
                return new TableName("", db.getFullName(), table.getName());
            }
        }
        return null;
    }

    public Table getTableByTableId(Long tableId) {
        for (Database db : fullNameToDb.values()) {
            Table table = db.getTableNullable(tableId);
            if (table != null) {
                return table;
            }
        }
        return null;
    }

    // Use tryLock to avoid potential dead lock
    private boolean tryLock(boolean mustLock) {
        while (true) {
            try {
                if (!lock.tryLock(Config.catalog_try_lock_timeout_ms, TimeUnit.MILLISECONDS)) {
                    if (LOG.isDebugEnabled()) {
                        // to see which thread held this lock for long time.
                        Thread owner = lock.getOwner();
                        if (owner != null) {
                            // There are many catalog timeout during regression test
                            // And this timeout should not happen very often, so it could be info log
                            LOG.info("catalog lock is held by: {}", Util.dumpThread(owner, 10));
                        }
                    }

                    if (mustLock) {
                        continue;
                    } else {
                        return false;
                    }
                }
                return true;
            } catch (InterruptedException e) {
                LOG.warn("got exception while getting catalog lock", e);
                if (mustLock) {
                    continue;
                } else {
                    return lock.isHeldByCurrentThread();
                }
            }
        }
    }

    public List<Database> getDbs() {
        return Lists.newArrayList(idToDb.values());
    }

    private void unlock() {
        if (lock.isHeldByCurrentThread()) {
            this.lock.unlock();
        }
    }

    /**
     * create the tablet inverted index from metadata.
     */
    public void recreateTabletInvertIndex() {
        // create inverted index
        TabletInvertedIndex invertedIndex = Env.getCurrentInvertedIndex();
        for (Database db : this.fullNameToDb.values()) {
            long dbId = db.getId();
            for (Table table : db.getTables()) {
                if (!table.isManagedTable()) {
                    continue;
                }

                OlapTable olapTable = (OlapTable) table;
                long tableId = olapTable.getId();
                Collection<Partition> allPartitions = olapTable.getAllPartitions();
                for (Partition partition : allPartitions) {
                    long partitionId = partition.getId();
                    TStorageMedium medium = olapTable.getPartitionInfo().getDataProperty(partitionId)
                            .getStorageMedium();
                    for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.ALL)) {
                        long indexId = index.getId();
                        int schemaHash = olapTable.getSchemaHashByIndexId(indexId);
                        for (Tablet tablet : index.getTablets()) {
                            TabletMeta tabletMeta = new TabletMeta(dbId, tableId, partitionId, indexId, schemaHash,
                                    medium);
                            long tabletId = tablet.getId();
                            invertedIndex.addTablet(tabletId, tabletMeta);
                            for (Replica replica : tablet.getReplicas()) {
                                invertedIndex.addReplica(tabletId, replica);
                            }
                        }
                    } // end for indices
                } // end for partitions
            } // end for tables
        } // end for dbs
    }

    /**
     * Entry of creating a database.
     *
     * @param stmt
     * @throws DdlException
     */
    public void createDb(CreateDbStmt stmt) throws DdlException {
        String fullDbName = stmt.getFullDbName();
        Map<String, String> properties = stmt.getProperties();

        long id = Env.getCurrentEnv().getNextId();
        Database db = new Database(id, fullDbName);
        db.setClusterName(SystemInfoService.DEFAULT_CLUSTER);
        // check and analyze database properties before create database
        db.setDbProperties(new DatabaseProperty(properties).checkAndBuildProperties());

        if (!tryLock(false)) {
            throw new DdlException("Failed to acquire catalog lock. Try again");
        }
        try {
            if (fullNameToDb.containsKey(fullDbName)) {
                if (stmt.isSetIfNotExists()) {
                    LOG.info("create database[{}] which already exists", fullDbName);
                    return;
                } else {
                    ErrorReport.reportDdlException(ErrorCode.ERR_DB_CREATE_EXISTS, fullDbName);
                }
            } else {
                unprotectCreateDb(db);
                Env.getCurrentEnv().getEditLog().logCreateDb(db);
            }
        } finally {
            unlock();
        }
        LOG.info("createDb dbName = " + fullDbName + ", id = " + id);

        // create tables in iceberg database
        if (db.getDbProperties().getIcebergProperty().isExist()) {
            icebergTableCreationRecordMgr.registerDb(db);
        }
    }

    /**
     * For replaying creating database.
     *
     * @param db
     */
    public void unprotectCreateDb(Database db) {
        idToDb.put(db.getId(), db);
        fullNameToDb.put(db.getFullName(), db);
        Env.getCurrentGlobalTransactionMgr().addDatabaseTransactionMgr(db.getId());
    }

    /**
     * replayCreateDb.
     *
     * @param db
     */
    public void replayCreateDb(Database db, String newDbName) {
        tryLock(true);
        try {
            if (!Strings.isNullOrEmpty(newDbName)) {
                db.setNameWithLock(newDbName);
            }
            unprotectCreateDb(db);
        } finally {
            unlock();
        }
    }

    public void dropDb(DropDbStmt stmt) throws DdlException {
        String dbName = stmt.getDbName();

        // 1. check if database exists
        if (!tryLock(false)) {
            throw new DdlException("Failed to acquire catalog lock. Try again");
        }
        try {
            if (!fullNameToDb.containsKey(dbName)) {
                if (stmt.isSetIfExists()) {
                    LOG.info("drop database[{}] which does not exist", dbName);
                    return;
                } else {
                    ErrorReport.reportDdlException(ErrorCode.ERR_DB_DROP_EXISTS, dbName);
                }
            }

            // 2. drop tables in db
            Database db = this.fullNameToDb.get(dbName);
            db.writeLock();
            long recycleTime = 0;
            try {
                if (!stmt.isForceDrop()) {
                    if (Env.getCurrentEnv().getGlobalTransactionMgr().existCommittedTxns(db.getId(), null, null)) {
                        throw new DdlException(
                                "There are still some transactions in the COMMITTED state waiting to be completed. "
                                        + "The database [" + dbName
                                        + "] cannot be dropped. If you want to forcibly drop(cannot be recovered),"
                                        + " please use \"DROP database FORCE\".");
                    }
                }

                // save table names for recycling
                Set<String> tableNames = db.getTableNamesWithLock();
                List<Table> tableList = db.getTablesOnIdOrder();
                Set<Long> tableIds = Sets.newHashSet();
                for (Table table : tableList) {
                    tableIds.add(table.getId());
                }
                MetaLockUtils.writeLockTables(tableList);
                try {
                    if (!stmt.isForceDrop()) {
                        for (Table table : tableList) {
                            if (table.isManagedTable()) {
                                OlapTable olapTable = (OlapTable) table;
                                if (olapTable.getState() != OlapTableState.NORMAL) {
                                    throw new DdlException("The table [" + olapTable.getState() + "]'s state is "
                                            + olapTable.getState() + ", cannot be dropped."
                                            + " please cancel the operation on olap table firstly."
                                            + " If you want to forcibly drop(cannot be recovered),"
                                            + " please use \"DROP table FORCE\".");
                                }
                            }
                        }
                    }
                    unprotectDropDb(db, stmt.isForceDrop(), false, 0);
                } finally {
                    MetaLockUtils.writeUnlockTables(tableList);
                }

                if (!stmt.isForceDrop()) {
                    Env.getCurrentRecycleBin().recycleDatabase(db, tableNames, tableIds, false, 0);
                    recycleTime = Env.getCurrentRecycleBin().getRecycleTimeById(db.getId());
                } else {
                    Env.getCurrentEnv().eraseDatabase(db.getId(), false);
                }
            } finally {
                db.writeUnlock();
            }

            // 3. remove db from catalog
            idToDb.remove(db.getId());
            fullNameToDb.remove(db.getFullName());
            DropDbInfo info = new DropDbInfo(dbName, stmt.isForceDrop(), recycleTime);
            Env.getCurrentEnv().getEditLog().logDropDb(info);
            Env.getCurrentEnv().getQueryStats().clear(Env.getCurrentEnv().getCurrentCatalog().getId(), db.getId());
        } finally {
            unlock();
        }

        LOG.info("finish drop database[{}], is force : {}", dbName, stmt.isForceDrop());
    }

    public void unprotectDropDb(Database db, boolean isForeDrop, boolean isReplay, long recycleTime) {
        // drop Iceberg database table creation records
        if (db.getDbProperties().getIcebergProperty().isExist()) {
            icebergTableCreationRecordMgr.deregisterDb(db);
        }
        for (Table table : db.getTables()) {
            unprotectDropTable(db, table, isForeDrop, isReplay, recycleTime);
        }
        db.markDropped();
    }

    public void replayDropDb(String dbName, boolean isForceDrop, Long recycleTime) throws DdlException {
        tryLock(true);
        try {
            Database db = fullNameToDb.get(dbName);
            db.writeLock();
            try {
                Set<String> tableNames = db.getTableNamesWithLock();
                List<Table> tableList = db.getTablesOnIdOrder();
                Set<Long> tableIds = Sets.newHashSet();
                for (Table table : tableList) {
                    tableIds.add(table.getId());
                }
                MetaLockUtils.writeLockTables(tableList);
                try {
                    unprotectDropDb(db, isForceDrop, true, recycleTime);
                } finally {
                    MetaLockUtils.writeUnlockTables(tableList);
                }
                if (!isForceDrop) {
                    Env.getCurrentRecycleBin().recycleDatabase(db, tableNames, tableIds, true, recycleTime);
                } else {
                    Env.getCurrentEnv().eraseDatabase(db.getId(), false);
                }
                Env.getCurrentEnv().getQueryStats().clear(Env.getCurrentEnv().getInternalCatalog().getId(), db.getId());
            } finally {
                db.writeUnlock();
            }

            fullNameToDb.remove(dbName);
            idToDb.remove(db.getId());
        } finally {
            unlock();
        }
    }

    public void recoverDatabase(RecoverDbStmt recoverStmt) throws DdlException {
        // check is new db with same name already exist
        String newDbName = recoverStmt.getNewDbName();
        if (!Strings.isNullOrEmpty(newDbName)) {
            if (getDb(newDbName).isPresent()) {
                throw new DdlException("Database[" + newDbName + "] already exist.");
            }
        } else {
            if (getDb(recoverStmt.getDbName()).isPresent()) {
                throw new DdlException("Database[" + recoverStmt.getDbName() + "] already exist.");
            }
        }

        Database db = Env.getCurrentRecycleBin().recoverDatabase(recoverStmt.getDbName(), recoverStmt.getDbId());

        // add db to catalog
        if (!tryLock(false)) {
            throw new DdlException("Failed to acquire catalog lock. Try again");
        }
        db.writeLock();
        List<Table> tableList = db.getTablesOnIdOrder();
        MetaLockUtils.writeLockTables(tableList);
        try {
            if (!Strings.isNullOrEmpty(newDbName)) {
                if (fullNameToDb.containsKey(newDbName)) {
                    throw new DdlException("Database[" + newDbName + "] already exist.");
                    // it's ok that we do not put db back to CatalogRecycleBin
                    // cause this db cannot recover any more
                }
            } else {
                if (fullNameToDb.containsKey(db.getFullName())) {
                    throw new DdlException("Database[" + db.getFullName() + "] already exist.");
                    // it's ok that we do not put db back to CatalogRecycleBin
                    // cause this db cannot recover any more
                }
            }
            if (!Strings.isNullOrEmpty(newDbName)) {
                try {
                    db.writeUnlock();
                    db.setNameWithLock(newDbName);
                } finally {
                    db.writeLock();
                }
            }
            fullNameToDb.put(db.getFullName(), db);
            idToDb.put(db.getId(), db);
            // log
            RecoverInfo recoverInfo = new RecoverInfo(db.getId(), -1L, -1L, newDbName, "", "");
            Env.getCurrentEnv().getEditLog().logRecoverDb(recoverInfo);
            db.unmarkDropped();
        } finally {
            MetaLockUtils.writeUnlockTables(tableList);
            db.writeUnlock();
            unlock();
        }

        LOG.info("recover database[{}]", db.getId());
    }

    public void recoverTable(RecoverTableStmt recoverStmt) throws DdlException {
        String dbName = recoverStmt.getDbName();
        String tableName = recoverStmt.getTableName();
        String newTableName = recoverStmt.getNewTableName();

        Database db = (Database) getDbOrDdlException(dbName);
        db.writeLockOrDdlException();
        try {
            if (Strings.isNullOrEmpty(newTableName)) {
                if (db.getTable(tableName).isPresent()) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_EXISTS_ERROR, tableName);
                }
            } else {
                if (db.getTable(newTableName).isPresent()) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_EXISTS_ERROR, newTableName);
                }
            }
            if (!Env.getCurrentRecycleBin().recoverTable(db, tableName, recoverStmt.getTableId(), newTableName)) {
                ErrorReport.reportDdlException(ErrorCode.ERR_UNKNOWN_TABLE, tableName, dbName);
            }
        } finally {
            db.writeUnlock();
        }
    }

    public void recoverPartition(RecoverPartitionStmt recoverStmt) throws DdlException {
        String dbName = recoverStmt.getDbName();
        String tableName = recoverStmt.getTableName();

        Database db = getDbOrDdlException(dbName);
        OlapTable olapTable = db.getOlapTableOrDdlException(tableName);
        olapTable.writeLockOrDdlException();
        try {
            String partitionName = recoverStmt.getPartitionName();
            String newPartitionName = recoverStmt.getNewPartitionName();
            if (Strings.isNullOrEmpty(newPartitionName)) {
                if (olapTable.getPartition(partitionName) != null) {
                    throw new DdlException("partition[" + partitionName + "] "
                            + "already exist in table[" + tableName + "]");
                }
            } else {
                if (olapTable.getPartition(newPartitionName) != null) {
                    throw new DdlException("partition[" + newPartitionName + "] "
                            + "already exist in table[" + tableName + "]");
                }
            }

            Env.getCurrentRecycleBin().recoverPartition(db.getId(), olapTable, partitionName,
                    recoverStmt.getPartitionId(), newPartitionName);
        } finally {
            olapTable.writeUnlock();
        }
    }

    public void replayEraseDatabase(long dbId) throws DdlException {
        Env.getCurrentRecycleBin().replayEraseDatabase(dbId);
    }

    public void replayRecoverDatabase(RecoverInfo info) {
        long dbId = info.getDbId();
        String newDbName = info.getNewDbName();
        Database db = Env.getCurrentRecycleBin().replayRecoverDatabase(dbId);

        // add db to catalog
        replayCreateDb(db, newDbName);
        db.unmarkDropped();
        LOG.info("replay recover db[{}]", dbId);
    }

    public void alterDatabaseQuota(AlterDatabaseQuotaStmt stmt) throws DdlException {
        String dbName = stmt.getDbName();
        Database db = (Database) getDbOrDdlException(dbName);
        QuotaType quotaType = stmt.getQuotaType();
        db.writeLockOrDdlException();
        try {
            if (quotaType == QuotaType.DATA) {
                db.setDataQuota(stmt.getQuota());
            } else if (quotaType == QuotaType.REPLICA) {
                db.setReplicaQuota(stmt.getQuota());
            } else if (quotaType == QuotaType.TRANSACTION) {
                db.setTransactionQuotaSize(stmt.getQuota());
            }
            long quota = stmt.getQuota();
            DatabaseInfo dbInfo = new DatabaseInfo(dbName, "", quota, quotaType);
            Env.getCurrentEnv().getEditLog().logAlterDb(dbInfo);
        } finally {
            db.writeUnlock();
        }
    }

    public void replayAlterDatabaseQuota(String dbName, long quota, QuotaType quotaType) throws MetaNotFoundException {
        Database db = (Database) getDbOrMetaException(dbName);
        db.writeLock();
        try {
            if (quotaType == QuotaType.DATA) {
                db.setDataQuota(quota);
            } else if (quotaType == QuotaType.REPLICA) {
                db.setReplicaQuota(quota);
            } else if (quotaType == QuotaType.TRANSACTION) {
                db.setTransactionQuotaSize(quota);
            }
        } finally {
            db.writeUnlock();
        }
    }

    public void alterDatabaseProperty(AlterDatabasePropertyStmt stmt) throws DdlException {
        String dbName = stmt.getDbName();
        Database db = (Database) getDbOrDdlException(dbName);
        long dbId = db.getId();
        Map<String, String> properties = stmt.getProperties();

        db.writeLockOrDdlException();
        try {
            boolean update = db.updateDbProperties(properties);
            if (!update) {
                return;
            }

            AlterDatabasePropertyInfo info = new AlterDatabasePropertyInfo(dbId, dbName, properties);
            Env.getCurrentEnv().getEditLog().logAlterDatabaseProperty(info);
        } finally {
            db.writeUnlock();
        }
    }

    public void replayAlterDatabaseProperty(String dbName, Map<String, String> properties)
            throws MetaNotFoundException {
        Database db = (Database) getDbOrMetaException(dbName);
        db.writeLock();
        try {
            db.replayUpdateDbProperties(properties);
        } finally {
            db.writeUnlock();
        }
    }

    public void renameDatabase(AlterDatabaseRename stmt) throws DdlException {
        String fullDbName = stmt.getDbName();
        String newFullDbName = stmt.getNewDbName();

        if (fullDbName.equals(newFullDbName)) {
            throw new DdlException("Same database name");
        }

        Database db = null;
        if (!tryLock(false)) {
            throw new DdlException("Failed to acquire catalog lock. Try again");
        }
        try {
            // check if db exists
            db = fullNameToDb.get(fullDbName);
            if (db == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, fullDbName);
            }

            if (db.getDbState() == DbState.LINK || db.getDbState() == DbState.MOVE) {
                ErrorReport.reportDdlException(ErrorCode.ERR_CLUSTER_RENAME_DB_ERR, fullDbName);
            }
            // check if name is already used
            if (fullNameToDb.get(newFullDbName) != null) {
                throw new DdlException("Database name[" + newFullDbName + "] is already used");
            }
            // 1. rename db
            db.setNameWithLock(newFullDbName);

            // 2. add to meta. check again
            fullNameToDb.remove(fullDbName);
            fullNameToDb.put(newFullDbName, db);

            DatabaseInfo dbInfo = new DatabaseInfo(fullDbName, newFullDbName, -1L, QuotaType.NONE);
            Env.getCurrentEnv().getEditLog().logDatabaseRename(dbInfo);
        } finally {
            unlock();
        }

        LOG.info("rename database[{}] to [{}]", fullDbName, newFullDbName);
    }

    public void replayRenameDatabase(String dbName, String newDbName) {
        tryLock(true);
        try {
            Database db = fullNameToDb.get(dbName);
            db.setName(newDbName);
            fullNameToDb.remove(dbName);
            fullNameToDb.put(newDbName, db);
        } finally {
            unlock();
        }

        LOG.info("replay rename database {} to {}", dbName, newDbName);
    }

    // Drop table
    public void dropTable(DropTableStmt stmt) throws DdlException {
        String dbName = stmt.getDbName();
        String tableName = stmt.getTableName();

        // check database
        Database db = (Database) getDbOrDdlException(dbName);
        if (db.isMysqlCompatibleDatabase()) {
            throw new DdlException("Drop table from this database is not allowed.");
        }

        db.writeLockOrDdlException();
        try {
            Table table = db.getTableNullable(tableName);
            if (table == null) {
                if (stmt.isSetIfExists()) {
                    LOG.info("drop table[{}] which does not exist", tableName);
                    return;
                } else {
                    ErrorReport.reportDdlException(ErrorCode.ERR_UNKNOWN_TABLE, tableName, dbName);
                }
            }
            // Check if a view
            if (stmt.isView()) {
                if (!(table instanceof View)) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_WRONG_OBJECT, dbName, tableName, "VIEW");
                }
            } else {
                if (table instanceof View || (!stmt.isMaterializedView() && table instanceof MaterializedView)) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_WRONG_OBJECT, dbName, tableName, "TABLE");
                }
            }

            if (!stmt.isForceDrop()) {
                if (Env.getCurrentEnv().getGlobalTransactionMgr().existCommittedTxns(db.getId(), table.getId(), null)) {
                    throw new DdlException(
                            "There are still some transactions in the COMMITTED state waiting to be completed. "
                                    + "The table [" + tableName
                                    + "] cannot be dropped. If you want to forcibly drop(cannot be recovered),"
                                    + " please use \"DROP table FORCE\".");
                }
            }
            table.writeLock();
            long recycleTime = 0;
            try {
                if (table instanceof OlapTable && !stmt.isForceDrop()) {
                    OlapTable olapTable = (OlapTable) table;
                    if ((olapTable.getState() != OlapTableState.NORMAL)) {
                        throw new DdlException("The table [" + tableName + "]'s state is " + olapTable.getState()
                                + ", cannot be dropped." + " please cancel the operation on olap table firstly."
                                + " If you want to forcibly drop(cannot be recovered),"
                                + " please use \"DROP table FORCE\".");
                    }
                }
                unprotectDropTable(db, table, stmt.isForceDrop(), false, 0);
                if (!stmt.isForceDrop()) {
                    recycleTime = Env.getCurrentRecycleBin().getRecycleTimeById(table.getId());
                }
            } finally {
                table.writeUnlock();
            }
            DropInfo info = new DropInfo(db.getId(), table.getId(), tableName, -1L, stmt.isForceDrop(), recycleTime);
            Env.getCurrentEnv().getEditLog().logDropTable(info);
            Env.getCurrentEnv().getQueryStats().clear(Env.getCurrentEnv().getCurrentCatalog().getId(),
                    db.getId(), table.getId());
            Env.getCurrentEnv().getAnalysisManager().removeTableStats(table.getId());
        } finally {
            db.writeUnlock();
        }
        LOG.info("finished dropping table: {} from db: {}, is force: {}", tableName, dbName, stmt.isForceDrop());
    }

    public boolean unprotectDropTable(Database db, Table table, boolean isForceDrop, boolean isReplay,
            long recycleTime) {
        if (table.getType() == TableType.ELASTICSEARCH) {
            esRepository.deRegisterTable(table.getId());
        } else if (table.getType() == TableType.OLAP) {
            // drop all temp partitions of this table, so that there is no temp partitions in recycle bin,
            // which make things easier.
            ((OlapTable) table).dropAllTempPartitions();
        } else if (table.getType() == TableType.ICEBERG) {
            // drop Iceberg database table creation record
            icebergTableCreationRecordMgr.deregisterTable(db, (IcebergTable) table);
        }

        db.dropTable(table.getName());
        if (!isForceDrop) {
            Env.getCurrentRecycleBin().recycleTable(db.getId(), table, isReplay, recycleTime);
        } else {
            if (table.getType() == TableType.OLAP) {
                Env.getCurrentEnv().onEraseOlapTable((OlapTable) table, isReplay);
            }
        }

        if (table instanceof MaterializedView) {
            List<Long> dropIds = Env.getCurrentEnv().getMTMVJobManager().showJobs(db.getFullName(), table.getName())
                    .stream().map(MTMVJob::getId).collect(Collectors.toList());
            Env.getCurrentEnv().getMTMVJobManager().dropJobs(dropIds, isReplay);
            LOG.info("Drop related {} mv job.", dropIds.size());
        }
        LOG.info("finished dropping table[{}] in db[{}]", table.getName(), db.getFullName());
        return true;
    }

    public void replayDropTable(Database db, long tableId, boolean isForceDrop,
            Long recycleTime) throws MetaNotFoundException {
        Table table = db.getTableOrMetaException(tableId);
        db.writeLock();
        table.writeLock();
        try {
            unprotectDropTable(db, table, isForceDrop, true, recycleTime);
            Env.getCurrentEnv().getQueryStats().clear(Env.getCurrentInternalCatalog().getId(), db.getId(),
                    tableId);
        } finally {
            table.writeUnlock();
            db.writeUnlock();
        }
    }

    public void replayEraseTable(long tableId) {
        Env.getCurrentRecycleBin().replayEraseTable(tableId);
    }

    public void replayRecoverTable(RecoverInfo info) throws MetaNotFoundException, DdlException {
        Database db = (Database) getDbOrMetaException(info.getDbId());
        db.writeLockOrDdlException();
        try {
            Env.getCurrentRecycleBin().replayRecoverTable(db, info.getTableId(), info.getNewTableName());
        } finally {
            db.writeUnlock();
        }
    }

    private void unprotectAddReplica(OlapTable olapTable, ReplicaPersistInfo info) {
        LOG.debug("replay add a replica {}", info);
        Partition partition = olapTable.getPartition(info.getPartitionId());
        MaterializedIndex materializedIndex = partition.getIndex(info.getIndexId());
        Tablet tablet = materializedIndex.getTablet(info.getTabletId());

        // for compatibility
        int schemaHash = info.getSchemaHash();
        if (schemaHash == -1) {
            schemaHash = olapTable.getSchemaHashByIndexId(info.getIndexId());
        }

        Replica replica = new Replica(info.getReplicaId(), info.getBackendId(), info.getVersion(), schemaHash,
                info.getDataSize(),
                info.getRemoteDataSize(), info.getRowCount(), ReplicaState.NORMAL, info.getLastFailedVersion(),
                info.getLastSuccessVersion());
        tablet.addReplica(replica);
    }

    private void unprotectUpdateReplica(OlapTable olapTable, ReplicaPersistInfo info) {
        LOG.debug("replay update a replica {}", info);
        Partition partition = olapTable.getPartition(info.getPartitionId());
        MaterializedIndex materializedIndex = partition.getIndex(info.getIndexId());
        Tablet tablet = materializedIndex.getTablet(info.getTabletId());
        Replica replica = tablet.getReplicaByBackendId(info.getBackendId());
        Preconditions.checkNotNull(replica, info);
        replica.updateVersionInfo(info.getVersion(), info.getDataSize(), info.getRemoteDataSize(), info.getRowCount());
        replica.setBad(false);
    }

    public void replayAddReplica(ReplicaPersistInfo info) throws MetaNotFoundException {
        Database db = (Database) getDbOrMetaException(info.getDbId());
        OlapTable olapTable = (OlapTable) db.getTableOrMetaException(info.getTableId(), TableType.OLAP);
        olapTable.writeLock();
        try {
            unprotectAddReplica(olapTable, info);
        } finally {
            olapTable.writeUnlock();
        }
    }

    public void replayUpdateReplica(ReplicaPersistInfo info) throws MetaNotFoundException {
        Database db = (Database) getDbOrMetaException(info.getDbId());
        OlapTable olapTable = (OlapTable) db.getTableOrMetaException(info.getTableId(), TableType.OLAP);
        olapTable.writeLock();
        try {
            unprotectUpdateReplica(olapTable, info);
        } finally {
            olapTable.writeUnlock();
        }
    }

    public void unprotectDeleteReplica(OlapTable olapTable, ReplicaPersistInfo info) {
        Partition partition = olapTable.getPartition(info.getPartitionId());
        MaterializedIndex materializedIndex = partition.getIndex(info.getIndexId());
        Tablet tablet = materializedIndex.getTablet(info.getTabletId());
        tablet.deleteReplicaByBackendId(info.getBackendId());
    }

    public void replayDeleteReplica(ReplicaPersistInfo info) throws MetaNotFoundException {
        Database db = (Database) getDbOrMetaException(info.getDbId());
        OlapTable olapTable = (OlapTable) db.getTableOrMetaException(info.getTableId(), TableType.OLAP);
        olapTable.writeLock();
        try {
            unprotectDeleteReplica(olapTable, info);
        } finally {
            olapTable.writeUnlock();
        }
    }

    /**
     * Following is the step to create an olap table:
     * 1. create columns
     * 2. create partition info
     * 3. create distribution info
     * 4. set table id and base index id
     * 5. set bloom filter columns
     * 6. set and build TableProperty includes:
     * 6.1. dynamicProperty
     * 6.2. replicationNum
     * 6.3. inMemory
     * 6.4. storageFormat
     * 6.5. compressionType
     * 7. set index meta
     * 8. check colocation properties
     * 9. create tablet in BE
     * 10. add this table to FE's meta
     * 11. add this table to ColocateGroup if necessary
     */
    public void createTable(CreateTableStmt stmt) throws UserException {
        String engineName = stmt.getEngineName();
        String dbName = stmt.getDbName();
        String tableName = stmt.getTableName();

        // check if db exists
        Database db = getDbOrDdlException(dbName);
        // InfoSchemaDb and MysqlDb can not create table manually
        if (db.isMysqlCompatibleDatabase()) {
            ErrorReport.reportDdlException(ErrorCode.ERR_CANT_CREATE_TABLE, tableName,
                    ErrorCode.ERR_CANT_CREATE_TABLE.getCode(), "not supported create table in this database");
        }

        // only internal table should check quota and cluster capacity
        if (!stmt.isExternal()) {
            // check cluster capacity
            Env.getCurrentSystemInfo().checkAvailableCapacity();
            // check db quota
            db.checkQuota();
        }

        // check if table exists in db
        if (db.getTable(tableName).isPresent()) {
            if (stmt.isSetIfNotExists()) {
                LOG.info("create table[{}] which already exists", tableName);
                return;
            } else {
                ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_EXISTS_ERROR, tableName);
            }
        }

        if (engineName.equals("olap")) {
            createOlapTable(db, stmt);
            return;
        } else if (engineName.equals("odbc")) {
            createOdbcTable(db, stmt);
            return;
        } else if (engineName.equals("mysql")) {
            createMysqlTable(db, stmt);
            return;
        } else if (engineName.equals("broker")) {
            createBrokerTable(db, stmt);
            return;
        } else if (engineName.equalsIgnoreCase("elasticsearch") || engineName.equalsIgnoreCase("es")) {
            createEsTable(db, stmt);
            return;
        } else if (engineName.equalsIgnoreCase("hive")) {
            createHiveTable(db, stmt);
            return;
        } else if (engineName.equalsIgnoreCase("iceberg")) {
            IcebergCatalogMgr.createIcebergTable(db, stmt);
            return;
        } else if (engineName.equalsIgnoreCase("jdbc")) {
            createJdbcTable(db, stmt);
            return;
        } else {
            ErrorReport.reportDdlException(ErrorCode.ERR_UNKNOWN_STORAGE_ENGINE, engineName);
        }
        Preconditions.checkState(false);
    }

    public void createTableLike(CreateTableLikeStmt stmt) throws DdlException {
        try {
            DatabaseIf db = getDbOrDdlException(stmt.getExistedDbName());
            TableIf table = db.getTableOrDdlException(stmt.getExistedTableName());

            if (table.getType() == TableType.VIEW) {
                throw new DdlException("Not support create table from a View");
            }

            List<String> createTableStmt = Lists.newArrayList();
            table.readLock();
            try {
                if (table.getType() == TableType.OLAP) {
                    if (!CollectionUtils.isEmpty(stmt.getRollupNames())) {
                        OlapTable olapTable = (OlapTable) table;
                        for (String rollupIndexName : stmt.getRollupNames()) {
                            if (!olapTable.hasMaterializedIndex(rollupIndexName)) {
                                throw new DdlException("Rollup index[" + rollupIndexName + "] not exists in Table["
                                        + olapTable.getName() + "]");
                            }
                        }
                    }
                } else if (!CollectionUtils.isEmpty(stmt.getRollupNames()) || stmt.isWithAllRollup()) {
                    throw new DdlException("Table[" + table.getName() + "] is external, not support rollup copy");
                }

                Env.getDdlStmt(stmt, stmt.getDbName(), table, createTableStmt, null, null, false, false, true, -1L,
                        false, false);
                if (createTableStmt.isEmpty()) {
                    ErrorReport.reportDdlException(ErrorCode.ERROR_CREATE_TABLE_LIKE_EMPTY, "CREATE");
                }
            } finally {
                table.readUnlock();
            }
            CreateTableStmt parsedCreateTableStmt = (CreateTableStmt) SqlParserUtils.parseAndAnalyzeStmt(
                    createTableStmt.get(0), ConnectContext.get());
            parsedCreateTableStmt.setTableName(stmt.getTableName());
            parsedCreateTableStmt.setIfNotExists(stmt.isIfNotExists());
            createTable(parsedCreateTableStmt);
        } catch (UserException e) {
            throw new DdlException("Failed to execute CREATE TABLE LIKE " + stmt.getExistedTableName() + ". Reason: "
                    + e.getMessage());
        }
    }

    /**
     * Create table for select.
     **/
    public void createTableAsSelect(CreateTableAsSelectStmt stmt) throws DdlException {
        try {
            List<String> columnNames = stmt.getColumnNames();
            CreateTableStmt createTableStmt = stmt.getCreateTableStmt();
            QueryStmt queryStmt = stmt.getQueryStmt();
            KeysDesc keysDesc = createTableStmt.getKeysDesc();
            ArrayList<Expr> resultExprs = queryStmt.getResultExprs();
            ArrayList<String> colLabels = queryStmt.getColLabels();
            int size = resultExprs.size();
            // Check columnNames
            int colNameIndex = 0;
            for (int i = 0; i < size; ++i) {
                String name;
                if (columnNames != null) {
                    // use custom column names
                    name = columnNames.get(i);
                } else {
                    name = colLabels.get(i);
                }
                try {
                    FeNameFormat.checkColumnName(name);
                } catch (AnalysisException exception) {
                    if (ConnectContext.get() != null) {
                        ConnectContext.get().getState().reset();
                    }
                    name = "_col" + (colNameIndex++);
                }
                TypeDef typeDef;
                Expr resultExpr = resultExprs.get(i);
                Type resultType = resultExpr.getType();
                if (resultExpr instanceof FunctionCallExpr
                        && resultExpr.getType().getPrimitiveType().equals(PrimitiveType.VARCHAR)
                        && resultExpr.getType().getLength() == -1) {
                    resultType = ScalarType.createVarchar(ScalarType.MAX_VARCHAR_LENGTH);
                }
                if (resultType.isStringType() && (keysDesc == null || !keysDesc.containsCol(name))) {
                    switch (resultType.getPrimitiveType()) {
                        case STRING:
                            typeDef = new TypeDef(ScalarType.createStringType());
                            break;
                        case VARCHAR:
                            typeDef = new TypeDef(ScalarType.createVarchar(resultType.getLength()));
                            break;
                        case CHAR:
                            typeDef = new TypeDef(ScalarType.createCharType(resultType.getLength()));
                            break;
                        default:
                            throw new DdlException("Unsupported string type for ctas");
                    }
                } else if (resultType.isDecimalV2() && resultType.equals(ScalarType.DECIMALV2)) {
                    typeDef = new TypeDef(ScalarType.createDecimalType(27, 9));
                } else if (resultType.isDecimalV3()) {
                    typeDef = new TypeDef(ScalarType.createDecimalV3Type(resultType.getPrecision(),
                            ((ScalarType) resultType).getScalarScale()));
                } else if (resultType.isNull()) {
                    // if typeDef is NULL_TYPE, be will core when executing CTAS expression,
                    // we change it to tinyint nullable.
                    typeDef = TypeDef.create(PrimitiveType.TINYINT);
                } else {
                    typeDef = new TypeDef(resultExpr.getType());
                }
                if (i == 0) {
                    // If this is the first column, because olap table does not support the first column to be
                    // string, float, double or array, we should check and modify its type
                    // For string type, change it to varchar.
                    // For other unsupported types, just remain unchanged, the analysis phash of create table stmt
                    // will handle it.
                    if (typeDef.getType().getPrimitiveType() == PrimitiveType.STRING) {
                        typeDef = TypeDef.createVarchar(ScalarType.MAX_VARCHAR_LENGTH);
                    }
                }
                ColumnDef columnDef;
                if (resultExpr.getSrcSlotRef() == null) {
                    columnDef = new ColumnDef(name, typeDef, false, null,
                            true, false, new DefaultValue(false, null), "");
                } else {
                    Column column = resultExpr.getSrcSlotRef().getDesc().getColumn();
                    boolean setDefault = StringUtils.isNotBlank(column.getDefaultValue());
                    DefaultValue defaultValue;
                    if (column.getDefaultValueExprDef() != null) {
                        if (column.getDefaultValueExprDef().getPrecision() != null) {
                            defaultValue = new DefaultValue(setDefault, column.getDefaultValue(),
                                column.getDefaultValueExprDef().getExprName(),
                                column.getDefaultValueExprDef().getPrecision());
                        } else {
                            defaultValue = new DefaultValue(setDefault, column.getDefaultValue(),
                                column.getDefaultValueExprDef().getExprName());
                        }
                    } else {
                        defaultValue = new DefaultValue(setDefault, column.getDefaultValue());
                    }
                    columnDef = new ColumnDef(name, typeDef, false, null,
                            column.isAllowNull(), false, defaultValue, column.getComment());
                }
                createTableStmt.addColumnDef(columnDef);
                // set first column as default distribution
                if (createTableStmt.getDistributionDesc() == null && i == 0) {
                    createTableStmt.setDistributionDesc(new HashDistributionDesc(10, Lists.newArrayList(name)));
                }
            }
            Analyzer dummyRootAnalyzer = new Analyzer(Env.getCurrentEnv(), ConnectContext.get());
            createTableStmt.analyze(dummyRootAnalyzer);
            createTable(createTableStmt);
        } catch (UserException e) {
            throw new DdlException("Failed to execute CTAS Reason: " + e.getMessage());
        }
    }

    public void replayCreateTable(String dbName, Table table) throws MetaNotFoundException {

        Database db = this.fullNameToDb.get(dbName);
        try {
            db.createTableWithLock(table, true, false);
        } catch (DdlException e) {
            throw new MetaNotFoundException(e.getMessage());
        }
        // add to inverted index
        if (table.isManagedTable()) {
            TabletInvertedIndex invertedIndex = Env.getCurrentInvertedIndex();
            OlapTable olapTable = (OlapTable) table;
            long dbId = db.getId();
            long tableId = table.getId();
            for (Partition partition : olapTable.getAllPartitions()) {
                long partitionId = partition.getId();
                TStorageMedium medium = olapTable.getPartitionInfo().getDataProperty(partitionId)
                        .getStorageMedium();
                for (MaterializedIndex mIndex : partition.getMaterializedIndices(IndexExtState.ALL)) {
                    long indexId = mIndex.getId();
                    int schemaHash = olapTable.getSchemaHashByIndexId(indexId);
                    for (Tablet tablet : mIndex.getTablets()) {
                        TabletMeta tabletMeta = new TabletMeta(dbId, tableId, partitionId, indexId, schemaHash,
                                medium);
                        long tabletId = tablet.getId();
                        invertedIndex.addTablet(tabletId, tabletMeta);
                        for (Replica replica : tablet.getReplicas()) {
                            invertedIndex.addReplica(tabletId, replica);
                        }
                    }
                }
            } // end for partitions
            if (!Env.isCheckpointThread()) {
                DynamicPartitionUtil.registerOrRemoveDynamicPartitionTable(dbId, olapTable, true);
            }
        }
    }

    public void addPartitionLike(Database db, String tableName, AddPartitionLikeClause addPartitionLikeClause)
            throws DdlException {
        try {
            Table table = db.getTableOrDdlException(tableName);

            if (table.getType() != TableType.OLAP) {
                throw new DdlException("Only support create partition from a OLAP table");
            }

            // Lock the table to prevent other SQL from performing write operation during table structure modification
            AddPartitionClause clause = null;
            table.readLock();
            try {
                String partitionName = addPartitionLikeClause.getPartitionName();
                String existedName = addPartitionLikeClause.getExistedPartitionName();
                OlapTable olapTable = (OlapTable) table;
                Partition part = olapTable.getPartition(existedName);
                if (part == null) {
                    throw new DdlException("Failed to ADD PARTITION" + partitionName + " LIKE "
                            + existedName + ". Reason: " + "partition " + existedName + "not exist");
                }
                PartitionInfo partitionInfo = olapTable.getPartitionInfo();
                SinglePartitionDesc newPartitionDesc;
                if (partitionInfo instanceof SinglePartitionInfo) {
                    newPartitionDesc = new SinglePartitionDesc(false, partitionName,
                            PartitionKeyDesc.DUMMY_KEY_DESC, Maps.newHashMap());
                } else {
                    PartitionDesc partitionDesc = partitionInfo.toPartitionDesc((OlapTable) table);
                    SinglePartitionDesc oldPartitionDesc = partitionDesc.getSinglePartitionDescByName(existedName);
                    if (oldPartitionDesc == null) {
                        throw new DdlException("Failed to ADD PARTITION" + partitionName + " LIKE "
                                + existedName + ". Reason: " + "partition " + existedName + "desc not exist");
                    }
                    newPartitionDesc = new SinglePartitionDesc(false, partitionName,
                            oldPartitionDesc.getPartitionKeyDesc(), oldPartitionDesc.getProperties());
                }
                DistributionDesc distributionDesc = part.getDistributionInfo().toDistributionDesc();
                Map<String, String> properties = newPartitionDesc.getProperties();
                clause = new AddPartitionClause(newPartitionDesc, distributionDesc,
                        properties, addPartitionLikeClause.getIsTempPartition());
            } finally {
                table.readUnlock();
            }
            addPartition(db, tableName, clause);

        } catch (UserException e) {
            throw new DdlException("Failed to ADD PARTITION " + addPartitionLikeClause.getPartitionName()
                    + " LIKE " + addPartitionLikeClause.getExistedPartitionName() + ". Reason: " + e.getMessage());
        }
    }

    public void addPartition(Database db, String tableName, AddPartitionClause addPartitionClause) throws DdlException {
        SinglePartitionDesc singlePartitionDesc = addPartitionClause.getSingeRangePartitionDesc();
        DistributionDesc distributionDesc = addPartitionClause.getDistributionDesc();
        boolean isTempPartition = addPartitionClause.isTempPartition();

        DistributionInfo distributionInfo;
        Map<Long, MaterializedIndexMeta> indexIdToMeta;
        Set<String> bfColumns;
        String partitionName = singlePartitionDesc.getPartitionName();
        BinlogConfig binlogConfig;

        // check
        OlapTable olapTable = db.getOlapTableOrDdlException(tableName);
        olapTable.readLock();
        try {
            olapTable.checkNormalStateForAlter();
            // check partition type
            PartitionInfo partitionInfo = olapTable.getPartitionInfo();
            // check partition name
            if (olapTable.checkPartitionNameExist(partitionName)) {
                if (singlePartitionDesc.isSetIfNotExists()) {
                    LOG.info("add partition[{}] which already exists", partitionName);
                    return;
                } else {
                    ErrorReport.reportDdlException(ErrorCode.ERR_SAME_NAME_PARTITION, partitionName);
                }
            }

            Map<String, String> properties = singlePartitionDesc.getProperties();

            /*
             * sql: alter table test_tbl add  partition  xxx values less than ('xxx') properties('mutable' = 'false');
             *
             * sql_parser.cup definition:
             * AddPartitionClause:
             *      KW_ADD ... single_partition_desc:desc opt_distribution:distribution opt_properties:properties)
             * SinglePartitionDesc:
             *      single_partition_desc ::= KW_PARTITION ... ident:partName KW_VALUES KW_LESS KW_THAN
             *                       partition_key_desc:desc opt_key_value_map:properties)
             *
             * If there is no opt_distribution definition, the properties in SQL is ambiguous to JCUP. It can bind
             * properties to AddPartitionClause(`opt_properties`) or SinglePartitionDesc(`opt_key_value_map`).
             * And JCUP choose to bind to AddPartitionClause, so we should add properties of AddPartitionClause to
             * SinglePartitionDesc's properties here.
             */
            if (null != addPartitionClause.getProperties()) {
                properties.putAll(addPartitionClause.getProperties());
            }

            // partition properties should inherit table properties
            ReplicaAllocation replicaAlloc = olapTable.getDefaultReplicaAllocation();
            if (!properties.containsKey(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM) && !properties.containsKey(
                    PropertyAnalyzer.PROPERTIES_REPLICATION_ALLOCATION)) {
                properties.put(PropertyAnalyzer.PROPERTIES_REPLICATION_ALLOCATION, replicaAlloc.toCreateStmt());
            }
            if (!properties.containsKey(PropertyAnalyzer.PROPERTIES_INMEMORY)) {
                properties.put(PropertyAnalyzer.PROPERTIES_INMEMORY, olapTable.isInMemory().toString());
            }
            if (!properties.containsKey(PropertyAnalyzer.PROPERTIES_DISABLE_AUTO_COMPACTION)) {
                properties.put(PropertyAnalyzer.PROPERTIES_DISABLE_AUTO_COMPACTION,
                        olapTable.disableAutoCompaction().toString());
            }
            if (!properties.containsKey(PropertyAnalyzer.PROPERTIES_ENABLE_SINGLE_REPLICA_COMPACTION)) {
                properties.put(PropertyAnalyzer.PROPERTIES_ENABLE_SINGLE_REPLICA_COMPACTION,
                        olapTable.enableSingleReplicaCompaction().toString());
            }
            if (!properties.containsKey(PropertyAnalyzer.PROPERTIES_STORE_ROW_COLUMN)) {
                properties.put(PropertyAnalyzer.PROPERTIES_STORE_ROW_COLUMN,
                        olapTable.storeRowColumn().toString());
            }
            if (!properties.containsKey(PropertyAnalyzer.PROPERTIES_SKIP_WRITE_INDEX_ON_LOAD)) {
                properties.put(PropertyAnalyzer.PROPERTIES_SKIP_WRITE_INDEX_ON_LOAD,
                        olapTable.skipWriteIndexOnLoad().toString());
            }
            if (!properties.containsKey(PropertyAnalyzer.PROPERTIES_COMPACTION_POLICY)) {
                properties.put(PropertyAnalyzer.PROPERTIES_COMPACTION_POLICY, olapTable.getCompactionPolicy());
            }
            if (!properties.containsKey(PropertyAnalyzer.PROPERTIES_TIME_SERIES_COMPACTION_GOAL_SIZE_MBYTES)) {
                properties.put(PropertyAnalyzer.PROPERTIES_TIME_SERIES_COMPACTION_GOAL_SIZE_MBYTES,
                                                olapTable.getTimeSeriesCompactionGoalSizeMbytes().toString());
            }
            if (!properties.containsKey(PropertyAnalyzer.PROPERTIES_TIME_SERIES_COMPACTION_FILE_COUNT_THRESHOLD)) {
                properties.put(PropertyAnalyzer.PROPERTIES_TIME_SERIES_COMPACTION_FILE_COUNT_THRESHOLD,
                                                olapTable.getTimeSeriesCompactionFileCountThreshold().toString());
            }
            if (!properties.containsKey(PropertyAnalyzer.PROPERTIES_TIME_SERIES_COMPACTION_TIME_THRESHOLD_SECONDS)) {
                properties.put(PropertyAnalyzer.PROPERTIES_TIME_SERIES_COMPACTION_TIME_THRESHOLD_SECONDS,
                                                olapTable.getTimeSeriesCompactionTimeThresholdSeconds().toString());
            }
            if (!properties.containsKey(PropertyAnalyzer.PROPERTIES_STORAGE_POLICY)) {
                properties.put(PropertyAnalyzer.PROPERTIES_STORAGE_POLICY, olapTable.getStoragePolicy());
            }

            singlePartitionDesc.analyze(partitionInfo.getPartitionColumns().size(), properties);
            partitionInfo.createAndCheckPartitionItem(singlePartitionDesc, isTempPartition);

            // get distributionInfo
            List<Column> baseSchema = olapTable.getBaseSchema();
            DistributionInfo defaultDistributionInfo = olapTable.getDefaultDistributionInfo();
            if (distributionDesc != null) {
                distributionInfo = distributionDesc.toDistributionInfo(baseSchema);
                // for now. we only support modify distribution's bucket num
                if (distributionInfo.getType() != defaultDistributionInfo.getType()) {
                    throw new DdlException("Cannot assign different distribution type. default is: "
                            + defaultDistributionInfo.getType());
                }

                if (distributionInfo.getType() == DistributionInfoType.HASH) {
                    HashDistributionInfo hashDistributionInfo = (HashDistributionInfo) distributionInfo;
                    if (hashDistributionInfo.getBucketNum() <= 0) {
                        throw new DdlException("Cannot assign hash distribution buckets less than 1");
                    }
                    if (!hashDistributionInfo.sameDistributionColumns((HashDistributionInfo) defaultDistributionInfo)) {
                        throw new DdlException("Cannot assign hash distribution with different distribution cols. "
                                + "new is: " + hashDistributionInfo.getDistributionColumns() + " default is: "
                                + ((HashDistributionInfo) distributionInfo).getDistributionColumns());
                    }
                } else if (distributionInfo.getType() == DistributionInfoType.RANDOM) {
                    RandomDistributionInfo randomDistributionInfo = (RandomDistributionInfo) distributionInfo;
                    if (randomDistributionInfo.getBucketNum() <= 0) {
                        throw new DdlException("Cannot assign random distribution buckets less than 1");
                    }
                }
            } else {
                // make sure partition-dristribution-info is deep copied from default-distribution-info
                distributionInfo = defaultDistributionInfo.toDistributionDesc().toDistributionInfo(baseSchema);
            }

            // check colocation
            if (Env.getCurrentColocateIndex().isColocateTable(olapTable.getId())) {
                String fullGroupName = GroupId.getFullGroupName(db.getId(), olapTable.getColocateGroup());
                ColocateGroupSchema groupSchema = Env.getCurrentColocateIndex().getGroupSchema(fullGroupName);
                Preconditions.checkNotNull(groupSchema);
                groupSchema.checkDistribution(distributionInfo);
                groupSchema.checkReplicaAllocation(singlePartitionDesc.getReplicaAlloc());
            }

            indexIdToMeta = olapTable.getCopiedIndexIdToMeta();
            bfColumns = olapTable.getCopiedBfColumns();

            // get BinlogConfig
            binlogConfig = new BinlogConfig(olapTable.getBinlogConfig());
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        } finally {
            olapTable.readUnlock();
        }

        Preconditions.checkNotNull(distributionInfo);
        Preconditions.checkNotNull(olapTable);
        Preconditions.checkNotNull(indexIdToMeta);

        // create partition outside db lock
        DataProperty dataProperty = singlePartitionDesc.getPartitionDataProperty();
        Preconditions.checkNotNull(dataProperty);
        // check replica quota if this operation done
        long indexNum = indexIdToMeta.size();
        long bucketNum = distributionInfo.getBucketNum();
        long replicaNum = singlePartitionDesc.getReplicaAlloc().getTotalReplicaNum();
        long totalReplicaNum = indexNum * bucketNum * replicaNum;
        if (totalReplicaNum >= db.getReplicaQuotaLeftWithLock()) {
            throw new DdlException("Database " + db.getFullName() + " table " + tableName + " add partition increasing "
                    + totalReplicaNum + " of replica exceeds quota[" + db.getReplicaQuota() + "]");
        }
        Set<Long> tabletIdSet = new HashSet<>();
        long bufferSize = 1 + totalReplicaNum + indexNum * bucketNum;
        IdGeneratorBuffer idGeneratorBuffer = Env.getCurrentEnv().getIdGeneratorBuffer(bufferSize);
        String storagePolicy = olapTable.getStoragePolicy();
        if (!Strings.isNullOrEmpty(dataProperty.getStoragePolicy())) {
            storagePolicy = dataProperty.getStoragePolicy();
        }
        try {
            long partitionId = idGeneratorBuffer.getNextId();
            Partition partition = createPartitionWithIndices(db.getClusterName(), db.getId(), olapTable.getId(),
                    olapTable.getName(), olapTable.getBaseIndexId(), partitionId, partitionName, indexIdToMeta,
                    distributionInfo, dataProperty.getStorageMedium(), singlePartitionDesc.getReplicaAlloc(),
                    singlePartitionDesc.getVersionInfo(), bfColumns, olapTable.getBfFpp(), tabletIdSet,
                    olapTable.getCopiedIndexes(), singlePartitionDesc.isInMemory(), olapTable.getStorageFormat(),
                    singlePartitionDesc.getTabletType(), olapTable.getCompressionType(), olapTable.getDataSortInfo(),
                    olapTable.getEnableUniqueKeyMergeOnWrite(), storagePolicy, idGeneratorBuffer,
                    olapTable.disableAutoCompaction(), olapTable.enableSingleReplicaCompaction(),
                    olapTable.skipWriteIndexOnLoad(), olapTable.getCompactionPolicy(),
                    olapTable.getTimeSeriesCompactionGoalSizeMbytes(),
                    olapTable.getTimeSeriesCompactionFileCountThreshold(),
                    olapTable.getTimeSeriesCompactionTimeThresholdSeconds(),
                    olapTable.storeRowColumn(),
                    binlogConfig, dataProperty.isStorageMediumSpecified());

            // check again
            olapTable = db.getOlapTableOrDdlException(tableName);
            olapTable.writeLockOrDdlException();
            try {
                olapTable.checkNormalStateForAlter();
                // check partition name
                if (olapTable.checkPartitionNameExist(partitionName)) {
                    if (singlePartitionDesc.isSetIfNotExists()) {
                        LOG.info("add partition[{}] which already exists", partitionName);
                        return;
                    } else {
                        ErrorReport.reportDdlException(ErrorCode.ERR_SAME_NAME_PARTITION, partitionName);
                    }
                }

                // check if meta changed
                // rollup index may be added or dropped during add partition operation.
                // schema may be changed during add partition operation.
                boolean metaChanged = false;
                if (olapTable.getIndexNameToId().size() != indexIdToMeta.size()) {
                    metaChanged = true;
                } else {
                    // compare schemaHash
                    for (Map.Entry<Long, MaterializedIndexMeta> entry : olapTable.getIndexIdToMeta().entrySet()) {
                        long indexId = entry.getKey();
                        if (!indexIdToMeta.containsKey(indexId)) {
                            metaChanged = true;
                            break;
                        }
                        if (indexIdToMeta.get(indexId).getSchemaHash() != entry.getValue().getSchemaHash()) {
                            metaChanged = true;
                            break;
                        }

                        List<Column> oldSchema = indexIdToMeta.get(indexId).getSchema();
                        List<Column> newSchema = entry.getValue().getSchema();
                        if (oldSchema.size() != newSchema.size()) {
                            LOG.warn("schema column size diff, old schema {}, new schema {}", oldSchema, newSchema);
                            metaChanged = true;
                            break;
                        } else {
                            List<Column> oldSchemaCopy = Lists.newArrayList(oldSchema);
                            List<Column> newSchemaCopy = Lists.newArrayList(newSchema);
                            oldSchemaCopy.sort((Column a, Column b) -> a.getUniqueId() - b.getUniqueId());
                            newSchemaCopy.sort((Column a, Column b) -> a.getUniqueId() - b.getUniqueId());
                            for (int i = 0; i < oldSchemaCopy.size(); ++i) {
                                if (!oldSchemaCopy.get(i).equals(newSchemaCopy.get(i))) {
                                    LOG.warn("schema diff, old schema {}, new schema {}", oldSchemaCopy.get(i),
                                            newSchemaCopy.get(i));
                                    metaChanged = true;
                                    break;
                                }
                            }
                        }
                    }
                }



                if (metaChanged) {
                    throw new DdlException("Table[" + tableName + "]'s meta has been changed. try again.");
                }

                // check partition type
                PartitionInfo partitionInfo = olapTable.getPartitionInfo();

                // update partition info
                partitionInfo.handleNewSinglePartitionDesc(singlePartitionDesc, partitionId, isTempPartition);

                if (isTempPartition) {
                    olapTable.addTempPartition(partition);
                } else {
                    olapTable.addPartition(partition);
                }

                // log
                PartitionPersistInfo info = null;
                if (partitionInfo.getType() == PartitionType.RANGE) {
                    info = new PartitionPersistInfo(db.getId(), olapTable.getId(), partition,
                            partitionInfo.getItem(partitionId).getItems(), ListPartitionItem.DUMMY_ITEM, dataProperty,
                            partitionInfo.getReplicaAllocation(partitionId), partitionInfo.getIsInMemory(partitionId),
                            isTempPartition, partitionInfo.getIsMutable(partitionId));
                } else if (partitionInfo.getType() == PartitionType.LIST) {
                    info = new PartitionPersistInfo(db.getId(), olapTable.getId(), partition,
                            RangePartitionItem.DUMMY_ITEM, partitionInfo.getItem(partitionId), dataProperty,
                            partitionInfo.getReplicaAllocation(partitionId), partitionInfo.getIsInMemory(partitionId),
                            isTempPartition, partitionInfo.getIsMutable(partitionId));
                } else {
                    info = new PartitionPersistInfo(db.getId(), olapTable.getId(), partition,
                            RangePartitionItem.DUMMY_ITEM, ListPartitionItem.DUMMY_ITEM, dataProperty,
                            partitionInfo.getReplicaAllocation(partitionId), partitionInfo.getIsInMemory(partitionId),
                            isTempPartition, partitionInfo.getIsMutable(partitionId));
                }
                Env.getCurrentEnv().getEditLog().logAddPartition(info);

                LOG.info("succeed in creating partition[{}], temp: {}", partitionId, isTempPartition);
            } finally {
                olapTable.writeUnlock();
            }
        } catch (DdlException e) {
            for (Long tabletId : tabletIdSet) {
                Env.getCurrentInvertedIndex().deleteTablet(tabletId);
            }
            throw e;
        }
    }

    public void replayAddPartition(PartitionPersistInfo info) throws MetaNotFoundException {
        Database db = (Database) getDbOrMetaException(info.getDbId());
        OlapTable olapTable = (OlapTable) db.getTableOrMetaException(info.getTableId(), TableType.OLAP);
        olapTable.writeLock();
        try {
            Partition partition = info.getPartition();
            PartitionInfo partitionInfo = olapTable.getPartitionInfo();
            if (info.isTempPartition()) {
                olapTable.addTempPartition(partition);
            } else {
                olapTable.addPartition(partition);
            }

            PartitionItem partitionItem = null;
            if (partitionInfo.getType() == PartitionType.RANGE) {
                partitionItem = new RangePartitionItem(info.getRange());
            } else if (partitionInfo.getType() == PartitionType.LIST) {
                partitionItem = info.getListPartitionItem();
            }

            partitionInfo.unprotectHandleNewSinglePartitionDesc(partition.getId(), info.isTempPartition(),
                    partitionItem, info.getDataProperty(), info.getReplicaAlloc(), info.isInMemory(), info.isMutable());

            // add to inverted index
            TabletInvertedIndex invertedIndex = Env.getCurrentInvertedIndex();
            for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.ALL)) {
                long indexId = index.getId();
                int schemaHash = olapTable.getSchemaHashByIndexId(indexId);
                for (Tablet tablet : index.getTablets()) {
                    TabletMeta tabletMeta = new TabletMeta(info.getDbId(), info.getTableId(), partition.getId(),
                            index.getId(), schemaHash, info.getDataProperty().getStorageMedium());
                    long tabletId = tablet.getId();
                    invertedIndex.addTablet(tabletId, tabletMeta);
                    for (Replica replica : tablet.getReplicas()) {
                        invertedIndex.addReplica(tabletId, replica);
                    }
                }
            }
        } finally {
            olapTable.writeUnlock();
        }
    }

    public void dropPartition(Database db, OlapTable olapTable, DropPartitionClause clause) throws DdlException {
        Preconditions.checkArgument(olapTable.isWriteLockHeldByCurrentThread());

        String partitionName = clause.getPartitionName();
        boolean isTempPartition = clause.isTempPartition();

        olapTable.checkNormalStateForAlter();
        if (!olapTable.checkPartitionNameExist(partitionName, isTempPartition)) {
            if (clause.isSetIfExists()) {
                LOG.info("drop partition[{}] which does not exist", partitionName);
                return;
            } else {
                ErrorReport.reportDdlException(ErrorCode.ERR_DROP_PARTITION_NON_EXISTENT, partitionName);
            }
        }

        PartitionInfo partitionInfo = olapTable.getPartitionInfo();
        if (partitionInfo.getType() != PartitionType.RANGE && partitionInfo.getType() != PartitionType.LIST) {
            throw new DdlException("Alter table [" + olapTable.getName() + "] failed. Not a partitioned table");
        }

        // drop
        long recycleTime = 0;
        if (isTempPartition) {
            olapTable.dropTempPartition(partitionName, true);
        } else {
            Partition partition = null;
            if (!clause.isForceDrop()) {
                partition = olapTable.getPartition(partitionName);
                if (partition != null) {
                    if (Env.getCurrentEnv().getGlobalTransactionMgr()
                            .existCommittedTxns(db.getId(), olapTable.getId(), partition.getId())) {
                        throw new DdlException(
                                "There are still some transactions in the COMMITTED state waiting to be completed."
                                        + " The partition [" + partitionName
                                        + "] cannot be dropped. If you want to forcibly drop(cannot be recovered),"
                                        + " please use \"DROP partition FORCE\".");
                    }
                }
            }
            olapTable.dropPartition(db.getId(), partitionName, clause.isForceDrop());
            if (!clause.isForceDrop() && partition != null) {
                recycleTime = Env.getCurrentRecycleBin().getRecycleTimeById(partition.getId());
            }
        }

        // log
        DropPartitionInfo info = new DropPartitionInfo(db.getId(), olapTable.getId(), partitionName, isTempPartition,
                clause.isForceDrop(), recycleTime);
        Env.getCurrentEnv().getEditLog().logDropPartition(info);

        LOG.info("succeed in dropping partition[{}], table : [{}-{}], is temp : {}, is force : {}",
                partitionName, olapTable.getId(), olapTable.getName(), isTempPartition, clause.isForceDrop());
    }

    public void replayDropPartition(DropPartitionInfo info) throws MetaNotFoundException {
        Database db = (Database) getDbOrMetaException(info.getDbId());
        OlapTable olapTable = (OlapTable) db.getTableOrMetaException(info.getTableId(), TableType.OLAP);
        olapTable.writeLock();
        try {
            if (info.isTempPartition()) {
                olapTable.dropTempPartition(info.getPartitionName(), true);
            } else {
                Partition partition = olapTable.dropPartition(info.getDbId(), info.getPartitionName(),
                        info.isForceDrop());
                if (!info.isForceDrop() && partition != null && info.getRecycleTime() != 0) {
                    Env.getCurrentRecycleBin().setRecycleTimeByIdForReplay(partition.getId(), info.getRecycleTime());
                }
            }
        } finally {
            olapTable.writeUnlock();
        }
    }

    public void replayErasePartition(long partitionId) {
        Env.getCurrentRecycleBin().replayErasePartition(partitionId);
    }

    public void replayRecoverPartition(RecoverInfo info) throws MetaNotFoundException, DdlException {
        Database db = (Database) getDbOrMetaException(info.getDbId());
        OlapTable olapTable = (OlapTable) db.getTableOrMetaException(info.getTableId(), TableType.OLAP);
        olapTable.writeLockOrDdlException();
        try {
            Env.getCurrentRecycleBin().replayRecoverPartition(olapTable, info.getPartitionId(),
                    info.getNewPartitionName());
        } finally {
            olapTable.writeUnlock();
        }
    }

    private Partition createPartitionWithIndices(String clusterName, long dbId, long tableId, String tableName,
            long baseIndexId, long partitionId, String partitionName, Map<Long, MaterializedIndexMeta> indexIdToMeta,
            DistributionInfo distributionInfo, TStorageMedium storageMedium, ReplicaAllocation replicaAlloc,
            Long versionInfo, Set<String> bfColumns, double bfFpp, Set<Long> tabletIdSet, List<Index> indexes,
            boolean isInMemory, TStorageFormat storageFormat, TTabletType tabletType, TCompressionType compressionType,
            DataSortInfo dataSortInfo, boolean enableUniqueKeyMergeOnWrite, String storagePolicy,
            IdGeneratorBuffer idGeneratorBuffer, boolean disableAutoCompaction,
            boolean enableSingleReplicaCompaction, boolean skipWriteIndexOnLoad,
            String compactionPolicy, Long timeSeriesCompactionGoalSizeMbytes,
            Long timeSeriesCompactionFileCountThreshold, Long timeSeriesCompactionTimeThresholdSeconds,
            boolean storeRowColumn, BinlogConfig binlogConfig,
            boolean isStorageMediumSpecified) throws DdlException {
        // create base index first.
        Preconditions.checkArgument(baseIndexId != -1);
        MaterializedIndex baseIndex = new MaterializedIndex(baseIndexId, IndexState.NORMAL);

        // create partition with base index
        Partition partition = new Partition(partitionId, partitionName, baseIndex, distributionInfo);

        // add to index map
        Map<Long, MaterializedIndex> indexMap = new HashMap<>();
        indexMap.put(baseIndexId, baseIndex);

        // create rollup index if has
        for (long indexId : indexIdToMeta.keySet()) {
            if (indexId == baseIndexId) {
                continue;
            }

            MaterializedIndex rollup = new MaterializedIndex(indexId, IndexState.NORMAL);
            indexMap.put(indexId, rollup);
        }

        // version and version hash
        if (versionInfo != null) {
            partition.updateVisibleVersion(versionInfo);
            partition.setNextVersion(versionInfo + 1);
        }
        long version = partition.getVisibleVersion();

        short totalReplicaNum = replicaAlloc.getTotalReplicaNum();
        for (Map.Entry<Long, MaterializedIndex> entry : indexMap.entrySet()) {
            long indexId = entry.getKey();
            MaterializedIndex index = entry.getValue();
            MaterializedIndexMeta indexMeta = indexIdToMeta.get(indexId);

            // create tablets
            int schemaHash = indexMeta.getSchemaHash();
            TabletMeta tabletMeta = new TabletMeta(dbId, tableId, partitionId, indexId, schemaHash, storageMedium);
            createTablets(clusterName, index, ReplicaState.NORMAL, distributionInfo, version, replicaAlloc, tabletMeta,
                    tabletIdSet, idGeneratorBuffer, isStorageMediumSpecified);

            boolean ok = false;
            String errMsg = null;

            // add create replica task for olap
            short shortKeyColumnCount = indexMeta.getShortKeyColumnCount();
            TStorageType storageType = indexMeta.getStorageType();
            List<Column> schema = indexMeta.getSchema();
            KeysType keysType = indexMeta.getKeysType();
            int totalTaskNum = index.getTablets().size() * totalReplicaNum;
            MarkedCountDownLatch<Long, Long> countDownLatch = new MarkedCountDownLatch<Long, Long>(totalTaskNum);
            AgentBatchTask batchTask = new AgentBatchTask();
            for (Tablet tablet : index.getTablets()) {
                long tabletId = tablet.getId();
                for (Replica replica : tablet.getReplicas()) {
                    long backendId = replica.getBackendId();
                    long replicaId = replica.getId();
                    countDownLatch.addMark(backendId, tabletId);
                    CreateReplicaTask task = new CreateReplicaTask(backendId, dbId, tableId, partitionId, indexId,
                            tabletId, replicaId, shortKeyColumnCount, schemaHash, version, keysType, storageType,
                            storageMedium, schema, bfColumns, bfFpp, countDownLatch, indexes, isInMemory, tabletType,
                            dataSortInfo, compressionType, enableUniqueKeyMergeOnWrite, storagePolicy,
                            disableAutoCompaction, enableSingleReplicaCompaction, skipWriteIndexOnLoad,
                            compactionPolicy, timeSeriesCompactionGoalSizeMbytes,
                            timeSeriesCompactionFileCountThreshold, timeSeriesCompactionTimeThresholdSeconds,
                            storeRowColumn, binlogConfig);

                    task.setStorageFormat(storageFormat);
                    batchTask.addTask(task);
                    // add to AgentTaskQueue for handling finish report.
                    // not for resending task
                    AgentTaskQueue.addTask(task);
                }
            }
            AgentTaskExecutor.submit(batchTask);

            // estimate timeout
            long timeout = DbUtil.getCreateReplicasTimeoutMs(totalTaskNum);
            try {
                ok = countDownLatch.await(timeout, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                LOG.warn("InterruptedException: ", e);
                ok = false;
            }

            if (!ok || !countDownLatch.getStatus().ok()) {
                // clear tasks
                AgentTaskQueue.removeBatchTask(batchTask, TTaskType.CREATE);

                int quorumReplicaNum = totalReplicaNum / 2 + 1;
                Map<Long, Integer> failedTabletCounter = Maps.newHashMap();
                countDownLatch.getLeftMarks().stream().forEach(
                        item -> failedTabletCounter.put(item.getValue(),
                                failedTabletCounter.getOrDefault(item.getValue(), 0) + 1));
                boolean createFailed = failedTabletCounter.values().stream().anyMatch(
                        failedNum -> (totalReplicaNum - failedNum) < quorumReplicaNum);
                errMsg = createFailed ? "Failed to create partition[" + partitionName + "]."
                        : "Failed to create some replicas when create partition[" + partitionName + "].";

                if (!countDownLatch.getStatus().ok()) {
                    errMsg += " Error: " + countDownLatch.getStatus().getErrorMsg();
                    if (countDownLatch.getStatus().getErrorCode() == TStatusCode.TIMEOUT) {
                        SystemInfoService infoService = Env.getCurrentSystemInfo();
                        Set<String> downBeSet = countDownLatch.getLeftMarks().stream()
                                .map(item -> infoService.getBackend(item.getKey()))
                                .filter(backend -> !backend.isAlive())
                                .map(Backend::getHost)
                                .collect(Collectors.toSet());
                        if (null != downBeSet || downBeSet.size() != 0) {
                            String downBE = StringUtils.join(downBeSet, ",");
                            errMsg += "The BE " + downBE + " is down,please check BE status!";
                        }
                    }
                } else {
                    errMsg += "Timeout:" + (timeout / 1000) + " seconds.";
                    // only show at most 3 results
                    List<String> subList = countDownLatch.getLeftMarks().stream().limit(3)
                            .map(item -> "(backendId = " + item.getKey() + ", tabletId = "  + item.getValue() + ")")
                            .collect(Collectors.toList());
                    if (!subList.isEmpty()) {
                        errMsg += " Unfinished: " + Joiner.on(", ").join(subList);
                    }
                }

                LOG.warn(errMsg);
                if (createFailed) {
                    throw new DdlException(errMsg);
                }
            }

            if (index.getId() != baseIndexId) {
                // add rollup index to partition
                partition.createRollupIndex(index);
            }
        } // end for indexMap

        LOG.info("succeed in creating partition[{}-{}], table : [{}-{}]", partitionId, partitionName,
                tableId, tableName);

        return partition;
    }

    // Create olap table and related base index synchronously.
    private void createOlapTable(Database db, CreateTableStmt stmt) throws UserException {
        String tableName = stmt.getTableName();
        LOG.debug("begin create olap table: {}", tableName);

        BinlogConfig dbBinlogConfig;
        db.readLock();
        try {
            dbBinlogConfig = new BinlogConfig(db.getBinlogConfig());
        } finally {
            db.readUnlock();
        }
        BinlogConfig createTableBinlogConfig = new BinlogConfig(dbBinlogConfig);
        createTableBinlogConfig.mergeFromProperties(stmt.getProperties());
        if (dbBinlogConfig.isEnable() && !createTableBinlogConfig.isEnable()) {
            throw new DdlException("Cannot create table with binlog disabled when database binlog enable");
        }
        stmt.getProperties().putAll(createTableBinlogConfig.toProperties());

        // get keys type
        KeysDesc keysDesc = stmt.getKeysDesc();
        Preconditions.checkNotNull(keysDesc);
        KeysType keysType = keysDesc.getKeysType();
        int keysColumnSize = keysDesc.keysColumnSize();
        boolean isKeysRequired = !(keysType == KeysType.DUP_KEYS && keysColumnSize == 0);

        // create columns
        List<Column> baseSchema = stmt.getColumns();
        validateColumns(baseSchema, isKeysRequired);
        checkAutoIncColumns(baseSchema, keysType);

        // analyze replica allocation
        ReplicaAllocation replicaAlloc = PropertyAnalyzer.analyzeReplicaAllocation(stmt.getProperties(), "");
        if (replicaAlloc.isNotSet()) {
            replicaAlloc = ReplicaAllocation.DEFAULT_ALLOCATION;
        }

        long bufferSize = IdGeneratorUtil.getBufferSizeForCreateTable(stmt, replicaAlloc);
        IdGeneratorBuffer idGeneratorBuffer = Env.getCurrentEnv().getIdGeneratorBuffer(bufferSize);

        // create partition info
        PartitionDesc partitionDesc = stmt.getPartitionDesc();
        PartitionInfo partitionInfo = null;
        Map<String, Long> partitionNameToId = Maps.newHashMap();
        if (partitionDesc != null) {
            PartitionDesc partDesc = partitionDesc;
            for (SinglePartitionDesc desc : partDesc.getSinglePartitionDescs()) {
                long partitionId = idGeneratorBuffer.getNextId();
                partitionNameToId.put(desc.getPartitionName(), partitionId);
            }
            partitionInfo = partitionDesc.toPartitionInfo(baseSchema, partitionNameToId, false);
        } else {
            if (DynamicPartitionUtil.checkDynamicPartitionPropertiesExist(stmt.getProperties())) {
                throw new DdlException("Only support dynamic partition properties on range partition table");
            }
            long partitionId = idGeneratorBuffer.getNextId();
            // use table name as single partition name
            partitionNameToId.put(tableName, partitionId);
            partitionInfo = new SinglePartitionInfo();
        }

        // create distribution info
        DistributionDesc distributionDesc = stmt.getDistributionDesc();
        Preconditions.checkNotNull(distributionDesc);
        DistributionInfo defaultDistributionInfo = distributionDesc.toDistributionInfo(baseSchema);

        // calc short key column count
        short shortKeyColumnCount = Env.calcShortKeyColumnCount(baseSchema, stmt.getProperties(), isKeysRequired);
        LOG.debug("create table[{}] short key column count: {}", tableName, shortKeyColumnCount);

        // create table
        long tableId = idGeneratorBuffer.getNextId();
        TableType tableType = OlapTableFactory.getTableType(stmt);
        OlapTable olapTable = (OlapTable) new OlapTableFactory()
                .init(tableType)
                .withTableId(tableId)
                .withTableName(tableName)
                .withSchema(baseSchema)
                .withKeysType(keysType)
                .withPartitionInfo(partitionInfo)
                .withDistributionInfo(defaultDistributionInfo)
                .withExtraParams(stmt)
                .build();
        olapTable.setComment(stmt.getComment());

        // set base index id
        long baseIndexId = idGeneratorBuffer.getNextId();
        olapTable.setBaseIndexId(baseIndexId);

        // set base index info to table
        // this should be done before create partition.
        Map<String, String> properties = stmt.getProperties();

        short minLoadReplicaNum = -1;
        try {
            minLoadReplicaNum = PropertyAnalyzer.analyzeMinLoadReplicaNum(properties);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }
        if (minLoadReplicaNum > replicaAlloc.getTotalReplicaNum()) {
            throw new DdlException("Failed to check min load replica num [" + minLoadReplicaNum + "]  <= "
                    + "default replica num [" + replicaAlloc.getTotalReplicaNum() + "]");
        }
        olapTable.setMinLoadReplicaNum(minLoadReplicaNum);

        // get use light schema change
        Boolean enableLightSchemaChange;
        try {
            enableLightSchemaChange = PropertyAnalyzer.analyzeUseLightSchemaChange(properties);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }
        // use light schema change optimization
        olapTable.setEnableLightSchemaChange(enableLightSchemaChange);

        boolean disableAutoCompaction = false;
        try {
            disableAutoCompaction = PropertyAnalyzer.analyzeDisableAutoCompaction(properties);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }
        // use light schema change optimization
        olapTable.setDisableAutoCompaction(disableAutoCompaction);

        // set compaction policy
        String compactionPolicy = PropertyAnalyzer.SIZE_BASED_COMPACTION_POLICY;
        try {
            compactionPolicy = PropertyAnalyzer.analyzeCompactionPolicy(properties);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }
        olapTable.setCompactionPolicy(compactionPolicy);

        if (!compactionPolicy.equals(PropertyAnalyzer.TIME_SERIES_COMPACTION_POLICY)
                && (properties.containsKey(PropertyAnalyzer.PROPERTIES_TIME_SERIES_COMPACTION_GOAL_SIZE_MBYTES)
                || properties.containsKey(PropertyAnalyzer.PROPERTIES_TIME_SERIES_COMPACTION_FILE_COUNT_THRESHOLD)
                || properties
                        .containsKey(PropertyAnalyzer.PROPERTIES_TIME_SERIES_COMPACTION_TIME_THRESHOLD_SECONDS))) {
            throw new DdlException("only time series compaction policy support for time series config");
        }

        // set time series compaction goal size
        long timeSeriesCompactionGoalSizeMbytes
                                    = PropertyAnalyzer.TIME_SERIES_COMPACTION_GOAL_SIZE_MBYTES_DEFAULT_VALUE;
        try {
            timeSeriesCompactionGoalSizeMbytes = PropertyAnalyzer
                                        .analyzeTimeSeriesCompactionGoalSizeMbytes(properties);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }
        olapTable.setTimeSeriesCompactionGoalSizeMbytes(timeSeriesCompactionGoalSizeMbytes);

        // set time series compaction file count threshold
        long timeSeriesCompactionFileCountThreshold
                                    = PropertyAnalyzer.TIME_SERIES_COMPACTION_FILE_COUNT_THRESHOLD_DEFAULT_VALUE;
        try {
            timeSeriesCompactionFileCountThreshold = PropertyAnalyzer
                                    .analyzeTimeSeriesCompactionFileCountThreshold(properties);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }
        olapTable.setTimeSeriesCompactionFileCountThreshold(timeSeriesCompactionFileCountThreshold);

        // set time series compaction time threshold
        long timeSeriesCompactionTimeThresholdSeconds
                                     = PropertyAnalyzer.TIME_SERIES_COMPACTION_TIME_THRESHOLD_SECONDS_DEFAULT_VALUE;
        try {
            timeSeriesCompactionTimeThresholdSeconds = PropertyAnalyzer
                                    .analyzeTimeSeriesCompactionTimeThresholdSeconds(properties);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }
        olapTable.setTimeSeriesCompactionTimeThresholdSeconds(timeSeriesCompactionTimeThresholdSeconds);

        // get storage format
        TStorageFormat storageFormat = TStorageFormat.V2; // default is segment v2
        try {
            storageFormat = PropertyAnalyzer.analyzeStorageFormat(properties);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }
        olapTable.setStorageFormat(storageFormat);

        // get compression type
        TCompressionType compressionType = TCompressionType.LZ4;
        try {
            compressionType = PropertyAnalyzer.analyzeCompressionType(properties);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }
        olapTable.setCompressionType(compressionType);

        // check data sort properties
        DataSortInfo dataSortInfo = PropertyAnalyzer.analyzeDataSortInfo(properties, keysType,
                keysDesc.keysColumnSize(), storageFormat);
        olapTable.setDataSortInfo(dataSortInfo);

        boolean enableUniqueKeyMergeOnWrite = false;
        if (keysType == KeysType.UNIQUE_KEYS) {
            try {
                enableUniqueKeyMergeOnWrite = PropertyAnalyzer.analyzeUniqueKeyMergeOnWrite(properties);
            } catch (AnalysisException e) {
                throw new DdlException(e.getMessage());
            }
        }
        olapTable.setEnableUniqueKeyMergeOnWrite(enableUniqueKeyMergeOnWrite);

        boolean enableSingleReplicaCompaction = false;
        try {
            enableSingleReplicaCompaction = PropertyAnalyzer.analyzeEnableSingleReplicaCompaction(properties);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }
        if (enableUniqueKeyMergeOnWrite && enableSingleReplicaCompaction) {
            throw new DdlException(PropertyAnalyzer.PROPERTIES_ENABLE_SINGLE_REPLICA_COMPACTION
                    + " property is not supported for merge-on-write table");
        }
        olapTable.setEnableSingleReplicaCompaction(enableSingleReplicaCompaction);

        // analyze bloom filter columns
        Set<String> bfColumns = null;
        double bfFpp = 0;
        try {
            bfColumns = PropertyAnalyzer.analyzeBloomFilterColumns(properties, baseSchema, keysType);
            if (bfColumns != null && bfColumns.isEmpty()) {
                bfColumns = null;
            }

            bfFpp = PropertyAnalyzer.analyzeBloomFilterFpp(properties);
            if (bfColumns != null && bfFpp == 0) {
                bfFpp = FeConstants.default_bloom_filter_fpp;
            } else if (bfColumns == null) {
                bfFpp = 0;
            }

            olapTable.setBloomFilterInfo(bfColumns, bfFpp);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }

        Index.checkConflict(stmt.getIndexes(), bfColumns);

        olapTable.setReplicationAllocation(replicaAlloc);

        // set auto bucket
        boolean isAutoBucket = PropertyAnalyzer.analyzeBooleanProp(properties, PropertyAnalyzer.PROPERTIES_AUTO_BUCKET,
                false);
        olapTable.setIsAutoBucket(isAutoBucket);

        // set estimate partition size
        if (isAutoBucket) {
            String estimatePartitionSize = PropertyAnalyzer.analyzeEstimatePartitionSize(properties);
            olapTable.setEstimatePartitionSize(estimatePartitionSize);
        }

        // set in memory
        boolean isInMemory = PropertyAnalyzer.analyzeBooleanProp(properties, PropertyAnalyzer.PROPERTIES_INMEMORY,
                false);
        if (isInMemory) {
            throw new AnalysisException("Not support set 'in_memory'='true' now!");
        }
        olapTable.setIsInMemory(false);

        boolean isBeingSynced = PropertyAnalyzer.analyzeIsBeingSynced(properties, false);
        olapTable.setIsBeingSynced(isBeingSynced);
        if (isBeingSynced) {
            // erase colocate table, storage policy
            olapTable.ignoreInvaildPropertiesWhenSynced(properties);
            // remark auto bucket
            if (isAutoBucket) {
                olapTable.markAutoBucket();
            }
        }

        boolean storeRowColumn = false;
        try {
            storeRowColumn = PropertyAnalyzer.analyzeStoreRowColumn(properties);
            if (storeRowColumn && !enableLightSchemaChange) {
                throw new DdlException(
                        "Row store column rely on light schema change, enable light schema change first");
            }
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }
        olapTable.setStoreRowColumn(storeRowColumn);

        // set skip inverted index on load
        boolean skipWriteIndexOnLoad = PropertyAnalyzer.analyzeBooleanProp(properties,
                PropertyAnalyzer.PROPERTIES_SKIP_WRITE_INDEX_ON_LOAD, false);
        olapTable.setSkipWriteIndexOnLoad(skipWriteIndexOnLoad);

        boolean isMutable = PropertyAnalyzer.analyzeBooleanProp(properties, PropertyAnalyzer.PROPERTIES_MUTABLE, true);

        // set storage policy
        String storagePolicy = PropertyAnalyzer.analyzeStoragePolicy(properties);
        Env.getCurrentEnv().getPolicyMgr().checkStoragePolicyExist(storagePolicy);
        if (olapTable.getEnableUniqueKeyMergeOnWrite()
                && !Strings.isNullOrEmpty(storagePolicy)) {
            throw new AnalysisException(
                    "Can not create UNIQUE KEY table that enables Merge-On-write"
                     + " with storage policy(" + storagePolicy + ")");
        }
        // Consider one situation: if the table has no storage policy but some partitions
        // have their own storage policy then it might be erased by the following function.
        // So we only set the storage policy if the table's policy is not null or empty
        if (!Strings.isNullOrEmpty(storagePolicy)) {
            olapTable.setStoragePolicy(storagePolicy);
        }

        TTabletType tabletType;
        try {
            tabletType = PropertyAnalyzer.analyzeTabletType(properties);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }

        // set binlog config
        try {
            Map<String, String> binlogConfigMap = PropertyAnalyzer.analyzeBinlogConfig(properties);
            if (binlogConfigMap != null) {
                BinlogConfig binlogConfig = new BinlogConfig();
                binlogConfig.mergeFromProperties(binlogConfigMap);
                olapTable.setBinlogConfig(binlogConfig);
            }
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }
        BinlogConfig binlogConfigForTask = new BinlogConfig(olapTable.getBinlogConfig());

        if (partitionInfo.getType() == PartitionType.UNPARTITIONED) {
            // if this is an unpartitioned table, we should analyze data property and replication num here.
            // if this is a partitioned table, there properties are already analyzed
            // in RangePartitionDesc analyze phase.

            // use table name as this single partition name
            long partitionId = partitionNameToId.get(tableName);
            DataProperty dataProperty = null;
            try {
                dataProperty = PropertyAnalyzer.analyzeDataProperty(stmt.getProperties(),
                        new DataProperty(DataProperty.DEFAULT_STORAGE_MEDIUM));
            } catch (AnalysisException e) {
                throw new DdlException(e.getMessage());
            }
            Preconditions.checkNotNull(dataProperty);
            partitionInfo.setDataProperty(partitionId, dataProperty);
            partitionInfo.setReplicaAllocation(partitionId, replicaAlloc);
            partitionInfo.setIsInMemory(partitionId, isInMemory);
            partitionInfo.setTabletType(partitionId, tabletType);
            partitionInfo.setIsMutable(partitionId, isMutable);
        }

        // check colocation properties
        try {
            String colocateGroup = PropertyAnalyzer.analyzeColocate(properties);
            if (colocateGroup != null) {
                if (defaultDistributionInfo.getType() == DistributionInfoType.RANDOM) {
                    throw new AnalysisException("Random distribution for colocate table is unsupported");
                }
                String fullGroupName = GroupId.getFullGroupName(db.getId(), colocateGroup);
                ColocateGroupSchema groupSchema = Env.getCurrentColocateIndex().getGroupSchema(fullGroupName);
                if (groupSchema != null) {
                    // group already exist, check if this table can be added to this group
                    groupSchema.checkColocateSchema(olapTable);
                    groupSchema.checkDynamicPartition(properties, olapTable.getDefaultDistributionInfo());
                }
                // add table to this group, if group does not exist, create a new one
                Env.getCurrentColocateIndex()
                        .addTableToGroup(db.getId(), olapTable, fullGroupName, null /* generate group id inside */);
                olapTable.setColocateGroup(colocateGroup);
            }
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }

        // get base index storage type. default is COLUMN
        TStorageType baseIndexStorageType = null;
        try {
            baseIndexStorageType = PropertyAnalyzer.analyzeStorageType(properties);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }
        Preconditions.checkNotNull(baseIndexStorageType);
        // set base index meta
        int schemaVersion = 0;
        try {
            schemaVersion = PropertyAnalyzer.analyzeSchemaVersion(properties);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }
        int schemaHash = Util.generateSchemaHash();
        olapTable.setIndexMeta(baseIndexId, tableName, baseSchema, schemaVersion, schemaHash, shortKeyColumnCount,
                baseIndexStorageType, keysType, olapTable.getIndexes());

        for (AlterClause alterClause : stmt.getRollupAlterClauseList()) {
            if (olapTable.isDuplicateWithoutKey()) {
                throw new DdlException("Duplicate table without keys do not support add rollup!");
            }
            AddRollupClause addRollupClause = (AddRollupClause) alterClause;

            Long baseRollupIndex = olapTable.getIndexIdByName(tableName);

            // get storage type for rollup index
            TStorageType rollupIndexStorageType = null;
            try {
                rollupIndexStorageType = PropertyAnalyzer.analyzeStorageType(addRollupClause.getProperties());
            } catch (AnalysisException e) {
                throw new DdlException(e.getMessage());
            }
            Preconditions.checkNotNull(rollupIndexStorageType);
            // set rollup index meta to olap table
            List<Column> rollupColumns = Env.getCurrentEnv().getMaterializedViewHandler()
                    .checkAndPrepareMaterializedView(addRollupClause, olapTable, baseRollupIndex, false);
            short rollupShortKeyColumnCount = Env.calcShortKeyColumnCount(rollupColumns, alterClause.getProperties(),
                    true/*isKeysRequired*/);
            int rollupSchemaHash = Util.generateSchemaHash();
            long rollupIndexId = idGeneratorBuffer.getNextId();
            olapTable.setIndexMeta(rollupIndexId, addRollupClause.getRollupName(), rollupColumns, schemaVersion,
                    rollupSchemaHash, rollupShortKeyColumnCount, rollupIndexStorageType, keysType, null);
        }

        // analyse sequence map column
        String sequenceMapCol = null;
        try {
            sequenceMapCol = PropertyAnalyzer.analyzeSequenceMapCol(properties, olapTable.getKeysType());
            if (sequenceMapCol != null) {
                Column col = olapTable.getColumn(sequenceMapCol);
                if (col == null) {
                    throw new DdlException("The specified sequence column[" + sequenceMapCol + "] not exists");
                }
                if (!col.getType().isFixedPointType() && !col.getType().isDateType()) {
                    throw new DdlException("Sequence type only support integer types and date types");
                }
                olapTable.setSequenceMapCol(sequenceMapCol);
                olapTable.setSequenceInfo(col.getType());
            }
        } catch (Exception e) {
            throw new DdlException(e.getMessage());
        }

        // analyse sequence type
        Type sequenceColType = null;
        try {
            sequenceColType = PropertyAnalyzer.analyzeSequenceType(properties, olapTable.getKeysType());
            if (sequenceMapCol != null && sequenceColType != null) {
                throw new DdlException("The sequence_col and sequence_type cannot be set at the same time");
            }
            if (sequenceColType != null) {
                olapTable.setSequenceInfo(sequenceColType);
            }
        } catch (Exception e) {
            throw new DdlException(e.getMessage());
        }

        olapTable.initSchemaColumnUniqueId();
        olapTable.initAutoIncrentGenerator(db.getId());
        olapTable.rebuildFullSchema();

        // analyze version info
        Long versionInfo = null;
        try {
            versionInfo = PropertyAnalyzer.analyzeVersionInfo(properties);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }
        Preconditions.checkNotNull(versionInfo);

        // a set to record every new tablet created when create table
        // if failed in any step, use this set to do clear things
        Set<Long> tabletIdSet = new HashSet<>();
        // create partition
        try {
            if (partitionInfo.getType() == PartitionType.UNPARTITIONED) {
                if (storagePolicy.equals("") && properties != null && !properties.isEmpty()) {
                    // here, all properties should be checked
                    throw new DdlException("Unknown properties: " + properties);
                }
                // this is a 1-level partitioned table
                // use table name as partition name
                DistributionInfo partitionDistributionInfo = distributionDesc.toDistributionInfo(baseSchema);
                String partitionName = tableName;
                long partitionId = partitionNameToId.get(partitionName);

                // check replica quota if this operation done
                long indexNum = olapTable.getIndexIdToMeta().size();
                long bucketNum = partitionDistributionInfo.getBucketNum();
                long replicaNum = partitionInfo.getReplicaAllocation(partitionId).getTotalReplicaNum();
                long totalReplicaNum = indexNum * bucketNum * replicaNum;
                if (totalReplicaNum >= db.getReplicaQuotaLeftWithLock()) {
                    throw new DdlException(
                            "Database " + db.getFullName() + " create unpartitioned table " + tableName + " increasing "
                                    + totalReplicaNum + " of replica exceeds quota[" + db.getReplicaQuota() + "]");
                }
                Partition partition = createPartitionWithIndices(db.getClusterName(), db.getId(), olapTable.getId(),
                        olapTable.getName(), olapTable.getBaseIndexId(), partitionId, partitionName,
                        olapTable.getIndexIdToMeta(), partitionDistributionInfo,
                        partitionInfo.getDataProperty(partitionId).getStorageMedium(),
                        partitionInfo.getReplicaAllocation(partitionId), versionInfo, bfColumns, bfFpp, tabletIdSet,
                        olapTable.getCopiedIndexes(), isInMemory, storageFormat, tabletType, compressionType,
                        olapTable.getDataSortInfo(), olapTable.getEnableUniqueKeyMergeOnWrite(), storagePolicy,
                        idGeneratorBuffer, olapTable.disableAutoCompaction(),
                        olapTable.enableSingleReplicaCompaction(), skipWriteIndexOnLoad,
                        olapTable.getCompactionPolicy(), olapTable.getTimeSeriesCompactionGoalSizeMbytes(),
                        olapTable.getTimeSeriesCompactionFileCountThreshold(),
                        olapTable.getTimeSeriesCompactionTimeThresholdSeconds(),
                        storeRowColumn, binlogConfigForTask,
                        partitionInfo.getDataProperty(partitionId).isStorageMediumSpecified());
                olapTable.addPartition(partition);
            } else if (partitionInfo.getType() == PartitionType.RANGE
                    || partitionInfo.getType() == PartitionType.LIST) {
                try {
                    // just for remove entries in stmt.getProperties(),
                    // and then check if there still has unknown properties
                    PropertyAnalyzer.analyzeDataProperty(stmt.getProperties(),
                            new DataProperty(DataProperty.DEFAULT_STORAGE_MEDIUM));
                    if (partitionInfo.getType() == PartitionType.RANGE) {
                        DynamicPartitionUtil.checkAndSetDynamicPartitionProperty(olapTable, properties, db);
                    } else if (partitionInfo.getType() == PartitionType.LIST) {
                        if (DynamicPartitionUtil.checkDynamicPartitionPropertiesExist(properties)) {
                            throw new DdlException(
                                    "Only support dynamic partition properties on range partition table");
                        }
                    }

                    if (storagePolicy.equals("") && properties != null && !properties.isEmpty()) {
                        // here, all properties should be checked
                        throw new DdlException("Unknown properties: " + properties);
                    }
                } catch (AnalysisException e) {
                    throw new DdlException(e.getMessage());
                }

                // check replica quota if this operation done
                long totalReplicaNum = 0;
                for (Map.Entry<String, Long> entry : partitionNameToId.entrySet()) {
                    long indexNum = olapTable.getIndexIdToMeta().size();
                    long bucketNum = defaultDistributionInfo.getBucketNum();
                    long replicaNum = partitionInfo.getReplicaAllocation(entry.getValue()).getTotalReplicaNum();
                    totalReplicaNum += indexNum * bucketNum * replicaNum;
                }
                if (totalReplicaNum >= db.getReplicaQuotaLeftWithLock()) {
                    throw new DdlException(
                            "Database " + db.getFullName() + " create table " + tableName + " increasing "
                                    + totalReplicaNum + " of replica exceeds quota[" + db.getReplicaQuota() + "]");
                }

                // this is a 2-level partitioned tables
                for (Map.Entry<String, Long> entry : partitionNameToId.entrySet()) {
                    DataProperty dataProperty = partitionInfo.getDataProperty(entry.getValue());
                    DistributionInfo partitionDistributionInfo = distributionDesc.toDistributionInfo(baseSchema);
                    // use partition storage policy if it exist.
                    String partionStoragePolicy = partitionInfo.getStoragePolicy(entry.getValue());
                    if (olapTable.getEnableUniqueKeyMergeOnWrite()
                            && !Strings.isNullOrEmpty(partionStoragePolicy)) {
                        throw new AnalysisException(
                                "Can not create UNIQUE KEY table that enables Merge-On-write"
                                        + " with storage policy(" + partionStoragePolicy + ")");
                    }
                    // The table's storage policy has higher priority than partition's policy,
                    // so we'll directly use table's policy when it's set. Otherwise we use the
                    // partition's policy
                    if (!storagePolicy.isEmpty()) {
                        partionStoragePolicy = storagePolicy;
                    }
                    Env.getCurrentEnv().getPolicyMgr().checkStoragePolicyExist(partionStoragePolicy);

                    Partition partition = createPartitionWithIndices(db.getClusterName(), db.getId(),
                            olapTable.getId(), olapTable.getName(), olapTable.getBaseIndexId(), entry.getValue(),
                            entry.getKey(), olapTable.getIndexIdToMeta(), partitionDistributionInfo,
                            dataProperty.getStorageMedium(), partitionInfo.getReplicaAllocation(entry.getValue()),
                            versionInfo, bfColumns, bfFpp, tabletIdSet, olapTable.getCopiedIndexes(), isInMemory,
                            storageFormat, partitionInfo.getTabletType(entry.getValue()), compressionType,
                            olapTable.getDataSortInfo(), olapTable.getEnableUniqueKeyMergeOnWrite(),
                            partionStoragePolicy, idGeneratorBuffer, olapTable.disableAutoCompaction(),
                            olapTable.enableSingleReplicaCompaction(), skipWriteIndexOnLoad,
                            olapTable.getCompactionPolicy(), olapTable.getTimeSeriesCompactionGoalSizeMbytes(),
                            olapTable.getTimeSeriesCompactionFileCountThreshold(),
                            olapTable.getTimeSeriesCompactionTimeThresholdSeconds(),
                            storeRowColumn, binlogConfigForTask,
                            dataProperty.isStorageMediumSpecified());
                    olapTable.addPartition(partition);
                    olapTable.getPartitionInfo().getDataProperty(partition.getId())
                            .setStoragePolicy(partionStoragePolicy);
                }
            } else {
                throw new DdlException("Unsupported partition method: " + partitionInfo.getType().name());
            }

            Pair<Boolean, Boolean> result = db.createTableWithLock(olapTable, false, stmt.isSetIfNotExists());
            if (!result.first) {
                ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_EXISTS_ERROR, tableName);
            }

            if (result.second) {
                if (Env.getCurrentColocateIndex().isColocateTable(tableId)) {
                    // if this is a colocate table, its table id is already added to colocate group
                    // so we should remove the tableId here
                    Env.getCurrentColocateIndex().removeTable(tableId);
                }
                for (Long tabletId : tabletIdSet) {
                    Env.getCurrentInvertedIndex().deleteTablet(tabletId);
                }
                LOG.info("duplicate create table[{};{}], skip next steps", tableName, tableId);
            } else {
                // we have added these index to memory, only need to persist here
                if (Env.getCurrentColocateIndex().isColocateTable(tableId)) {
                    GroupId groupId = Env.getCurrentColocateIndex().getGroup(tableId);
                    Map<Tag, List<List<Long>>> backendsPerBucketSeq = Env.getCurrentColocateIndex()
                            .getBackendsPerBucketSeq(groupId);
                    ColocatePersistInfo info = ColocatePersistInfo.createForAddTable(groupId, tableId,
                            backendsPerBucketSeq);
                    Env.getCurrentEnv().getEditLog().logColocateAddTable(info);
                }
                LOG.info("successfully create table[{};{}]", tableName, tableId);
                // register or remove table from DynamicPartition after table created
                DynamicPartitionUtil.registerOrRemoveDynamicPartitionTable(db.getId(), olapTable, false);
                Env.getCurrentEnv().getDynamicPartitionScheduler()
                        .executeDynamicPartitionFirstTime(db.getId(), olapTable.getId());
                Env.getCurrentEnv().getDynamicPartitionScheduler()
                        .createOrUpdateRuntimeInfo(tableId, DynamicPartitionScheduler.LAST_UPDATE_TIME,
                                TimeUtils.getCurrentFormatTime());
            }
        } catch (DdlException e) {
            for (Long tabletId : tabletIdSet) {
                Env.getCurrentInvertedIndex().deleteTablet(tabletId);
            }
            // only remove from memory, because we have not persist it
            if (Env.getCurrentColocateIndex().isColocateTable(tableId)) {
                Env.getCurrentColocateIndex().removeTable(tableId);
            }

            throw e;
        }

        if (olapTable instanceof MaterializedView && MTMVJobFactory.isGenerateJob((MaterializedView) olapTable)) {
            List<MTMVJob> jobs = MTMVJobFactory.buildJob((MaterializedView) olapTable, db.getFullName());
            for (MTMVJob job : jobs) {
                Env.getCurrentEnv().getMTMVJobManager().createJob(job, false);
            }
            LOG.info("Create related {} mv job.", jobs.size());
        }
    }

    private void createMysqlTable(Database db, CreateTableStmt stmt) throws DdlException {
        String tableName = stmt.getTableName();

        List<Column> columns = stmt.getColumns();

        long tableId = Env.getCurrentEnv().getNextId();
        MysqlTable mysqlTable = new MysqlTable(tableId, tableName, columns, stmt.getProperties());
        mysqlTable.setComment(stmt.getComment());
        if (!db.createTableWithLock(mysqlTable, false, stmt.isSetIfNotExists()).first) {
            ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_EXISTS_ERROR, tableName);
        }
        LOG.info("successfully create table[{}-{}]", tableName, tableId);
    }

    private void createOdbcTable(Database db, CreateTableStmt stmt) throws DdlException {
        String tableName = stmt.getTableName();
        List<Column> columns = stmt.getColumns();

        long tableId = Env.getCurrentEnv().getNextId();
        OdbcTable odbcTable = new OdbcTable(tableId, tableName, columns, stmt.getProperties());
        odbcTable.setComment(stmt.getComment());
        if (!db.createTableWithLock(odbcTable, false, stmt.isSetIfNotExists()).first) {
            ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_EXISTS_ERROR, tableName);
        }
        LOG.info("successfully create table[{}-{}]", tableName, tableId);
    }

    private Table createEsTable(Database db, CreateTableStmt stmt) throws DdlException, AnalysisException {
        String tableName = stmt.getTableName();

        // validate props to get column from es.
        EsTable esTable = new EsTable(tableName, stmt.getProperties());

        // create columns
        List<Column> baseSchema = stmt.getColumns();

        if (baseSchema.isEmpty()) {
            baseSchema = esTable.genColumnsFromEs();
        }
        validateColumns(baseSchema, true);
        esTable.setNewFullSchema(baseSchema);

        // create partition info
        PartitionDesc partitionDesc = stmt.getPartitionDesc();
        PartitionInfo partitionInfo;
        Map<String, Long> partitionNameToId = Maps.newHashMap();
        if (partitionDesc != null) {
            partitionInfo = partitionDesc.toPartitionInfo(baseSchema, partitionNameToId, false);
        } else {
            long partitionId = Env.getCurrentEnv().getNextId();
            // use table name as single partition name
            partitionNameToId.put(tableName, partitionId);
            partitionInfo = new SinglePartitionInfo();
        }
        esTable.setPartitionInfo(partitionInfo);

        long tableId = Env.getCurrentEnv().getNextId();
        esTable.setId(tableId);
        esTable.setComment(stmt.getComment());
        esTable.syncTableMetaData();
        if (!db.createTableWithLock(esTable, false, stmt.isSetIfNotExists()).first) {
            ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_EXISTS_ERROR, tableName);
        }
        LOG.info("successfully create table{} with id {}", tableName, tableId);
        return esTable;
    }

    private void createBrokerTable(Database db, CreateTableStmt stmt) throws DdlException {
        String tableName = stmt.getTableName();

        List<Column> columns = stmt.getColumns();

        long tableId = Env.getCurrentEnv().getNextId();
        BrokerTable brokerTable = new BrokerTable(tableId, tableName, columns, stmt.getProperties());
        brokerTable.setComment(stmt.getComment());
        brokerTable.setBrokerProperties(stmt.getExtProperties());

        if (!db.createTableWithLock(brokerTable, false, stmt.isSetIfNotExists()).first) {
            ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_EXISTS_ERROR, tableName);
        }
        LOG.info("successfully create table[{}-{}]", tableName, tableId);
    }

    private void createHiveTable(Database db, CreateTableStmt stmt) throws DdlException {
        String tableName = stmt.getTableName();
        List<Column> columns = stmt.getColumns();
        long tableId = Env.getCurrentEnv().getNextId();
        HiveTable hiveTable = new HiveTable(tableId, tableName, columns, stmt.getProperties());
        hiveTable.setComment(stmt.getComment());
        // check hive table whether exists in hive database
        HiveConf hiveConf = new HiveConf();
        hiveConf.set(HMSProperties.HIVE_METASTORE_URIS,
                hiveTable.getHiveProperties().get(HMSProperties.HIVE_METASTORE_URIS));
        if (!Strings.isNullOrEmpty(hiveTable.getHiveProperties().get(HMSProperties.HIVE_VERSION))) {
            hiveConf.set(HMSProperties.HIVE_VERSION, hiveTable.getHiveProperties().get(HMSProperties.HIVE_VERSION));
        }
        PooledHiveMetaStoreClient client = new PooledHiveMetaStoreClient(hiveConf, 1);
        if (!client.tableExists(hiveTable.getHiveDb(), hiveTable.getHiveTable())) {
            throw new DdlException(String.format("Table [%s] dose not exist in Hive.", hiveTable.getHiveDbTable()));
        }
        // check hive table if exists in doris database
        if (!db.createTableWithLock(hiveTable, false, stmt.isSetIfNotExists()).first) {
            ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_EXISTS_ERROR, tableName);
        }
        LOG.info("successfully create table[{}-{}]", tableName, tableId);
    }

    private void createJdbcTable(Database db, CreateTableStmt stmt) throws DdlException {
        String tableName = stmt.getTableName();
        List<Column> columns = stmt.getColumns();

        long tableId = Env.getCurrentEnv().getNextId();

        JdbcTable jdbcTable = new JdbcTable(tableId, tableName, columns, stmt.getProperties());
        jdbcTable.setComment(stmt.getComment());
        // check table if exists
        if (!db.createTableWithLock(jdbcTable, false, stmt.isSetIfNotExists()).first) {
            ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_EXISTS_ERROR, tableName);
        }
        LOG.info("successfully create table[{}-{}]", tableName, tableId);
    }

    @VisibleForTesting
    public void createTablets(String clusterName, MaterializedIndex index, ReplicaState replicaState,
            DistributionInfo distributionInfo, long version, ReplicaAllocation replicaAlloc, TabletMeta tabletMeta,
            Set<Long> tabletIdSet, IdGeneratorBuffer idGeneratorBuffer, boolean isStorageMediumSpecified)
            throws DdlException {
        ColocateTableIndex colocateIndex = Env.getCurrentColocateIndex();
        Map<Tag, List<List<Long>>> backendsPerBucketSeq = null;
        GroupId groupId = null;
        if (colocateIndex.isColocateTable(tabletMeta.getTableId())) {
            if (distributionInfo.getType() == DistributionInfoType.RANDOM) {
                throw new DdlException("Random distribution for colocate table is unsupported");
            }
            // if this is a colocate table, try to get backend seqs from colocation index.
            groupId = colocateIndex.getGroup(tabletMeta.getTableId());
            backendsPerBucketSeq = colocateIndex.getBackendsPerBucketSeq(groupId);
        }

        // chooseBackendsArbitrary is true, means this may be the first table of colocation group,
        // or this is just a normal table, and we can choose backends arbitrary.
        // otherwise, backends should be chosen from backendsPerBucketSeq;
        boolean chooseBackendsArbitrary = backendsPerBucketSeq == null || backendsPerBucketSeq.isEmpty();
        if (chooseBackendsArbitrary) {
            backendsPerBucketSeq = Maps.newHashMap();
        }

        Map<Tag, Integer> nextIndexs = new HashMap<>();

        if (Config.enable_round_robin_create_tablet) {
            for (Map.Entry<Tag, Short> entry : replicaAlloc.getAllocMap().entrySet()) {
                int startPos = Env.getCurrentSystemInfo().getStartPosOfRoundRobin(entry.getKey(),
                        tabletMeta.getStorageMedium());
                if (startPos == -1) {
                    throw new DdlException("The number of BEs that match the policy is insufficient");
                }
                nextIndexs.put(entry.getKey(), startPos);
            }
        }

        for (int i = 0; i < distributionInfo.getBucketNum(); ++i) {
            // create a new tablet with random chosen backends
            Tablet tablet = new Tablet(idGeneratorBuffer.getNextId());

            // add tablet to inverted index first
            index.addTablet(tablet, tabletMeta);
            tabletIdSet.add(tablet.getId());

            // get BackendIds
            Map<Tag, List<Long>> chosenBackendIds;
            if (chooseBackendsArbitrary) {
                // This is the first colocate table in the group, or just a normal table,
                // choose backends
                if (Config.enable_round_robin_create_tablet) {
                    if (!Config.disable_storage_medium_check) {
                        chosenBackendIds = Env.getCurrentSystemInfo()
                                .getBeIdRoundRobinForReplicaCreation(replicaAlloc, tabletMeta.getStorageMedium(),
                                        nextIndexs);
                    } else {
                        chosenBackendIds = Env.getCurrentSystemInfo()
                                .getBeIdRoundRobinForReplicaCreation(replicaAlloc, null,
                                        nextIndexs);
                    }
                } else {
                    if (!Config.disable_storage_medium_check) {
                        chosenBackendIds = Env.getCurrentSystemInfo()
                                .selectBackendIdsForReplicaCreation(replicaAlloc, tabletMeta.getStorageMedium(),
                                        isStorageMediumSpecified, false);
                    } else {
                        chosenBackendIds = Env.getCurrentSystemInfo()
                                .selectBackendIdsForReplicaCreation(replicaAlloc, null,
                                        isStorageMediumSpecified, false);
                    }
                }

                for (Map.Entry<Tag, List<Long>> entry : chosenBackendIds.entrySet()) {
                    backendsPerBucketSeq.putIfAbsent(entry.getKey(), Lists.newArrayList());
                    backendsPerBucketSeq.get(entry.getKey()).add(entry.getValue());
                }
            } else {
                // get backends from existing backend sequence
                chosenBackendIds = Maps.newHashMap();
                for (Map.Entry<Tag, List<List<Long>>> entry : backendsPerBucketSeq.entrySet()) {
                    chosenBackendIds.put(entry.getKey(), entry.getValue().get(i));
                }
            }
            // create replicas
            short totalReplicaNum = (short) 0;
            for (List<Long> backendIds : chosenBackendIds.values()) {
                for (long backendId : backendIds) {
                    long replicaId = idGeneratorBuffer.getNextId();
                    Replica replica = new Replica(replicaId, backendId, replicaState, version,
                            tabletMeta.getOldSchemaHash());
                    tablet.addReplica(replica);
                    totalReplicaNum++;
                }
            }
            Preconditions.checkState(totalReplicaNum == replicaAlloc.getTotalReplicaNum(),
                    totalReplicaNum + " vs. " + replicaAlloc.getTotalReplicaNum());
        }

        if (groupId != null && chooseBackendsArbitrary) {
            colocateIndex.addBackendsPerBucketSeq(groupId, backendsPerBucketSeq);
            ColocatePersistInfo info = ColocatePersistInfo.createForBackendsPerBucketSeq(groupId, backendsPerBucketSeq);
            Env.getCurrentEnv().getEditLog().logColocateBackendsPerBucketSeq(info);
        }
    }

    /*
     * generate and check columns' order and key's existence,
     */
    private void validateColumns(List<Column> columns, boolean isKeysRequired) throws DdlException {
        if (columns.isEmpty()) {
            ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_MUST_HAVE_COLUMNS);
        }

        boolean encounterValue = false;
        boolean hasKey = false;
        for (Column column : columns) {
            if (column.isKey()) {
                if (encounterValue) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_OLAP_KEY_MUST_BEFORE_VALUE);
                }
                hasKey = true;
            } else {
                encounterValue = true;
            }
        }

        if (!hasKey && isKeysRequired) {
            ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_MUST_HAVE_KEYS);
        }
    }

    /*
     * check column's auto increment property
     */
    private void checkAutoIncColumns(List<Column> columns, KeysType type) throws DdlException {
        boolean encounterAutoIncColumn = false;
        for (Column column : columns) {
            if (column.isAutoInc()) {
                if (encounterAutoIncColumn) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_MORE_THAN_ONE_AUTO_INCREMENT_COLUMN);
                }
                encounterAutoIncColumn = true;
                if (column.isAllowNull()) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_AUTO_INCREMENT_COLUMN_NULLABLE);
                }
                if (column.getDefaultValue() != null) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_AUTO_INCREMENT_COLUMN_WITH_DEFAULT_VALUE);
                }
                if (!column.getType().isBigIntType()) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_AUTO_INCREMENT_COLUMN_NOT_BIGINT_TYPE);
                }
            }
        }
        if (encounterAutoIncColumn && type == KeysType.AGG_KEYS) {
            ErrorReport.reportDdlException(ErrorCode.ERR_AUTO_INCREMENT_COLUMN_IN_AGGREGATE_TABLE);
        }
    }

    /*
     * Truncate specified table or partitions.
     * The main idea is:
     *
     * 1. using the same schema to create new table(partitions)
     * 2. use the new created table(partitions) to replace the old ones.
     *
     * if no partition specified, it will truncate all partitions of this table, including all temp partitions,
     * otherwise, it will only truncate those specified partitions.
     *
     */
    public void truncateTable(TruncateTableStmt truncateTableStmt) throws DdlException {
        TableRef tblRef = truncateTableStmt.getTblRef();
        TableName dbTbl = tblRef.getName();

        // check, and save some info which need to be checked again later
        Map<String, Long> origPartitions = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        Map<Long, DistributionInfo> partitionsDistributionInfo = Maps.newHashMap();
        OlapTable copiedTbl;

        boolean truncateEntireTable = tblRef.getPartitionNames() == null;

        Database db = (Database) getDbOrDdlException(dbTbl.getDb());
        OlapTable olapTable = db.getOlapTableOrDdlException(dbTbl.getTbl());

        BinlogConfig binlogConfig;
        olapTable.readLock();
        try {
            olapTable.checkNormalStateForAlter();
            if (!truncateEntireTable) {
                for (String partName : tblRef.getPartitionNames().getPartitionNames()) {
                    Partition partition = olapTable.getPartition(partName);
                    if (partition == null) {
                        throw new DdlException("Partition " + partName + " does not exist");
                    }
                    origPartitions.put(partName, partition.getId());
                    partitionsDistributionInfo.put(partition.getId(), partition.getDistributionInfo());
                }
            } else {
                for (Partition partition : olapTable.getPartitions()) {
                    origPartitions.put(partition.getName(), partition.getId());
                    partitionsDistributionInfo.put(partition.getId(), partition.getDistributionInfo());
                }
            }
            // if table currently has no partitions, this sql like empty command and do nothing, should return directly
            // at the same time, it will avoid throwing IllegalStateException when `bufferSize` equals zero
            if (origPartitions.isEmpty()) {
                return;
            }
            copiedTbl = olapTable.selectiveCopy(origPartitions.keySet(), IndexExtState.VISIBLE, false);

            binlogConfig = new BinlogConfig(olapTable.getBinlogConfig());
        } finally {
            olapTable.readUnlock();
        }

        // 2. use the copied table to create partitions
        List<Partition> newPartitions = Lists.newArrayList();
        // tabletIdSet to save all newly created tablet ids.
        Set<Long> tabletIdSet = Sets.newHashSet();
        long bufferSize = IdGeneratorUtil.getBufferSizeForTruncateTable(copiedTbl, origPartitions.values());
        IdGeneratorBuffer idGeneratorBuffer = Env.getCurrentEnv().getIdGeneratorBuffer(bufferSize);
        try {
            for (Map.Entry<String, Long> entry : origPartitions.entrySet()) {
                // the new partition must use new id
                // If we still use the old partition id, the behavior of current load jobs on this partition
                // will be undefined.
                // By using a new id, load job will be aborted(just like partition is dropped),
                // which is the right behavior.
                long oldPartitionId = entry.getValue();
                long newPartitionId = idGeneratorBuffer.getNextId();
                Partition newPartition = createPartitionWithIndices(db.getClusterName(), db.getId(), copiedTbl.getId(),
                        copiedTbl.getName(), copiedTbl.getBaseIndexId(), newPartitionId, entry.getKey(),
                        copiedTbl.getIndexIdToMeta(), partitionsDistributionInfo.get(oldPartitionId),
                        copiedTbl.getPartitionInfo().getDataProperty(oldPartitionId).getStorageMedium(),
                        copiedTbl.getPartitionInfo().getReplicaAllocation(oldPartitionId), null /* version info */,
                        copiedTbl.getCopiedBfColumns(), copiedTbl.getBfFpp(), tabletIdSet, copiedTbl.getCopiedIndexes(),
                        copiedTbl.isInMemory(), copiedTbl.getStorageFormat(),
                        copiedTbl.getPartitionInfo().getTabletType(oldPartitionId), copiedTbl.getCompressionType(),
                        copiedTbl.getDataSortInfo(), copiedTbl.getEnableUniqueKeyMergeOnWrite(),
                        olapTable.getPartitionInfo().getDataProperty(oldPartitionId).getStoragePolicy(),
                        idGeneratorBuffer, olapTable.disableAutoCompaction(),
                        olapTable.enableSingleReplicaCompaction(), olapTable.skipWriteIndexOnLoad(),
                        olapTable.getCompactionPolicy(), olapTable.getTimeSeriesCompactionGoalSizeMbytes(),
                        olapTable.getTimeSeriesCompactionFileCountThreshold(),
                        olapTable.getTimeSeriesCompactionTimeThresholdSeconds(),
                        olapTable.storeRowColumn(), binlogConfig,
                        copiedTbl.getPartitionInfo().getDataProperty(oldPartitionId).isStorageMediumSpecified());
                newPartitions.add(newPartition);
            }
        } catch (DdlException e) {
            // create partition failed, remove all newly created tablets
            for (Long tabletId : tabletIdSet) {
                Env.getCurrentInvertedIndex().deleteTablet(tabletId);
            }
            throw e;
        }
        Preconditions.checkState(origPartitions.size() == newPartitions.size());

        // all partitions are created successfully, try to replace the old partitions.
        // before replacing, we need to check again.
        // Things may be changed outside the table lock.
        olapTable = (OlapTable) db.getTableOrDdlException(copiedTbl.getId());
        olapTable.writeLockOrDdlException();
        try {
            olapTable.checkNormalStateForAlter();
            // check partitions
            for (Map.Entry<String, Long> entry : origPartitions.entrySet()) {
                Partition partition = copiedTbl.getPartition(entry.getValue());
                if (partition == null || !partition.getName().equalsIgnoreCase(entry.getKey())) {
                    throw new DdlException("Partition [" + entry.getKey() + "] is changed");
                }
            }

            // check if meta changed
            // rollup index may be added or dropped, and schema may be changed during creating partition operation.
            boolean metaChanged = false;
            if (olapTable.getIndexNameToId().size() != copiedTbl.getIndexNameToId().size()) {
                metaChanged = true;
            } else {
                // compare schemaHash
                Map<Long, Integer> copiedIndexIdToSchemaHash = copiedTbl.getIndexIdToSchemaHash();
                for (Map.Entry<Long, Integer> entry : olapTable.getIndexIdToSchemaHash().entrySet()) {
                    long indexId = entry.getKey();
                    if (!copiedIndexIdToSchemaHash.containsKey(indexId)) {
                        metaChanged = true;
                        break;
                    }
                    if (!copiedIndexIdToSchemaHash.get(indexId).equals(entry.getValue())) {
                        metaChanged = true;
                        break;
                    }
                }

                List<Column> oldSchema = copiedTbl.getFullSchema();
                List<Column> newSchema = olapTable.getFullSchema();
                if (oldSchema.size() != newSchema.size()) {
                    LOG.warn("schema column size diff, old schema {}, new schema {}", oldSchema, newSchema);
                    metaChanged = true;
                } else {
                    List<Column> oldSchemaCopy = Lists.newArrayList(oldSchema);
                    List<Column> newSchemaCopy = Lists.newArrayList(newSchema);
                    oldSchemaCopy.sort((Column a, Column b) -> a.getUniqueId() - b.getUniqueId());
                    newSchemaCopy.sort((Column a, Column b) -> a.getUniqueId() - b.getUniqueId());
                    for (int i = 0; i < oldSchemaCopy.size(); ++i) {
                        if (!oldSchemaCopy.get(i).equals(newSchemaCopy.get(i))) {
                            LOG.warn("schema diff, old schema {}, new schema {}", oldSchemaCopy.get(i),
                                    newSchemaCopy.get(i));
                            metaChanged = true;
                            break;
                        }
                    }
                }
            }

            if (metaChanged) {
                throw new DdlException("Table[" + copiedTbl.getName() + "]'s meta has been changed. try again.");
            }

            // replace
            truncateTableInternal(olapTable, newPartitions, truncateEntireTable);

            // write edit log
            TruncateTableInfo info =
                    new TruncateTableInfo(db.getId(), db.getFullName(), olapTable.getId(), olapTable.getName(),
                            newPartitions,
                            truncateEntireTable, truncateTableStmt.toSqlWithoutTable());
            Env.getCurrentEnv().getEditLog().logTruncateTable(info);
        } finally {
            olapTable.writeUnlock();
        }

        LOG.info("finished to truncate table {}, partitions: {}", tblRef.getName().toSql(), tblRef.getPartitionNames());
    }

    private void truncateTableInternal(OlapTable olapTable, List<Partition> newPartitions, boolean isEntireTable) {
        // use new partitions to replace the old ones.
        Set<Long> oldTabletIds = Sets.newHashSet();
        for (Partition newPartition : newPartitions) {
            Partition oldPartition = olapTable.replacePartition(newPartition);
            // save old tablets to be removed
            for (MaterializedIndex index : oldPartition.getMaterializedIndices(IndexExtState.ALL)) {
                index.getTablets().forEach(t -> {
                    oldTabletIds.add(t.getId());
                });
            }
        }

        if (isEntireTable) {
            // drop all temp partitions
            olapTable.dropAllTempPartitions();
        }

        // remove the tablets in old partitions
        for (Long tabletId : oldTabletIds) {
            Env.getCurrentInvertedIndex().deleteTablet(tabletId);
        }
    }

    public void replayTruncateTable(TruncateTableInfo info) throws MetaNotFoundException {
        Database db = (Database) getDbOrMetaException(info.getDbId());
        OlapTable olapTable = (OlapTable) db.getTableOrMetaException(info.getTblId(), TableType.OLAP);
        olapTable.writeLock();
        try {
            truncateTableInternal(olapTable, info.getPartitions(), info.isEntireTable());

            // add tablet to inverted index
            TabletInvertedIndex invertedIndex = Env.getCurrentInvertedIndex();
            for (Partition partition : info.getPartitions()) {
                long partitionId = partition.getId();
                TStorageMedium medium = olapTable.getPartitionInfo().getDataProperty(partitionId)
                        .getStorageMedium();
                for (MaterializedIndex mIndex : partition.getMaterializedIndices(IndexExtState.ALL)) {
                    long indexId = mIndex.getId();
                    int schemaHash = olapTable.getSchemaHashByIndexId(indexId);
                    for (Tablet tablet : mIndex.getTablets()) {
                        TabletMeta tabletMeta = new TabletMeta(db.getId(), olapTable.getId(), partitionId, indexId,
                                schemaHash, medium);
                        long tabletId = tablet.getId();
                        invertedIndex.addTablet(tabletId, tabletMeta);
                        for (Replica replica : tablet.getReplicas()) {
                            invertedIndex.addReplica(tabletId, replica);
                        }
                    }
                }
            }
        } finally {
            olapTable.writeUnlock();
        }
    }

    public void replayAlterExternalTableSchema(String dbName, String tableName, List<Column> newSchema)
            throws MetaNotFoundException {
        Database db = (Database) getDbOrMetaException(dbName);
        Table table = db.getTableOrMetaException(tableName);
        table.writeLock();
        try {
            table.setNewFullSchema(newSchema);
        } finally {
            table.writeUnlock();
        }
    }

    // for test only
    public void clearDbs() {
        if (idToDb != null) {
            idToDb.clear();
        }
        if (fullNameToDb != null) {
            fullNameToDb.clear();
        }
    }

    @Deprecated
    public long loadCluster(DataInputStream dis, long checksum) throws IOException, DdlException {
        int clusterCount = dis.readInt();
        checksum ^= clusterCount;
        Preconditions.checkState(clusterCount <= 1, clusterCount);
        if (clusterCount == 1) {
            // read the old cluster
            Cluster oldCluster = Cluster.read(dis);
            checksum ^= oldCluster.getId();
        }
        return checksum;
    }

    public long saveDb(CountingDataOutputStream dos, long checksum) throws IOException {
        // 2 is for information_schema db & mysql db, which does not need to be persisted.
        // And internal database could not be dropped, so we assert dbCount >= 0
        int dbCount = idToDb.size() - MysqlCompatibleDatabase.COUNT;
        if (dbCount < 0) {
            throw new IOException("Invalid database count");
        }

        checksum ^= dbCount;
        dos.writeInt(dbCount);

        for (Map.Entry<Long, Database> entry : idToDb.entrySet()) {
            Database db = entry.getValue();
            // Don't write internal database meta.
            if (!db.isMysqlCompatibleDatabase()) {
                checksum ^= entry.getKey();
                db.write(dos);
            }
        }
        return checksum;
    }

    public long loadDb(DataInputStream dis, long checksum) throws IOException, DdlException {
        int dbCount = dis.readInt();
        long newChecksum = checksum ^ dbCount;
        for (long i = 0; i < dbCount; ++i) {
            Database db = new Database();
            db.readFields(dis);
            newChecksum ^= db.getId();

            Database dbPrev = fullNameToDb.get(db.getFullName());
            if (dbPrev != null) {
                String errMsg;
                if (dbPrev.isMysqlCompatibleDatabase() || db.isMysqlCompatibleDatabase()) {
                    errMsg = String.format(
                        "Mysql compatibility problem, previous checkpoint already has a database with full name "
                        + "%s. If its name is mysql, try to add mysqldb_replace_name=\"mysql_comp\" in fe.conf.",
                        db.getFullName());
                } else {
                    errMsg = String.format("Logical error, duplicated database fullname: %s, id: %d %d.",
                                    db.getFullName(), db.getId(), fullNameToDb.get(db.getFullName()).getId());
                }
                throw new IOException(errMsg);
            }
            idToDb.put(db.getId(), db);
            fullNameToDb.put(db.getFullName(), db);
            Env.getCurrentGlobalTransactionMgr().addDatabaseTransactionMgr(db.getId());

            db.analyze();
        }
        // ATTN: this should be done after load Db, and before loadAlterJob
        recreateTabletInvertIndex();
        // rebuild es state state
        getEsRepository().loadTableFromCatalog();
        LOG.info("finished replay databases from image");
        return newChecksum;
    }

    @Override
    public ConcurrentHashMap<Long, DatabaseIf> getIdToDb() {
        return new ConcurrentHashMap<>(idToDb);
    }

    @Override
    public Collection<DatabaseIf<? extends TableIf>> getAllDbs() {
        return new HashSet<>(idToDb.values());
    }

    public void replayAutoIncrementIdUpdateLog(AutoIncrementIdUpdateLog log) throws MetaNotFoundException {
        Database db = getDbOrMetaException(log.getDbId());
        OlapTable olapTable = (OlapTable) db.getTableOrMetaException(log.getTableId(), TableType.OLAP);
        olapTable.getAutoIncrementGenerator().applyChange(log.getColumnId(), log.getBatchEndId());
    }

    @Override
    public boolean enableAutoAnalyze() {
        return true;
    }
}
