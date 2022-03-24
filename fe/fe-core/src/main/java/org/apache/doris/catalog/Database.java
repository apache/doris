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

package org.apache.doris.catalog;

import org.apache.doris.catalog.Table.TableType;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.persist.CreateTableInfo;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nullable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * Internal representation of db-related metadata. Owned by Catalog instance.
 * Not thread safe.
 * <p/>
 * The static initialization method loadDb is the only way to construct a Db
 * object.
 * <p/>
 * Tables are stored in a map from the table name to the table object. They may
 * be loaded 'eagerly' at construction or 'lazily' on first reference. Tables
 * are accessed via getTable which may trigger a metadata read in two cases: *
 * if the table has never been loaded * if the table loading failed on the
 * previous attempt
 */
public class Database extends MetaObject implements Writable {
    private static final Logger LOG = LogManager.getLogger(Database.class);

    private long id;
    private volatile String fullQualifiedName;
    private String clusterName;
    private ReentrantReadWriteLock rwLock;

    // table family group map
    private Map<Long, Table> idToTable;
    private Map<String, Table> nameToTable;
    // table name lower cast -> table name
    private Map<String, String> lowerCaseToTableName;

    // user define function
    private ConcurrentMap<String, ImmutableList<Function>> name2Function = Maps.newConcurrentMap();
    // user define encryptKey for current db
    private DatabaseEncryptKey dbEncryptKey;

    private volatile long dataQuotaBytes;

    private volatile long replicaQuotaSize;

    private volatile boolean isDropped;

    public enum DbState {
        NORMAL, LINK, MOVE
    }

    private String attachDbName;
    private DbState dbState;

    private DatabaseProperty dbProperties = new DatabaseProperty();

    public Database() {
        this(0, null);
    }

    public Database(long id, String name) {
        this.id = id;
        this.fullQualifiedName = name;
        if (this.fullQualifiedName == null) {
            this.fullQualifiedName = "";
        }
        this.rwLock = new ReentrantReadWriteLock(true);
        this.idToTable = Maps.newConcurrentMap();
        this.nameToTable = Maps.newConcurrentMap();
        this.lowerCaseToTableName = Maps.newConcurrentMap();
        this.dataQuotaBytes = Config.default_db_data_quota_bytes;
        this.replicaQuotaSize = Config.default_db_replica_quota_size;
        this.dbState = DbState.NORMAL;
        this.attachDbName = "";
        this.clusterName = "";
        this.dbEncryptKey = new DatabaseEncryptKey();
    }

    public void markDropped() {
        isDropped = true;
    }

    public void unmarkDropped() {
        isDropped = false;
    }

    public void readLock() {
        this.rwLock.readLock().lock();
    }

    public void readUnlock() {
        this.rwLock.readLock().unlock();
    }

    public void writeLock() {
        this.rwLock.writeLock().lock();
    }

    public void writeUnlock() {
        this.rwLock.writeLock().unlock();
    }

    public boolean tryWriteLock(long timeout, TimeUnit unit) {
        try {
            return this.rwLock.writeLock().tryLock(timeout, unit);
        } catch (InterruptedException e) {
            LOG.warn("failed to try write lock at db[" + id + "]", e);
            return false;
        }
    }

    public boolean isWriteLockHeldByCurrentThread() {
        return this.rwLock.writeLock().isHeldByCurrentThread();
    }

    public boolean writeLockIfExist() {
        if (!isDropped) {
            this.rwLock.writeLock().lock();
            return true;
        }
        return false;
    }

    public <E extends Exception> void writeLockOrException(E e) throws E {
        writeLock();
        if (isDropped) {
            writeUnlock();
            throw e;
        }
    }

    public void writeLockOrDdlException() throws DdlException {
        writeLockOrException(new DdlException("unknown db, dbName=" + fullQualifiedName));
    }

    public long getId() {
        return id;
    }

    public String getFullName() {
        return fullQualifiedName;
    }

    public void setNameWithLock(String newName) {
        writeLock();
        try {
            this.fullQualifiedName = newName;
        } finally {
            writeUnlock();
        }
    }

    public void setDataQuota(long newQuota) {
        Preconditions.checkArgument(newQuota >= 0L);
        LOG.info("database[{}] set quota from {} to {}", fullQualifiedName, dataQuotaBytes, newQuota);
        this.dataQuotaBytes = newQuota;
    }

    public void setReplicaQuota(long newQuota) {
        Preconditions.checkArgument(newQuota >= 0L);
        LOG.info("database[{}] set replica quota from {} to {}", fullQualifiedName, replicaQuotaSize, newQuota);
        this.replicaQuotaSize = newQuota;
    }

    public long getDataQuota() {
        return dataQuotaBytes;
    }

    public long getReplicaQuota() {
        return replicaQuotaSize;
    }

    public DatabaseProperty getDbProperties() {
        return dbProperties;
    }

    public void setDbProperties(DatabaseProperty dbProperties) {
        this.dbProperties = dbProperties;
    }

    public long getUsedDataQuotaWithLock() {
        long usedDataQuota = 0;
        readLock();
        try {
            for (Table table : this.idToTable.values()) {
                if (table.getType() != TableType.OLAP) {
                    continue;
                }

                OlapTable olapTable = (OlapTable) table;
                olapTable.readLock();
                try {
                    usedDataQuota = usedDataQuota + olapTable.getDataSize();
                } finally {
                    olapTable.readUnlock();
                }
            }
            return usedDataQuota;
        } finally {
            readUnlock();
        }
    }


    public long getReplicaQuotaLeftWithLock() {
        long usedReplicaQuota = 0;
        readLock();
        try {
            for (Table table : this.idToTable.values()) {
                if (table.getType() != TableType.OLAP) {
                    continue;
                }

                OlapTable olapTable = (OlapTable) table;
                olapTable.readLock();
                try {
                    usedReplicaQuota = usedReplicaQuota + olapTable.getReplicaCount();
                } finally {
                    olapTable.readUnlock();
                }
            }

            long leftReplicaQuota = replicaQuotaSize - usedReplicaQuota;
            return Math.max(leftReplicaQuota, 0L);
        } finally {
            readUnlock();
        }
    }

    public void checkDataSizeQuota() throws DdlException {
        Pair<Double, String> quotaUnitPair = DebugUtil.getByteUint(dataQuotaBytes);
        String readableQuota = DebugUtil.DECIMAL_FORMAT_SCALE_3.format(quotaUnitPair.first) + " "
                + quotaUnitPair.second;
        long usedDataQuota = getUsedDataQuotaWithLock();
        long leftDataQuota = Math.max(dataQuotaBytes - usedDataQuota, 0);

        Pair<Double, String> leftQuotaUnitPair = DebugUtil.getByteUint(leftDataQuota);
        String readableLeftQuota = DebugUtil.DECIMAL_FORMAT_SCALE_3.format(leftQuotaUnitPair.first) + " "
                + leftQuotaUnitPair.second;

        LOG.info("database[{}] data quota: left bytes: {} / total: {}",
                fullQualifiedName, readableLeftQuota, readableQuota);

        if (leftDataQuota <= 0L) {
            throw new DdlException("Database[" + fullQualifiedName
                    + "] data size exceeds quota[" + readableQuota + "]");
        }
    }

    public void checkReplicaQuota() throws DdlException {
        long leftReplicaQuota = getReplicaQuotaLeftWithLock();
        LOG.info("database[{}] replica quota: left number: {} / total: {}",
                fullQualifiedName, leftReplicaQuota, replicaQuotaSize);

        if (leftReplicaQuota <= 0L) {
            throw new DdlException("Database[" + fullQualifiedName
                    + "] replica number exceeds quota[" + replicaQuotaSize + "]");
        }
    }

    public void checkQuota() throws DdlException {
        checkDataSizeQuota();
        checkReplicaQuota();
    }

    private boolean isTableExist(String tableName) {
        if (Catalog.isTableNamesCaseInsensitive()) {
            tableName = lowerCaseToTableName.get(tableName.toLowerCase());
            if (tableName == null) {
                return false;
            }
        }
        return nameToTable.containsKey(tableName);
    }

    // return pair <success?, table exist?>
    public Pair<Boolean, Boolean> createTableWithLock(Table table, boolean isReplay, boolean setIfNotExist) throws DdlException {
        boolean result = true;
        // if a table is already exists, then edit log won't be executed
        // some caller of this method may need to know this message
        boolean isTableExist = false;
        writeLockOrDdlException();
        try {
            String tableName = table.getName();
            if (Catalog.isStoredTableNamesLowerCase()) {
                tableName = tableName.toLowerCase();
            }
            if (isTableExist(tableName)) {
                result = setIfNotExist;
                isTableExist = true;
            } else {
                idToTable.put(table.getId(), table);
                nameToTable.put(table.getName(), table);
                lowerCaseToTableName.put(tableName.toLowerCase(), tableName);

                if (!isReplay) {
                    // Write edit log
                    CreateTableInfo info = new CreateTableInfo(fullQualifiedName, table);
                    Catalog.getCurrentCatalog().getEditLog().logCreateTable(info);
                }
                if (table.getType() == TableType.ELASTICSEARCH) {
                    Catalog.getCurrentCatalog().getEsRepository().registerTable((EsTable) table);
                }
            }
            return Pair.create(result, isTableExist);
        } finally {
            writeUnlock();
        }
    }

    public boolean createTable(Table table) {
        boolean result = true;
        String tableName = table.getName();
        if (Catalog.isStoredTableNamesLowerCase()) {
            tableName = tableName.toLowerCase();
        }
        if (isTableExist(tableName)) {
            result = false;
        } else {
            idToTable.put(table.getId(), table);
            nameToTable.put(table.getName(), table);
            lowerCaseToTableName.put(tableName.toLowerCase(), tableName);
        }
        table.unmarkDropped();
        return result;
    }

    public void dropTable(String tableName) {
        if (Catalog.isStoredTableNamesLowerCase()) {
            tableName = tableName.toLowerCase();
        }
        Table table = getTableNullable(tableName);
        if (table != null) {
            this.nameToTable.remove(tableName);
            this.idToTable.remove(table.getId());
            this.lowerCaseToTableName.remove(tableName.toLowerCase());
            table.markDropped();
        }
    }

    public List<Table> getTables() {
        return new ArrayList<>(idToTable.values());
    }

    // tables must get read or write table in fixed order to avoid potential dead lock
    public List<Table> getTablesOnIdOrder() {
        return idToTable.values().stream()
                .sorted(Comparator.comparing(Table::getId))
                .collect(Collectors.toList());
    }

    public List<Table> getViews() {
        List<Table> views = new ArrayList<>();
        for (Table table : idToTable.values()) {
            if (table.getType() == TableType.VIEW) {
                views.add(table);
            }
        }
        return views;
    }

    /**
     *  this method is used for get existed table list by table id list, if table not exist, just ignore it.
     */
    public List<Table> getTablesOnIdOrderIfExist(List<Long> tableIdList) {
        List<Table> tableList = Lists.newArrayListWithCapacity(tableIdList.size());
        for (Long tableId : tableIdList) {
            Table table = idToTable.get(tableId);
            if (table != null) {
                tableList.add(table);
            }
        }
        if (tableList.size() > 1) {
            return tableList.stream().sorted(Comparator.comparing(Table::getId)).collect(Collectors.toList());
        }
        return tableList;
    }

    public List<Table> getTablesOnIdOrderOrThrowException(List<Long> tableIdList) throws MetaNotFoundException {
        List<Table> tableList = Lists.newArrayListWithCapacity(tableIdList.size());
        for (Long tableId : tableIdList) {
            Table table = idToTable.get(tableId);
            if (table == null) {
                throw new MetaNotFoundException("unknown table, tableId=" + tableId);
            }
            tableList.add(table);
        }
        if (tableList.size() > 1) {
            return tableList.stream().sorted(Comparator.comparing(Table::getId)).collect(Collectors.toList());
        }
        return tableList;
    }

    public Set<String> getTableNamesWithLock() {
        readLock();
        try {
            return new HashSet<>(this.nameToTable.keySet());
        } finally {
            readUnlock();
        }
    }

    /**
     * This is a thread-safe method when nameToTable is a concurrent hash map
     */
    @Nullable
    public Table getTableNullable(String tableName) {
        if (Catalog.isStoredTableNamesLowerCase()) {
            tableName = tableName.toLowerCase();
        }
        if (Catalog.isTableNamesCaseInsensitive()) {
            tableName = lowerCaseToTableName.get(tableName.toLowerCase());
            if (tableName == null) {
                return null;
            }
        }
        return nameToTable.get(tableName);
    }

    /**
     * This is a thread-safe method when idToTable is a concurrent hash map
     */
    @Nullable
    public Table getTableNullable(long tableId) {
        return idToTable.get(tableId);
    }

    public Optional<Table> getTable(String tableName) {
        return Optional.ofNullable(getTableNullable(tableName));
    }

    public Optional<Table> getTable(long tableId) {
        return Optional.ofNullable(getTableNullable(tableId));
    }

    public <E extends Exception> Table getTableOrException(String tableName, java.util.function.Function<String, E> e) throws E {
        Table table = getTableNullable(tableName);
        if (table == null) {
            throw e.apply(tableName);
        }
        return table;
    }

    public <E extends Exception> Table getTableOrException(long tableId, java.util.function.Function<Long, E> e) throws E {
        Table table = getTableNullable(tableId);
        if (table == null) {
            throw e.apply(tableId);
        }
        return table;
    }

    public Table getTableOrMetaException(String tableName) throws MetaNotFoundException {
        return getTableOrException(tableName, t -> new MetaNotFoundException("unknown table, tableName=" + t));
    }

    public Table getTableOrMetaException(long tableId) throws MetaNotFoundException {
        return getTableOrException(tableId, t -> new MetaNotFoundException("unknown table, tableId=" + t));
    }

    @SuppressWarnings("unchecked")
    public <T extends Table> T getTableOrMetaException(String tableName, TableType tableType) throws MetaNotFoundException {
        Table table = getTableOrMetaException(tableName);
        if (table.getType() != tableType) {
            throw new MetaNotFoundException("table type is not " + tableType + ", tableName=" + tableName + ", type=" + table.getType());
        }
        return (T) table;
    }

    @SuppressWarnings("unchecked")
    public <T extends Table> T getTableOrMetaException(long tableId, TableType tableType) throws MetaNotFoundException {
        Table table = getTableOrMetaException(tableId);
        if (table.getType() != tableType) {
            throw new MetaNotFoundException("table type is not " + tableType + ", tableId=" + tableId + ", type=" + table.getType());
        }
        return (T) table;
    }

    public Table getTableOrDdlException(String tableName) throws DdlException {
        return getTableOrException(tableName, t -> new DdlException(ErrorCode.ERR_BAD_TABLE_ERROR.formatErrorMsg(t)));
    }

    public OlapTable getOlapTableOrDdlException(String tableName) throws DdlException {
        Table table = getTableOrDdlException(tableName);
        if (!(table instanceof OlapTable)) {
            throw new DdlException(ErrorCode.ERR_NOT_OLAP_TABLE.formatErrorMsg(tableName));
        }
        return (OlapTable) table;
    }

    public Table getTableOrDdlException(long tableId) throws DdlException {
        return getTableOrException(tableId, t -> new DdlException(ErrorCode.ERR_BAD_TABLE_ERROR.formatErrorMsg(t)));
    }

    public Table getTableOrAnalysisException(String tableName) throws AnalysisException {
        return getTableOrException(tableName, t -> new AnalysisException(ErrorCode.ERR_UNKNOWN_TABLE.formatErrorMsg(t
                , fullQualifiedName)));
    }

    public OlapTable getOlapTableOrAnalysisException(String tableName) throws AnalysisException {
        Table table = getTableOrAnalysisException(tableName);
        if (!(table instanceof OlapTable)) {
            throw new AnalysisException(ErrorCode.ERR_NOT_OLAP_TABLE.formatErrorMsg(tableName));
        }
        return (OlapTable) table;
    }

    public Table getTableOrAnalysisException(long tableId) throws AnalysisException {
        return getTableOrException(tableId, t -> new AnalysisException(ErrorCode.ERR_BAD_TABLE_ERROR.formatErrorMsg(t)));
    }

    public int getMaxReplicationNum() {
        int ret = 0;
        readLock();
        try {
            for (Table table : idToTable.values()) {
                if (table.getType() != TableType.OLAP) {
                    continue;
                }
                OlapTable olapTable = (OlapTable) table;
                table.readLock();
                try {
                    for (Partition partition : olapTable.getAllPartitions()) {
                        short replicationNum = olapTable.getPartitionInfo().getReplicaAllocation(partition.getId()).getTotalReplicaNum();
                        if (ret < replicationNum) {
                            ret = replicationNum;
                        }
                    }
                } finally {
                    table.readUnlock();
                }
            }
        } finally {
            readUnlock();
        }
        return ret;
    }

    public static Database read(DataInput in) throws IOException {
        Database db = new Database();
        db.readFields(in);
        return db;
    }

    @Override
    public String getSignature(int signatureVersion) {
        StringBuilder sb = new StringBuilder(signatureVersion);
        sb.append(fullQualifiedName);
        String md5 = DigestUtils.md5Hex(sb.toString());
        LOG.debug("get signature of database {}: {}. signature string: {}", fullQualifiedName, md5, sb.toString());
        return md5;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);

        out.writeLong(id);
        Text.writeString(out, fullQualifiedName);
        // write tables
        int numTables = nameToTable.size();
        out.writeInt(numTables);
        for (Map.Entry<String, Table> entry : nameToTable.entrySet()) {
            entry.getValue().write(out);
        }

        out.writeLong(dataQuotaBytes);
        Text.writeString(out, clusterName);
        Text.writeString(out, dbState.name());
        Text.writeString(out, attachDbName);

        // write functions
        out.writeInt(name2Function.size());
        for (Entry<String, ImmutableList<Function>> entry : name2Function.entrySet()) {
            Text.writeString(out, entry.getKey());
            out.writeInt(entry.getValue().size());
            for (Function function : entry.getValue()) {
                function.write(out);
            }
        }

        // write encryptKeys
        dbEncryptKey.write(out);

        out.writeLong(replicaQuotaSize);
        dbProperties.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);

        id = in.readLong();
        fullQualifiedName = Text.readString(in);
        // read groups
        int numTables = in.readInt();
        for (int i = 0; i < numTables; ++i) {
            Table table = Table.read(in);
            String tableName = table.getName();
            nameToTable.put(tableName, table);
            idToTable.put(table.getId(), table);
            lowerCaseToTableName.put(tableName.toLowerCase(), tableName);
        }

        // read quota
        dataQuotaBytes = in.readLong();
        clusterName = Text.readString(in);
        dbState = DbState.valueOf(Text.readString(in));
        attachDbName = Text.readString(in);

        int numEntries = in.readInt();
        for (int i = 0; i < numEntries; ++i) {
            String name = Text.readString(in);
            ImmutableList.Builder<Function> builder = ImmutableList.builder();
            int numFunctions = in.readInt();
            for (int j = 0; j < numFunctions; ++j) {
                builder.add(Function.read(in));
            }

            name2Function.put(name, builder.build());
        }

        // read encryptKeys
        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_102) {
            dbEncryptKey = DatabaseEncryptKey.read(in);
        }

        replicaQuotaSize = in.readLong();

        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_105) {
            dbProperties = DatabaseProperty.read(in);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof Database)) {
            return false;
        }

        Database database = (Database) obj;

        if (idToTable != database.idToTable) {
            if (idToTable.size() != database.idToTable.size()) {
                return false;
            }
            for (Entry<Long, Table> entry : idToTable.entrySet()) {
                long key = entry.getKey();
                if (!database.idToTable.containsKey(key)) {
                    return false;
                }
                if (!entry.getValue().equals(database.idToTable.get(key))) {
                    return false;
                }
            }
        }

        return (id == database.id) && (fullQualifiedName.equals(database.fullQualifiedName)
                && dataQuotaBytes == database.dataQuotaBytes);
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public DbState getDbState() {
        return dbState;
    }

    public void setDbState(DbState dbState) {
        if (dbState == null) {
            return;
        }
        this.dbState = dbState;
    }

    public void setAttachDb(String name) {
        this.attachDbName = name;
    }

    public String getAttachDb() {
        return this.attachDbName;
    }

    public void setName(String name) {
        this.fullQualifiedName = name;
    }

    public synchronized void addFunction(Function function) throws UserException {
        addFunctionImpl(function, false);
        Catalog.getCurrentCatalog().getEditLog().logAddFunction(function);
    }

    public synchronized void replayAddFunction(Function function) {
        try {
            addFunctionImpl(function, true);
        } catch (UserException e) {
            Preconditions.checkArgument(false);
        }
    }

    // return true if add success, false
    private void addFunctionImpl(Function function, boolean isReplay) throws UserException {
        String functionName = function.getFunctionName().getFunction();
        List<Function> existFuncs = name2Function.get(functionName);
        if (!isReplay) {
            if (existFuncs != null) {
                for (Function existFunc : existFuncs) {
                    if (function.compare(existFunc, Function.CompareMode.IS_IDENTICAL)) {
                        throw new UserException("function already exists");
                    }
                }
            }
            // Get function id for this UDF, use CatalogIdGenerator. Only get function id
            // when isReplay is false
            long functionId = Catalog.getCurrentCatalog().getNextId();
            function.setId(functionId);
        }

        ImmutableList.Builder<Function> builder = ImmutableList.builder();
        if (existFuncs != null) {
            builder.addAll(existFuncs);
        }
        builder.add(function);
        name2Function.put(functionName, builder.build());
    }

    public synchronized void dropFunction(FunctionSearchDesc function) throws UserException {
        dropFunctionImpl(function);
        Catalog.getCurrentCatalog().getEditLog().logDropFunction(function);
    }

    public synchronized void replayDropFunction(FunctionSearchDesc functionSearchDesc) {
        try {
            dropFunctionImpl(functionSearchDesc);
        } catch (UserException e) {
            Preconditions.checkArgument(false);
        }
    }

    private void dropFunctionImpl(FunctionSearchDesc function) throws UserException {
        String functionName = function.getName().getFunction();
        List<Function> existFuncs = name2Function.get(functionName);
        if (existFuncs == null) {
            throw new UserException("Unknown function, function=" + function.toString());
        }
        boolean isFound = false;
        ImmutableList.Builder<Function> builder = ImmutableList.builder();
        for (Function existFunc : existFuncs) {
            if (function.isIdentical(existFunc)) {
                isFound = true;
            } else {
                builder.add(existFunc);
            }
        }
        if (!isFound) {
            throw new UserException("Unknown function, function=" + function.toString());
        }
        ImmutableList<Function> newFunctions = builder.build();
        if (newFunctions.isEmpty()) {
            name2Function.remove(functionName);
        } else {
            name2Function.put(functionName, newFunctions);
        }
    }

    public synchronized Function getFunction(Function desc, Function.CompareMode mode) {
        List<Function> fns = name2Function.get(desc.getFunctionName().getFunction());
        if (fns == null) {
            return null;
        }
        return Function.getFunction(fns, desc, mode);
    }

    public synchronized Function getFunction(FunctionSearchDesc function) throws AnalysisException {
        String functionName = function.getName().getFunction();
        List<Function> existFuncs = name2Function.get(functionName);
        if (existFuncs == null) {
            throw new AnalysisException("Unknown function, function=" + function.toString());
        }

        for (Function existFunc : existFuncs) {
            if (function.isIdentical(existFunc)) {
                return existFunc;
            }
        }
        throw new AnalysisException("Unknown function, function=" + function.toString());
    }

    public synchronized List<Function> getFunctions() {
        List<Function> functions = Lists.newArrayList();
        for (Map.Entry<String, ImmutableList<Function>> entry : name2Function.entrySet()) {
            functions.addAll(entry.getValue());
        }
        return functions;
    }

    public boolean isInfoSchemaDb() {
        return ClusterNamespace.getNameFromFullName(fullQualifiedName).equalsIgnoreCase(InfoSchemaDb.DATABASE_NAME);
    }

    public synchronized void addEncryptKey(EncryptKey encryptKey) throws UserException {
        addEncryptKeyImpl(encryptKey, false);
        Catalog.getCurrentCatalog().getEditLog().logAddEncryptKey(encryptKey);
    }

    public synchronized void replayAddEncryptKey(EncryptKey encryptKey) {
        try {
            addEncryptKeyImpl(encryptKey, true);
        } catch (UserException e) {
            Preconditions.checkArgument(false);
        }
    }

    private void addEncryptKeyImpl(EncryptKey encryptKey, boolean isReplay) throws UserException {
        String keyName = encryptKey.getEncryptKeyName().getKeyName();
        EncryptKey existKey = dbEncryptKey.getName2EncryptKey().get(keyName);
        if (!isReplay) {
            if (existKey != null) {
                if (existKey.isIdentical(encryptKey)) {
                    throw new UserException("encryptKey [" + existKey.getEncryptKeyName().toString() + "] already exists");
                }
            }
        }

        dbEncryptKey.getName2EncryptKey().put(keyName, encryptKey);
    }

    public synchronized void dropEncryptKey(EncryptKeySearchDesc encryptKeySearchDesc) throws UserException {
        dropEncryptKeyImpl(encryptKeySearchDesc);
        Catalog.getCurrentCatalog().getEditLog().logDropEncryptKey(encryptKeySearchDesc);
    }

    public synchronized void replayDropEncryptKey(EncryptKeySearchDesc encryptKeySearchDesc) {
        try {
            dropEncryptKeyImpl(encryptKeySearchDesc);
        } catch (UserException e) {
            Preconditions.checkArgument(false);
        }
    }

    private void dropEncryptKeyImpl(EncryptKeySearchDesc encryptKeySearchDesc) throws UserException {
        String keyName = encryptKeySearchDesc.getKeyEncryptKeyName().getKeyName();
        EncryptKey existKey = dbEncryptKey.getName2EncryptKey().get(keyName);
        if (existKey == null) {
            throw new UserException("Unknown encryptKey, encryptKey=" + encryptKeySearchDesc.toString());
        }
        boolean isFound = false;
        if (encryptKeySearchDesc.isIdentical(existKey)) {
            isFound = true;
        }
        if (!isFound) {
            throw new UserException("Unknown encryptKey, encryptKey=" + encryptKeySearchDesc.toString());
        }
        dbEncryptKey.getName2EncryptKey().remove(keyName);
    }

    public synchronized List<EncryptKey> getEncryptKeys() {
        List<EncryptKey> encryptKeys = Lists.newArrayList();
        for (Map.Entry<String, EncryptKey> entry : dbEncryptKey.getName2EncryptKey().entrySet()) {
            encryptKeys.add(entry.getValue());
        }
        return encryptKeys;
    }

    public synchronized EncryptKey getEncryptKey(String keyName) {
        if (dbEncryptKey.getName2EncryptKey().containsKey(keyName)) {
            return dbEncryptKey.getName2EncryptKey().get(keyName);
        }
        return null;
    }
}
