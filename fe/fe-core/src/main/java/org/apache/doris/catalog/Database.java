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

import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.persist.CreateTableInfo;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
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
public class Database extends MetaObject implements Writable, DatabaseIf<Table> {
    private static final Logger LOG = LogManager.getLogger(Database.class);

    private static final String TRANSACTION_QUOTA_SIZE = "transactionQuotaSize";

    @SerializedName(value = "id")
    private long id;
    @SerializedName(value = "fullQualifiedName")
    private volatile String fullQualifiedName;
    @SerializedName(value = "clusterName")
    private String clusterName;
    private ReentrantReadWriteLock rwLock;

    // table family group map
    private Map<Long, Table> idToTable;
    @SerializedName(value = "nameToTable")
    private Map<String, Table> nameToTable;
    // table name lower cast -> table name
    private Map<String, String> lowerCaseToTableName;

    // user define function
    @SerializedName(value = "name2Function")
    private ConcurrentMap<String, ImmutableList<Function>> name2Function = Maps.newConcurrentMap();
    // user define encryptKey for current db
    @SerializedName(value = "dbEncryptKey")
    private DatabaseEncryptKey dbEncryptKey;

    @SerializedName(value = "dataQuotaBytes")
    private volatile long dataQuotaBytes;

    @SerializedName(value = "replicaQuotaSize")
    private volatile long replicaQuotaSize;

    private volatile long transactionQuotaSize;

    private volatile boolean isDropped;

    public enum DbState {
        NORMAL, LINK, MOVE
    }

    @SerializedName(value = "attachDbName")
    private String attachDbName;
    @SerializedName(value = "dbState")
    private DbState dbState;

    @SerializedName(value = "dbProperties")
    private DatabaseProperty dbProperties = new DatabaseProperty();

    private BinlogConfig binlogConfig = new BinlogConfig();

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
        this.transactionQuotaSize = Config.default_db_max_running_txn_num == -1L
                ? Config.max_running_txn_num_per_db
                : Config.default_db_max_running_txn_num;
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
            for (Table table : idToTable.values()) {
                table.setQualifiedDbName(fullQualifiedName);
            }
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

    public void setTransactionQuotaSize(long newQuota) {
        writeLock();
        try {
            Preconditions.checkArgument(newQuota >= 0L);
            LOG.info("database[{}] try to set transaction quota from {} to {}",
                    fullQualifiedName, transactionQuotaSize, newQuota);
            this.transactionQuotaSize = newQuota;
            this.dbProperties.put(TRANSACTION_QUOTA_SIZE, String.valueOf(transactionQuotaSize));
        } finally {
            writeUnlock();
        }
    }

    public long getDataQuota() {
        return dataQuotaBytes;
    }

    public long getReplicaQuota() {
        return replicaQuotaSize;
    }

    public long getTransactionQuotaSize() {
        return transactionQuotaSize;
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

    public long getReplicaCountWithLock() {
        readLock();
        try {
            long usedReplicaCount = 0;
            for (Table table : this.idToTable.values()) {
                if (table.getType() != TableType.OLAP) {
                    continue;
                }

                OlapTable olapTable = (OlapTable) table;
                olapTable.readLock();
                try {
                    usedReplicaCount = usedReplicaCount + olapTable.getReplicaCount();
                } finally {
                    olapTable.readUnlock();
                }
            }
            return usedReplicaCount;
        } finally {
            readUnlock();
        }
    }

    public long getReplicaQuotaLeftWithLock() {
        long leftReplicaQuota = replicaQuotaSize - getReplicaCountWithLock();
        return Math.max(leftReplicaQuota, 0L);
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

    public boolean isTableExist(String tableName) {
        if (Env.isTableNamesCaseInsensitive()) {
            tableName = lowerCaseToTableName.get(tableName.toLowerCase());
            if (tableName == null) {
                return false;
            }
        }
        return nameToTable.containsKey(tableName);
    }

    // return pair <success?, table exist?>
    public Pair<Boolean, Boolean> createTableWithLock(
            Table table, boolean isReplay, boolean setIfNotExist) throws DdlException {
        boolean result = true;
        // if a table is already exists, then edit log won't be executed
        // some caller of this method may need to know this message
        boolean isTableExist = false;
        table.setQualifiedDbName(fullQualifiedName);
        writeLockOrDdlException();
        try {
            String tableName = table.getName();
            if (Env.isStoredTableNamesLowerCase()) {
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
                    Env.getCurrentEnv().getEditLog().logCreateTable(info);
                }
                if (table.getType() == TableType.ELASTICSEARCH) {
                    Env.getCurrentEnv().getEsRepository().registerTable((EsTable) table);
                }
            }
            return Pair.of(result, isTableExist);
        } finally {
            writeUnlock();
        }
    }

    public boolean createTable(Table table) {
        boolean result = true;
        table.setQualifiedDbName(fullQualifiedName);
        String tableName = table.getName();
        if (Env.isStoredTableNamesLowerCase()) {
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
        if (Env.isStoredTableNamesLowerCase()) {
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

    @Override
    public CatalogIf getCatalog() {
        return Env.getCurrentInternalCatalog();
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
     * this method is used for get existed table list by table id list, if table not exist, just ignore it.
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
    @Override
    public Table getTableNullable(String tableName) {
        if (Env.isStoredTableNamesLowerCase()) {
            tableName = tableName.toLowerCase();
        }
        if (Env.isTableNamesCaseInsensitive()) {
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
    @Override
    public Table getTableNullable(long tableId) {
        return idToTable.get(tableId);
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
                        short replicationNum = olapTable.getPartitionInfo()
                                .getReplicaAllocation(partition.getId()).getTotalReplicaNum();
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
        if (Env.getCurrentEnvJournalVersion() < FeMetaVersion.VERSION_127) {
            Database db = new Database();
            db.readFields(in);
            return db;
        }
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, Database.class);
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
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    private void discardHudiTable() {
        Iterator<Entry<String, Table>> iterator = nameToTable.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Table> entry = iterator.next();
            if (entry.getValue().getType() == TableType.HUDI) {
                LOG.warn("hudi table is deprecated, discard it. table name: {}", entry.getKey());
                iterator.remove();
                idToTable.remove(entry.getValue().getId());
            }
        }
    }

    public void analyze() {
        for (Table table : nameToTable.values()) {
            table.analyze(getFullName());
        }
    }

    @Deprecated
    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);

        id = in.readLong();
        fullQualifiedName = Text.readString(in);
        // read groups
        int numTables = in.readInt();
        for (int i = 0; i < numTables; ++i) {
            Table table = Table.read(in);
            table.setQualifiedDbName(fullQualifiedName);
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

        FunctionUtil.readFields(in, this.getFullName(), name2Function);

        // read encryptKeys
        if (Env.getCurrentEnvJournalVersion() >= FeMetaVersion.VERSION_102) {
            dbEncryptKey = DatabaseEncryptKey.read(in);
        }

        replicaQuotaSize = in.readLong();

        if (Env.getCurrentEnvJournalVersion() >= FeMetaVersion.VERSION_105) {
            dbProperties = DatabaseProperty.read(in);
            String txnQuotaStr = dbProperties.getOrDefault(TRANSACTION_QUOTA_SIZE,
                    String.valueOf(Config.max_running_txn_num_per_db));
            transactionQuotaSize = Long.parseLong(txnQuotaStr);
            binlogConfig = dbProperties.getBinlogConfig();
        } else {
            transactionQuotaSize = Config.default_db_max_running_txn_num == -1L
                    ? Config.max_running_txn_num_per_db
                    : Config.default_db_max_running_txn_num;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, fullQualifiedName, dataQuotaBytes);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof Database)) {
            return false;
        }

        Database other = (Database) obj;

        return id == other.id
                && idToTable.equals(other.idToTable)
                && fullQualifiedName.equals(other.fullQualifiedName)
                && dataQuotaBytes == other.dataQuotaBytes;
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
        for (Table table : nameToTable.values()) {
            table.setQualifiedDbName(name);
        }
    }

    public synchronized void addFunction(Function function, boolean ifNotExists) throws UserException {
        function.checkWritable();
        if (FunctionUtil.addFunctionImpl(function, ifNotExists, false, name2Function)) {
            Env.getCurrentEnv().getEditLog().logAddFunction(function);
            FunctionUtil.translateToNereids(this.getFullName(), function);
        }
    }

    public synchronized void replayAddFunction(Function function) {
        try {
            FunctionUtil.addFunctionImpl(function, false, true, name2Function);
            FunctionUtil.translateToNereids(this.getFullName(), function);
        } catch (UserException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized void dropFunction(FunctionSearchDesc function, boolean ifExists) throws UserException {
        if (FunctionUtil.dropFunctionImpl(function, ifExists, name2Function)) {
            Env.getCurrentEnv().getEditLog().logDropFunction(function);
            FunctionUtil.dropFromNereids(this.getFullName(), function);
            LOG.info("finished to drop function {}", function.getName().getFunction());
        }
    }

    public synchronized void replayDropFunction(FunctionSearchDesc functionSearchDesc) {
        try {
            // Set ifExists to true to avoid throw exception if function is not found.
            // It should not happen but the reason is unknown, so add warn log for debug.
            if (!FunctionUtil.dropFunctionImpl(functionSearchDesc, true, name2Function)) {
                LOG.warn("failed to find function to drop: {} when replay, skip",
                        functionSearchDesc.getName().getFunction());
            }
            FunctionUtil.dropFromNereids(this.getFullName(), functionSearchDesc);
            LOG.info("finished to replay drop function {}", functionSearchDesc.getName().getFunction());
        } catch (UserException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized Function getFunction(Function desc, Function.CompareMode mode) {
        return FunctionUtil.getFunction(desc, mode, name2Function);
    }

    public synchronized Function getFunction(FunctionSearchDesc function) throws AnalysisException {
        return FunctionUtil.getFunction(function, name2Function);
    }

    public synchronized List<Function> getFunctions() {
        return FunctionUtil.getFunctions(name2Function);
    }

    public synchronized void addEncryptKey(EncryptKey encryptKey, boolean ifNotExists) throws UserException {
        if (addEncryptKeyImpl(encryptKey, false, ifNotExists)) {
            Env.getCurrentEnv().getEditLog().logAddEncryptKey(encryptKey);
        }
    }

    public synchronized void replayAddEncryptKey(EncryptKey encryptKey) {
        try {
            addEncryptKeyImpl(encryptKey, true, true);
        } catch (UserException e) {
            Preconditions.checkArgument(false);
        }
    }

    private boolean addEncryptKeyImpl(EncryptKey encryptKey, boolean isReplay, boolean ifNotExists)
            throws UserException {
        String keyName = encryptKey.getEncryptKeyName().getKeyName();
        EncryptKey existKey = dbEncryptKey.getName2EncryptKey().get(keyName);
        if (!isReplay) {
            if (existKey != null) {
                if (existKey.isIdentical(encryptKey)) {
                    if (ifNotExists) {
                        return false;
                    }
                    throw new UserException("encryptKey ["
                            + existKey.getEncryptKeyName().toString() + "] already exists");
                }
            }
        }

        dbEncryptKey.getName2EncryptKey().put(keyName, encryptKey);
        return true;
    }

    public synchronized void dropEncryptKey(EncryptKeySearchDesc encryptKeySearchDesc, boolean ifExists)
            throws UserException {
        if (dropEncryptKeyImpl(encryptKeySearchDesc, ifExists)) {
            Env.getCurrentEnv().getEditLog().logDropEncryptKey(encryptKeySearchDesc);
        }
    }

    public synchronized void replayDropEncryptKey(EncryptKeySearchDesc encryptKeySearchDesc) {
        try {
            dropEncryptKeyImpl(encryptKeySearchDesc, true);
        } catch (UserException e) {
            Preconditions.checkArgument(false);
        }
    }

    private boolean dropEncryptKeyImpl(EncryptKeySearchDesc encryptKeySearchDesc, boolean ifExists)
            throws UserException {
        String keyName = encryptKeySearchDesc.getKeyEncryptKeyName().getKeyName();
        EncryptKey existKey = dbEncryptKey.getName2EncryptKey().get(keyName);
        if (existKey == null) {
            if (ifExists) {
                return false;
            }
            throw new UserException("Unknown encryptKey, encryptKey=" + encryptKeySearchDesc.toString());
        }
        boolean isFound = false;
        if (encryptKeySearchDesc.isIdentical(existKey)) {
            isFound = true;
        }
        if (!isFound) {
            if (ifExists) {
                return false;
            }
            throw new UserException("Unknown encryptKey, encryptKey=" + encryptKeySearchDesc.toString());
        }
        dbEncryptKey.getName2EncryptKey().remove(keyName);
        return true;
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

    @Override
    public Map<Long, TableIf> getIdToTable() {
        return new HashMap<>(idToTable);
    }

    public void replayUpdateDbProperties(Map<String, String> properties) {
        dbProperties.updateProperties(properties);
        if (PropertyAnalyzer.hasBinlogConfig(properties)) {
            binlogConfig = dbProperties.getBinlogConfig();
        }
    }

    public boolean updateDbProperties(Map<String, String> properties) throws DdlException {
        if (PropertyAnalyzer.hasBinlogConfig(properties)) {
            BinlogConfig oldBinlogConfig = getBinlogConfig();
            BinlogConfig newBinlogConfig = BinlogConfig.fromProperties(properties);

            if (newBinlogConfig.isEnable() && !oldBinlogConfig.isEnable()) {
                // check all tables binlog enable is true
                for (Table table : idToTable.values()) {
                    if (table.getType() != TableType.OLAP) {
                        continue;
                    }

                    OlapTable olapTable = (OlapTable) table;
                    olapTable.readLock();
                    try {
                        if (!olapTable.getBinlogConfig().isEnable()) {
                            String errMsg = String
                                    .format("binlog is not enable in table[%s] in db [%s]", table.getName(),
                                            getFullName());
                            throw new DdlException(errMsg);
                        }
                    } finally {
                        olapTable.readUnlock();
                    }
                }
            }
        }

        replayUpdateDbProperties(properties);
        return true;
    }

    public BinlogConfig getBinlogConfig() {
        return binlogConfig;
    }

    public String toJson() {
        return GsonUtils.GSON.toJson(this);
    }

    @Override
    public String toString() {
        return toJson();
    }

    // Return ture if database is created for mysql compatibility.
    // Currently, we have two dbs that are created for this purpose, InformationSchemaDb and MysqlDb,
    public boolean isMysqlCompatibleDatabase() {
        return false;
    }

}
