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

package org.apache.doris.dictionary;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.mtmv.MTMVRelatedTableIf;
import org.apache.doris.nereids.trees.plans.commands.info.CreateDictionaryInfo;
import org.apache.doris.nereids.trees.plans.commands.info.DictionaryColumnDefinition;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.util.RelationUtil;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TDictionaryTable;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Dictionary metadata, including its structure and data source information. saved in
 */
public class Dictionary extends Table {
    private static final Logger LOG = LogManager.getLogger(Dictionary.class);

    // TODO: maybe dictionary should also be able to be created in external catalog.
    @SerializedName(value = "dbName")
    private final String dbName;

    // dict name use base class's name. these 3 is expected not to be null
    @SerializedName(value = "sourceCtlName")
    private final String sourceCtlName;
    @SerializedName(value = "sourceDbName")
    private final String sourceDbName;
    @SerializedName(value = "sourceTableName")
    private final String sourceTableName;

    @SerializedName(value = "columns")
    private final List<DictionaryColumnDefinition> columns;

    // createTime saved in base class

    // lastUpdateTime in milliseconds
    @SerializedName(value = "lastUpdateTime")
    private long lastUpdateTime;

    // when longer than lastUpdateTime + dataLifetimeSecs, data is out of date.
    @SerializedName(value = "dataLifetimeSecs")
    private long dataLifetimeSecs;

    @SerializedName(value = "memoryLimit")
    private long memoryLimit;

    public enum DictionaryStatus {
        NORMAL, // normal status
        LOADING, // wait load task finishs
        OUT_OF_DATE // wait load task be scheduled
    }

    private final AtomicReference<DictionaryStatus> status = new AtomicReference<>();

    @SerializedName(value = "layout")
    private final LayoutType layout;
    @SerializedName(value = "version")
    private long version; // every time dictionary is updated, version will increase by 1

    @SerializedName(value = "skipNullKey")
    private boolean skipNullKey;

    // not record this. whenever restart FE or switch Master, it will be reset. then lead to reload dictionary.
    private long srcVersion = 0;
    // if srcVersion same with this, we could skip automatically update.
    private long latestInvalidVersion = 0;

    private volatile List<DictionaryDistribution> dataDistributions; // every time update, reset with a new list
    private String lastUpdateResult;

    // we need this to call Table's constructor with no args which construct new rwLock and more.
    // for gson only and it will set variables soon. so no need to set them.
    public Dictionary() {
        super(TableType.DICTIONARY);
        this.dbName = null;
        this.sourceCtlName = null;
        this.sourceDbName = null;
        this.sourceTableName = null;
        this.columns = null;
        this.lastUpdateTime = 0;
        this.dataLifetimeSecs = 0;
        this.status.set(DictionaryStatus.OUT_OF_DATE); // not replay by gson
        this.layout = null;
        this.version = 0;
        this.skipNullKey = false;
        this.memoryLimit = 0;
        resetDataDistributions(); // not replay by gson
        this.lastUpdateResult = new String();
    }

    public Dictionary(CreateDictionaryInfo info, long uniqueId) {
        super(uniqueId, info.getDictName(), TableType.DICTIONARY, /* source table's schema as dict's FullSchema */info
                .getColumns().stream().map(DictionaryColumnDefinition::getOriginColumn).collect(Collectors.toList()));
        this.dbName = info.getDbName();
        this.sourceCtlName = info.getSourceCtlName();
        this.sourceDbName = info.getSourceDbName();
        this.sourceTableName = info.getSourceTableName();
        this.columns = info.getColumns();
        this.lastUpdateTime = createTime;
        this.dataLifetimeSecs = info.getDataLifetime();
        // no data in the beginning so it's OUT_OF_DATE
        this.status.set(DictionaryStatus.OUT_OF_DATE);
        this.layout = info.getLayout();
        this.version = 1;
        this.skipNullKey = info.skipNullKey();
        this.memoryLimit = info.getMemoryLimit();
        resetDataDistributions();
        this.lastUpdateResult = new String();
    }

    public String getDbName() {
        return dbName;
    }

    public String getSourceCtlName() {
        return sourceCtlName;
    }

    public String getSourceDbName() {
        return sourceDbName;
    }

    public String getSourceTableName() {
        return sourceTableName;
    }

    public List<DictionaryColumnDefinition> getDicColumns() {
        return columns;
    }

    public Column getOriginColumn(String name) {
        for (DictionaryColumnDefinition column : columns) {
            if (column.getName().equalsIgnoreCase(name)) {
                return column.getOriginColumn();
            }
        }
        throw new IllegalArgumentException("Column " + name + " not found in dictionary " + getName());
    }

    public List<String> getSourceQualifiedName() {
        List<String> qualifiedName = Lists.newArrayList();
        if (Strings.isNullOrEmpty(sourceCtlName) || Strings.isNullOrEmpty(sourceDbName)
                || Strings.isNullOrEmpty(sourceTableName)) {
            throw new IllegalArgumentException(
                    "dictionary's source name " + qualifiedName.toString() + "is not completed");
        }
        qualifiedName.add(sourceCtlName);
        qualifiedName.add(sourceDbName);
        qualifiedName.add(sourceTableName);
        return qualifiedName;
    }

    @Override
    public Database getDatabase() {
        return Env.getCurrentInternalCatalog().getDbNullable(dbName);
    }

    @Override
    public List<String> getFullQualifiers() {
        return ImmutableList.of(Env.getCurrentEnv().getInternalCatalog().getName(), dbName, getName());
    }

    public List<String> getColumnNames() {
        return columns.stream().map(DictionaryColumnDefinition::getName).collect(Collectors.toList());
    }

    public long getNextRefreshTime() {
        return lastUpdateTime + dataLifetimeSecs * 1000;
    }

    public long getDataLifetimeSecs() {
        return dataLifetimeSecs;
    }

    public DataType getColumnType(String columnName) {
        for (DictionaryColumnDefinition column : columns) {
            if (column.getName().equalsIgnoreCase(columnName)) {
                return DataType.fromCatalogType(column.getType());
            }
        }
        throw new IllegalArgumentException("Column " + columnName + " not found in dictionary " + getName());
    }

    public List<DataType> getKeyColumnTypes() {
        List<DataType> keyTypes = Lists.newArrayList();
        for (DictionaryColumnDefinition column : columns) {
            if (column.isKey()) {
                keyTypes.add(DataType.fromCatalogType(column.getType()));
            }
        }
        if (keyTypes.isEmpty()) {
            throw new IllegalArgumentException("Key column not found in dictionary " + getName());
        }
        return keyTypes;
    }

    public void increaseVersion() {
        this.version++;
    }

    public void decreaseVersion() {
        this.version--;
    }

    public long getVersion() {
        return version;
    }

    public boolean skipNullKey() {
        return skipNullKey;
    }

    public long getMemoryLimit() {
        return memoryLimit;
    }

    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void updateLastUpdateTime() {
        this.lastUpdateTime = System.currentTimeMillis();
    }

    /**
     * @return true if source table's version is newer than this dictionary's version(need update dictionary).
     */
    public boolean hasNewerSourceVersion() {
        TableIf tableIf = RelationUtil.getTable(getSourceQualifiedName(), Env.getCurrentEnv(), Optional.empty());
        if (tableIf == null) {
            throw new RuntimeException(getName() + "'s source table not found");
        }
        if (tableIf instanceof MTMVRelatedTableIf) { // include OlapTable and some External tables
            long tableVersionNow = ((MTMVRelatedTableIf) tableIf).getNewestUpdateVersionOrTime();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Dictionary " + getName() + " src's now version is " + tableVersionNow + ", old is "
                        + srcVersion + ", status is " + status.get().name());
            }
            if (tableVersionNow < srcVersion) {
                // maybe drop and recreate. but if so, this dictionary should be dropped as well.
                // so should not happen.
                throw new RuntimeException("Dictionary " + getName() + "'s source table's version " + tableVersionNow
                        + " is smaller than dictionary's " + srcVersion);
            } else if (tableVersionNow > srcVersion && tableVersionNow != latestInvalidVersion) {
                // if src is a illegal version, we can skip it.
                return true;
            } else {
                return false;
            }
        }
        // just update for tables without version information.
        return true;
    }

    // when refresh success, update srcVersion.
    public void updateSrcVersion(long value) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Dictionary " + getName() + ": update srcVersion from " + srcVersion + " to " + value);
        }
        srcVersion = value;
    }

    public long getSrcVersion() {
        return srcVersion;
    }

    public void updateLatestInvalidVersion(long value) {
        if (value < latestInvalidVersion) {
            throw new RuntimeException("latestInvalidVersion of " + getName() + " should be greater than "
                    + latestInvalidVersion + ", but got " + value);
        }
        latestInvalidVersion = value;
    }

    /**
     * if has latestInvalidVersion and the base table's data not changed, we can skip update.
     */
    public boolean checkBaseDataValid() {
        TableIf tableIf = RelationUtil.getTable(getSourceQualifiedName(), Env.getCurrentEnv(), Optional.empty());
        if (tableIf == null) {
            throw new RuntimeException(getName() + "'s source table not found");
        }
        if (tableIf instanceof MTMVRelatedTableIf) { // include OlapTable and some External tables
            long tableVersionNow = ((MTMVRelatedTableIf) tableIf).getNewestUpdateVersionOrTime();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Dictionary " + getName() + ": src's now version is " + tableVersionNow + ", old is "
                        + srcVersion);
            }
            // if not the known invalid version, maybe valid. so return true. otherwise we skip it.
            return tableVersionNow != latestInvalidVersion;
        }
        return true;
    }

    public DictionaryStatus getStatus() {
        return status.get();
    }

    public boolean trySetStatusIf(DictionaryStatus currentStatus, DictionaryStatus newStatus) {
        return status.compareAndSet(currentStatus, newStatus);
    }

    public boolean trySetStatus(DictionaryStatus newStatus) {
        // use get and compare to avoid ABA problem
        while (true) {
            DictionaryStatus currentStatus = status.get();

            if (currentStatus == DictionaryStatus.NORMAL) {
                if (status.compareAndSet(currentStatus, newStatus)) {
                    return true;
                } else {
                    continue; // status changed, retry
                }
            } else if (currentStatus == DictionaryStatus.LOADING) {
                // may success or failed, cannot accept another loading
                if (newStatus == DictionaryStatus.NORMAL || newStatus == DictionaryStatus.OUT_OF_DATE) {
                    if (status.compareAndSet(currentStatus, newStatus)) {
                        return true;
                    } else {
                        continue; // status changed, retry
                    }
                }
                return false;
            } else if (currentStatus == DictionaryStatus.OUT_OF_DATE) {
                // we could load or drop it
                if (newStatus == DictionaryStatus.LOADING) {
                    if (status.compareAndSet(currentStatus, newStatus)) {
                        return true;
                    } else {
                        continue; // status changed, retry
                    }
                }
                return false;
            }
            return false; // unknown status
        }
    }

    public LayoutType getLayout() {
        return layout;
    }

    public String prettyPrintDistributions() {
        if (dataDistributions == null) {
            return ""; // no records
        }
        return "{" + StringUtils.join(dataDistributions, ", ") + "}";
    }

    public void setDataDistributions(List<DictionaryDistribution> dataDistributions) {
        this.dataDistributions = dataDistributions;
    }

    public List<DictionaryDistribution> getDataDistributions() {
        return dataDistributions;
    }

    public void resetDataDistributions() {
        this.dataDistributions = Lists.newArrayList();
    }

    public boolean dataCompleted() {
        List<Long> aliveBEs = Env.getCurrentSystemInfo().getAllBackendIds(true);
        if (dataDistributions.size() < aliveBEs.size()) {
            // greater is OK. may be BEs down.
            return false;
        }
        // if only there's alive BE not find in dataDistributions, return false
        Set<Long> beIdsHasData = Sets.newHashSet();
        for (DictionaryDistribution distribution : dataDistributions) {
            if (distribution.getVersion() < version) {
                return false;
            }
            beIdsHasData.add(distribution.getBackendId());
        }
        for (Long backendId : aliveBEs) {
            if (!beIdsHasData.contains(backendId)) {
                // some of BE does not have data
                return false;
            }
        }
        return true;
    }

    // get BEs which are out of date.
    public List<Backend> filterOutdatedBEs(List<Backend> backends) {
        if (dataDistributions == null || dataDistributions.isEmpty()) {
            // only called when do partial load. it bases on collection of data distributions.
            // so dataDistributions should not be null.
            LOG.warn("dataDistributions of " + getName() + " is " + (dataDistributions == null ? "null" : "empty")
                    + ". should not happen");
            return backends;
        }
        Set<Long> validBEs = Sets.newHashSet();
        for (DictionaryDistribution distribution : dataDistributions) {
            if (distribution.getVersion() == version) {
                validBEs.add(distribution.getBackendId());
            }
        }
        // get those not valid. maybe: 1. version not match(outdated) 2. not in dataDistributions(newcomers)
        return backends.stream().filter(backend -> !validBEs.contains(backend.getId())).collect(Collectors.toList());
    }

    public void setLastUpdateResult(String lastUpdateResult) {
        // get readable time
        String now = TimeUtils.getCurrentFormatTime();
        this.lastUpdateResult = now + ": " + lastUpdateResult;
    }

    public String getLastUpdateResult() {
        return lastUpdateResult;
    }

    @Override
    public TTableDescriptor toThrift() {
        TDictionaryTable tDictionaryTable = new TDictionaryTable();
        TTableDescriptor tTableDescriptor = new TTableDescriptor(id, TTableType.DICTIONARY_TABLE, fullSchema.size(), 0,
                getName(), dbName);
        tTableDescriptor.setDictionaryTable(tDictionaryTable);
        return tTableDescriptor;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static Dictionary read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, Dictionary.class);
    }

    @Override
    public String toString() {
        return "Dictionary{" + getName() + "}";
    }
}
