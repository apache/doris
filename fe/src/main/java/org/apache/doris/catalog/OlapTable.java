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

import org.apache.doris.alter.AlterJob.JobState;
import org.apache.doris.alter.RollupJob;
import org.apache.doris.analysis.AddPartitionClause;
import org.apache.doris.analysis.AddRollupClause;
import org.apache.doris.analysis.AlterClause;
import org.apache.doris.analysis.AlterTableStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.analysis.DistributionDesc;
import org.apache.doris.analysis.KeysDesc;
import org.apache.doris.analysis.PartitionDesc;
import org.apache.doris.analysis.RangePartitionDesc;
import org.apache.doris.analysis.SingleRangePartitionDesc;
import org.apache.doris.analysis.TableName;
import org.apache.doris.backup.Status;
import org.apache.doris.backup.Status.ErrCode;
import org.apache.doris.catalog.DistributionInfo.DistributionInfoType;
import org.apache.doris.catalog.Replica.ReplicaState;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.io.DeepCopy;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.common.util.Util;
import org.apache.doris.thrift.TOlapTable;
import org.apache.doris.thrift.TStorageType;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.Adler32;

/**
 * Internal representation of tableFamilyGroup-related metadata. A OlaptableFamilyGroup contains several tableFamily.
 */
public class OlapTable extends Table {
    private static final Logger LOG = LogManager.getLogger(OlapTable.class);

    public enum OlapTableState {
        NORMAL,
        ROLLUP,
        SCHEMA_CHANGE,
        @Deprecated
        BACKUP,
        RESTORE,
        RESTORE_WITH_LOAD
    }

    private OlapTableState state;
    // index id -> table's schema
    private Map<Long, List<Column>> indexIdToSchema;
    // index id -> table's schema version
    private Map<Long, Integer> indexIdToSchemaVersion;
    // index id -> table's schema hash
    private Map<Long, Integer> indexIdToSchemaHash;
    // index id -> table's short key column count
    private Map<Long, Short> indexIdToShortKeyColumnCount;
    // index id -> table's storage type
    private Map<Long, TStorageType> indexIdToStorageType;
    // index name -> index id
    private Map<String, Long> indexNameToId;

    private KeysType keysType;
    private PartitionInfo partitionInfo;
    private DistributionInfo defaultDistributionInfo;

    private Map<Long, Partition> idToPartition;
    private Map<String, Partition> nameToPartition;

    // bloom filter columns
    private Set<String> bfColumns;
    private double bfFpp;

    public OlapTable() {
        // for persist
        super(TableType.OLAP);
        this.indexIdToSchema = new HashMap<Long, List<Column>>();
        this.indexIdToSchemaHash = new HashMap<Long, Integer>();
        this.indexIdToSchemaVersion = new HashMap<Long, Integer>();

        this.indexIdToShortKeyColumnCount = new HashMap<Long, Short>();
        this.indexIdToStorageType = new HashMap<Long, TStorageType>();

        this.indexNameToId = new HashMap<String, Long>();

        this.idToPartition = new HashMap<Long, Partition>();
        this.nameToPartition = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);

        this.bfColumns = null;
        this.bfFpp = 0;
    }

    public OlapTable(long id, String tableName, List<Column> baseSchema,
                     KeysType keysType, PartitionInfo partitionInfo, DistributionInfo defaultDistributionInfo) {
        super(id, tableName, TableType.OLAP, baseSchema);

        this.state = OlapTableState.NORMAL;

        this.indexIdToSchema = new HashMap<Long, List<Column>>();
        this.indexIdToSchemaHash = new HashMap<Long, Integer>();
        this.indexIdToSchemaVersion = new HashMap<Long, Integer>();

        this.indexIdToShortKeyColumnCount = new HashMap<Long, Short>();
        this.indexIdToStorageType = new HashMap<Long, TStorageType>();

        this.indexNameToId = new HashMap<String, Long>();

        this.idToPartition = new HashMap<Long, Partition>();
        this.nameToPartition = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);

        this.keysType = keysType;
        this.partitionInfo = partitionInfo;
        this.defaultDistributionInfo = defaultDistributionInfo;

        this.bfColumns = null;
        this.bfFpp = 0;
    }

    public void setState(OlapTableState state) {
        this.state = state;
    }

    public OlapTableState getState() {
        return state;
    }

    public void setName(String newName) {
        // change name in indexNameToId
        long baseIndexId = indexNameToId.remove(this.name);
        indexNameToId.put(newName, baseIndexId);

        // change name
        this.name = newName;

        // change single partition name
        if (this.partitionInfo.getType() == PartitionType.UNPARTITIONED) {
            // use for loop, because if we use getPartition(partitionName),
            // we may not be able to get partition because this is a bug fix
            for (Partition partition : getPartitions()) {
                partition.setName(newName);
                nameToPartition.clear();
                nameToPartition.put(newName, partition);
                break;
            }
        }
    }

    public boolean hasMaterializedIndex(String indexName) {
        return indexNameToId.containsKey(indexName);
    }

    public void setIndexSchemaInfo(Long indexId, String indexName, List<Column> schema, int schemaVersion,
                                   int schemaHash, short shortKeyColumnCount) {
        if (indexName == null) {
            Preconditions.checkState(indexNameToId.containsValue(indexId));
        } else {
            indexNameToId.put(indexName, indexId);
        }
        indexIdToSchema.put(indexId, schema);
        indexIdToSchemaVersion.put(indexId, schemaVersion);
        indexIdToSchemaHash.put(indexId, schemaHash);
        indexIdToShortKeyColumnCount.put(indexId, shortKeyColumnCount);
    }
    public void setIndexStorageType(Long indexId, TStorageType newStorageType) {
        Preconditions.checkState(newStorageType == TStorageType.COLUMN);
        indexIdToStorageType.put(indexId, newStorageType);
    }

    public void deleteIndexInfo(String indexName) {
        long indexId = this.indexNameToId.remove(indexName);

        indexIdToSchema.remove(indexId);
        indexIdToSchemaVersion.remove(indexId);
        indexIdToSchemaHash.remove(indexId);
        indexIdToShortKeyColumnCount.remove(indexId);
        indexIdToStorageType.remove(indexId);
    }

    public Map<String, Long> getIndexNameToId() {
        return indexNameToId;
    }

    public Long getIndexIdByName(String indexName) {
        return indexNameToId.get(indexName);
    }

    public String getIndexNameById(long indexId) {
        for (Map.Entry<String, Long> entry : indexNameToId.entrySet()) {
            if (entry.getValue() == indexId) {
                return entry.getKey();
            }
        }
        return null;
    }

    public Status resetIdsForRestore(Catalog catalog, Database db, int restoreReplicationNum) {
        // table id
        id = catalog.getNextId();

        // copy an origin index id to name map
        Map<Long, String> origIdxIdToName = Maps.newHashMap();
        for (Map.Entry<String, Long> entry : indexNameToId.entrySet()) {
            origIdxIdToName.put(entry.getValue(), entry.getKey());
        }

        // reset all 'indexIdToXXX' map
        for (Map.Entry<Long, String> entry : origIdxIdToName.entrySet()) {
            long newIdxId = 0;
            if (entry.getValue().equals(name)) {
                // base index
                newIdxId = id;
            } else {
                newIdxId = catalog.getNextId();
            }
            indexIdToSchema.put(newIdxId, indexIdToSchema.remove(entry.getKey()));
            indexIdToSchemaHash.put(newIdxId, indexIdToSchemaHash.remove(entry.getKey()));
            indexIdToSchemaVersion.put(newIdxId, indexIdToSchemaVersion.remove(entry.getKey()));
            indexIdToShortKeyColumnCount.put(newIdxId, indexIdToShortKeyColumnCount.remove(entry.getKey()));
            indexIdToStorageType.put(newIdxId, indexIdToStorageType.remove(entry.getKey()));
            indexNameToId.put(entry.getValue(), newIdxId);
        }

        // generate a partition name to id map
        Map<String, Long> origPartNameToId = Maps.newHashMap();
        for (Partition partition : idToPartition.values()) {
            origPartNameToId.put(partition.getName(), partition.getId());
        }

        // reset partition info and idToPartition map
        if (partitionInfo.getType() == PartitionType.RANGE) {
            RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
            for (Map.Entry<String, Long> entry : origPartNameToId.entrySet()) {
                long newPartId = catalog.getNextId();
                rangePartitionInfo.idToDataProperty.put(newPartId,
                                                        rangePartitionInfo.idToDataProperty.remove(entry.getValue()));
                rangePartitionInfo.idToReplicationNum.remove(entry.getValue());
                rangePartitionInfo.idToReplicationNum.put(newPartId,
                                                          (short) restoreReplicationNum);
                rangePartitionInfo.getIdToRange().put(newPartId,
                                                      rangePartitionInfo.getIdToRange().remove(entry.getValue()));

                idToPartition.put(newPartId, idToPartition.remove(entry.getValue()));
            }
        } else {
            // Single partitioned
            long newPartId = catalog.getNextId();
            for (Map.Entry<String, Long> entry : origPartNameToId.entrySet()) {
                partitionInfo.idToDataProperty.put(newPartId, partitionInfo.idToDataProperty.remove(entry.getValue()));
                partitionInfo.idToReplicationNum.remove(entry.getValue());
                partitionInfo.idToReplicationNum.put(newPartId, (short) restoreReplicationNum);
                idToPartition.put(newPartId, idToPartition.remove(entry.getValue()));
            }
        }

        // for each partition, reset rollup index map
        for (Map.Entry<Long, Partition> entry : idToPartition.entrySet()) {
            Partition partition = entry.getValue();
            for (Map.Entry<Long, String> entry2 : origIdxIdToName.entrySet()) {
                MaterializedIndex idx = partition.getIndex(entry2.getKey());
                long newIdxId = indexNameToId.get(entry2.getValue());
                idx.setIdForRestore(newIdxId);
                if (newIdxId != id) {
                    // not base table, reset
                    partition.deleteRollupIndex(entry2.getKey());
                    partition.createRollupIndex(idx);
                }

                // generate new tablets in origin tablet order
                int tabletNum = idx.getTablets().size();
                idx.clearTabletsForRestore();
                for (int i = 0; i < tabletNum; i++) {
                    long newTabletId = catalog.getNextId();
                    Tablet newTablet = new Tablet(newTabletId);
                    idx.addTablet(newTablet, null /* tablet meta */, true /* is restore */);

                    // replicas
                    List<Long> beIds = Catalog.getCurrentSystemInfo().seqChooseBackendIds(partitionInfo.getReplicationNum(entry.getKey()),
                                                                                          true, true,
                                                                                          db.getClusterName());
                    if (beIds == null) {
                        return new Status(ErrCode.COMMON_ERROR, "failed to find "
                                + partitionInfo.getReplicationNum(entry.getKey())
                                + " different hosts to create table: " + name);
                    }
                    for (Long beId : beIds) {
                        long newReplicaId = catalog.getNextId();
                        Replica replica = new Replica(newReplicaId, beId, ReplicaState.NORMAL,
                                partition.getVisibleVersion(), partition.getVisibleVersionHash());
                        newTablet.addReplica(replica, true /* is restore */);
                    }
                }
            }

            // reset partition id
            partition.setIdForRestore(entry.getKey());
        }

        return Status.OK;
    }

    // schema
    public Map<Long, List<Column>> getIndexIdToSchema() {
        return indexIdToSchema;
    }

    public Map<Long, List<Column>> getCopiedIndexIdToSchema() {
        Map<Long, List<Column>> copiedIndexIdToSchema = new HashMap<Long, List<Column>>();
        copiedIndexIdToSchema.putAll(indexIdToSchema);
        return copiedIndexIdToSchema;
    }

    public List<Column> getSchemaByIndexId(Long indexId) {
        return indexIdToSchema.get(indexId);
    }

    public List<Column> getKeyColumnsByIndexId(Long indexId) {
        ArrayList<Column> keyColumns = Lists.newArrayList();
        List<Column> allColumns = this.getSchemaByIndexId(indexId);
        for (Column column : allColumns) {
            if (column.isKey()) {
                keyColumns.add(column);
            }
        }

        return keyColumns;
    }

    // schema version
    public int getSchemaVersionByIndexId(Long indexId) {
        if (indexIdToSchemaVersion.containsKey(indexId)) {
            return indexIdToSchemaVersion.get(indexId);
        }
        return -1;
    }

    // schemaHash
    public Map<Long, Integer> getIndexIdToSchemaHash() {
        return indexIdToSchemaHash;
    }

    public Map<Long, Integer> getCopiedIndexIdToSchemaHash() {
        Map<Long, Integer> copiedIndexIdToSchemaHash = new HashMap<Long, Integer>();
        copiedIndexIdToSchemaHash.putAll(indexIdToSchemaHash);
        return copiedIndexIdToSchemaHash;
    }

    public int getSchemaHashByIndexId(Long indexId) {
        if (indexIdToSchemaHash.containsKey(indexId)) {
            return indexIdToSchemaHash.get(indexId);
        }
        return -1;
    }

    // short key
    public Map<Long, Short> getIndexIdToShortKeyColumnCount() {
        return indexIdToShortKeyColumnCount;
    }

    public Map<Long, Short> getCopiedIndexIdToShortKeyColumnCount() {
        Map<Long, Short> copiedIndexIdToShortKeyColumnCount = new HashMap<Long, Short>();
        copiedIndexIdToShortKeyColumnCount.putAll(indexIdToShortKeyColumnCount);
        return copiedIndexIdToShortKeyColumnCount;
    }

    public short getShortKeyColumnCountByIndexId(Long indexId) {
        if (indexIdToShortKeyColumnCount.containsKey(indexId)) {
            return indexIdToShortKeyColumnCount.get(indexId);
        }
        return (short) -1;
    }

    // storage type
    public Map<Long, TStorageType> getIndexIdToStorageType() {
        return indexIdToStorageType;
    }

    public Map<Long, TStorageType> getCopiedIndexIdToStorageType() {
        Map<Long, TStorageType> copiedIndexIdToStorageType = new HashMap<Long, TStorageType>();
        copiedIndexIdToStorageType.putAll(indexIdToStorageType);
        return copiedIndexIdToStorageType;
    }

    public void setStorageTypeToIndex(Long indexId, TStorageType storageType) {
        indexIdToStorageType.put(indexId, storageType);
    }

    public TStorageType getStorageTypeByIndexId(Long indexId) {
        return indexIdToStorageType.get(indexId);
    }

    public KeysType getKeysType() {
        return keysType;
    }

    public PartitionInfo getPartitionInfo() {
        return partitionInfo;
    }

    public DistributionInfo getDefaultDistributionInfo() {
        return defaultDistributionInfo;
    }

    public void renamePartition(String partitionName, String newPartitionName) {
        if (partitionInfo.getType() == PartitionType.UNPARTITIONED) {
            // bug fix
            for (Partition partition : idToPartition.values()) {
                partition.setName(newPartitionName);
                nameToPartition.clear();
                nameToPartition.put(newPartitionName, partition);
                LOG.info("rename patition {} in table {}", newPartitionName, name);
                break;
            }
        } else {
            Partition partition = nameToPartition.remove(partitionName);
            partition.setName(newPartitionName);
            nameToPartition.put(newPartitionName, partition);
        }
    }

    public void addPartition(Partition partition) {
        idToPartition.put(partition.getId(), partition);
        nameToPartition.put(partition.getName(), partition);
    }

    public Partition dropPartition(long dbId, String partitionName) {
        return dropPartition(dbId, partitionName, false);
    }

    public Partition dropPartition(long dbId, String partitionName, boolean isRestore) {
        Partition partition = nameToPartition.get(partitionName);
        if (partition != null) {
            idToPartition.remove(partition.getId());
            nameToPartition.remove(partitionName);

            Preconditions.checkState(partitionInfo.getType() == PartitionType.RANGE);
            RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
            
            if (!isRestore) {
                // recycle partition
                Catalog.getCurrentRecycleBin().recyclePartition(dbId, id, partition,
                                          rangePartitionInfo.getRange(partition.getId()),
                                          rangePartitionInfo.getDataProperty(partition.getId()),
                                          rangePartitionInfo.getReplicationNum(partition.getId()));
            }

            // drop partition info
            rangePartitionInfo.dropPartition(partition.getId());
        }
        return partition;
    }

    public Partition dropPartitionForBackup(String partitionName) {
        return dropPartition(-1, partitionName, true);
    }

    public Collection<Partition> getPartitions() {
        return idToPartition.values();
    }

    public Partition getPartition(long partitionId) {
        return idToPartition.get(partitionId);
    }

    public Partition getPartition(String partitionName) {
        return nameToPartition.get(partitionName);
    }

    public Set<String> getPartitionNames() {
        return Sets.newHashSet(nameToPartition.keySet());
    }

    public Set<String> getCopiedBfColumns() {
        if (bfColumns == null) {
            return null;
        }

        return Sets.newHashSet(bfColumns);
    }

    public double getBfFpp() {
        return bfFpp;
    }

    public void setBloomFilterInfo(Set<String> bfColumns, double bfFpp) {
        this.bfColumns = bfColumns;
        this.bfFpp = bfFpp;
    }
    
    // when the table is creating new rollup and enter finishing state, should tell be not auto load to new rollup
    // it is used for stream load
    // the caller should get db lock when call this method
    public boolean shouldLoadToNewRollup() {
        RollupJob rollupJob = (RollupJob) Catalog.getInstance().getRollupHandler().getAlterJob(id);
        if (rollupJob != null && rollupJob.getState() == JobState.FINISHING) {
            return false;
        }
        return true;
    }

    public TTableDescriptor toThrift() {
        TOlapTable tOlapTable = new TOlapTable(getName());
        TTableDescriptor tTableDescriptor = new TTableDescriptor(id, TTableType.OLAP_TABLE,
                baseSchema.size(), 0, getName(), "");
        tTableDescriptor.setOlapTable(tOlapTable);
        return tTableDescriptor;
    }

    public long getRowCount() {
        long rowCount = 0;
        for (Map.Entry<Long, Partition> entry : idToPartition.entrySet()) {
            rowCount += ((Partition) entry.getValue()).getBaseIndex().getRowCount();
        }
        return rowCount;
    }

    public AlterTableStmt toAddRollupStmt(String dbName, Collection<Long> indexIds) {
        List<AlterClause> alterClauses = Lists.newArrayList();
        for (Map.Entry<String, Long> entry : indexNameToId.entrySet()) {
            String indexName = entry.getKey();
            long indexId = entry.getValue();
            if (!indexIds.contains(indexId)) {
                continue;
            }

            // cols
            List<String> columnNames = Lists.newArrayList();
            for (Column column : indexIdToSchema.get(indexId)) {
                columnNames.add(column.getName());
            }
            
            // properties
            Map<String, String> properties = Maps.newHashMap();
            properties.put(PropertyAnalyzer.PROPERTIES_STORAGE_TYPE, indexIdToStorageType.get(indexId).name());
            properties.put(PropertyAnalyzer.PROPERTIES_SHORT_KEY, indexIdToShortKeyColumnCount.get(indexId).toString());
            properties.put(PropertyAnalyzer.PROPERTIES_SCHEMA_VERSION, indexIdToSchemaVersion.get(indexId).toString());

            AddRollupClause addRollupClause = new AddRollupClause(indexName, columnNames, null, null, properties);
            alterClauses.add(addRollupClause);
        }

        AlterTableStmt alterTableStmt = new AlterTableStmt(new TableName(dbName, name), alterClauses);
        return alterTableStmt;
    }

    public AlterTableStmt toAddPartitionStmt(String dbName, String partitionName) {
        Preconditions.checkState(partitionInfo.getType() == PartitionType.RANGE);
        RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
        List<AlterClause> alterClauses = Lists.newArrayList();
        
        Partition partition = nameToPartition.get(partitionName);
        Map<String, String> properties = Maps.newHashMap();
        long version = partition.getVisibleVersion();
        long versionHash = partition.getVisibleVersionHash();
        properties.put(PropertyAnalyzer.PROPERTIES_VERSION_INFO, version + "," + versionHash);
        properties.put(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM,
                       String.valueOf(partitionInfo.getReplicationNum(partition.getId())));

        SingleRangePartitionDesc singleDesc =
                rangePartitionInfo.toSingleRangePartitionDesc(partition.getId(), partitionName, properties);
        DistributionDesc distributionDesc = partition.getDistributionInfo().toDistributionDesc();

        AddPartitionClause addPartitionClause = new AddPartitionClause(singleDesc, distributionDesc, null);
        alterClauses.add(addPartitionClause);
        AlterTableStmt stmt = new AlterTableStmt(new TableName(dbName, name), alterClauses);
        return stmt;
    }

    @Override
    public CreateTableStmt toCreateTableStmt(String dbName) {
        Map<String, String> properties = Maps.newHashMap();

        // partition
        PartitionDesc partitionDesc = null;
        if (partitionInfo.getType() == PartitionType.RANGE) {
            RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
            List<Column> partitionColumns = rangePartitionInfo.getPartitionColumns();
            List<String> partitionColNames = Lists.newArrayList();
            for (Column partCol : partitionColumns) {
                partitionColNames.add(partCol.getName());
            }

            List<SingleRangePartitionDesc> singlePartitionDescs = Lists.newArrayList();
            partitionDesc = new RangePartitionDesc(partitionColNames, singlePartitionDescs);
        } else {
            Short replicationNum = partitionInfo.getReplicationNum(nameToPartition.get(name).getId());
            properties.put(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, replicationNum.toString());
            // and partition version info here for non-partitioned table
            Partition partition = getPartition(name);
            Preconditions.checkNotNull(partition);
            long version = partition.getVisibleVersion();
            long versionHash = partition.getVisibleVersionHash();
            String versionProp = Joiner.on(",").join(version, versionHash);
            properties.put(PropertyAnalyzer.PROPERTIES_VERSION_INFO, versionProp);
        }

        // keys
        List<String> keysColumnNames = Lists.newArrayList();
        for (Column column : baseSchema) {
            if (column.isKey()) {
                keysColumnNames.add(column.getName());
            }
        }
        KeysDesc keysDesc = new KeysDesc(keysType, keysColumnNames);

        // distribution
        DistributionDesc distributionDesc = defaultDistributionInfo.toDistributionDesc();

        // other properties
        properties.put(PropertyAnalyzer.PROPERTIES_SHORT_KEY, indexIdToShortKeyColumnCount.get(id).toString());
        properties.put(PropertyAnalyzer.PROPERTIES_STORAGE_TYPE, indexIdToStorageType.get(id).name());
        if (bfColumns != null) {
            String bfCols = Joiner.on(",").join(bfColumns);
            properties.put(PropertyAnalyzer.PROPERTIES_BF_COLUMNS, bfCols);
            properties.put(PropertyAnalyzer.PROPERTIES_BF_FPP, String.valueOf(bfFpp));
        }
        properties.put(PropertyAnalyzer.PROPERTIES_SCHEMA_VERSION, indexIdToSchemaVersion.get(id).toString());

        CreateTableStmt stmt = new CreateTableStmt(false, false, new TableName(dbName, name), baseSchema,
                                                   type.name(), keysDesc, partitionDesc, distributionDesc,
                                                   properties, null);
        return stmt;
    }

    public int getSignature(int signatureVersion, List<String> partNames) {
        Adler32 adler32 = new Adler32();
        adler32.update(signatureVersion);
        final String charsetName = "UTF-8";

        try {
            // table name
            adler32.update(name.getBytes(charsetName));
            LOG.debug("signature. table name: {}", name);
            // type
            adler32.update(type.name().getBytes(charsetName));
            LOG.debug("signature. table type: {}", type.name());

            // all indices(should be in order)
            Set<String> indexNames = Sets.newTreeSet();
            indexNames.addAll(indexNameToId.keySet());
            for (String indexName : indexNames) {
                long indexId = indexNameToId.get(indexName);
                adler32.update(indexName.getBytes(charsetName));
                LOG.debug("signature. index name: {}", indexName);
                // schema hash
                adler32.update(indexIdToSchemaHash.get(indexId));
                LOG.debug("signature. index schema hash: {}", indexIdToSchemaHash.get(indexId));
                // short key column count
                adler32.update(indexIdToShortKeyColumnCount.get(indexId));
                LOG.debug("signature. index short key: {}", indexIdToShortKeyColumnCount.get(indexId));
                // storage type
                adler32.update(indexIdToStorageType.get(indexId).name().getBytes(charsetName));
                LOG.debug("signature. index storage type: {}", indexIdToStorageType.get(indexId));
            }

            // bloom filter
            if (bfColumns != null && !bfColumns.isEmpty()) {
                for (String bfCol : bfColumns) {
                    adler32.update(bfCol.getBytes());
                    LOG.debug("signature. bf col: {}", bfCol);
                }
                adler32.update(String.valueOf(bfFpp).getBytes());
                LOG.debug("signature. bf fpp: {}", bfFpp);
            }

            // partition type
            adler32.update(partitionInfo.getType().name().getBytes(charsetName));
            LOG.debug("signature. partition type: {}", partitionInfo.getType().name());
            // partition columns
            if (partitionInfo.getType() == PartitionType.RANGE) {
                RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
                List<Column> partitionColumns = rangePartitionInfo.getPartitionColumns();
                adler32.update(Util.schemaHash(0, partitionColumns, null, 0));
                LOG.debug("signature. partition col hash: {}", Util.schemaHash(0, partitionColumns, null, 0));
            }

            // partition and distribution
            Collections.sort(partNames, String.CASE_INSENSITIVE_ORDER);
            for (String partName : partNames) {
                Partition partition = getPartition(partName);
                Preconditions.checkNotNull(partition, partName);
                adler32.update(partName.getBytes(charsetName));
                LOG.debug("signature. partition name: {}", partName);
                DistributionInfo distributionInfo = partition.getDistributionInfo();
                adler32.update(distributionInfo.getType().name().getBytes(charsetName));
                if (distributionInfo.getType() == DistributionInfoType.HASH) {
                    HashDistributionInfo hashDistributionInfo = (HashDistributionInfo) distributionInfo;
                    adler32.update(Util.schemaHash(0, hashDistributionInfo.getDistributionColumns(), null, 0));
                    LOG.debug("signature. distribution col hash: {}",
                              Util.schemaHash(0, hashDistributionInfo.getDistributionColumns(), null, 0));
                    adler32.update(hashDistributionInfo.getBucketNum());
                    LOG.debug("signature. bucket num: {}", hashDistributionInfo.getBucketNum());
                }
            }

        } catch (UnsupportedEncodingException e) {
            LOG.error("encoding error", e);
            return -1;
        }

        LOG.debug("signature: {}", Math.abs((int) adler32.getValue()));
        return Math.abs((int) adler32.getValue());
    }

    public Status getIntersectPartNamesWith(OlapTable anotherTbl, List<String> intersectPartNames) {
        if (this.getPartitionInfo().getType() != anotherTbl.getPartitionInfo().getType()) {
            return new Status(ErrCode.COMMON_ERROR, "Table's partition type is different");
        }

        Set<String> intersect = this.getPartitionNames();
        intersect.retainAll(anotherTbl.getPartitionNames());
        intersectPartNames.addAll(intersect);
        return Status.OK;
    }

    @Override
    public boolean isPartitioned() {
        int numSegs = 0;
        for (Partition part : getPartitions()) {
            numSegs += part.getDistributionInfo().getBucketNum();
            if (numSegs > 1) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);

        // state
        Text.writeString(out, state.name());

        // indices' schema
        int counter = indexNameToId.size();
        out.writeInt(counter);
        for (Map.Entry<String, Long> entry : indexNameToId.entrySet()) {
            String indexName = entry.getKey();
            long indexId = entry.getValue();
            Text.writeString(out, indexName);
            out.writeLong(indexId);
            // schema
            out.writeInt(indexIdToSchema.get(indexId).size());
            for (Column column : indexIdToSchema.get(indexId)) {
                column.write(out);
            }

            // storage type
            Text.writeString(out, indexIdToStorageType.get(indexId).name());

            // indices's schema version
            out.writeInt(indexIdToSchemaVersion.get(indexId));

            // indices's schema hash
            out.writeInt(indexIdToSchemaHash.get(indexId));

            // indices's short key column count
            out.writeShort(indexIdToShortKeyColumnCount.get(indexId));
        }

        Text.writeString(out, keysType.name());
        Text.writeString(out, partitionInfo.getType().name());
        partitionInfo.write(out);
        Text.writeString(out, defaultDistributionInfo.getType().name());
        defaultDistributionInfo.write(out);

        // partitions
        int partitionCount = idToPartition.size();
        out.writeInt(partitionCount);
        for (Partition partition : idToPartition.values()) {
            partition.write(out);
        }

        // bloom filter columns
        if (bfColumns == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeInt(bfColumns.size());
            for (String bfColumn : bfColumns) {
                Text.writeString(out, bfColumn);
            }
            out.writeDouble(bfFpp);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);

        this.state = OlapTableState.valueOf(Text.readString(in));

        // indices's schema
        int counter = in.readInt();
        for (int i = 0; i < counter; i++) {
            String indexName = Text.readString(in);
            long indexId = in.readLong();
            this.indexNameToId.put(indexName, indexId);

            // schema
            int colCount = in.readInt();
            List<Column> schema = new LinkedList<Column>();
            for (int j = 0; j < colCount; j++) {
                Column column = Column.read(in);
                schema.add(column);
            }
            this.indexIdToSchema.put(indexId, schema);

            // storage type
            TStorageType type = TStorageType.valueOf(Text.readString(in));
            this.indexIdToStorageType.put(indexId, type);

            // indices's schema version
            this.indexIdToSchemaVersion.put(indexId, in.readInt());

            // indices's schema hash
            this.indexIdToSchemaHash.put(indexId, in.readInt());

            // indices's short key column count
            this.indexIdToShortKeyColumnCount.put(indexId, in.readShort());
        }

        // partition and distribution info
        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_30) {
            keysType = KeysType.valueOf(Text.readString(in));
        } else {
            keysType = KeysType.AGG_KEYS;
        }

        PartitionType partType = PartitionType.valueOf(Text.readString(in));
        if (partType == PartitionType.UNPARTITIONED) {
            partitionInfo = PartitionInfo.read(in);
        } else if (partType == PartitionType.RANGE) {
            partitionInfo = RangePartitionInfo.read(in);
        } else {
            throw new IOException("invalid partition type: " + partType);
        }

        DistributionInfoType distriType = DistributionInfoType.valueOf(Text.readString(in));
        if (distriType == DistributionInfoType.HASH) {
            defaultDistributionInfo = HashDistributionInfo.read(in);
        } else if (distriType == DistributionInfoType.RANDOM) {
            defaultDistributionInfo = RandomDistributionInfo.read(in);
        } else {
            throw new IOException("invalid distribution type: " + distriType);
        }

        int partitionCount = in.readInt();
        for (int i = 0; i < partitionCount; ++i) {
            Partition partition = Partition.read(in);
            idToPartition.put(partition.getId(), partition);
            nameToPartition.put(partition.getName(), partition);
        }

        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_9) {
            if (in.readBoolean()) {
                int bfColumnCount = in.readInt();
                bfColumns = Sets.newHashSet();
                for (int i = 0; i < bfColumnCount; i++) {
                    bfColumns.add(Text.readString(in));
                }

                bfFpp = in.readDouble();
            }
        }
    }

    public boolean equals(Table table) {
        if (this == table) {
            return true;
        }
        if (!(table instanceof OlapTable)) {
            return false;
        }

        return true;
    }

    public OlapTable selectiveCopy(Collection<String> reservedPartNames) {
        OlapTable copied = new OlapTable();
        if (!DeepCopy.copy(this, copied)) {
            LOG.warn("failed to copy olap table: " + getName());
            return null;
        }
        
        if (reservedPartNames == null || reservedPartNames.isEmpty()) {
            // reserve all
            return copied;
        }
        
        Set<String> partNames = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        partNames.addAll(copied.getPartitionNames());
        
        for (String partName : partNames) {
            if (!reservedPartNames.contains(partName)) {
                copied.dropPartitionForBackup(partName);
            }
        }
        
        return copied;
    }
}
