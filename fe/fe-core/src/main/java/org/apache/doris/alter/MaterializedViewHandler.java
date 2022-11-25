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

package org.apache.doris.alter;

import org.apache.doris.analysis.AddRollupClause;
import org.apache.doris.analysis.AlterClause;
import org.apache.doris.analysis.CancelAlterTableStmt;
import org.apache.doris.analysis.CancelStmt;
import org.apache.doris.analysis.CreateMaterializedViewStmt;
import org.apache.doris.analysis.CreateMultiTableMaterializedViewStmt;
import org.apache.doris.analysis.DropMaterializedViewStmt;
import org.apache.doris.analysis.DropRollupClause;
import org.apache.doris.analysis.MVColumnItem;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexState;
import org.apache.doris.catalog.MetaIdGenerator.IdGeneratorBuffer;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.OlapTable.OlapTableState;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Replica.ReplicaState;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.IdGeneratorUtil;
import org.apache.doris.common.util.ListComparator;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.common.util.Util;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.persist.BatchDropInfo;
import org.apache.doris.persist.DropInfo;
import org.apache.doris.persist.EditLog;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.thrift.TStorageFormat;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/*
 * MaterializedViewHandler is responsible for ADD/DROP materialized view.
 * For compatible with older version, it is also responsible for ADD/DROP rollup.
 * In function level, the mv completely covers the rollup in the future.
 * In grammar level, there is some difference between mv and rollup.
 */
public class MaterializedViewHandler extends AlterHandler {
    private static final Logger LOG = LogManager.getLogger(MaterializedViewHandler.class);
    public static final String NEW_STORAGE_FORMAT_INDEX_NAME_PREFIX = "__v2_";

    public MaterializedViewHandler() {
        super("materialized view");
    }

    // for batch submit rollup job, tableId -> jobId
    // keep table's not final state job size. The job size determine's table's state, = 0 means table is normal,
    // otherwise is rollup
    private Map<Long, Set<Long>> tableNotFinalStateJobMap = new ConcurrentHashMap<>();
    // keep table's running job,used for concurrency limit
    // table id -> set of running job ids
    private Map<Long, Set<Long>> tableRunningJobMap = new ConcurrentHashMap<>();

    @Override
    public void addAlterJobV2(AlterJobV2 alterJob) {
        super.addAlterJobV2(alterJob);
        addAlterJobV2ToTableNotFinalStateJobMap(alterJob);
    }

    protected void batchAddAlterJobV2(List<AlterJobV2> alterJobV2List) {
        for (AlterJobV2 alterJobV2 : alterJobV2List) {
            addAlterJobV2(alterJobV2);
        }
    }

    // return true iff job is actually added this time
    private boolean addAlterJobV2ToTableNotFinalStateJobMap(AlterJobV2 alterJobV2) {
        if (alterJobV2.isDone()) {
            LOG.warn("try to add a final job({}) to a unfinal set. db: {}, tbl: {}",
                    alterJobV2.getJobId(), alterJobV2.getDbId(), alterJobV2.getTableId());
            return false;
        }

        Long tableId = alterJobV2.getTableId();
        Long jobId = alterJobV2.getJobId();

        synchronized (tableNotFinalStateJobMap) {
            Set<Long> tableNotFinalStateJobIdSet = tableNotFinalStateJobMap.get(tableId);
            if (tableNotFinalStateJobIdSet == null) {
                tableNotFinalStateJobIdSet = new HashSet<>();
                tableNotFinalStateJobMap.put(tableId, tableNotFinalStateJobIdSet);
            }
            return tableNotFinalStateJobIdSet.add(jobId);
        }
    }

    /**
     *
     * @param alterJobV2
     * @return true iif we really removed a job from tableNotFinalStateJobMap,
     *         and there is no running job of this table
     *         false otherwise.
     */
    private boolean removeAlterJobV2FromTableNotFinalStateJobMap(AlterJobV2 alterJobV2) {
        Long tableId = alterJobV2.getTableId();
        Long jobId = alterJobV2.getJobId();

        synchronized (tableNotFinalStateJobMap) {
            Set<Long> tableNotFinalStateJobIdset = tableNotFinalStateJobMap.get(tableId);
            if (tableNotFinalStateJobIdset == null) {
                // This could happen when this job is already removed before.
                // return false, so that we will not set table's to NORMAL again.
                return false;
            }
            tableNotFinalStateJobIdset.remove(jobId);
            if (tableNotFinalStateJobIdset.size() == 0) {
                tableNotFinalStateJobMap.remove(tableId);
                return true;
            }
            return false;
        }
    }

    /**
     * There are 2 main steps in this function.
     * Step1: validate the request.
     *   Step1.1: semantic analysis: the name of olapTable must be same as the base table name in addMVClause.
     *   Step1.2: base table validation: the status of base table and partition could be NORMAL.
     *   Step1.3: materialized view validation: the name and columns of mv is checked.
     * Step2: create mv job
     * @param addMVClause
     * @param db
     * @param olapTable
     * @throws DdlException
     */
    public void processCreateMaterializedView(CreateMaterializedViewStmt addMVClause, Database db, OlapTable olapTable)
            throws DdlException, AnalysisException {
        olapTable.writeLockOrDdlException();
        try {
            olapTable.checkStableAndNormal(db.getClusterName());
            if (olapTable.existTempPartitions()) {
                throw new DdlException("Can not alter table when there are temp partitions in table");
            }

            // Step1.1: semantic analysis
            // TODO(ML): support the materialized view as base index
            if (!addMVClause.getBaseIndexName().equals(olapTable.getName())) {
                throw new DdlException("The name of table in from clause must be same as the name of alter table");
            }
            // Step1.2: base table validation
            String baseIndexName = addMVClause.getBaseIndexName();
            String mvIndexName = addMVClause.getMVName();
            LOG.info("process add materialized view[{}] based on [{}]", mvIndexName, baseIndexName);

            // avoid conflict against with batch add rollup job
            Preconditions.checkState(olapTable.getState() == OlapTableState.NORMAL);

            long baseIndexId = checkAndGetBaseIndex(baseIndexName, olapTable);
            // Step1.3: mv clause validation
            List<Column> mvColumns = checkAndPrepareMaterializedView(addMVClause, olapTable);

            // Step2: create mv job
            RollupJobV2 rollupJobV2 = createMaterializedViewJob(mvIndexName, baseIndexName, mvColumns,
                    addMVClause.getProperties(), olapTable, db, baseIndexId,
                    addMVClause.getMVKeysType(), addMVClause.getOrigStmt());

            addAlterJobV2(rollupJobV2);

            olapTable.setState(OlapTableState.ROLLUP);

            Env.getCurrentEnv().getEditLog().logAlterJob(rollupJobV2);
            LOG.info("finished to create materialized view job: {}", rollupJobV2.getJobId());
        } finally {
            olapTable.writeUnlock();
        }
    }

    /**
     * There are 2 main steps.
     * Step1: validate the request
     *   Step1.1: base table validation: the status of base table and partition could be NORMAL.
     *   Step1.2: rollup validation: the name and columns of rollup is checked.
     * Step2: create rollup job
     * @param alterClauses
     * @param db
     * @param olapTable
     * @throws DdlException
     * @throws AnalysisException
     */
    public void processBatchAddRollup(List<AlterClause> alterClauses, Database db, OlapTable olapTable)
            throws DdlException, AnalysisException {
        checkReplicaCount(olapTable);
        Map<String, RollupJobV2> rollupNameJobMap = new LinkedHashMap<>();
        // save job id for log
        Set<Long> logJobIdSet = new HashSet<>();
        olapTable.writeLockOrDdlException();
        try {
            if (olapTable.existTempPartitions()) {
                throw new DdlException("Can not alter table when there are temp partitions in table");
            }

            // 1 check and make rollup job
            for (AlterClause alterClause : alterClauses) {
                AddRollupClause addRollupClause = (AddRollupClause) alterClause;

                // step 1 check whether current alter is change storage format
                String rollupIndexName = addRollupClause.getRollupName();
                boolean changeStorageFormat = false;
                if (rollupIndexName.equalsIgnoreCase(olapTable.getName())) {
                    // for upgrade test to create segment v2 rollup index by using the sql:
                    // alter table table_name add rollup table_name (columns) properties ("storage_format" = "v2");
                    Map<String, String> properties = addRollupClause.getProperties();
                    if (properties == null || !properties.containsKey(PropertyAnalyzer.PROPERTIES_STORAGE_FORMAT)
                            || !properties.get(PropertyAnalyzer.PROPERTIES_STORAGE_FORMAT).equalsIgnoreCase("v2")) {
                        throw new DdlException("Table[" + olapTable.getName() + "] can not "
                                + "add segment v2 rollup index without setting storage format to v2.");
                    }
                    rollupIndexName = NEW_STORAGE_FORMAT_INDEX_NAME_PREFIX + olapTable.getName();
                    changeStorageFormat = true;
                }

                // get base index schema
                String baseIndexName = addRollupClause.getBaseRollupName();
                if (baseIndexName == null) {
                    // use table name as base table name
                    baseIndexName = olapTable.getName();
                }

                // step 2 alter clause validation
                // step 2.1 check whether base index already exists in catalog
                long baseIndexId = checkAndGetBaseIndex(baseIndexName, olapTable);

                // step 2.2  check rollup schema
                List<Column> rollupSchema = checkAndPrepareMaterializedView(
                        addRollupClause, olapTable, baseIndexId, changeStorageFormat);

                // step 3 create rollup job
                RollupJobV2 alterJobV2 = createMaterializedViewJob(rollupIndexName, baseIndexName, rollupSchema,
                        addRollupClause.getProperties(), olapTable, db, baseIndexId, olapTable.getKeysType(), null);

                rollupNameJobMap.put(addRollupClause.getRollupName(), alterJobV2);
                logJobIdSet.add(alterJobV2.getJobId());
            }

            // set table' state to ROLLUP before adding rollup jobs.
            // so that when the AlterHandler thread run the jobs, it will see the expected table's state.
            // ATTN: This order is not mandatory, because database lock will protect us,
            // but this order is more reasonable
            olapTable.setState(OlapTableState.ROLLUP);
            // 2 batch submit rollup job
            List<AlterJobV2> rollupJobV2List = new ArrayList<>(rollupNameJobMap.values());
            batchAddAlterJobV2(rollupJobV2List);

            BatchAlterJobPersistInfo batchAlterJobV2 = new BatchAlterJobPersistInfo(rollupJobV2List);
            Env.getCurrentEnv().getEditLog().logBatchAlterJob(batchAlterJobV2);
            LOG.info("finished to create materialized view job: {}", logJobIdSet);

        } catch (Exception e) {
            // remove tablet which has already inserted into TabletInvertedIndex
            TabletInvertedIndex tabletInvertedIndex = Env.getCurrentInvertedIndex();
            for (RollupJobV2 rollupJobV2 : rollupNameJobMap.values()) {
                for (MaterializedIndex index : rollupJobV2.getPartitionIdToRollupIndex().values()) {
                    for (Tablet tablet : index.getTablets()) {
                        tabletInvertedIndex.deleteTablet(tablet.getId());
                    }
                }
            }
            throw e;
        } finally {
            olapTable.writeUnlock();
        }
    }

    /**
     * Step1: All replicas of the materialized view index will be created in meta and added to TabletInvertedIndex
     * Step2: Set table's state to ROLLUP.
     *
     * @param mvName
     * @param baseIndexName
     * @param mvColumns
     * @param properties
     * @param olapTable
     * @param db
     * @param baseIndexId
     * @throws DdlException
     * @throws AnalysisException
     */
    private RollupJobV2 createMaterializedViewJob(String mvName, String baseIndexName,
            List<Column> mvColumns, Map<String, String> properties,
            OlapTable olapTable, Database db, long baseIndexId, KeysType mvKeysType,
            OriginStatement origStmt) throws DdlException, AnalysisException {
        if (mvKeysType == null) {
            // assign rollup index's key type, same as base index's
            mvKeysType = olapTable.getKeysType();
        }
        // get rollup schema hash
        int mvSchemaHash = Util.generateSchemaHash();
        // get short key column count
        short mvShortKeyColumnCount = Env.calcShortKeyColumnCount(mvColumns, properties);
        // get timeout
        long timeoutMs = PropertyAnalyzer.analyzeTimeout(properties, Config.alter_table_timeout_second) * 1000;

        // create rollup job
        long dbId = db.getId();
        long tableId = olapTable.getId();
        int baseSchemaHash = olapTable.getSchemaHashByIndexId(baseIndexId);
        Env env = Env.getCurrentEnv();
        long bufferSize = IdGeneratorUtil.getBufferSizeForAlterTable(olapTable, Sets.newHashSet(baseIndexId));
        IdGeneratorBuffer idGeneratorBuffer = env.getIdGeneratorBuffer(bufferSize);
        long jobId = idGeneratorBuffer.getNextId();
        long mvIndexId = idGeneratorBuffer.getNextId();
        RollupJobV2 mvJob = new RollupJobV2(jobId, dbId, tableId, olapTable.getName(), timeoutMs,
                baseIndexId, mvIndexId, baseIndexName, mvName,
                mvColumns, baseSchemaHash, mvSchemaHash,
                mvKeysType, mvShortKeyColumnCount, origStmt);
        String newStorageFormatIndexName = NEW_STORAGE_FORMAT_INDEX_NAME_PREFIX + olapTable.getName();
        if (mvName.equals(newStorageFormatIndexName)) {
            mvJob.setStorageFormat(TStorageFormat.V2);
        } else {
            // use base table's storage format as the mv's format
            mvJob.setStorageFormat(olapTable.getStorageFormat());
        }

        /*
         * create all rollup indexes. and set state.
         * After setting, Tables' state will be ROLLUP
         */
        List<Tablet> addedTablets = Lists.newArrayList();
        for (Partition partition : olapTable.getPartitions()) {
            long partitionId = partition.getId();
            TStorageMedium medium = olapTable.getPartitionInfo().getDataProperty(partitionId).getStorageMedium();
            // index state is SHADOW
            MaterializedIndex mvIndex = new MaterializedIndex(mvIndexId, IndexState.SHADOW);
            MaterializedIndex baseIndex = partition.getIndex(baseIndexId);
            TabletMeta mvTabletMeta = new TabletMeta(dbId, tableId, partitionId, mvIndexId, mvSchemaHash, medium);
            short replicationNum = olapTable.getPartitionInfo().getReplicaAllocation(partitionId).getTotalReplicaNum();
            for (Tablet baseTablet : baseIndex.getTablets()) {
                long baseTabletId = baseTablet.getId();
                long mvTabletId = idGeneratorBuffer.getNextId();

                Tablet newTablet = new Tablet(mvTabletId);
                mvIndex.addTablet(newTablet, mvTabletMeta);
                addedTablets.add(newTablet);

                mvJob.addTabletIdMap(partitionId, mvTabletId, baseTabletId);
                List<Replica> baseReplicas = baseTablet.getReplicas();

                int healthyReplicaNum = 0;
                for (Replica baseReplica : baseReplicas) {
                    long mvReplicaId = idGeneratorBuffer.getNextId();
                    long backendId = baseReplica.getBackendId();
                    if (baseReplica.getState() == ReplicaState.CLONE
                            || baseReplica.getState() == ReplicaState.DECOMMISSION
                            || baseReplica.getState() == ReplicaState.COMPACTION_TOO_SLOW
                            || baseReplica.getLastFailedVersion() > 0) {
                        LOG.info("base replica {} of tablet {} state is {}, and last failed version is {},"
                                        + " skip creating rollup replica", baseReplica.getId(), baseTabletId,
                                baseReplica.getState(), baseReplica.getLastFailedVersion());
                        continue;
                    }
                    Preconditions.checkState(baseReplica.getState() == ReplicaState.NORMAL,
                            baseReplica.getState());
                    // replica's init state is ALTER, so that tablet report process will ignore its report
                    Replica mvReplica = new Replica(mvReplicaId, backendId, ReplicaState.ALTER,
                            Partition.PARTITION_INIT_VERSION, mvSchemaHash);
                    newTablet.addReplica(mvReplica);
                    healthyReplicaNum++;
                } // end for baseReplica

                if (healthyReplicaNum < replicationNum / 2 + 1) {
                    /*
                     * TODO(cmy): This is a bad design.
                     * Because in the rollup job, we will only send tasks to the rollup replicas that have been created,
                     * without checking whether the quorum of replica number are satisfied.
                     * This will cause the job to fail until we find that the quorum of replica number
                     * is not satisfied until the entire job is done.
                     * So here we check the replica number strictly and do not allow to submit the job
                     * if the quorum of replica number is not satisfied.
                     */
                    for (Tablet tablet : addedTablets) {
                        Env.getCurrentInvertedIndex().deleteTablet(tablet.getId());
                    }
                    throw new DdlException("tablet " + baseTabletId + " has few healthy replica: " + healthyReplicaNum);
                }
            } // end for baseTablets

            mvJob.addMVIndex(partitionId, mvIndex);

            LOG.debug("create materialized view index {} based on index {} in partition {}",
                    mvIndexId, baseIndexId, partitionId);
        } // end for partitions

        LOG.info("finished to create materialized view job: {}", mvJob.getJobId());
        return mvJob;
    }

    private List<Column> checkAndPrepareMaterializedView(CreateMaterializedViewStmt addMVClause, OlapTable olapTable)
            throws DdlException {
        // check if mv index already exists
        if (olapTable.hasMaterializedIndex(addMVClause.getMVName())) {
            throw new DdlException("Materialized view[" + addMVClause.getMVName() + "] already exists");
        }
        // check if mv columns are valid
        // a. Aggregate or Unique table:
        //     1. For aggregate table, mv columns with aggregate function should be same as base schema
        //     2. For aggregate table, the column which is the key of base table should be the key of mv as well.
        // b. Duplicate table:
        //     1. Columns resolved by semantics are legal
        // update mv columns
        List<MVColumnItem> mvColumnItemList = addMVClause.getMVColumnItemList();
        List<Column> newMVColumns = Lists.newArrayList();
        int numOfKeys = 0;
        if (olapTable.getKeysType().isAggregationFamily()) {
            if (addMVClause.getMVKeysType() != KeysType.AGG_KEYS) {
                throw new DdlException("The materialized view of aggregation"
                        + " or unique table must has grouping columns");
            }
            for (MVColumnItem mvColumnItem : mvColumnItemList) {
                String mvColumnName = mvColumnItem.getName();
                Column baseColumn = olapTable.getColumn(mvColumnName);
                if (mvColumnItem.isKey()) {
                    ++numOfKeys;
                }
                if (baseColumn == null) {
                    throw new DdlException("The mv column of agg or uniq table cannot be transformed "
                            + "from original column[" + mvColumnItem.getBaseColumnName() + "]");
                }
                Preconditions.checkNotNull(baseColumn, "Column[" + mvColumnName + "] does not exist");
                AggregateType baseAggregationType = baseColumn.getAggregationType();
                AggregateType mvAggregationType = mvColumnItem.getAggregationType();
                if (baseColumn.isKey() && !mvColumnItem.isKey()) {
                    throw new DdlException("The column[" + mvColumnName + "] must be the key of materialized view");
                }
                if (baseAggregationType != mvAggregationType) {
                    throw new DdlException(
                            "The aggregation type of column[" + mvColumnName + "] must be same as the aggregate "
                                    + "type of base column in aggregate table");
                }
                if (baseAggregationType != null && baseAggregationType.isReplaceFamily() && olapTable
                        .getKeysNum() != numOfKeys) {
                    throw new DdlException(
                            "The materialized view should contain all keys of base table if there is a" + " REPLACE "
                                    + "value");
                }
                newMVColumns.add(mvColumnItem.toMVColumn(olapTable));
            }
        } else {
            Set<String> partitionOrDistributedColumnName = olapTable.getPartitionColumnNames();
            partitionOrDistributedColumnName.addAll(olapTable.getDistributionColumnNames());
            for (MVColumnItem mvColumnItem : mvColumnItemList) {
                if (partitionOrDistributedColumnName.contains(mvColumnItem.getBaseColumnName().toLowerCase())
                        && mvColumnItem.getAggregationType() != null) {
                    throw new DdlException("The partition and distributed columns " + mvColumnItem.getBaseColumnName()
                            + " must be key column in mv");
                }
                newMVColumns.add(mvColumnItem.toMVColumn(olapTable));
            }
        }
        if (KeysType.UNIQUE_KEYS == olapTable.getKeysType() && olapTable.hasDeleteSign()) {
            newMVColumns.add(new Column(olapTable.getDeleteSignColumn()));
        }
        if (KeysType.UNIQUE_KEYS == olapTable.getKeysType() && olapTable.hasSequenceCol()) {
            newMVColumns.add(new Column(olapTable.getSequenceCol()));
        }
        // if the column is array type, we forbid to create materialized view
        for (Column column : newMVColumns) {
            if (column.getDataType() == PrimitiveType.ARRAY) {
                throw new DdlException("The array column[" + column + "] not support to create materialized view");
            }
        }

        if (olapTable.getEnableLightSchemaChange()) {
            int nextColUniqueId = Column.COLUMN_UNIQUE_ID_INIT_VALUE + 1;
            for (Column column : newMVColumns) {
                column.setUniqueId(nextColUniqueId);
                nextColUniqueId++;
            }
        } else {
            newMVColumns.forEach(column -> {
                column.setUniqueId(Column.COLUMN_UNIQUE_ID_INIT_VALUE);
            });
        }
        LOG.debug("lightSchemaChange:{}, newMVColumns:{}", olapTable.getEnableLightSchemaChange(), newMVColumns);
        return newMVColumns;
    }

    public List<Column> checkAndPrepareMaterializedView(AddRollupClause addRollupClause, OlapTable olapTable,
                                                        long baseIndexId, boolean changeStorageFormat)
            throws DdlException {
        String rollupIndexName = addRollupClause.getRollupName();
        List<String> rollupColumnNames = addRollupClause.getColumnNames();
        if (changeStorageFormat) {
            String newStorageFormatIndexName = NEW_STORAGE_FORMAT_INDEX_NAME_PREFIX + olapTable.getName();
            rollupIndexName = newStorageFormatIndexName;
            // Must get all columns including invisible columns.
            // Because in alter process, all columns must be considered.
            List<Column> columns = olapTable.getSchemaByIndexId(baseIndexId, true);
            // create the same schema as base table
            rollupColumnNames.clear();
            for (Column column : columns) {
                rollupColumnNames.add(column.getName());
            }
        }

        // 2. check if rollup index already exists
        if (olapTable.hasMaterializedIndex(rollupIndexName)) {
            throw new DdlException("Rollup index[" + rollupIndexName + "] already exists");
        }

        // 3. check if rollup columns are valid
        // a. all columns should exist in base rollup schema
        // b. value after key
        // c. if rollup contains REPLACE column, all keys on base index should be included.
        // d. if base index has sequence column for unique_keys, rollup should add the sequence column
        List<Column> rollupSchema = new ArrayList<Column>();
        // check (a)(b)
        boolean meetValue = false;
        boolean hasKey = false;
        boolean meetReplaceValue = false;
        KeysType keysType = olapTable.getKeysType();
        Map<String, Column> baseColumnNameToColumn = Maps.newHashMap();
        for (Column column : olapTable.getSchemaByIndexId(baseIndexId, true)) {
            baseColumnNameToColumn.put(column.getName(), column);
        }
        LOG.debug("baseSchema:{}", olapTable.getSchemaByIndexId(baseIndexId, true));
        if (keysType.isAggregationFamily()) {
            int keysNumOfRollup = 0;
            for (String columnName : rollupColumnNames) {
                Column baseColumn = baseColumnNameToColumn.get(columnName);
                if (baseColumn == null) {
                    throw new DdlException("Column[" + columnName + "] does not exist");
                }
                if (baseColumn.isKey() && meetValue) {
                    throw new DdlException("Invalid column order. value should be after key");
                }
                if (baseColumn.isKey()) {
                    keysNumOfRollup += 1;
                    hasKey = true;
                } else {
                    meetValue = true;
                    if (baseColumn.getAggregationType().isReplaceFamily()) {
                        meetReplaceValue = true;
                    }
                }
                Column oneColumn = new Column(baseColumn);
                rollupSchema.add(oneColumn);
            }

            if (!hasKey) {
                throw new DdlException("No key column is found");
            }
            if (KeysType.UNIQUE_KEYS == keysType || meetReplaceValue) {
                // rollup of unique key table or rollup with REPLACE value
                // should have all keys of base table
                if (keysNumOfRollup != olapTable.getKeysNum()) {
                    if (KeysType.UNIQUE_KEYS == keysType) {
                        throw new DdlException("Rollup should contains all unique keys in basetable");
                    } else {
                        throw new DdlException("Rollup should contains all keys if there is a REPLACE value");
                    }
                }
                // add hidden column to rollup table

                if (KeysType.UNIQUE_KEYS == olapTable.getKeysType() && olapTable.hasDeleteSign()) {
                    rollupSchema.add(new Column(olapTable.getDeleteSignColumn()));
                }
                if (KeysType.UNIQUE_KEYS == olapTable.getKeysType() && olapTable.hasSequenceCol()) {
                    rollupSchema.add(new Column(olapTable.getSequenceCol()));
                }
            }
        } else if (KeysType.DUP_KEYS == keysType) {
            // supplement the duplicate key
            if (addRollupClause.getDupKeys() == null || addRollupClause.getDupKeys().isEmpty()) {
                // check the column meta
                for (int i = 0; i < rollupColumnNames.size(); i++) {
                    String columnName = rollupColumnNames.get(i);
                    Column baseColumn = baseColumnNameToColumn.get(columnName);
                    if (baseColumn == null) {
                        throw new DdlException("Column[" + columnName + "] does not exist in base index");
                    }
                    Column rollupColumn = new Column(baseColumn);
                    rollupSchema.add(rollupColumn);
                }
                if (changeStorageFormat) {
                    return rollupSchema;
                }
                // Supplement key of MV columns
                int theBeginIndexOfValue = 0;
                int keySizeByte = 0;
                for (; theBeginIndexOfValue < rollupSchema.size(); theBeginIndexOfValue++) {
                    Column column = rollupSchema.get(theBeginIndexOfValue);
                    keySizeByte += column.getType().getIndexSize();
                    if (theBeginIndexOfValue + 1 > FeConstants.shortkey_max_column_count
                            || keySizeByte > FeConstants.shortkey_maxsize_bytes) {
                        if (theBeginIndexOfValue == 0 && column.getType().getPrimitiveType().isCharFamily()) {
                            column.setIsKey(true);
                            theBeginIndexOfValue++;
                        }
                        break;
                    }
                    if (column.getType().isFloatingPointType()) {
                        break;
                    }
                    if (column.getType().getPrimitiveType() == PrimitiveType.VARCHAR) {
                        column.setIsKey(true);
                        theBeginIndexOfValue++;
                        break;
                    }
                    column.setIsKey(true);
                }
                if (theBeginIndexOfValue == 0) {
                    throw new DdlException("The first column could not be float or double");
                }
                // Supplement value of MV columns
                for (; theBeginIndexOfValue < rollupSchema.size(); theBeginIndexOfValue++) {
                    Column rollupColumn = rollupSchema.get(theBeginIndexOfValue);
                    rollupColumn.setIsKey(false);
                    rollupColumn.setAggregationType(AggregateType.NONE, true);
                }
            } else {
                /*
                 * eg.
                 * Base Table's schema is (k1,k2,k3,k4,k5) dup key (k1,k2,k3).
                 * The following rollup is allowed:
                 * 1. (k1) dup key (k1)
                 * 2. (k2,k3) dup key (k2)
                 * 3. (k1,k2,k3) dup key (k1,k2)
                 *
                 * The following rollup is forbidden:
                 * 1. (k1) dup key (k2)
                 * 2. (k2,k3) dup key (k3,k2)
                 * 3. (k1,k2,k3) dup key (k2,k3)
                 */
                // user specify the duplicate keys for rollup index
                List<String> dupKeys = addRollupClause.getDupKeys();
                if (dupKeys.size() > rollupColumnNames.size()) {
                    throw new DdlException("Num of duplicate keys should less than or equal to num of rollup columns.");
                }

                for (int i = 0; i < rollupColumnNames.size(); i++) {
                    String rollupColName = rollupColumnNames.get(i);
                    boolean isKey = false;
                    if (i < dupKeys.size()) {
                        String dupKeyName = dupKeys.get(i);
                        if (!rollupColName.equalsIgnoreCase(dupKeyName)) {
                            throw new DdlException("Duplicate keys should be the prefix of rollup columns");
                        }
                        isKey = true;
                    }

                    Column baseColumn = baseColumnNameToColumn.get(rollupColName);
                    if (baseColumn == null) {
                        throw new DdlException("Column[" + rollupColName + "] does not exist");
                    }

                    if (isKey && meetValue) {
                        throw new DdlException("Invalid column order. key should before all values: " + rollupColName);
                    }

                    Column oneColumn = new Column(baseColumn);
                    if (isKey) {
                        hasKey = true;
                        oneColumn.setIsKey(true);
                        oneColumn.setAggregationType(null, false);
                    } else {
                        meetValue = true;
                        oneColumn.setIsKey(false);
                        oneColumn.setAggregationType(AggregateType.NONE, true);
                    }
                    rollupSchema.add(oneColumn);
                }
            }
        }
        if (olapTable.getEnableLightSchemaChange()) {
            int nextColUniqueId = Column.COLUMN_UNIQUE_ID_INIT_VALUE + 1;
            for (Column column : rollupSchema) {
                column.setUniqueId(nextColUniqueId);
                nextColUniqueId++;
            }
        } else {
            rollupSchema.forEach(column -> {
                column.setUniqueId(Column.COLUMN_UNIQUE_ID_INIT_VALUE);
            });
        }
        LOG.debug("lightSchemaChange:{}, rollupSchema:{}, baseSchema:{}",
                olapTable.getEnableLightSchemaChange(), rollupSchema, olapTable.getSchemaByIndexId(baseIndexId, true));
        return rollupSchema;
    }

    /**
     *
     * @param baseIndexName
     * @param olapTable
     * @return
     * @throws DdlException
     */
    private long checkAndGetBaseIndex(String baseIndexName, OlapTable olapTable) throws DdlException {
        // up to here, table's state can only be NORMAL
        Preconditions.checkState(olapTable.getState() == OlapTableState.NORMAL, olapTable.getState().name());

        Long baseIndexId = olapTable.getIndexIdByName(baseIndexName);
        if (baseIndexId == null) {
            throw new DdlException("Base index[" + baseIndexName + "] does not exist");
        }
        // check state
        for (Partition partition : olapTable.getPartitions()) {
            MaterializedIndex baseIndex = partition.getIndex(baseIndexId);
            // up to here. index's state should only be NORMAL
            Preconditions.checkState(baseIndex.getState() == IndexState.NORMAL, baseIndex.getState().name());
        }
        return baseIndexId;
    }

    public void processBatchDropRollup(List<AlterClause> dropRollupClauses, Database db, OlapTable olapTable)
            throws DdlException, MetaNotFoundException {
        olapTable.writeLockOrDdlException();
        try {
            if (olapTable.existTempPartitions()) {
                throw new DdlException("Can not alter table when there are temp partitions in table");
            }

            // check drop rollup index operation
            for (AlterClause alterClause : dropRollupClauses) {
                DropRollupClause dropRollupClause = (DropRollupClause) alterClause;
                checkDropMaterializedView(dropRollupClause.getRollupName(), olapTable);
            }

            // drop data in memory
            Set<Long> indexIdSet = new HashSet<>();
            Set<String> rollupNameSet = new HashSet<>();
            for (AlterClause alterClause : dropRollupClauses) {
                DropRollupClause dropRollupClause = (DropRollupClause) alterClause;
                String rollupIndexName = dropRollupClause.getRollupName();
                long rollupIndexId = dropMaterializedView(rollupIndexName, olapTable);
                indexIdSet.add(rollupIndexId);
                rollupNameSet.add(rollupIndexName);
            }

            // batch log drop rollup operation
            EditLog editLog = Env.getCurrentEnv().getEditLog();
            long dbId = db.getId();
            long tableId = olapTable.getId();
            editLog.logBatchDropRollup(new BatchDropInfo(dbId, tableId, indexIdSet));
            LOG.info("finished drop rollup index[{}] in table[{}]",
                    String.join("", rollupNameSet), olapTable.getName());
        } finally {
            olapTable.writeUnlock();
        }
    }

    public void processDropMaterializedView(DropMaterializedViewStmt dropMaterializedViewStmt, Database db,
                                            OlapTable olapTable) throws DdlException, MetaNotFoundException {
        olapTable.writeLockOrDdlException();
        try {
            // check table state
            if (olapTable.getState() != OlapTableState.NORMAL) {
                throw new DdlException("Table[" + olapTable.getName() + "]'s state is not NORMAL. "
                        + "Do not allow doing DROP ops");
            }

            String mvName = dropMaterializedViewStmt.getMvName();
            // Step1: check drop mv index operation
            checkDropMaterializedView(mvName, olapTable);
            // Step2; drop data in memory
            long mvIndexId = dropMaterializedView(mvName, olapTable);
            // Step3: log drop mv operation
            EditLog editLog = Env.getCurrentEnv().getEditLog();
            editLog.logDropRollup(new DropInfo(db.getId(), olapTable.getId(), mvIndexId, false, 0));
            LOG.info("finished drop materialized view [{}] in table [{}]", mvName, olapTable.getName());
        } catch (MetaNotFoundException e) {
            if (dropMaterializedViewStmt.isIfExists()) {
                LOG.info(e.getMessage());
            } else {
                throw e;
            }
        } finally {
            olapTable.writeUnlock();
        }
    }

    /**
     * Make sure we got db write lock before using this method.
     * Up to here, table's state can only be NORMAL.
     *
     * @param mvName
     * @param olapTable
     */
    private void checkDropMaterializedView(String mvName, OlapTable olapTable)
            throws DdlException, MetaNotFoundException {
        Preconditions.checkState(olapTable.getState() == OlapTableState.NORMAL, olapTable.getState().name());
        if (mvName.equals(olapTable.getName())) {
            throw new DdlException("Cannot drop base index by using DROP ROLLUP or DROP MATERIALIZED VIEW.");
        }

        if (!olapTable.hasMaterializedIndex(mvName)) {
            throw new MetaNotFoundException(
                    "Materialized view [" + mvName + "] does not exist in table [" + olapTable.getName() + "]");
        }

        long mvIndexId = olapTable.getIndexIdByName(mvName);
        int mvSchemaHash = olapTable.getSchemaHashByIndexId(mvIndexId);
        Preconditions.checkState(mvSchemaHash != -1);

        for (Partition partition : olapTable.getPartitions()) {
            MaterializedIndex materializedIndex = partition.getIndex(mvIndexId);
            Preconditions.checkNotNull(materializedIndex);
        }
    }

    /**
     * Return mv index id which has been dropped
     *
     * @param mvName
     * @param olapTable
     * @return
     */
    private long dropMaterializedView(String mvName, OlapTable olapTable) {
        long mvIndexId = olapTable.getIndexIdByName(mvName);
        TabletInvertedIndex invertedIndex = Env.getCurrentInvertedIndex();
        for (Partition partition : olapTable.getPartitions()) {
            MaterializedIndex rollupIndex = partition.getIndex(mvIndexId);
            // delete rollup index
            partition.deleteRollupIndex(mvIndexId);
            // remove tablets from inverted index
            for (Tablet tablet : rollupIndex.getTablets()) {
                long tabletId = tablet.getId();
                invertedIndex.deleteTablet(tabletId);
            }
        }
        olapTable.deleteIndexInfo(mvName);
        return mvIndexId;
    }

    public void replayDropRollup(DropInfo dropInfo, Env env) throws MetaNotFoundException {
        long dbId = dropInfo.getDbId();
        long tableId = dropInfo.getTableId();
        long rollupIndexId = dropInfo.getIndexId();

        TabletInvertedIndex invertedIndex = Env.getCurrentInvertedIndex();
        Database db = env.getInternalCatalog().getDbOrMetaException(dbId);
        OlapTable olapTable = (OlapTable) db.getTableOrMetaException(tableId, Table.TableType.OLAP);
        olapTable.writeLock();
        try {
            for (Partition partition : olapTable.getPartitions()) {
                MaterializedIndex rollupIndex = partition.deleteRollupIndex(rollupIndexId);

                if (!Env.isCheckpointThread()) {
                    // remove from inverted index
                    for (Tablet tablet : rollupIndex.getTablets()) {
                        invertedIndex.deleteTablet(tablet.getId());
                    }
                }
            }

            String rollupIndexName = olapTable.getIndexNameById(rollupIndexId);
            olapTable.deleteIndexInfo(rollupIndexName);
        } finally {
            olapTable.writeUnlock();
        }
        LOG.info("replay drop rollup {}", dropInfo.getIndexId());
    }

    @Override
    protected void runAfterCatalogReady() {
        super.runAfterCatalogReady();
        runAlterJobV2();
    }

    private Map<Long, AlterJobV2> getAlterJobsCopy() {
        return new HashMap<>(alterJobsV2);
    }

    private void removeJobFromRunningQueue(AlterJobV2 alterJob) {
        synchronized (tableRunningJobMap) {
            Set<Long> runningJobIdSet = tableRunningJobMap.get(alterJob.getTableId());
            if (runningJobIdSet != null) {
                runningJobIdSet.remove(alterJob.getJobId());
                if (runningJobIdSet.size() == 0) {
                    tableRunningJobMap.remove(alterJob.getTableId());
                }
            }
        }
    }

    private void changeTableStatus(long dbId, long tableId, OlapTableState olapTableState) {
        try {
            Database db = Env.getCurrentInternalCatalog().getDbOrMetaException(dbId);
            OlapTable olapTable = (OlapTable) db.getTableOrMetaException(tableId, Table.TableType.OLAP);
            olapTable.writeLockOrMetaException();
            try {
                if (olapTable.getState() == olapTableState) {
                    return;
                }
                olapTable.setState(olapTableState);
            } finally {
                olapTable.writeUnlock();
            }
        } catch (MetaNotFoundException e) {
            LOG.warn("[INCONSISTENT META] changing table status failed after rollup job done", e);
        }
    }

    // replay the alter job v2
    @Override
    public void replayAlterJobV2(AlterJobV2 alterJob) {
        super.replayAlterJobV2(alterJob);
        if (!alterJob.isDone()) {
            addAlterJobV2ToTableNotFinalStateJobMap(alterJob);
            changeTableStatus(alterJob.getDbId(), alterJob.getTableId(), OlapTableState.ROLLUP);
            LOG.info("set table's state to ROLLUP, table id: {}, job id: {}", alterJob.getTableId(),
                    alterJob.getJobId());
        } else if (removeAlterJobV2FromTableNotFinalStateJobMap(alterJob)) {
            changeTableStatus(alterJob.getDbId(), alterJob.getTableId(), OlapTableState.NORMAL);
            LOG.info("set table's state to NORMAL, table id: {}, job id: {}", alterJob.getTableId(),
                    alterJob.getJobId());
        } else {
            LOG.info("not set table's state, table id: {}, is job done: {}, job id: {}", alterJob.getTableId(),
                    alterJob.isDone(), alterJob.getJobId());
        }
    }

    /**
     *  create tablet and alter tablet in be is thread safe,so we can run rollup job for one table concurrently
     */
    private void runAlterJobWithConcurrencyLimit(RollupJobV2 rollupJobV2) {
        if (rollupJobV2.isDone()) {
            return;
        }

        if (rollupJobV2.isTimeout()) {
            // in run(), the timeout job will be cancelled.
            rollupJobV2.run();
            return;
        }

        // check if rollup job can be run within limitation.
        long tblId = rollupJobV2.getTableId();
        long jobId = rollupJobV2.getJobId();
        boolean shouldJobRun = false;
        synchronized (tableRunningJobMap) {
            Set<Long> tableRunningJobSet = tableRunningJobMap.get(tblId);
            if (tableRunningJobSet == null) {
                tableRunningJobSet = new HashSet<>();
                tableRunningJobMap.put(tblId, tableRunningJobSet);
            }

            // current job is already in running
            if (tableRunningJobSet.contains(jobId)) {
                shouldJobRun = true;
            } else if (tableRunningJobSet.size() < Config.max_running_rollup_job_num_per_table) {
                // add current job to running queue
                tableRunningJobSet.add(jobId);
                shouldJobRun = true;
            } else {
                LOG.debug("number of running alter job {} in table {} exceed limit {}. job {} is suspended",
                        tableRunningJobSet.size(), rollupJobV2.getTableId(),
                        Config.max_running_rollup_job_num_per_table, rollupJobV2.getJobId());
                shouldJobRun = false;
            }
        }

        if (shouldJobRun) {
            rollupJobV2.run();
        }
    }

    private void runAlterJobV2() {
        for (Map.Entry<Long, AlterJobV2> entry : getAlterJobsCopy().entrySet()) {
            RollupJobV2 alterJob = (RollupJobV2) entry.getValue();
            // run alter job
            runAlterJobWithConcurrencyLimit(alterJob);
            // the following check should be right after job's running, so that the table's state
            // can be changed to NORMAL immediately after the last alter job of the table is done.
            //
            // ATTN(cmy): there is still a short gap between "job finish" and "table become normal",
            // so if user send next alter job right after the "job finish",
            // it may encounter "table's state not NORMAL" error.

            if (alterJob.isDone()) {
                onJobDone(alterJob);
            }
        }
    }

    // remove job from running queue and state map, also set table's state to NORMAL if this is
    // the last running job of the table.
    private void onJobDone(AlterJobV2 alterJob) {
        removeJobFromRunningQueue(alterJob);
        if (removeAlterJobV2FromTableNotFinalStateJobMap(alterJob)) {
            changeTableStatus(alterJob.getDbId(), alterJob.getTableId(), OlapTableState.NORMAL);
            LOG.info("set table's state to NORMAL, table id: {}, job id: {}", alterJob.getTableId(),
                    alterJob.getJobId());
        } else {
            LOG.debug("not set table's state, table id: {}, is job done: {}, job id: {}", alterJob.getTableId(),
                    alterJob.isDone(), alterJob.getJobId());
        }
    }

    @Override
    public List<List<Comparable>> getAlterJobInfosByDb(Database db) {
        List<List<Comparable>> rollupJobInfos = new LinkedList<List<Comparable>>();

        getAlterJobV2Infos(db, rollupJobInfos);

        // sort by
        // "JobId", "TableName", "CreateTime", "FinishedTime", "BaseIndexName", "RollupIndexName"
        ListComparator<List<Comparable>> comparator = new ListComparator<List<Comparable>>(0, 1, 2, 3, 4, 5);
        Collections.sort(rollupJobInfos, comparator);

        return rollupJobInfos;
    }

    private void getAlterJobV2Infos(Database db, List<List<Comparable>> rollupJobInfos) {
        ConnectContext ctx = ConnectContext.get();
        for (AlterJobV2 alterJob : alterJobsV2.values()) {
            if (alterJob.getDbId() != db.getId()) {
                continue;
            }
            if (ctx != null) {
                if (!Env.getCurrentEnv().getAuth().checkTblPriv(ctx, db.getFullName(),
                        alterJob.getTableName(), PrivPredicate.ALTER)) {
                    continue;
                }
            }
            alterJob.getInfo(rollupJobInfos);
        }
    }

    @Override
    public void process(List<AlterClause> alterClauses, String clusterName, Database db, OlapTable olapTable)
            throws DdlException, AnalysisException, MetaNotFoundException {
        Optional<AlterClause> alterClauseOptional = alterClauses.stream().findAny();
        if (alterClauseOptional.isPresent()) {
            if (alterClauseOptional.get() instanceof AddRollupClause) {
                processBatchAddRollup(alterClauses, db, olapTable);
            } else if (alterClauseOptional.get() instanceof DropRollupClause) {
                processBatchDropRollup(alterClauses, db, olapTable);
            } else {
                Preconditions.checkState(false);
            }
        }
    }

    @Override
    public void cancel(CancelStmt stmt) throws DdlException {
        CancelAlterTableStmt cancelAlterTableStmt = (CancelAlterTableStmt) stmt;

        String dbName = cancelAlterTableStmt.getDbName();
        String tableName = cancelAlterTableStmt.getTableName();
        Preconditions.checkState(!Strings.isNullOrEmpty(dbName));
        Preconditions.checkState(!Strings.isNullOrEmpty(tableName));

        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(dbName);

        List<AlterJobV2> rollupJobV2List = new ArrayList<>();
        OlapTable olapTable;
        try {
            olapTable = (OlapTable) db.getTableOrMetaException(tableName, Table.TableType.OLAP);
        } catch (MetaNotFoundException e) {
            throw new DdlException(e.getMessage());
        }
        olapTable.writeLock();
        try {
            if (olapTable.getState() != OlapTableState.ROLLUP
                    && olapTable.getState() != OlapTableState.WAITING_STABLE) {
                throw new DdlException("Table[" + tableName + "] is not under ROLLUP. "
                        + "Use 'ALTER TABLE DROP ROLLUP' if you want to.");
            }

            // find from new alter jobs first
            if (cancelAlterTableStmt.getAlterJobIdList() != null) {
                for (Long jobId : cancelAlterTableStmt.getAlterJobIdList()) {
                    AlterJobV2 alterJobV2 = getUnfinishedAlterJobV2ByJobId(jobId);
                    if (alterJobV2 == null) {
                        continue;
                    }
                    rollupJobV2List.add(getUnfinishedAlterJobV2ByJobId(jobId));
                }
            } else {
                rollupJobV2List = getUnfinishedAlterJobV2ByTableId(olapTable.getId());
            }
            if (rollupJobV2List.size() == 0) {
                // Alter job v1 is not supported, delete related code
                throw new DdlException("Table[" + tableName + "] is not under ROLLUP. Maybe it has old alter job");
            }
        } finally {
            olapTable.writeUnlock();
        }

        // alter job v2's cancel must be called outside the table lock
        if (rollupJobV2List.size() != 0) {
            for (AlterJobV2 alterJobV2 : rollupJobV2List) {
                alterJobV2.cancel("user cancelled");
                if (alterJobV2.isDone()) {
                    onJobDone(alterJobV2);
                }
            }
            return;
        }
    }

    // just for ut
    public Map<Long, Set<Long>> getTableRunningJobMap() {
        return tableRunningJobMap;
    }

    public void processCreateMultiTablesMaterializedView(CreateMultiTableMaterializedViewStmt addMVClause)
            throws UserException {
        Map<String, OlapTable> olapTables = addMVClause.getOlapTables();
        try {
            olapTables.values().forEach(Table::writeLock);
            Env.getCurrentEnv().createTable(addMVClause);
        } finally {
            olapTables.values().forEach(Table::writeUnlock);
        }
    }
}
