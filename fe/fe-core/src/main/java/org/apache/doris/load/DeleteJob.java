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

package org.apache.doris.load;

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.IsNullPredicate;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.Predicate;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.MarkedCountDownLatch;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.planner.ColumnBound;
import org.apache.doris.planner.ColumnRange;
import org.apache.doris.planner.ListPartitionPrunerV2;
import org.apache.doris.planner.PartitionPruner;
import org.apache.doris.planner.RangePartitionPrunerV2;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.task.PushTask;
import org.apache.doris.thrift.TColumn;
import org.apache.doris.thrift.TPriority;
import org.apache.doris.thrift.TPushType;
import org.apache.doris.thrift.TTaskType;
import org.apache.doris.transaction.AbstractTxnStateChangeCallback;
import org.apache.doris.transaction.GlobalTransactionMgr;
import org.apache.doris.transaction.TabletCommitInfo;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TransactionStatus;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class DeleteJob extends AbstractTxnStateChangeCallback implements DeleteJobLifeCycle {
    private static final Logger LOG = LogManager.getLogger(DeleteJob.class);

    public static final String DELETE_PREFIX = "delete_";

    public enum DeleteState {
        UN_QUORUM,
        QUORUM_FINISHED,
        FINISHED
    }

    private DeleteState state;

    // jobId(listenerId). use in beginTransaction to callback function
    private final long id;
    // transaction id.
    private long signature;
    private final String label;
    private final Set<Long> totalTablets;
    private final Set<Long> quorumTablets;
    private final Set<Long> finishedTablets;
    Map<Long, TabletDeleteInfo> tabletDeleteInfoMap;
    private final Set<PushTask> pushTasks;
    private final DeleteInfo deleteInfo;

    private final  Map<Long, Short> partitionReplicaNum;

    private Database targetDb;

    private OlapTable targetTbl;

    private List<Partition> partitions;

    private List<Predicate> deleteConditions;

    private MarkedCountDownLatch<Long, Long> countDownLatch;

    public DeleteJob(long id, long transactionId, String label,
                     Map<Long, Short> partitionReplicaNum, DeleteInfo deleteInfo) {
        this.id = id;
        this.signature = transactionId;
        this.label = label;
        this.deleteInfo = deleteInfo;
        totalTablets = Sets.newHashSet();
        finishedTablets = Sets.newHashSet();
        quorumTablets = Sets.newHashSet();
        tabletDeleteInfoMap = Maps.newConcurrentMap();
        pushTasks = Sets.newHashSet();
        state = DeleteState.UN_QUORUM;
        this.partitionReplicaNum = partitionReplicaNum;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * check and update if this job's state is QUORUM_FINISHED or FINISHED
     * The meaning of state:
     * QUORUM_FINISHED: For each tablet there are more than half of its replicas have been finished
     * FINISHED: All replicas of this job have finished
     */
    private void checkAndUpdateQuorum() throws MetaNotFoundException {
        long dbId = deleteInfo.getDbId();
        Env.getCurrentInternalCatalog().getDbOrMetaException(dbId);

        for (TabletDeleteInfo tDeleteInfo : getTabletDeleteInfo()) {
            Short replicaNum = partitionReplicaNum.get(tDeleteInfo.getPartitionId());
            if (replicaNum == null) {
                // should not happen
                throw new MetaNotFoundException("Unknown partition "
                        + tDeleteInfo.getPartitionId() + " when commit delete job");
            }
            if (tDeleteInfo.getFinishedReplicas().size() == replicaNum) {
                finishedTablets.add(tDeleteInfo.getTabletId());
            }
            if (tDeleteInfo.getFinishedReplicas().size() >= replicaNum / 2 + 1) {
                quorumTablets.add(tDeleteInfo.getTabletId());
            }
        }

        int dropCounter = 0;
        TabletInvertedIndex invertedIndex = Env.getCurrentInvertedIndex();
        for (long tabletId : totalTablets) {
            if (invertedIndex.getTabletMeta(tabletId) == null) {
                // tablet does not exist.
                // This may happen during the delete operation, and the schema change task ends,
                // causing the old tablet to be deleted.
                // We think this situation is normal. In order to ensure that the delete task can end normally
                // here we regard these deleted tablets as completed.
                finishedTablets.add(tabletId);
                dropCounter++;
                LOG.warn("tablet {} has been dropped when checking delete job {}", tabletId, id);
            }
        }

        LOG.info("check delete job quorum, transaction id: {}, total tablets: {},"
                        + " quorum tablets: {}, dropped tablets: {}",
                signature, totalTablets.size(), quorumTablets.size(), dropCounter);

        if (finishedTablets.containsAll(totalTablets)) {
            this.state = DeleteState.FINISHED;
        } else if (quorumTablets.containsAll(totalTablets)) {
            this.state = DeleteState.QUORUM_FINISHED;
        }
    }

    public DeleteState getState() {
        return this.state;
    }

    private void addTablet(long tabletId) {
        totalTablets.add(tabletId);
    }

    public void addPushTask(PushTask pushTask) {
        pushTasks.add(pushTask);
    }

    public void addFinishedReplica(long partitionId, long tabletId, Replica replica) {
        tabletDeleteInfoMap.putIfAbsent(tabletId, new TabletDeleteInfo(partitionId, tabletId));
        TabletDeleteInfo tDeleteInfo = tabletDeleteInfoMap.get(tabletId);
        tDeleteInfo.addFinishedReplica(replica);
    }

    public DeleteInfo getDeleteInfo() {
        return deleteInfo;
    }

    public String getLabel() {
        return this.label;
    }

    @Override
    public long getId() {
        return this.id;
    }

    @Override
    public void afterVisible(TransactionState txnState, boolean txnOperated) {
        if (!txnOperated) {
            return;
        }
        executeFinish();
        Env.getCurrentEnv().getEditLog().logFinishDelete(deleteInfo);
    }

    @Override
    public void afterAborted(TransactionState txnState, boolean txnOperated, String txnStatusChangeReason) {
        // just to clean the callback
        Env.getCurrentGlobalTransactionMgr().getCallbackFactory().removeCallback(getId());
    }

    public void executeFinish() {
        this.state = DeleteState.FINISHED;
        Env.getCurrentEnv().getDeleteHandler().recordFinishedJob(this);
        Env.getCurrentGlobalTransactionMgr().getCallbackFactory().removeCallback(getId());
    }

    public long getTransactionId() {
        return this.signature;
    }

    public Collection<TabletDeleteInfo> getTabletDeleteInfo() {
        return tabletDeleteInfoMap.values();
    }

    public long getTimeoutMs() {
        if (FeConstants.runningUnitTest) {
            // for making unit test run fast
            return 1000;
        }
        // timeout is between 30 seconds to 5 min
        long timeout = Math.max(totalTablets.size() * Config.tablet_delete_timeout_second * 1000L, 30000L);
        return Math.min(timeout, Config.delete_job_max_timeout_second * 1000L);
    }

    public void setTargetDb(Database targetDb) {
        this.targetDb = targetDb;
    }

    public void setTargetTbl(OlapTable targetTbl) {
        this.targetTbl = targetTbl;
    }

    public void setPartitions(List<Partition> partitions) {
        this.partitions = partitions;
    }

    public void setDeleteConditions(List<Predicate> deleteConditions) {
        this.deleteConditions = deleteConditions;
    }

    public void setCountDownLatch(MarkedCountDownLatch<Long, Long> countDownLatch) {
        this.countDownLatch = countDownLatch;
    }

    @Override
    public long beginTxn() throws Exception {
        long txnId = Env.getCurrentGlobalTransactionMgr().beginTransaction(deleteInfo.getDbId(),
                Lists.newArrayList(deleteInfo.getTableId()), label, null,
                new TransactionState.TxnCoordinator(
                        TransactionState.TxnSourceType.FE, FrontendOptions.getLocalHostAddress()),
                TransactionState.LoadJobSourceType.FRONTEND, id, Config.stream_load_default_timeout_second);
        this.signature = txnId;
        Env.getCurrentGlobalTransactionMgr().getCallbackFactory().addCallback(this);
        return txnId;
    }

    @Override
    public void dispatch() throws Exception {
        // task sent to be
        AgentBatchTask batchTask = new AgentBatchTask();
        for (Partition partition : partitions) {
            for (MaterializedIndex index : partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                long indexId = index.getId();
                int schemaHash = targetTbl.getSchemaHashByIndexId(indexId);

                List<TColumn> columnsDesc = Lists.newArrayList();
                for (Column column : targetTbl.getSchemaByIndexId(indexId)) {
                    columnsDesc.add(column.toThrift());
                }

                for (Tablet tablet : index.getTablets()) {
                    long tabletId = tablet.getId();

                    // set push type
                    TPushType type = TPushType.DELETE;

                    for (Replica replica : tablet.getReplicas()) {
                        long replicaId = replica.getId();
                        long backendId = replica.getBackendId();
                        countDownLatch.addMark(backendId, tabletId);

                        // create push task for each replica
                        PushTask pushTask = new PushTask(null,
                                replica.getBackendId(), targetDb.getId(), targetTbl.getId(),
                                partition.getId(), indexId,
                                tabletId, replicaId, schemaHash,
                                -1, "", -1, 0,
                                -1, type, deleteConditions,
                                true, TPriority.NORMAL,
                                TTaskType.REALTIME_PUSH,
                                signature,
                                Env.getCurrentGlobalTransactionMgr()
                                        .getTransactionIDGenerator().getNextTransactionId(),
                                columnsDesc);
                        pushTask.setIsSchemaChanging(false);
                        pushTask.setCountDownLatch(countDownLatch);

                        if (AgentTaskQueue.addTask(pushTask)) {
                            batchTask.addTask(pushTask);
                            addPushTask(pushTask);
                            addTablet(tabletId);
                        }
                    }
                }
            }
        }

        // submit push tasks
        if (batchTask.getTaskNum() > 0) {
            AgentTaskExecutor.submit(batchTask);
        }
    }

    @Override
    public void await() throws Exception {
        long timeoutMs = getTimeoutMs();
        boolean ok = countDownLatch.await(timeoutMs, TimeUnit.MILLISECONDS);
        if (ok) {
            return;
        }

        //handle failure
        String errMsg = "";
        List<Map.Entry<Long, Long>> unfinishedMarks = countDownLatch.getLeftMarks();
        // only show at most 5 results
        List<Map.Entry<Long, Long>> subList = unfinishedMarks.subList(0, Math.min(unfinishedMarks.size(), 5));
        if (!subList.isEmpty()) {
            errMsg = "unfinished replicas [BackendId=TabletId]: " + Joiner.on(", ").join(subList);
        }
        LOG.warn(errMsg);
        checkAndUpdateQuorum();
        switch (state) {
            case UN_QUORUM:
                LOG.warn("delete job timeout: transactionId {}, timeout {}, {}",
                        signature, timeoutMs, errMsg);
                throw new UserException(String.format("delete job timeout, timeout(ms):%s, msg:%s", timeoutMs, errMsg));
            case QUORUM_FINISHED:
            case FINISHED:
                long nowQuorumTimeMs = System.currentTimeMillis();
                long endQuorumTimeoutMs = nowQuorumTimeMs + timeoutMs / 2;
                // if job's state is quorum_finished then wait for a period of time and commit it.
                while (state == DeleteState.QUORUM_FINISHED
                        && endQuorumTimeoutMs > nowQuorumTimeMs) {
                    checkAndUpdateQuorum();
                    Thread.sleep(1000);
                    nowQuorumTimeMs = System.currentTimeMillis();
                    LOG.debug("wait for quorum finished delete job: {}, txn id: {}",
                            id, signature);
                }
                break;
            default:
                throw new IllegalStateException("wrong delete job state: " + state.name());
        }
    }

    @Override
    public String commit() throws Exception {
        TabletInvertedIndex currentInvertedIndex = Env.getCurrentInvertedIndex();
        List<TabletCommitInfo> tabletCommitInfos = Lists.newArrayList();
        tabletDeleteInfoMap.forEach((tabletId, deleteInfo) -> deleteInfo.getFinishedReplicas()
                .forEach(replica -> {
                    if (currentInvertedIndex.getTabletIdByReplica(replica.getId()) == null) {
                        LOG.warn("could not find tablet id for replica {}, the tablet maybe dropped", replica);
                        return;
                    }
                    tabletCommitInfos.add(new TabletCommitInfo(tabletId, replica.getBackendId()));
                }));
        boolean visible = Env.getCurrentGlobalTransactionMgr()
                .commitAndPublishTransaction(targetDb, Lists.newArrayList(targetTbl),
                        signature, tabletCommitInfos, getTimeoutMs());

        StringBuilder sb = new StringBuilder();
        sb.append("{'label':'").append(label);
        sb.append("', 'txnId':'").append(signature)
                .append("', 'status':'");
        if (visible) {
            sb.append(TransactionStatus.VISIBLE.name()).append("'");
            sb.append("}");
        } else {
            // Although publish is unfinished we should tell user that commit already success.
            sb.append(TransactionStatus.COMMITTED.name()).append("'");
            String msg = "delete job is committed but may be taking effect later";
            sb.append(", 'msg':'").append(msg).append("'");
            sb.append("}");
        }
        return sb.toString();
    }

    @Override
    public void cancel(String reason) {
        GlobalTransactionMgr globalTransactionMgr = Env.getCurrentGlobalTransactionMgr();
        try {
            globalTransactionMgr.abortTransaction(deleteInfo.getDbId(), signature, reason);
        } catch (Exception e) {
            TransactionState state = globalTransactionMgr.getTransactionState(
                    deleteInfo.getDbId(), signature);
            if (state == null) {
                LOG.warn("cancel delete job failed because txn not found, transactionId: {}",
                        signature);
            } else if (state.getTransactionStatus() == TransactionStatus.COMMITTED
                    || state.getTransactionStatus() == TransactionStatus.VISIBLE) {
                LOG.warn("cancel delete job failed because it has been committed, transactionId: {}",
                        signature);
            } else {
                LOG.warn("errors while abort transaction", e);
            }
        }
    }

    @Override
    public void cleanUp() {
        for (PushTask pushTask : pushTasks) {
            AgentTaskQueue.removePushTask(pushTask.getBackendId(), pushTask.getSignature(),
                    pushTask.getVersion(),
                    pushTask.getPushType(), pushTask.getTaskType());
        }
    }

    public static class BuildParams {

        private final Database db;
        private final OlapTable table;

        private final Collection<String> partitionNames;

        private final List<Predicate> deleteConditions;

        public BuildParams(Database db, OlapTable table,
                           Collection<String> partitionNames,
                           List<Predicate> deleteConditions) {
            this.db = db;
            this.table = table;
            this.partitionNames = partitionNames;
            this.deleteConditions = deleteConditions;
        }

        public OlapTable getTable() {
            return table;
        }

        public Collection<String> getPartitionNames() {
            return partitionNames;
        }

        public Database getDb() {
            return db;
        }

        public List<Predicate> getDeleteConditions() {
            return deleteConditions;
        }
    }

    public static class Builder {

        public DeleteJob buildWith(BuildParams params) throws Exception {
            List<Partition> partitions = getSelectedPartitions(params.getTable(),
                    params.getPartitionNames(), params.getDeleteConditions());
            Map<Long, Short> partitionReplicaNum = partitions.stream()
                    .collect(Collectors.toMap(
                            Partition::getId,
                            partition ->
                                    params.getTable()
                                            .getPartitionInfo()
                                            .getReplicaAllocation(partition.getId())
                                            .getTotalReplicaNum()));
            // generate label
            String label = DELETE_PREFIX + UUID.randomUUID();
            //generate jobId
            long jobId = Env.getCurrentEnv().getNextId();
            DeleteInfo deleteInfo = new DeleteInfo(params.getDb().getId(), params.getTable().getId(),
                    params.getTable().getName(), getDeleteCondString(params.getDeleteConditions()));
            DeleteJob deleteJob = new DeleteJob(jobId, -1, label, partitionReplicaNum, deleteInfo);
            long replicaNum = partitions.stream().mapToLong(Partition::getAllReplicaCount).sum();
            deleteJob.setPartitions(partitions);
            deleteJob.setDeleteConditions(params.getDeleteConditions());
            deleteJob.setTargetDb(params.getDb());
            deleteJob.setTargetTbl(params.getTable());
            deleteJob.setCountDownLatch(new MarkedCountDownLatch<>((int) replicaNum));
            return deleteJob;
        }

        private List<Partition> getSelectedPartitions(OlapTable olapTable, Collection<String> partitionNames,
                                                      List<Predicate> deleteConditions) throws Exception {
            if (partitionNames.isEmpty()) {
                // Try to get selected partitions if no partition specified in delete statement
                // Use PartitionPruner to generate the select partitions
                if (olapTable.getPartitionInfo().getType() == PartitionType.RANGE
                        || olapTable.getPartitionInfo().getType() == PartitionType.LIST) {
                    Set<String> partitionColumnNameSet = olapTable.getPartitionColumnNames();
                    Map<String, ColumnRange> columnNameToRange = Maps.newHashMap();
                    for (String colName : partitionColumnNameSet) {
                        ColumnRange columnRange = createColumnRange(olapTable, colName, deleteConditions);
                        // Not all partition columns are involved in predicate conditions
                        if (columnRange != null) {
                            columnNameToRange.put(colName, columnRange);
                        }
                    }

                    Collection<Long> selectedPartitionId = null;
                    if (!columnNameToRange.isEmpty()) {
                        PartitionInfo partitionInfo = olapTable.getPartitionInfo();
                        Map<Long, PartitionItem> keyItemMap = partitionInfo.getIdToItem(false);
                        PartitionPruner pruner = olapTable.getPartitionInfo().getType() == PartitionType.RANGE
                                ? new RangePartitionPrunerV2(keyItemMap, partitionInfo.getPartitionColumns(),
                                columnNameToRange)
                                : new ListPartitionPrunerV2(keyItemMap, partitionInfo.getPartitionColumns(),
                                columnNameToRange);
                        selectedPartitionId = pruner.prune();
                    }
                    // selectedPartitionId is empty means no partition matches conditions.
                    // How to return empty set in such case?
                    if (selectedPartitionId != null && !selectedPartitionId.isEmpty()) {
                        for (long partitionId : selectedPartitionId) {
                            partitionNames.add(olapTable.getPartition(partitionId).getName());
                        }
                    } else {
                        if (!ConnectContext.get().getSessionVariable().isDeleteWithoutPartition()) {
                            throw new UserException("This is a range or list partitioned table."
                                    + " You should specify partition in delete stmt,"
                                    + " or set delete_without_partition to true");
                        } else {
                            partitionNames.addAll(olapTable.getPartitionNames());
                        }
                    }
                } else if (olapTable.getPartitionInfo().getType() == PartitionType.UNPARTITIONED) {
                    // this is an un-partitioned table, use table name as partition name
                    partitionNames.add(olapTable.getName());
                } else {
                    throw new UserException("Unknown partition type: " + olapTable.getPartitionInfo().getType());
                }
            }
            List<Partition> partitions = Lists.newArrayList();
            for (String partName : partitionNames) {
                Partition partition = olapTable.getPartition(partName);
                if (partition == null) {
                    throw new DdlException("Partition does not exist. name: " + partName);
                }
                partitions.add(partition);
            }
            return partitions;
        }

        // Return null if there is no filter for the partition column
        private ColumnRange createColumnRange(OlapTable table, String colName, List<Predicate> conditions)
                throws AnalysisException {

            ColumnRange result = ColumnRange.create();
            Type type =
                    table.getBaseSchema().stream().filter(c -> c.getName().equalsIgnoreCase(colName))
                            .findFirst().get().getType();

            boolean hasRange = false;
            for (Predicate predicate : conditions) {
                List<Range<ColumnBound>> bounds = createColumnRange(colName, predicate, type);
                if (bounds != null) {
                    hasRange = true;
                    result.intersect(bounds);
                }
            }
            if (hasRange) {
                return result;
            } else {
                return null;
            }
        }

        // Return null if the condition is not related to the partition column,
        // or the operator is not supported.
        private List<Range<ColumnBound>> createColumnRange(String colName, Predicate condition, Type type)
                throws AnalysisException {
            List<Range<ColumnBound>> result = Lists.newLinkedList();
            if (condition instanceof BinaryPredicate) {
                BinaryPredicate binaryPredicate = (BinaryPredicate) condition;
                if (!(binaryPredicate.getChild(0) instanceof SlotRef)) {
                    return null;
                }
                String columnName = ((SlotRef) binaryPredicate.getChild(0)).getColumnName();
                if (!colName.equalsIgnoreCase(columnName)) {
                    return null;
                }
                ColumnBound bound = ColumnBound.of(
                        LiteralExpr.create(binaryPredicate.getChild(1).getStringValue(), type));
                switch (binaryPredicate.getOp()) {
                    case EQ:
                        result.add(Range.closed(bound, bound));
                        break;
                    case GE:
                        result.add(Range.atLeast(bound));
                        break;
                    case GT:
                        result.add(Range.greaterThan(bound));
                        break;
                    case LT:
                        result.add(Range.lessThan(bound));
                        break;
                    case LE:
                        result.add(Range.atMost(bound));
                        break;
                    case NE:
                        result.add(Range.lessThan(bound));
                        result.add(Range.greaterThan(bound));
                        break;
                    default:
                        return null;
                }
            } else if (condition instanceof InPredicate) {
                InPredicate inPredicate = (InPredicate) condition;
                if (!(inPredicate.getChild(0) instanceof SlotRef)) {
                    return null;
                }
                String columnName = ((SlotRef) inPredicate.getChild(0)).getColumnName();
                if (!colName.equals(columnName)) {
                    return null;
                }
                if (inPredicate.isNotIn()) {
                    return null;
                }
                for (int i = 1; i <= inPredicate.getInElementNum(); i++) {
                    ColumnBound bound = ColumnBound.of(LiteralExpr
                            .create(inPredicate.getChild(i).getStringValue(), type));
                    result.add(Range.closed(bound, bound));
                }
            } else {
                return null;
            }
            return result;
        }

        private List<String> getDeleteCondString(List<Predicate> conditions) {
            List<String> deleteConditions = Lists.newArrayListWithCapacity(conditions.size());
            // save delete conditions
            for (Predicate condition : conditions) {
                if (condition instanceof BinaryPredicate) {
                    BinaryPredicate binaryPredicate = (BinaryPredicate) condition;
                    SlotRef slotRef = (SlotRef) binaryPredicate.getChild(0);
                    String columnName = slotRef.getColumnName();
                    String sb = columnName + " " + binaryPredicate.getOp().name() + " \""
                            + binaryPredicate.getChild(1).getStringValue() + "\"";
                    deleteConditions.add(sb);
                } else if (condition instanceof IsNullPredicate) {
                    IsNullPredicate isNullPredicate = (IsNullPredicate) condition;
                    SlotRef slotRef = (SlotRef) isNullPredicate.getChild(0);
                    String columnName = slotRef.getColumnName();
                    StringBuilder sb = new StringBuilder();
                    sb.append(columnName);
                    if (isNullPredicate.isNotNull()) {
                        sb.append(" IS NOT NULL");
                    } else {
                        sb.append(" IS NULL");
                    }
                    deleteConditions.add(sb.toString());
                } else if (condition instanceof InPredicate) {
                    InPredicate inPredicate = (InPredicate) condition;
                    SlotRef slotRef = (SlotRef) inPredicate.getChild(0);
                    String columnName = slotRef.getColumnName();
                    StringBuilder strBuilder = new StringBuilder();
                    String notStr = inPredicate.isNotIn() ? "NOT " : "";
                    strBuilder.append(columnName).append(" ").append(notStr).append("IN (");
                    for (int i = 1; i <= inPredicate.getInElementNum(); ++i) {
                        strBuilder.append(inPredicate.getChild(i).toSql());
                        strBuilder.append((i != inPredicate.getInElementNum()) ? ", " : "");
                    }
                    strBuilder.append(")");
                    deleteConditions.add(strBuilder.toString());
                }
            }
            return deleteConditions;
        }
    }
}
