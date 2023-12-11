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

package org.apache.doris.job.extensions.mtmv;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.task.AbstractTask;
import org.apache.doris.mtmv.MTMVPartitionInfo.MTMVPartitionType;
import org.apache.doris.mtmv.MTMVPlanUtil;
import org.apache.doris.mtmv.MTMVRefreshEnum.RefreshMethod;
import org.apache.doris.mtmv.MTMVRelation;
import org.apache.doris.mtmv.MTMVUtil;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.trees.plans.commands.UpdateMvByPartitionCommand;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TCell;
import org.apache.doris.thrift.TRow;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

public class MTMVTask extends AbstractTask {
    private static final Logger LOG = LogManager.getLogger(MTMVTask.class);
    public static final Long MAX_HISTORY_TASKS_NUM = 100L;

    public static final ImmutableList<Column> SCHEMA = ImmutableList.of(
            new Column("TaskId", ScalarType.createStringType()),
            new Column("JobId", ScalarType.createStringType()),
            new Column("JobName", ScalarType.createStringType()),
            new Column("MvId", ScalarType.createStringType()),
            new Column("MvDatabaseId", ScalarType.createStringType()),
            new Column("Status", ScalarType.createStringType()),
            new Column("ErrorMsg", ScalarType.createStringType()),
            new Column("CreateTime", ScalarType.createStringType()),
            new Column("StartTime", ScalarType.createStringType()),
            new Column("FinishTime", ScalarType.createStringType()),
            new Column("DurationMs", ScalarType.createStringType()),
            new Column("TaskContext", ScalarType.createStringType()),
            new Column("RefreshMode", ScalarType.createStringType()),
            new Column("RefreshPartitions", ScalarType.createStringType()));

    public static final ImmutableMap<String, Integer> COLUMN_TO_INDEX;

    static {
        ImmutableMap.Builder<String, Integer> builder = new ImmutableMap.Builder();
        for (int i = 0; i < SCHEMA.size(); i++) {
            builder.put(SCHEMA.get(i).getName().toLowerCase(), i);
        }
        COLUMN_TO_INDEX = builder.build();
    }

    public enum MTMVTaskTriggerMode {
        MANUAL,
        SYSTEM
    }

    public enum MTMVTaskRefreshMode {
        FULL,
        PARTITION,
        NOT_REFRESH
    }

    @SerializedName(value = "di")
    private long dbId;
    @SerializedName(value = "mi")
    private long mtmvId;
    @SerializedName("taskContext")
    private MTMVTaskContext taskContext;
    @SerializedName("refreshPartitions")
    List<String> refreshPartitions;
    @SerializedName("refreshMode")
    MTMVTaskRefreshMode refreshMode;

    private MTMV mtmv;
    private MTMVRelation relation;
    private StmtExecutor executor;

    public MTMVTask() {
    }

    public MTMVTask(long dbId, long mtmvId, MTMVTaskContext taskContext) {
        this.dbId = Objects.requireNonNull(dbId);
        this.mtmvId = Objects.requireNonNull(mtmvId);
        this.taskContext = Objects.requireNonNull(taskContext);
    }

    @Override
    public void run() throws JobException {
        try {
            ConnectContext ctx = MTMVPlanUtil.createMTMVContext(mtmv);
            TUniqueId queryId = generateQueryId();
            // Every time a task is run, the relation is regenerated because baseTables and baseViews may change,
            // such as deleting a table and creating a view with the same name
            relation = MTMVPlanUtil.generateMTMVRelation(mtmv, ctx);

            Set<Long> refreshPartitionIds = Sets.newHashSet();
            refreshMode = getRefreshMode();
            Map<OlapTable, String> tableWithPartKey = Maps.newHashMap();
            if (refreshMode == MTMVTaskRefreshMode.NOT_REFRESH) {
                return;
            } else if (refreshMode == MTMVTaskRefreshMode.PARTITION) {
                OlapTable relatedTable = (OlapTable) MTMVUtil.getTable(mtmv.getMvPartitionInfo().getRelatedTable());
                relatedTable.writeLock();
                if (CollectionUtils.isEmpty(taskContext.getPartitions())) {
                    refreshPartitionIds = MTMVUtil.getMTMVStalePartitions(mtmv, relatedTable);
                } else {
                    refreshPartitionIds = MTMVUtil.getPartitionsIdsByNames(mtmv, taskContext.getPartitions());
                }
                tableWithPartKey.put(relatedTable, mtmv.getMvPartitionInfo().getRelatedCol());
            }
            refreshPartitions = MTMVUtil.getPartitionNamesByIds(mtmv, refreshPartitionIds);
            UpdateMvByPartitionCommand command = UpdateMvByPartitionCommand
                    .from(mtmv, refreshPartitionIds, tableWithPartKey);
            executor = new StmtExecutor(ctx, new LogicalPlanAdapter(command, ctx.getStatementContext()));
            ctx.setQueryId(queryId);
            command.run(ctx, executor);
        } catch (Throwable e) {
            e.printStackTrace();
            throw new JobException(e);
        }
    }

    @Override
    public synchronized void onFail() throws JobException {
        super.onFail();
        after();
    }

    @Override
    public synchronized void onSuccess() throws JobException {
        super.onSuccess();
        after();
    }

    @Override
    public synchronized void cancel() throws JobException {
        super.cancel();
        if (executor != null) {
            executor.cancel();
        }
        after();
    }

    @Override
    public void before() throws JobException {
        super.before();
        try {
            Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(dbId);
            mtmv = (MTMV) db.getTableOrMetaException(mtmvId, TableType.MATERIALIZED_VIEW);
            if (mtmv.getMvPartitionInfo().getPartitionType() == MTMVPartitionType.FOLLOW_BASE_TABLE) {
                OlapTable relatedTable = (OlapTable) MTMVUtil.getTable(mtmv.getMvPartitionInfo().getRelatedTable());
                MTMVUtil.alignMvPartition(mtmv, relatedTable);
            }
        } catch (UserException e) {
            LOG.warn(e);
            throw new JobException(e);
        }
    }

    @Override
    public TRow getTvfInfo() {
        TRow trow = new TRow();
        trow.addToColumnValue(new TCell().setStringVal(String.valueOf(super.getTaskId())));
        trow.addToColumnValue(new TCell().setStringVal(String.valueOf(super.getJobId())));
        trow.addToColumnValue(new TCell().setStringVal(super.getJobName()));
        trow.addToColumnValue(new TCell().setStringVal(String.valueOf(mtmvId)));
        trow.addToColumnValue(new TCell().setStringVal(String.valueOf(dbId)));
        trow.addToColumnValue(new TCell().setStringVal(super.getStatus() == null ? "-" : super.getStatus().toString()));
        trow.addToColumnValue(
                new TCell().setStringVal(StringUtils.isEmpty(super.getErrMsg()) ? "-" : super.getErrMsg()));
        trow.addToColumnValue(new TCell().setStringVal(getTimeString(super.getCreateTimeMs())));
        trow.addToColumnValue(new TCell().setStringVal(getTimeString(super.getStartTimeMs())));
        trow.addToColumnValue(new TCell().setStringVal(getTimeString(super.getFinishTimeMs())));
        trow.addToColumnValue(
                new TCell().setStringVal(String.valueOf(
                        super.getFinishTimeMs() == null ? "-" : super.getFinishTimeMs() - super.getStartTimeMs())));
        trow.addToColumnValue(new TCell().setStringVal(taskContext == null ? "-" : new Gson().toJson(taskContext)));
        trow.addToColumnValue(new TCell().setStringVal(refreshMode == null ? "-" : refreshMode.toString()));
        trow.addToColumnValue(
                new TCell().setStringVal(refreshPartitions == null ? "-" : new Gson().toJson(refreshPartitions)));
        return trow;
    }

    private String getTimeString(Long ms) {
        if (ms != null && ms != 0) {
            return TimeUtils.longToTimeString(ms);
        }
        return "-";
    }

    private TUniqueId generateQueryId() {
        UUID taskId = UUID.randomUUID();
        return new TUniqueId(taskId.getMostSignificantBits(), taskId.getLeastSignificantBits());
    }

    private void after() {
        Env.getCurrentEnv()
                .addMTMVTaskResult(new TableNameInfo(mtmv.getQualifiedDbName(), mtmv.getName()), this, relation);
        mtmv = null;
        relation = null;
        executor = null;
    }

    private MTMVTaskRefreshMode getRefreshMode() throws AnalysisException {
        // check whether the user manually triggers it
        if (taskContext.getTriggerMode() == MTMVTaskTriggerMode.MANUAL && !CollectionUtils
                .isEmpty(taskContext.getPartitions())) {
            return MTMVTaskRefreshMode.PARTITION;
        }
        // check if data is fresh
        Set<String> excludedTriggerTables = mtmv.getExcludedTriggerTables();
        boolean fresh = MTMVUtil.isMTMVSync(mtmv, relation.getBaseTables(), excludedTriggerTables);
        if (fresh) {
            return MTMVTaskRefreshMode.NOT_REFRESH;
        }
        // current, if partitionType is SELF_MANAGE, we can only FULL refresh
        if (mtmv.getMvPartitionInfo().getPartitionType() == MTMVPartitionType.SELF_MANAGE) {
            return MTMVTaskRefreshMode.FULL;
        }
        // if refreshMethod is COMPLETE, we only FULL refresh
        if (mtmv.getRefreshInfo().getRefreshMethod() == RefreshMethod.COMPLETE) {
            return MTMVTaskRefreshMode.FULL;
        }
        OlapTable relatedTable = (OlapTable) MTMVUtil.getTable(mtmv.getMvPartitionInfo().getRelatedTable());
        excludedTriggerTables.add(relatedTable.getName());
        // check if every table except relatedTable is fresh
        fresh = MTMVUtil.isMTMVSync(mtmv, relation.getBaseTables(), excludedTriggerTables);
        // if true, we can use `Partition`, otherwise must `FULL`
        if (fresh) {
            return MTMVTaskRefreshMode.PARTITION;
        } else {
            return MTMVTaskRefreshMode.FULL;
        }
    }

    public MTMVTaskContext getTaskContext() {
        return taskContext;
    }
}
