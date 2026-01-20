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

package org.apache.doris.load.routineload;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.nereids.load.NereidsLoadTaskInfo;
import org.apache.doris.nereids.load.NereidsStreamLoadPlanner;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TKinesisLoadInfo;
import org.apache.doris.thrift.TLoadSourceType;
import org.apache.doris.thrift.TPipelineFragmentParams;
import org.apache.doris.thrift.TPipelineWorkloadGroup;
import org.apache.doris.thrift.TPlanFragment;
import org.apache.doris.thrift.TRoutineLoadTask;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * Task info for Kinesis Routine Load.
 * 
 * Each task is responsible for consuming data from one or more Kinesis shards.
 * The task tracks the sequence number for each shard and reports progress back
 * to the FE after successful consumption.
 */
public class KinesisTaskInfo extends RoutineLoadTaskInfo {
    private static final Logger LOG = LogManager.getLogger(KinesisTaskInfo.class);

    private RoutineLoadManager routineLoadManager = Env.getCurrentEnv().getRoutineLoadManager();

    /**
     * Map from shard ID to starting sequence number for this task.
     */
    private Map<String, String> shardIdToSequenceNumber = Maps.newHashMap();

    /**
     * Create a new KinesisTaskInfo.
     *
     * @param id Task ID
     * @param jobId Job ID
     * @param timeoutMs Timeout in milliseconds
     * @param shardIdToSequenceNumber Initial shard positions
     * @param isMultiTable Whether this is a multi-table job
     */
    public KinesisTaskInfo(UUID id, long jobId, long timeoutMs, 
                           Map<String, String> shardIdToSequenceNumber,
                           boolean isMultiTable, long taskSubmitTimeMs, boolean isEof) {
        super(id, jobId, timeoutMs, isMultiTable, taskSubmitTimeMs, isEof);
        this.shardIdToSequenceNumber.putAll(shardIdToSequenceNumber);
    }

    /**
     * Create a new task from an old task with updated positions.
     */
    public KinesisTaskInfo(KinesisTaskInfo oldTask, ConcurrentMap<String, String> shardIdToSequenceNumber,
                           boolean isMultiTable) {
        super(UUID.randomUUID(), oldTask.getJobId(), oldTask.getTimeoutMs(), 
              oldTask.getBeId(), isMultiTable, oldTask.getLastScheduledTime(), oldTask.getIsEof());
        this.shardIdToSequenceNumber.putAll(shardIdToSequenceNumber);
    }

    /**
     * Get the list of shard IDs this task is responsible for.
     */
    public List<String> getShards() {
        return new ArrayList<>(shardIdToSequenceNumber.keySet());
    }

    /**
     * Get the shard to sequence number mapping.
     */
    public Map<String, String> getShardIdToSequenceNumber() {
        return shardIdToSequenceNumber;
    }

    @Override
    public TRoutineLoadTask createRoutineLoadTask() throws UserException {
        KinesisRoutineLoadJob routineLoadJob = 
                (KinesisRoutineLoadJob) routineLoadManager.getJob(jobId);

        // Create Thrift task object
        TRoutineLoadTask tRoutineLoadTask = new TRoutineLoadTask();
        TUniqueId queryId = new TUniqueId(id.getMostSignificantBits(), id.getLeastSignificantBits());
        tRoutineLoadTask.setId(queryId);
        tRoutineLoadTask.setJobId(jobId);
        tRoutineLoadTask.setTxnId(txnId);
        
        Database database = Env.getCurrentInternalCatalog().getDbOrMetaException(routineLoadJob.getDbId());
        tRoutineLoadTask.setDb(database.getFullName());
        
        // label = job_name+job_id+task_id+txn_id
        String label = Joiner.on("-").join(routineLoadJob.getName(),
                routineLoadJob.getId(), DebugUtil.printId(id), txnId);
        tRoutineLoadTask.setLabel(label);
        tRoutineLoadTask.setAuthCode(routineLoadJob.getAuthCode());
        
        // Set Kinesis-specific load info
        TKinesisLoadInfo tKinesisLoadInfo = new TKinesisLoadInfo();
        tKinesisLoadInfo.setRegion(routineLoadJob.getRegion());
        tKinesisLoadInfo.setStream(routineLoadJob.getStream());
        if (routineLoadJob.getEndpoint() != null) {
            tKinesisLoadInfo.setEndpoint(routineLoadJob.getEndpoint());
        }
        tKinesisLoadInfo.setShardBeginSequenceNumber(shardIdToSequenceNumber);
        tKinesisLoadInfo.setProperties(routineLoadJob.getConvertedCustomProperties());
        
        tRoutineLoadTask.setKinesisLoadInfo(tKinesisLoadInfo);
        tRoutineLoadTask.setType(TLoadSourceType.KINESIS);
        tRoutineLoadTask.setIsMultiTable(isMultiTable);

        // Set batch parameters
        adaptiveBatchParam(tRoutineLoadTask, routineLoadJob);

        if (!isMultiTable) {
            Table tbl = database.getTableOrMetaException(routineLoadJob.getTableId());
            tRoutineLoadTask.setTbl(tbl.getName());
            tRoutineLoadTask.setPipelineParams(rePlan(routineLoadJob));
        } else {
            Env.getCurrentEnv().getRoutineLoadManager().addMultiLoadTaskTxnIdToRoutineLoadJobId(txnId, jobId);
        }

        // Set format
        if (!routineLoadJob.getFormat().isEmpty() && routineLoadJob.getFormat().equalsIgnoreCase("json")) {
            tRoutineLoadTask.setFormat(TFileFormatType.FORMAT_JSON);
        } else {
            tRoutineLoadTask.setFormat(TFileFormatType.FORMAT_CSV_PLAIN);
        }

        tRoutineLoadTask.setMemtableOnSinkNode(routineLoadJob.isMemtableOnSinkNode());
        tRoutineLoadTask.setQualifiedUser(routineLoadJob.getUserIdentity().getQualifiedUser());
        tRoutineLoadTask.setCloudCluster(routineLoadJob.getCloudCluster());

        return tRoutineLoadTask;
    }

    /**
     * Set adaptive batch parameters based on the routine load job configuration.
     */
    private void adaptiveBatchParam(TRoutineLoadTask tRoutineLoadTask, RoutineLoadJob routineLoadJob) {
        long maxBatchIntervalS = routineLoadJob.getMaxBatchIntervalS();
        long maxBatchRows = routineLoadJob.getMaxBatchRows();
        long maxBatchSize = routineLoadJob.getMaxBatchSizeBytes();
        if (!isEof) {
            maxBatchIntervalS = Math.max(maxBatchIntervalS, Config.routine_load_adaptive_min_batch_interval_sec);
            maxBatchRows = Math.max(maxBatchRows, RoutineLoadJob.DEFAULT_MAX_BATCH_ROWS);
            maxBatchSize = Math.max(maxBatchSize, RoutineLoadJob.DEFAULT_MAX_BATCH_SIZE);
            this.timeoutMs = maxBatchIntervalS * Config.routine_load_task_timeout_multiplier * 1000;
        } else {
            this.timeoutMs = routineLoadJob.getTimeout() * 1000;
        }
        tRoutineLoadTask.setMaxIntervalS(maxBatchIntervalS);
        tRoutineLoadTask.setMaxBatchRows(maxBatchRows);
        tRoutineLoadTask.setMaxBatchSize(maxBatchSize);
    }

    private TPipelineFragmentParams rePlan(RoutineLoadJob routineLoadJob) throws UserException {
        TUniqueId loadId = new TUniqueId(id.getMostSignificantBits(), id.getLeastSignificantBits());
        // plan for each task, in case table has change(rollup or schema change)
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException(routineLoadJob.getDbId());
        NereidsLoadTaskInfo taskInfo = routineLoadJob.toNereidsRoutineLoadTaskInfo();
        taskInfo.setTimeout((int) (this.timeoutMs / 1000));
        NereidsStreamLoadPlanner planner = new NereidsStreamLoadPlanner(db,
                (OlapTable) db.getTableOrMetaException(routineLoadJob.getTableId(),
                Table.TableType.OLAP), taskInfo);
        TPipelineFragmentParams tExecPlanFragmentParams = routineLoadJob.plan(planner, loadId, txnId);
        TPlanFragment tPlanFragment = tExecPlanFragmentParams.getFragment();
        tPlanFragment.getOutputSink().getOlapTableSink().setTxnId(txnId);

        if (Config.enable_workload_group) {
            try {
                List<TPipelineWorkloadGroup> tWgList = new ArrayList<>();

                ConnectContext tmpContext = new ConnectContext();
                if (Config.isCloudMode()) {
                    tmpContext.setCloudCluster(routineLoadJob.getCloudCluster());
                }
                tmpContext.setCurrentUserIdentity(routineLoadJob.getUserIdentity());

                String wgName = routineLoadJob.getWorkloadGroup();
                if (!StringUtils.isEmpty(wgName)) {
                    tmpContext.getSessionVariable().setWorkloadGroup(wgName);
                }

                tWgList = Env.getCurrentEnv().getWorkloadGroupMgr().getWorkloadGroup(tmpContext)
                        .stream()
                        .map(e -> e.toThrift())
                        .collect(Collectors.toList());

                if (tWgList.size() != 0) {
                    tExecPlanFragmentParams.setWorkloadGroups(tWgList);
                }
            } catch (Throwable t) {
                LOG.info("Get workload group failed when replan kinesis, job id:{} , ", routineLoadJob.getTxnId(), t);
                throw t;
            }
        }

        return tExecPlanFragmentParams;
    }

    @Override
    protected String getTaskDataSourceProperties() {
        Gson gson = new Gson();
        return gson.toJson(shardIdToSequenceNumber);
    }

    @Override
    boolean hasMoreDataToConsume() throws UserException {
        KinesisRoutineLoadJob routineLoadJob = (KinesisRoutineLoadJob) routineLoadManager.getJob(jobId);
        return routineLoadJob.hasMoreDataToConsume(id, shardIdToSequenceNumber);
    }

    @Override
    public String toString() {
        return "KinesisTaskInfo{" 
                + "id=" + id 
                + ", jobId=" + jobId 
                + ", txnId=" + txnId 
                + ", shardIdToSequenceNumber=" + shardIdToSequenceNumber 
                + '}';
    }
}
