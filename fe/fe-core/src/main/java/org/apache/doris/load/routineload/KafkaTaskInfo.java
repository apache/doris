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
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.nereids.load.NereidsStreamLoadPlanner;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TKafkaLoadInfo;
import org.apache.doris.thrift.TLoadSourceType;
import org.apache.doris.thrift.TPipelineFragmentParams;
import org.apache.doris.thrift.TPipelineWorkloadGroup;
import org.apache.doris.thrift.TPlanFragment;
import org.apache.doris.thrift.TRoutineLoadTask;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class KafkaTaskInfo extends RoutineLoadTaskInfo {
    private RoutineLoadManager routineLoadManager = Env.getCurrentEnv().getRoutineLoadManager();

    private static final Logger LOG = LogManager.getLogger(KafkaTaskInfo.class);

    // <partitionId, offset to be consumed>
    private Map<Integer, Long> partitionIdToOffset;

    public KafkaTaskInfo(UUID id, long jobId,
                         long timeoutMs, Map<Integer, Long> partitionIdToOffset, boolean isMultiTable,
                         long lastScheduledTime, boolean isEof) {
        super(id, jobId, timeoutMs, isMultiTable, lastScheduledTime, isEof);
        this.partitionIdToOffset = partitionIdToOffset;
    }

    public KafkaTaskInfo(KafkaTaskInfo kafkaTaskInfo, Map<Integer, Long> partitionIdToOffset, boolean isMultiTable) {
        super(UUID.randomUUID(), kafkaTaskInfo.getJobId(),
                kafkaTaskInfo.getTimeoutMs(), kafkaTaskInfo.getBeId(), isMultiTable,
                kafkaTaskInfo.getLastScheduledTime(), kafkaTaskInfo.getIsEof());
        this.partitionIdToOffset = partitionIdToOffset;
    }

    public List<Integer> getPartitions() {
        return new ArrayList<>(partitionIdToOffset.keySet());
    }

    @Override
    public TRoutineLoadTask createRoutineLoadTask() throws UserException {
        KafkaRoutineLoadJob routineLoadJob = (KafkaRoutineLoadJob) routineLoadManager.getJob(jobId);

        // init tRoutineLoadTask and create plan fragment
        TRoutineLoadTask tRoutineLoadTask = new TRoutineLoadTask();
        TUniqueId queryId = new TUniqueId(id.getMostSignificantBits(), id.getLeastSignificantBits());
        tRoutineLoadTask.setId(queryId);
        tRoutineLoadTask.setJobId(jobId);
        tRoutineLoadTask.setTxnId(txnId);
        Database database =
                Env.getCurrentInternalCatalog().getDbOrMetaException(routineLoadJob.getDbId());

        tRoutineLoadTask.setDb(database.getFullName());
        // label = job_name+job_id+task_id+txn_id
        String label = Joiner.on("-").join(routineLoadJob.getName(),
                routineLoadJob.getId(), DebugUtil.printId(id), txnId);
        tRoutineLoadTask.setLabel(label);
        tRoutineLoadTask.setAuthCode(routineLoadJob.getAuthCode());
        TKafkaLoadInfo tKafkaLoadInfo = new TKafkaLoadInfo();
        tKafkaLoadInfo.setTopic(routineLoadJob.getTopic());
        tKafkaLoadInfo.setBrokers(routineLoadJob.getBrokerList());
        tKafkaLoadInfo.setPartitionBeginOffset(partitionIdToOffset);
        tKafkaLoadInfo.setProperties(routineLoadJob.getConvertedCustomProperties());
        tRoutineLoadTask.setKafkaLoadInfo(tKafkaLoadInfo);
        tRoutineLoadTask.setType(TLoadSourceType.KAFKA);
        tRoutineLoadTask.setIsMultiTable(isMultiTable);
        if (!isMultiTable) {
            Table tbl = database.getTableOrMetaException(routineLoadJob.getTableId());
            tRoutineLoadTask.setTbl(tbl.getName());
            tRoutineLoadTask.setPipelineParams(rePlan(routineLoadJob));
        } else {
            Env.getCurrentEnv().getRoutineLoadManager().addMultiLoadTaskTxnIdToRoutineLoadJobId(txnId, jobId);
        }
        tRoutineLoadTask.setMaxIntervalS(routineLoadJob.getMaxBatchIntervalS());
        tRoutineLoadTask.setMaxBatchRows(routineLoadJob.getMaxBatchRows());
        tRoutineLoadTask.setMaxBatchSize(routineLoadJob.getMaxBatchSizeBytes());
        if (!routineLoadJob.getFormat().isEmpty() && routineLoadJob.getFormat().equalsIgnoreCase("json")) {
            tRoutineLoadTask.setFormat(TFileFormatType.FORMAT_JSON);
        } else {
            tRoutineLoadTask.setFormat(TFileFormatType.FORMAT_CSV_PLAIN);
        }
        tRoutineLoadTask.setMemtableOnSinkNode(routineLoadJob.isMemtableOnSinkNode());
        tRoutineLoadTask.setQualifiedUser(routineLoadJob.getQualifiedUser());
        tRoutineLoadTask.setCloudCluster(routineLoadJob.getCloudCluster());
        return tRoutineLoadTask;
    }

    @Override
    protected String getTaskDataSourceProperties() {
        Gson gson = new Gson();
        return gson.toJson(partitionIdToOffset);
    }

    @Override
    boolean hasMoreDataToConsume() throws UserException {
        KafkaRoutineLoadJob routineLoadJob = (KafkaRoutineLoadJob) routineLoadManager.getJob(jobId);
        return routineLoadJob.hasMoreDataToConsume(id, partitionIdToOffset);
    }

    private TPipelineFragmentParams rePlan(RoutineLoadJob routineLoadJob) throws UserException {
        TUniqueId loadId = new TUniqueId(id.getMostSignificantBits(), id.getLeastSignificantBits());
        // plan for each task, in case table has change(rollup or schema change)
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException(routineLoadJob.getDbId());
        NereidsStreamLoadPlanner planner = new NereidsStreamLoadPlanner(db,
                (OlapTable) db.getTableOrMetaException(routineLoadJob.getTableId(),
                Table.TableType.OLAP), routineLoadJob.toNereidsRoutineLoadTaskInfo());
        TPipelineFragmentParams tExecPlanFragmentParams = routineLoadJob.plan(planner, loadId, txnId);
        TPlanFragment tPlanFragment = tExecPlanFragmentParams.getFragment();
        tPlanFragment.getOutputSink().getOlapTableSink().setTxnId(txnId);

        if (Config.enable_workload_group) {
            try {
                List<TPipelineWorkloadGroup> tWgList = new ArrayList<>();

                ConnectContext tmpContext = new ConnectContext();
                if (Config.isCloudMode()) {
                    String clusterName = ((CloudSystemInfoService) Env.getCurrentSystemInfo())
                            .getClusterNameByClusterId(routineLoadJob.getCloudClusterId());
                    if (Strings.isNullOrEmpty(clusterName)) {
                        LOG.warn("cluster name is empty, cluster id is {}, job id is {}",
                                routineLoadJob.getCloudClusterId(), routineLoadJob.getTxnId());
                        throw new UserException(String.format("cluster name is empty, cluster id is %s",
                                routineLoadJob.getCloudClusterId()));
                    }
                    tmpContext.setCloudCluster(clusterName);
                }
                tmpContext.setCurrentUserIdentity(routineLoadJob.getUserIdentity());
                tmpContext.setQualifiedUser(routineLoadJob.getUserIdentity().getQualifiedUser());

                String wgName = routineLoadJob.getWorkloadGroup();
                if (!StringUtils.isEmpty(wgName)) {
                    tmpContext.getSessionVariable().setWorkloadGroup(wgName);
                }

                tWgList = Env.getCurrentEnv().getWorkloadGroupMgr().getWorkloadGroup(tmpContext);

                if (tWgList.size() != 0) {
                    tExecPlanFragmentParams.setWorkloadGroups(tWgList);
                }
            } catch (Throwable t) {
                LOG.info("Get workload group failed when replan kafka, job id:{} , ", routineLoadJob.getTxnId(), t);
                throw t;
            }

        }

        return tExecPlanFragmentParams;
    }

    // implement method for compatibility
    public String getHeaderType() {
        return "";
    }
}
