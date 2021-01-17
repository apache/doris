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

package org.apache.doris.task;

import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.load.EtlSubmitResult;
import org.apache.doris.load.LoadErrorHub;
import org.apache.doris.load.LoadJob;
import org.apache.doris.load.MiniEtlTaskInfo;
import org.apache.doris.load.TableLoadInfo;
import org.apache.doris.planner.CsvScanNode;
import org.apache.doris.planner.DataPartition;
import org.apache.doris.planner.DataSplitSink;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanFragmentId;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.PaloInternalServiceVersion;
import org.apache.doris.thrift.TAgentResult;
import org.apache.doris.thrift.TAgentServiceVersion;
import org.apache.doris.thrift.TExecPlanFragmentParams;
import org.apache.doris.thrift.TLoadErrorHubInfo;
import org.apache.doris.thrift.TMiniLoadEtlTaskRequest;
import org.apache.doris.thrift.TPlanFragmentExecParams;
import org.apache.doris.thrift.TQueryOptions;
import org.apache.doris.thrift.TQueryType;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

public class MiniLoadPendingTask extends LoadPendingTask {
    private static final Logger LOG = LogManager.getLogger(MiniLoadPendingTask.class);

    // descriptor used to register all column and table need
    private final DescriptorTable desc;

    // destination Db and table get from request
    // Data will load to this table
    private OlapTable destTable;

    // dest desc
    private TupleDescriptor destTupleDesc;

    private DataSplitSink tableSink;

    private List<Pair<Long, TMiniLoadEtlTaskRequest>> requests;

    public MiniLoadPendingTask(LoadJob job) {
        super(job);
        this.desc = new DescriptorTable();
    }

    @Override
    protected void createEtlRequest() throws Exception {
        requests = Lists.newArrayList();
        for (MiniEtlTaskInfo taskInfo : job.getMiniEtlTasks().values()) {
            long taskId = taskInfo.getId();
            long backendId = taskInfo.getBackendId();
            long tableId = taskInfo.getTableId();

            // All the following operation will process when destTable's read lock are hold.
            destTable = (OlapTable) db.getTable(tableId);
            if (destTable == null) {
                throw new LoadException("table does not exist. id: " + tableId);
            }

            destTable.readLock();
            try {
                registerToDesc();
                tableSink = new DataSplitSink(destTable, destTupleDesc);

                // add schema hash to table load info
                TableLoadInfo tableLoadInfo = job.getTableLoadInfo(destTable.getId());
                for (Map.Entry<Long, Integer> entry : destTable.getIndexIdToSchemaHash().entrySet()) {
                    tableLoadInfo.addIndexSchemaHash(entry.getKey(), entry.getValue());
                }
                requests.add(new Pair<Long, TMiniLoadEtlTaskRequest>(backendId, createRequest(taskId)));
            } finally {
                destTable.readUnlock();
            }
        }
    }

    @Override
    protected EtlSubmitResult submitEtlJob(int retry) {
        LOG.info("begin submit mini load etl job: {}", job);

        for (Pair<Long, TMiniLoadEtlTaskRequest> pair : requests) {
            long backendId = pair.first;
            TMiniLoadEtlTaskRequest request = pair.second;

            Backend backend = Catalog.getCurrentSystemInfo().getBackend(backendId);
            if (!Catalog.getCurrentSystemInfo().checkBackendAvailable(backendId)) {
                String failMsg = "backend is null or is not alive";
                LOG.error(failMsg);
                TStatus tStatus = new TStatus(TStatusCode.CANCELLED);
                tStatus.setErrorMsgs(Lists.newArrayList(failMsg));
                return new EtlSubmitResult(tStatus, null);
            }

            AgentClient client = new AgentClient(backend.getHost(), backend.getBePort());
            TAgentResult submitResult = client.submitEtlTask(request);
            if (submitResult.getStatus().getStatusCode() != TStatusCode.OK) {
                return new EtlSubmitResult(submitResult.getStatus(), null);
            }
        }

        return new EtlSubmitResult(new TStatus(TStatusCode.OK), null);
    }

    private void registerToDesc() {
        destTupleDesc = desc.createTupleDescriptor();
        destTupleDesc.setTable(destTable);
        // Lock database and get its schema hash??
        // Make sure that import job has its corresponding schema
        for (Column col : destTable.getBaseSchema()) {
            SlotDescriptor slot = desc.addSlotDescriptor(destTupleDesc);
            // All this slot is needed
            slot.setIsMaterialized(true);
            slot.setColumn(col);
            if (true == col.isAllowNull()) {
                slot.setIsNullable(true);
            } else {
                slot.setIsNullable(false);
            }
        }
    }

    private TMiniLoadEtlTaskRequest createRequest(long taskId) throws LoadException {
        ScanNode csvScanNode = new CsvScanNode(new PlanNodeId(0), destTupleDesc, destTable, job);
        desc.computeMemLayout();
        try {
            csvScanNode.finalize(null);
        } catch (UserException e) {
            LOG.warn("csvScanNode finalize failed[err={}]", e);
            throw new LoadException("CSV scan finalize failed.", e);
        }
        PlanFragment fragment = new PlanFragment(new PlanFragmentId(0), csvScanNode, DataPartition.UNPARTITIONED);
        fragment.setSink(tableSink);

        try {
            fragment.finalize(null, false);
        } catch (Exception e) {
            LOG.info("fragment finalize failed.e = {}", e);
            throw new LoadException("Fragment finalize failed.", e);
        }

        TMiniLoadEtlTaskRequest request = new TMiniLoadEtlTaskRequest();
        request.setProtocolVersion(TAgentServiceVersion.V1);
        TExecPlanFragmentParams params = new TExecPlanFragmentParams();
        params.setProtocolVersion(PaloInternalServiceVersion.V1);
        params.setFragment(fragment.toThrift());
        params.setDescTbl(desc.toThrift());
        params.setImportLabel(job.getLabel());
        params.setDbName(db.getFullName());
        params.setLoadJobId(job.getId());

        LoadErrorHub.Param param = load.getLoadErrorHubInfo();
        if (param != null) {
            TLoadErrorHubInfo info = param.toThrift();
            if (info != null) {
                params.setLoadErrorHubInfo(info);
            }
        }

        TPlanFragmentExecParams execParams = new TPlanFragmentExecParams();
        // Only use fragment id
        TUniqueId uniqueId = new TUniqueId(job.getId(), taskId);
        execParams.setQueryId(new TUniqueId(uniqueId));
        execParams.setFragmentInstanceId(uniqueId);
        execParams.per_node_scan_ranges = Maps.newHashMap();
        execParams.per_exch_num_senders = Maps.newHashMap();
        execParams.destinations = Lists.newArrayList();
        params.setParams(execParams);
        TQueryOptions queryOptions = new TQueryOptions();
        queryOptions.setQueryType(TQueryType.LOAD);
        params.setQueryOptions(queryOptions);
        request.setParams(params);
        return request;
    }

}

