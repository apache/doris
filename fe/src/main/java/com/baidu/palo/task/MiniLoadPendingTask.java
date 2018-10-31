// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.baidu.palo.task;

import com.baidu.palo.analysis.DescriptorTable;
import com.baidu.palo.analysis.SlotDescriptor;
import com.baidu.palo.analysis.TupleDescriptor;
import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.catalog.Column;
import com.baidu.palo.catalog.OlapTable;
import com.baidu.palo.common.UserException;
import com.baidu.palo.common.LoadException;
import com.baidu.palo.common.Pair;
import com.baidu.palo.load.MiniEtlTaskInfo;
import com.baidu.palo.load.EtlSubmitResult;
import com.baidu.palo.load.LoadErrorHub;
import com.baidu.palo.load.LoadJob;
import com.baidu.palo.load.TableLoadInfo;
import com.baidu.palo.planner.CsvScanNode;
import com.baidu.palo.planner.DataPartition;
import com.baidu.palo.planner.DataSplitSink;
import com.baidu.palo.planner.PlanFragment;
import com.baidu.palo.planner.PlanFragmentId;
import com.baidu.palo.planner.PlanNodeId;
import com.baidu.palo.planner.ScanNode;
import com.baidu.palo.system.Backend;
import com.baidu.palo.thrift.PaloInternalServiceVersion;
import com.baidu.palo.thrift.TAgentResult;
import com.baidu.palo.thrift.TAgentServiceVersion;
import com.baidu.palo.thrift.TMiniLoadEtlTaskRequest;
import com.baidu.palo.thrift.TExecPlanFragmentParams;
import com.baidu.palo.thrift.TPlanFragmentExecParams;
import com.baidu.palo.thrift.TQueryOptions;
import com.baidu.palo.thrift.TQueryType;
import com.baidu.palo.thrift.TStatus;
import com.baidu.palo.thrift.TStatusCode;
import com.baidu.palo.thrift.TUniqueId;

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

            // All the following operation will process when destDb's read lock are hold.
            db.readLock();
            try {
                destTable = (OlapTable) db.getTable(tableId);
                if (destTable == null) {
                    throw new LoadException("table does not exist. id: " + tableId);
                }

                registerToDesc();
                tableSink = new DataSplitSink(destTable, destTupleDesc);

                // add schema hash to table load info
                TableLoadInfo tableLoadInfo = job.getTableLoadInfo(destTable.getId());
                for (Map.Entry<Long, Integer> entry : destTable.getIndexIdToSchemaHash().entrySet()) {
                    tableLoadInfo.addIndexSchemaHash(entry.getKey(), entry.getValue());
                }
                requests.add(new Pair<Long, TMiniLoadEtlTaskRequest>(backendId, createRequest(taskId)));
            } finally {
                db.readUnlock();
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
                tStatus.setError_msgs(Lists.newArrayList(failMsg));
                return new EtlSubmitResult(tStatus, null);
            }

            AgentClient client = new AgentClient(backend.getHost(), backend.getBePort());
            TAgentResult submitResult = client.submitEtlTask(request);
            if (submitResult.getStatus().getStatus_code() != TStatusCode.OK) {
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
            LOG.info("fragment finalize faild.e = {}", e);
            throw new LoadException("Fragment finalize failed.", e);
        }

        TMiniLoadEtlTaskRequest request = new TMiniLoadEtlTaskRequest();
        request.setProtocol_version(TAgentServiceVersion.V1);
        TExecPlanFragmentParams params = new TExecPlanFragmentParams();
        params.setProtocol_version(PaloInternalServiceVersion.V1);
        params.setFragment(fragment.toThrift());
        params.setDesc_tbl(desc.toThrift());
        params.setImport_label(job.getLabel());
        params.setDb_name(db.getFullName());
        params.setLoad_job_id(job.getId());

        LoadErrorHub.Param param = load.getLoadErrorHubInfo();
        if (param != null) {
            params.setLoad_error_hub_info(param.toThrift());
        }

        TPlanFragmentExecParams execParams = new TPlanFragmentExecParams();
        // Only use fragment id
        TUniqueId uniqueId = new TUniqueId(job.getId(), taskId);
        execParams.setQuery_id(new TUniqueId(uniqueId));
        execParams.setFragment_instance_id(uniqueId);
        execParams.per_node_scan_ranges = Maps.newHashMap();
        execParams.per_exch_num_senders = Maps.newHashMap();
        execParams.destinations = Lists.newArrayList();
        params.setParams(execParams);
        TQueryOptions queryOptions = new TQueryOptions();
        queryOptions.setQuery_type(TQueryType.LOAD);
        params.setQuery_options(queryOptions);
        request.setParams(params);
        return request;
    }

}

