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

package org.apache.doris.planner;

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.FederationBackendPolicy;
import org.apache.doris.datasource.FileScanNode;
import org.apache.doris.load.BrokerFileGroup;
import org.apache.doris.nereids.load.NereidsFileGroupInfo;
import org.apache.doris.nereids.load.NereidsLoadPlanInfoCollector;
import org.apache.doris.nereids.load.NereidsParamCreateContext;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.system.BeSelectionPolicy;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Map;

/**
 * FileLoadScanNode for broker load and stream load.
 */
public class FileLoadScanNode extends FileScanNode {

    public static class ParamCreateContext {
        public BrokerFileGroup fileGroup;
        public TupleDescriptor destTupleDescriptor;
        // === Set when init ===
        public TupleDescriptor srcTupleDescriptor;
        public Map<String, SlotDescriptor> srcSlotDescByName;
        public Map<String, Expr> exprMap;
        public String timezone;
        // === Set when init ===
        public TFileScanRangeParams params;
    }

    /**
     * External file scan node for load from file
     * These scan nodes do not have corresponding catalog/database/table info, so no need to do priv check
     */
    public FileLoadScanNode(PlanNodeId id, TupleDescriptor desc) {
        super(id, desc, "FILE_LOAD_SCAN_NODE", StatisticalType.FILE_SCAN_NODE, false);
    }

    public void finalizeForNereids(TUniqueId loadId, List<NereidsFileGroupInfo> fileGroupInfos,
            List<NereidsParamCreateContext> contexts, NereidsLoadPlanInfoCollector.LoadPlanInfo loadPlanInfo)
            throws UserException {
        Preconditions.checkState(contexts.size() == fileGroupInfos.size(),
                contexts.size() + " vs. " + fileGroupInfos.size());
        List<Expr> preFilterList = loadPlanInfo.getPreFilterExprList();
        if (preFilterList != null) {
            addPreFilterConjuncts(preFilterList);
        }
        List<Expr> postFilterList = loadPlanInfo.getPostFilterExprList();
        if (postFilterList != null) {
            addConjuncts(postFilterList);
        }
        // ATTN: for load scan node, do not use backend policy in ExternalScanNode.
        // Because backend policy in ExternalScanNode may only contain compute backend.
        // But for load job, we should select backends from all backends, both compute and mix.
        BeSelectionPolicy policy = new BeSelectionPolicy.Builder()
                .needQueryAvailable()
                .needLoadAvailable()
                .build();
        FederationBackendPolicy localBackendPolicy = new FederationBackendPolicy();
        localBackendPolicy.init(policy);
        for (int i = 0; i < contexts.size(); ++i) {
            NereidsParamCreateContext context = contexts.get(i);
            NereidsFileGroupInfo fileGroupInfo = fileGroupInfos.get(i);
            context.params = loadPlanInfo.toFileScanRangeParams(loadId, fileGroupInfo);
            createScanRangeLocations(context, fileGroupInfo, localBackendPolicy);
            this.selectedSplitNum += fileGroupInfo.getFileStatuses().size();
            for (TBrokerFileStatus fileStatus : fileGroupInfo.getFileStatuses()) {
                this.totalFileSize += fileStatus.getSize();
            }
        }
    }

    private void createScanRangeLocations(NereidsParamCreateContext context,
            NereidsFileGroupInfo fileGroupInfo, FederationBackendPolicy backendPolicy)
            throws UserException {
        fileGroupInfo.getFileStatusAndCalcInstance(backendPolicy);
        fileGroupInfo.createScanRangeLocations(context, backendPolicy, scanRangeLocations);
    }

    @Override
    protected void createScanRangeLocations() throws UserException {
        // do nothing, we have already created scan range locations in finalize
    }
}
