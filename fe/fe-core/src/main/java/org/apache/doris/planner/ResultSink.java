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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.util.VectorizedUtil;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TDataSink;
import org.apache.doris.thrift.TDataSinkType;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TNodeInfo;
import org.apache.doris.thrift.TPaloNodesInfo;
import org.apache.doris.thrift.TResultSink;

/**
 * Result sink that forwards data to
 * 1. the FE data receiver, which result the final query result to user client. Or,
 * 2. files that save the result data
 */
public class ResultSink extends DataSink {
    private final PlanNodeId exchNodeId;
    private boolean useTwoPhaseFetch = false;

    public ResultSink(PlanNodeId exchNodeId) {
        this.exchNodeId = exchNodeId;
    }

    @Override
    public String getExplainString(String prefix, TExplainLevel explainLevel) {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append(prefix);
        if (VectorizedUtil.isVectorized()) {
            strBuilder.append("V");
        }
        strBuilder.append("RESULT SINK\n");
        return strBuilder.toString();
    }

    public void setUseTwoPhaseReadOpt(boolean use) {
        useTwoPhaseFetch = use;
    }

    @Override
    protected TDataSink toThrift() {
        TDataSink result = new TDataSink(TDataSinkType.RESULT_SINK);
        TResultSink tResultSink = new TResultSink();
        tResultSink.setUseTwoPhaseFetch(useTwoPhaseFetch);
        result.setResultSink(tResultSink);
        if (useTwoPhaseFetch) {
            tResultSink.setNodesInfo(createNodesInfo());
        }
        return result;
    }

    @Override
    public PlanNodeId getExchNodeId() {
        return exchNodeId;
    }

    @Override
    public DataPartition getOutputPartition() {
        return null;
    }

    /**
    * Set the parameters used to fetch data by rowid column
    * after init().
    */
    private TPaloNodesInfo createNodesInfo() {
        TPaloNodesInfo nodesInfo = new TPaloNodesInfo();
        SystemInfoService systemInfoService = Env.getCurrentSystemInfo();
        for (Long id : systemInfoService.getBackendIds(true /*need alive*/)) {
            Backend backend = systemInfoService.getBackend(id);
            nodesInfo.addToNodes(new TNodeInfo(backend.getId(), 0, backend.getIp(), backend.getBrpcPort()));
        }
        return nodesInfo;
    }
}
