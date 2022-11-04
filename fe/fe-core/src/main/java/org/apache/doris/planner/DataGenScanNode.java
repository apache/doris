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

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.tablefunction.DataGenTableValuedFunction;
import org.apache.doris.tablefunction.TableValuedFunctionTask;
import org.apache.doris.thrift.TDataGenScanNode;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;
import org.apache.doris.thrift.TScanRangeLocation;
import org.apache.doris.thrift.TScanRangeLocations;

import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * This scan node is used for data source generated from memory.
 */
public class DataGenScanNode extends ScanNode {
    private static final Logger LOG = LogManager.getLogger(DataGenScanNode.class.getName());

    private List<TScanRangeLocations> shardScanRanges;
    private DataGenTableValuedFunction tvf;
    private boolean isFinalized = false;

    public DataGenScanNode(PlanNodeId id, TupleDescriptor desc,
                                       String planNodeName, DataGenTableValuedFunction tvf) {
        super(id, desc, planNodeName, StatisticalType.TABLE_VALUED_FUNCTION_NODE);
        this.tvf = tvf;
    }

    @Override
    public void init(Analyzer analyzer) throws UserException {
        super.init(analyzer);
        computeStats(analyzer);
    }

    @Override
    public int getNumInstances() {
        return shardScanRanges.size();
    }

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        return shardScanRanges;
    }

    @Override
    public void finalize(Analyzer analyzer) throws UserException {
        if (isFinalized) {
            return;
        }
        try {
            shardScanRanges = getShardLocations();
        } catch (AnalysisException e) {
            throw new UserException(e.getMessage());
        }

        isFinalized = true;
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.DATA_GEN_SCAN_NODE;
        TDataGenScanNode dataGenScanNode = new TDataGenScanNode();
        dataGenScanNode.setTupleId(desc.getId().asInt());
        dataGenScanNode.setFuncName(tvf.getDataGenFunctionName());
        msg.data_gen_scan_node = dataGenScanNode;
    }

    private List<TScanRangeLocations> getShardLocations() throws AnalysisException {
        List<TScanRangeLocations> result = Lists.newArrayList();
        for (TableValuedFunctionTask task : tvf.getTasks()) {
            TScanRangeLocations locations = new TScanRangeLocations();
            TScanRangeLocation location = new TScanRangeLocation();
            location.setBackendId(task.getBackend().getId());
            location.setServer(new TNetworkAddress(task.getBackend().getHost(), task.getBackend().getBePort()));
            locations.addToLocations(location);
            locations.setScanRange(task.getExecParams());
            result.add(locations);
        }
        return result;
    }
}
