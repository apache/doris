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

package org.apache.doris.datasource;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.common.UserException;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TScanRangeLocations;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * ExternalScanNode is used to unify data reading from external catalogs.
 * For this type of catalog, we only access its data through the scan node,
 * and after dividing the data of the catalog, the scan task is distributed to one or more Backends for execution.
 * For example:
 * hive, iceberg, hudi, es, odbc
 */
public abstract class ExternalScanNode extends ScanNode {
    private static final Logger LOG = LogManager.getLogger(ExternalScanNode.class);

    // set to false means this scan node does not need to check column priv.
    protected boolean needCheckColumnPriv;

    protected final FederationBackendPolicy backendPolicy = (ConnectContext.get() != null
            && ConnectContext.get().getSessionVariable().enableFileCache)
            ? new FederationBackendPolicy(NodeSelectionStrategy.CONSISTENT_HASHING)
            : new FederationBackendPolicy();

    public ExternalScanNode(PlanNodeId id, TupleDescriptor desc, String planNodeName, StatisticalType statisticalType,
            boolean needCheckColumnPriv) {
        super(id, desc, planNodeName, statisticalType);
        this.needCheckColumnPriv = needCheckColumnPriv;
    }

    @Override
    public void init(Analyzer analyzer) throws UserException {
        super.init(analyzer);
        computeStats(analyzer);
        computeColumnsFilter();
        initBackendPolicy();
    }

    // For Nereids
    @Override
    public void init() throws UserException {
        computeColumnsFilter();
        initBackendPolicy();
    }

    protected void initBackendPolicy() throws UserException {
        backendPolicy.init();
        numNodes = backendPolicy.numBackends();
    }

    public FederationBackendPolicy getBackendPolicy() {
        return backendPolicy;
    }

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("There is {} scanRangeLocations for execution.", scanRangeLocations.size());
        }
        return scanRangeLocations;
    }

    @Override
    public boolean needToCheckColumnPriv() {
        return this.needCheckColumnPriv;
    }

    @Override
    public int getNumInstances() {
        return scanRangeLocations.size();
    }

    protected void toThrift(TPlanNode msg) {
        super.toThrift(msg);
    }
}
