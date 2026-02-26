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
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.datasource.doris.RemoteOlapTable;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TDataSink;
import org.apache.doris.thrift.TDataSinkType;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TNodeInfo;
import org.apache.doris.thrift.TPaloNodesInfo;

import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

/**
 * RemoteOlapTableSink is a specialized OlapTableSink implementation
 * that writes data into a remote Doris cluster. It reuses most of the
 * local sink behaviors and only overrides the nodes information building
 * to route data to remote backends.
 */
public class RemoteOlapTableSink extends OlapTableSink {

    public RemoteOlapTableSink(RemoteOlapTable dstTable, TupleDescriptor tupleDescriptor,
            List<Long> partitionIds, boolean singleReplicaLoad,
            List<Expr> partitionExprs, Map<Long, Expr> syncMvWhereClauses) {
        super(dstTable, tupleDescriptor, partitionIds, singleReplicaLoad, partitionExprs, syncMvWhereClauses);
        masterAddress = getRemoteTable().getCatalog().getFeServiceClient().getMasterAddress();
    }

    private TNetworkAddress masterAddress;

    private RemoteOlapTable getRemoteTable() {
        return (RemoteOlapTable) getDstTable();
    }

    @Override
    public TPaloNodesInfo createPaloNodesInfo() {
        TPaloNodesInfo nodesInfo = new TPaloNodesInfo();
        ImmutableMap<Long, Backend> remoteBackends = getRemoteTable().getAllBackendsByAllCluster();
        for (Backend backend : remoteBackends.values()) {
            if (backend == null) {
                continue;
            }
            nodesInfo.addToNodes(new TNodeInfo(backend.getId(), 0, backend.getHost(), backend.getBrpcPort()));
        }
        return nodesInfo;
    }

    @Override
    protected TDataSinkType getDataSinkType() {
        // Keep the same sink type as local OlapTableSink so that BE side
        // can reuse the same execution pipeline.
        return TDataSinkType.OLAP_TABLE_SINK;
    }

    @Override
    protected TDataSink toThrift() {
        tDataSink.olap_table_sink.getPartition()
                .setMasterAddress(masterAddress);
        return tDataSink;
    }
}
