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

package org.apache.doris.tablefunction;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.thrift.TBackendsMetadataParams;
import org.apache.doris.thrift.TMetaScanRange;
import org.apache.doris.thrift.TMetadataType;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;

/**
 * The Implement of table valued function
 * backends().
 */
public class BackendsTableValuedFunction extends MetadataTableValuedFunction {
    public static final String NAME = "backends";

    public BackendsTableValuedFunction(Map<String, String> params) throws AnalysisException {
        if (params.size() !=  0) {
            throw new AnalysisException("backends table-valued-function does not support any params");
        }
    }

    @Override
    public TMetadataType getMetadataType() {
        return TMetadataType.BACKENDS;
    }

    @Override
    public TMetaScanRange getMetaScanRange() {
        TMetaScanRange metaScanRange = new TMetaScanRange();
        metaScanRange.setMetadataType(TMetadataType.BACKENDS);
        TBackendsMetadataParams backendsMetadataParams = new TBackendsMetadataParams();
        backendsMetadataParams.setClusterName("");
        metaScanRange.setBackendsParams(backendsMetadataParams);
        return metaScanRange;
    }

    @Override
    public String getTableName() {
        return "BackendsTableValuedFunction";
    }

    @Override
    public List<Column> getTableColumns() throws AnalysisException {
        List<Column> resColumns = Lists.newArrayList();
        resColumns.add(new Column("BackendId", ScalarType.createType(PrimitiveType.BIGINT)));
        resColumns.add(new Column("Cluster", ScalarType.createVarchar(64)));
        resColumns.add(new Column("IP", ScalarType.createVarchar(16)));
        resColumns.add(new Column("HeartbeatPort", ScalarType.createType(PrimitiveType.INT)));
        resColumns.add(new Column("BePort", ScalarType.createType(PrimitiveType.INT)));
        resColumns.add(new Column("HttpPort", ScalarType.createType(PrimitiveType.INT)));
        resColumns.add(new Column("BrpcPort", ScalarType.createType(PrimitiveType.INT)));
        resColumns.add(new Column("LastStartTime", ScalarType.createVarchar(32)));
        resColumns.add(new Column("LastHeartbeat", ScalarType.createVarchar(32)));
        resColumns.add(new Column("Alive", ScalarType.createVarchar(8)));
        resColumns.add(new Column("SystemDecommissioned", ScalarType.createVarchar(8)));
        resColumns.add(new Column("ClusterDecommissioned", ScalarType.createVarchar(8)));
        resColumns.add(new Column("TabletNum", ScalarType.createType(PrimitiveType.BIGINT)));
        resColumns.add(new Column("DataUsedCapacity", ScalarType.createType(PrimitiveType.BIGINT)));
        resColumns.add(new Column("AvailCapacity", ScalarType.createType(PrimitiveType.BIGINT)));
        resColumns.add(new Column("TotalCapacity", ScalarType.createType(PrimitiveType.BIGINT)));
        resColumns.add(new Column("UsedPct", ScalarType.createType(PrimitiveType.DOUBLE)));
        resColumns.add(new Column("MaxDiskUsedPct", ScalarType.createType(PrimitiveType.DOUBLE)));
        resColumns.add(new Column("RemoteUsedCapacity", ScalarType.createType(PrimitiveType.BIGINT)));
        resColumns.add(new Column("Tag", ScalarType.createVarchar(128)));
        resColumns.add(new Column("ErrMsg", ScalarType.createVarchar(2048)));
        resColumns.add(new Column("Version", ScalarType.createVarchar(64)));
        resColumns.add(new Column("Status", ScalarType.createVarchar(1024)));
        resColumns.add(new Column("HeartbeatFailureCounter", ScalarType.createType(PrimitiveType.INT)));
        resColumns.add(new Column("NodeRole", ScalarType.createVarchar(64)));
        return resColumns;
    }
}
