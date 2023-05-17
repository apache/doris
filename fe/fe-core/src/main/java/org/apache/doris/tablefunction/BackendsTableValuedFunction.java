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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;

/**
 * The Implement of table valued function
 * backends().
 */
public class BackendsTableValuedFunction extends MetadataTableValuedFunction {
    public static final String NAME = "backends";

    private static final ImmutableMap<String, Integer> COLUMN_TO_INDEX = new ImmutableMap.Builder<String, Integer>()
            .put("backendid", 0)
            .put("cluster", 1)
            .put("host", 2)
            .put("heartbeatport", 3)
            .put("beport", 4)
            .put("httpport", 5)
            .put("brpcport", 6)
            .put("laststarttime", 7)
            .put("lastheartbeat", 8)
            .put("alive", 9)
            .put("systemdecommissioned", 10)
            .put("clusterdecommissioned", 11)
            .put("tabletnum", 12)
            .put("datausedcapacity", 13)
            .put("availcapacity", 14)
            .put("totalcapacity", 15)
            .put("usedpct", 16)
            .put("maxdiskusedpct", 17)
            .put("remoteusedcapacity", 18)
            .put("tag", 19)
            .put("errmsg", 20)
            .put("version", 21)
            .put("status", 22)
            .put("heartbeatfailurecounter", 23)
            .put("noderole", 24)
            .build();

    public static Integer getColumnIndexFromColumnName(String columnName) {
        return COLUMN_TO_INDEX.get(columnName.toLowerCase());
    }

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
        resColumns.add(new Column("Cluster", ScalarType.createStringType()));
        resColumns.add(new Column("Host", ScalarType.createStringType()));
        resColumns.add(new Column("HeartbeatPort", ScalarType.createType(PrimitiveType.INT)));
        resColumns.add(new Column("BePort", ScalarType.createType(PrimitiveType.INT)));
        resColumns.add(new Column("HttpPort", ScalarType.createType(PrimitiveType.INT)));
        resColumns.add(new Column("BrpcPort", ScalarType.createType(PrimitiveType.INT)));
        resColumns.add(new Column("LastStartTime", ScalarType.createStringType()));
        resColumns.add(new Column("LastHeartbeat", ScalarType.createStringType()));
        resColumns.add(new Column("Alive", ScalarType.createType(PrimitiveType.BOOLEAN)));
        resColumns.add(new Column("SystemDecommissioned", ScalarType.createType(PrimitiveType.BOOLEAN)));
        resColumns.add(new Column("ClusterDecommissioned", ScalarType.createType(PrimitiveType.BOOLEAN)));
        resColumns.add(new Column("TabletNum", ScalarType.createType(PrimitiveType.BIGINT)));
        resColumns.add(new Column("DataUsedCapacity", ScalarType.createType(PrimitiveType.BIGINT)));
        resColumns.add(new Column("AvailCapacity", ScalarType.createType(PrimitiveType.BIGINT)));
        resColumns.add(new Column("TotalCapacity", ScalarType.createType(PrimitiveType.BIGINT)));
        resColumns.add(new Column("UsedPct", ScalarType.createType(PrimitiveType.DOUBLE)));
        resColumns.add(new Column("MaxDiskUsedPct", ScalarType.createType(PrimitiveType.DOUBLE)));
        resColumns.add(new Column("RemoteUsedCapacity", ScalarType.createType(PrimitiveType.BIGINT)));
        resColumns.add(new Column("Tag", ScalarType.createStringType()));
        resColumns.add(new Column("ErrMsg", ScalarType.createStringType()));
        resColumns.add(new Column("Version", ScalarType.createStringType()));
        resColumns.add(new Column("Status", ScalarType.createStringType()));
        resColumns.add(new Column("HeartbeatFailureCounter", ScalarType.createType(PrimitiveType.INT)));
        resColumns.add(new Column("NodeRole", ScalarType.createStringType()));
        return resColumns;
    }
}
