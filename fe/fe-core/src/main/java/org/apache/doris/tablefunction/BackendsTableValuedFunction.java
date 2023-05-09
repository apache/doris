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
            .put("ip", 2)
            .put("hostname", 3)
            .put("heartbeatport", 4)
            .put("beport", 5)
            .put("httpport", 6)
            .put("brpcport", 7)
            .put("laststarttime", 8)
            .put("lastheartbeat", 9)
            .put("alive", 10)
            .put("systemdecommissioned", 11)
            .put("clusterdecommissioned", 12)
            .put("tabletnum", 13)
            .put("datausedcapacity", 14)
            .put("availcapacity", 15)
            .put("totalcapacity", 16)
            .put("usedpct", 17)
            .put("maxdiskusedpct", 18)
            .put("remoteusedcapacity", 19)
            .put("tag", 20)
            .put("errmsg", 21)
            .put("version", 22)
            .put("status", 23)
            .put("heartbeatfailurecounter", 24)
            .put("noderole", 25)
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
        resColumns.add(new Column("IP", ScalarType.createStringType()));
        resColumns.add(new Column("HostName", ScalarType.createStringType()));
        resColumns.add(new Column("HeartbeatPort", ScalarType.createType(PrimitiveType.INT)));
        resColumns.add(new Column("BePort", ScalarType.createType(PrimitiveType.INT)));
        resColumns.add(new Column("HttpPort", ScalarType.createType(PrimitiveType.INT)));
        resColumns.add(new Column("BrpcPort", ScalarType.createType(PrimitiveType.INT)));
        resColumns.add(new Column("LastStartTime", ScalarType.createStringType()));
        resColumns.add(new Column("LastHeartbeat", ScalarType.createStringType()));
        resColumns.add(new Column("Alive", ScalarType.createStringType()));
        resColumns.add(new Column("SystemDecommissioned", ScalarType.createStringType()));
        resColumns.add(new Column("ClusterDecommissioned", ScalarType.createStringType()));
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
