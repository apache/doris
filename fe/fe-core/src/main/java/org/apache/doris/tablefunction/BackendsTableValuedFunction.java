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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

/**
 * The Implement of table valued function
 * backends().
 */
public class BackendsTableValuedFunction extends MetadataTableValuedFunction {
    public static final String NAME = "backends";

    private static final ImmutableList<Column> SCHEMA = ImmutableList.of(
            new Column("BackendId", ScalarType.createType(PrimitiveType.BIGINT)),
            new Column("Host", ScalarType.createStringType()),
            new Column("HeartbeatPort", ScalarType.createType(PrimitiveType.INT)),
            new Column("BePort", ScalarType.createType(PrimitiveType.INT)),
            new Column("HttpPort", ScalarType.createType(PrimitiveType.INT)),
            new Column("BrpcPort", ScalarType.createType(PrimitiveType.INT)),
            new Column("ArrowFlightSqlPort", ScalarType.createType(PrimitiveType.INT)),
            new Column("LastStartTime", ScalarType.createStringType()),
            new Column("LastHeartbeat", ScalarType.createStringType()),
            new Column("Alive", ScalarType.createType(PrimitiveType.BOOLEAN)),
            new Column("SystemDecommissioned", ScalarType.createType(PrimitiveType.BOOLEAN)),
            new Column("TabletNum", ScalarType.createType(PrimitiveType.BIGINT)),
            new Column("DataUsedCapacity", ScalarType.createType(PrimitiveType.BIGINT)),
            new Column("TrashUsedCapacity", ScalarType.createType(PrimitiveType.BIGINT)),
            new Column("AvailCapacity", ScalarType.createType(PrimitiveType.BIGINT)),
            new Column("TotalCapacity", ScalarType.createType(PrimitiveType.BIGINT)),
            new Column("UsedPct", ScalarType.createType(PrimitiveType.DOUBLE)),
            new Column("MaxDiskUsedPct", ScalarType.createType(PrimitiveType.DOUBLE)),
            new Column("RemoteUsedCapacity", ScalarType.createType(PrimitiveType.BIGINT)),
            new Column("Tag", ScalarType.createStringType()),
            new Column("ErrMsg", ScalarType.createStringType()),
            new Column("Version", ScalarType.createStringType()),
            new Column("Status", ScalarType.createStringType()),
            new Column("HeartbeatFailureCounter", ScalarType.createType(PrimitiveType.INT)),
            new Column("NodeRole", ScalarType.createStringType()));

    private static final ImmutableMap<String, Integer> COLUMN_TO_INDEX;

    static {
        ImmutableMap.Builder<String, Integer> builder = new ImmutableMap.Builder();
        for (int i = 0; i < SCHEMA.size(); i++) {
            builder.put(SCHEMA.get(i).getName().toLowerCase(), i);
        }
        COLUMN_TO_INDEX = builder.build();
    }

    public static Integer getColumnIndexFromColumnName(String columnName) {
        return COLUMN_TO_INDEX.get(columnName.toLowerCase());
    }

    public BackendsTableValuedFunction(Map<String, String> params) throws AnalysisException {
        if (params.size() != 0) {
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
        return SCHEMA;
    }
}

