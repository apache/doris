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
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.thrift.TFrontendsMetadataParams;
import org.apache.doris.thrift.TMetaScanRange;
import org.apache.doris.thrift.TMetadataType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

/**
 * The Implement of table valued function
 * frontends().
 */
public class FrontendsTableValuedFunction extends MetadataTableValuedFunction {
    public static final String NAME = "frontends";

    private static final ImmutableList<Column> SCHEMA = ImmutableList.of(
            new Column("Name", ScalarType.createStringType()),
            new Column("Host", ScalarType.createStringType()),
            new Column("EditLogPort", ScalarType.createStringType()),
            new Column("HttpPort", ScalarType.createStringType()),
            new Column("QueryPort", ScalarType.createStringType()),
            new Column("RpcPort", ScalarType.createStringType()),
            new Column("ArrowFlightSqlPort", ScalarType.createStringType()),
            new Column("Role", ScalarType.createStringType()),
            new Column("IsMaster", ScalarType.createStringType()),
            new Column("ClusterId", ScalarType.createStringType()),
            new Column("Join", ScalarType.createStringType()),
            new Column("Alive", ScalarType.createStringType()),
            new Column("ReplayedJournalId", ScalarType.createStringType()),
            new Column("LastStartTime", ScalarType.createStringType()),
            new Column("LastHeartbeat", ScalarType.createStringType()),
            new Column("IsHelper", ScalarType.createStringType()),
            new Column("ErrMsg", ScalarType.createStringType()),
            new Column("Version", ScalarType.createStringType()),
            new Column("CurrentConnected", ScalarType.createStringType()));

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

    public FrontendsTableValuedFunction(Map<String, String> params) throws AnalysisException {
        if (params.size() != 0) {
            throw new AnalysisException("frontends table-valued-function does not support any params");
        }
    }

    @Override
    public TMetadataType getMetadataType() {
        return TMetadataType.FRONTENDS;
    }

    @Override
    public TMetaScanRange getMetaScanRange() {
        TMetaScanRange metaScanRange = new TMetaScanRange();
        metaScanRange.setMetadataType(TMetadataType.FRONTENDS);
        TFrontendsMetadataParams frontendsMetadataParams = new TFrontendsMetadataParams();
        frontendsMetadataParams.setClusterName("");
        metaScanRange.setFrontendsParams(frontendsMetadataParams);
        return metaScanRange;
    }

    @Override
    public String getTableName() {
        return "FrontendsTableValuedFunction";
    }

    @Override
    public List<Column> getTableColumns() throws AnalysisException {
        return SCHEMA;
    }
}
