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

package org.apache.doris.catalog;

import org.apache.doris.analysis.ColumnDef;
import org.apache.doris.analysis.TypeDef;
import org.apache.doris.statistics.StatisticConstants;

import java.util.ArrayList;
import java.util.List;

public class InternalSchema {

    public static final List<ColumnDef> COL_STATS_SCHEMA;
    public static final List<ColumnDef> HISTO_STATS_SCHEMA;
    public static final List<ColumnDef> AUDIT_SCHEMA;

    static {
        // column statistics table
        COL_STATS_SCHEMA = new ArrayList<>();
        COL_STATS_SCHEMA.add(new ColumnDef("id", TypeDef.createVarchar(StatisticConstants.ID_LEN)));
        COL_STATS_SCHEMA.add(new ColumnDef("catalog_id", TypeDef.createVarchar(StatisticConstants.MAX_NAME_LEN)));
        COL_STATS_SCHEMA.add(new ColumnDef("db_id", TypeDef.createVarchar(StatisticConstants.MAX_NAME_LEN)));
        COL_STATS_SCHEMA.add(new ColumnDef("tbl_id", TypeDef.createVarchar(StatisticConstants.MAX_NAME_LEN)));
        COL_STATS_SCHEMA.add(new ColumnDef("idx_id", TypeDef.createVarchar(StatisticConstants.MAX_NAME_LEN)));
        COL_STATS_SCHEMA.add(new ColumnDef("col_id", TypeDef.createVarchar(StatisticConstants.MAX_NAME_LEN)));
        COL_STATS_SCHEMA.add(new ColumnDef("part_id", TypeDef.createVarchar(StatisticConstants.MAX_NAME_LEN), true));
        COL_STATS_SCHEMA.add(new ColumnDef("count", TypeDef.create(PrimitiveType.BIGINT), true));
        COL_STATS_SCHEMA.add(new ColumnDef("ndv", TypeDef.create(PrimitiveType.BIGINT), true));
        COL_STATS_SCHEMA.add(new ColumnDef("null_count", TypeDef.create(PrimitiveType.BIGINT), true));
        COL_STATS_SCHEMA.add(new ColumnDef("min", TypeDef.createVarchar(ScalarType.MAX_VARCHAR_LENGTH), true));
        COL_STATS_SCHEMA.add(new ColumnDef("max", TypeDef.createVarchar(ScalarType.MAX_VARCHAR_LENGTH), true));
        COL_STATS_SCHEMA.add(new ColumnDef("data_size_in_bytes", TypeDef.create(PrimitiveType.BIGINT), true));
        COL_STATS_SCHEMA.add(new ColumnDef("update_time", TypeDef.create(PrimitiveType.DATETIME)));

        // histogram_statistics table
        HISTO_STATS_SCHEMA = new ArrayList<>();
        HISTO_STATS_SCHEMA.add(new ColumnDef("id", TypeDef.createVarchar(StatisticConstants.ID_LEN)));
        HISTO_STATS_SCHEMA.add(new ColumnDef("catalog_id", TypeDef.createVarchar(StatisticConstants.MAX_NAME_LEN)));
        HISTO_STATS_SCHEMA.add(new ColumnDef("db_id", TypeDef.createVarchar(StatisticConstants.MAX_NAME_LEN)));
        HISTO_STATS_SCHEMA.add(new ColumnDef("tbl_id", TypeDef.createVarchar(StatisticConstants.MAX_NAME_LEN)));
        HISTO_STATS_SCHEMA.add(new ColumnDef("idx_id", TypeDef.createVarchar(StatisticConstants.MAX_NAME_LEN)));
        HISTO_STATS_SCHEMA.add(new ColumnDef("col_id", TypeDef.createVarchar(StatisticConstants.MAX_NAME_LEN)));
        HISTO_STATS_SCHEMA.add(new ColumnDef("sample_rate", TypeDef.create(PrimitiveType.DOUBLE)));
        HISTO_STATS_SCHEMA.add(new ColumnDef("buckets", TypeDef.createVarchar(ScalarType.MAX_VARCHAR_LENGTH)));
        HISTO_STATS_SCHEMA.add(new ColumnDef("update_time", TypeDef.create(PrimitiveType.DATETIME)));

        // audit table
        AUDIT_SCHEMA = new ArrayList<>();
        AUDIT_SCHEMA.add(new ColumnDef("query_id", TypeDef.createVarchar(48), true));
        AUDIT_SCHEMA.add(new ColumnDef("time", TypeDef.create(PrimitiveType.DATETIME), true));
        AUDIT_SCHEMA.add(new ColumnDef("client_ip", TypeDef.createVarchar(128), true));
        AUDIT_SCHEMA.add(new ColumnDef("user", TypeDef.createVarchar(128), true));
        AUDIT_SCHEMA.add(new ColumnDef("catalog", TypeDef.createVarchar(128), true));
        AUDIT_SCHEMA.add(new ColumnDef("db", TypeDef.createVarchar(128), true));
        AUDIT_SCHEMA.add(new ColumnDef("state", TypeDef.createVarchar(128), true));
        AUDIT_SCHEMA.add(new ColumnDef("error_code", TypeDef.create(PrimitiveType.INT), true));
        AUDIT_SCHEMA.add(new ColumnDef("error_message", TypeDef.create(PrimitiveType.STRING), true));
        AUDIT_SCHEMA.add(new ColumnDef("query_time", TypeDef.create(PrimitiveType.BIGINT), true));
        AUDIT_SCHEMA.add(new ColumnDef("scan_bytes", TypeDef.create(PrimitiveType.BIGINT), true));
        AUDIT_SCHEMA.add(new ColumnDef("scan_rows", TypeDef.create(PrimitiveType.BIGINT), true));
        AUDIT_SCHEMA.add(new ColumnDef("return_rows", TypeDef.create(PrimitiveType.BIGINT), true));
        AUDIT_SCHEMA.add(new ColumnDef("stmt_id", TypeDef.create(PrimitiveType.BIGINT), true));
        AUDIT_SCHEMA.add(new ColumnDef("is_query", TypeDef.create(PrimitiveType.TINYINT), true));
        AUDIT_SCHEMA.add(new ColumnDef("frontend_ip", TypeDef.createVarchar(128), true));
        AUDIT_SCHEMA.add(new ColumnDef("cpu_time_ms", TypeDef.create(PrimitiveType.BIGINT), true));
        AUDIT_SCHEMA.add(new ColumnDef("sql_hash", TypeDef.createVarchar(128), true));
        AUDIT_SCHEMA.add(new ColumnDef("sql_digest", TypeDef.createVarchar(128), true));
        AUDIT_SCHEMA.add(new ColumnDef("peak_memory_bytes", TypeDef.create(PrimitiveType.BIGINT), true));
        AUDIT_SCHEMA.add(new ColumnDef("workload_group", TypeDef.create(PrimitiveType.STRING), true));
        AUDIT_SCHEMA.add(new ColumnDef("stmt", TypeDef.create(PrimitiveType.STRING), true));
    }
}
