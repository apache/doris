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
import org.apache.doris.analysis.ColumnNullableType;
import org.apache.doris.analysis.TypeDef;
import org.apache.doris.common.UserException;
import org.apache.doris.plugin.audit.AuditLoader;
import org.apache.doris.statistics.StatisticConstants;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;

public class InternalSchema {

    // Do not use the original schema directly, because it may be modified by create table operation.
    public static final List<ColumnDef> TABLE_STATS_SCHEMA;
    public static final List<ColumnDef> PARTITION_STATS_SCHEMA;
    public static final List<ColumnDef> HISTO_STATS_SCHEMA;
    public static final List<ColumnDef> AUDIT_SCHEMA;

    static {
        // table statistics table
        TABLE_STATS_SCHEMA = new ArrayList<>();
        TABLE_STATS_SCHEMA.add(
                new ColumnDef("id", TypeDef.createVarchar(StatisticConstants.ID_LEN), ColumnNullableType.NOT_NULLABLE));
        TABLE_STATS_SCHEMA.add(new ColumnDef("catalog_id", TypeDef.createVarchar(StatisticConstants.MAX_NAME_LEN),
                ColumnNullableType.NOT_NULLABLE));
        TABLE_STATS_SCHEMA.add(new ColumnDef("db_id", TypeDef.createVarchar(StatisticConstants.MAX_NAME_LEN),
                ColumnNullableType.NOT_NULLABLE));
        TABLE_STATS_SCHEMA.add(new ColumnDef("tbl_id", TypeDef.createVarchar(StatisticConstants.MAX_NAME_LEN),
                ColumnNullableType.NOT_NULLABLE));
        TABLE_STATS_SCHEMA.add(new ColumnDef("idx_id", TypeDef.createVarchar(StatisticConstants.MAX_NAME_LEN),
                ColumnNullableType.NOT_NULLABLE));
        TABLE_STATS_SCHEMA.add(new ColumnDef("col_id", TypeDef.createVarchar(StatisticConstants.MAX_NAME_LEN),
                ColumnNullableType.NOT_NULLABLE));
        TABLE_STATS_SCHEMA.add(new ColumnDef("part_id", TypeDef.createVarchar(StatisticConstants.MAX_NAME_LEN),
                ColumnNullableType.NULLABLE));
        TABLE_STATS_SCHEMA
                .add(new ColumnDef("count", TypeDef.create(PrimitiveType.BIGINT), ColumnNullableType.NULLABLE));
        TABLE_STATS_SCHEMA.add(new ColumnDef("ndv", TypeDef.create(PrimitiveType.BIGINT), ColumnNullableType.NULLABLE));
        TABLE_STATS_SCHEMA
                .add(new ColumnDef("null_count", TypeDef.create(PrimitiveType.BIGINT), ColumnNullableType.NULLABLE));
        TABLE_STATS_SCHEMA.add(new ColumnDef("min", TypeDef.createVarchar(ScalarType.MAX_VARCHAR_LENGTH),
                ColumnNullableType.NULLABLE));
        TABLE_STATS_SCHEMA.add(new ColumnDef("max", TypeDef.createVarchar(ScalarType.MAX_VARCHAR_LENGTH),
                ColumnNullableType.NULLABLE));
        TABLE_STATS_SCHEMA.add(
                new ColumnDef("data_size_in_bytes", TypeDef.create(PrimitiveType.BIGINT), ColumnNullableType.NULLABLE));
        TABLE_STATS_SCHEMA.add(
                new ColumnDef("update_time", TypeDef.create(PrimitiveType.DATETIME), ColumnNullableType.NOT_NULLABLE));

        // partition statistics table
        PARTITION_STATS_SCHEMA = new ArrayList<>();
        PARTITION_STATS_SCHEMA.add(new ColumnDef("catalog_id", TypeDef.createVarchar(StatisticConstants.MAX_NAME_LEN),
                ColumnNullableType.NOT_NULLABLE));
        PARTITION_STATS_SCHEMA.add(new ColumnDef("db_id", TypeDef.createVarchar(StatisticConstants.MAX_NAME_LEN),
                ColumnNullableType.NOT_NULLABLE));
        PARTITION_STATS_SCHEMA.add(new ColumnDef("tbl_id", TypeDef.createVarchar(StatisticConstants.MAX_NAME_LEN),
                ColumnNullableType.NOT_NULLABLE));
        PARTITION_STATS_SCHEMA.add(new ColumnDef("idx_id", TypeDef.createVarchar(StatisticConstants.MAX_NAME_LEN),
                ColumnNullableType.NOT_NULLABLE));
        PARTITION_STATS_SCHEMA.add(new ColumnDef("part_name", TypeDef.createVarchar(StatisticConstants.MAX_NAME_LEN),
                ColumnNullableType.NOT_NULLABLE));
        PARTITION_STATS_SCHEMA.add(new ColumnDef("part_id", TypeDef.create(PrimitiveType.BIGINT),
                ColumnNullableType.NOT_NULLABLE));
        PARTITION_STATS_SCHEMA.add(new ColumnDef("col_id", TypeDef.createVarchar(StatisticConstants.MAX_NAME_LEN),
                ColumnNullableType.NOT_NULLABLE));
        PARTITION_STATS_SCHEMA
                .add(new ColumnDef("count", TypeDef.create(PrimitiveType.BIGINT), ColumnNullableType.NULLABLE));
        PARTITION_STATS_SCHEMA
                .add(new ColumnDef("ndv", TypeDef.create(PrimitiveType.HLL), ColumnNullableType.NOT_NULLABLE));
        PARTITION_STATS_SCHEMA
                .add(new ColumnDef("null_count", TypeDef.create(PrimitiveType.BIGINT), ColumnNullableType.NULLABLE));
        PARTITION_STATS_SCHEMA.add(new ColumnDef("min", TypeDef.createVarchar(ScalarType.MAX_VARCHAR_LENGTH),
                ColumnNullableType.NULLABLE));
        PARTITION_STATS_SCHEMA.add(new ColumnDef("max", TypeDef.createVarchar(ScalarType.MAX_VARCHAR_LENGTH),
                ColumnNullableType.NULLABLE));
        PARTITION_STATS_SCHEMA.add(
                new ColumnDef("data_size_in_bytes", TypeDef.create(PrimitiveType.BIGINT), ColumnNullableType.NULLABLE));
        PARTITION_STATS_SCHEMA.add(
                new ColumnDef("update_time", TypeDef.create(PrimitiveType.DATETIME), ColumnNullableType.NOT_NULLABLE));

        // histogram_statistics table
        HISTO_STATS_SCHEMA = new ArrayList<>();
        HISTO_STATS_SCHEMA.add(
                new ColumnDef("id", TypeDef.createVarchar(StatisticConstants.ID_LEN), ColumnNullableType.NOT_NULLABLE));
        HISTO_STATS_SCHEMA.add(new ColumnDef("catalog_id", TypeDef.createVarchar(StatisticConstants.MAX_NAME_LEN),
                ColumnNullableType.NOT_NULLABLE));
        HISTO_STATS_SCHEMA.add(new ColumnDef("db_id", TypeDef.createVarchar(StatisticConstants.MAX_NAME_LEN),
                ColumnNullableType.NOT_NULLABLE));
        HISTO_STATS_SCHEMA.add(new ColumnDef("tbl_id", TypeDef.createVarchar(StatisticConstants.MAX_NAME_LEN),
                ColumnNullableType.NOT_NULLABLE));
        HISTO_STATS_SCHEMA.add(new ColumnDef("idx_id", TypeDef.createVarchar(StatisticConstants.MAX_NAME_LEN),
                ColumnNullableType.NOT_NULLABLE));
        HISTO_STATS_SCHEMA.add(new ColumnDef("col_id", TypeDef.createVarchar(StatisticConstants.MAX_NAME_LEN),
                ColumnNullableType.NOT_NULLABLE));
        HISTO_STATS_SCHEMA.add(
                new ColumnDef("sample_rate", TypeDef.create(PrimitiveType.DOUBLE), ColumnNullableType.NOT_NULLABLE));
        HISTO_STATS_SCHEMA.add(new ColumnDef("buckets", TypeDef.createVarchar(ScalarType.MAX_VARCHAR_LENGTH),
                ColumnNullableType.NOT_NULLABLE));
        HISTO_STATS_SCHEMA.add(
                new ColumnDef("update_time", TypeDef.create(PrimitiveType.DATETIME), ColumnNullableType.NOT_NULLABLE));

        // audit table
        AUDIT_SCHEMA = new ArrayList<>();
        AUDIT_SCHEMA.add(new ColumnDef("query_id", TypeDef.createVarchar(48), ColumnNullableType.NULLABLE));
        AUDIT_SCHEMA.add(new ColumnDef("time", TypeDef.createDatetimeV2(3), ColumnNullableType.NULLABLE));
        AUDIT_SCHEMA.add(new ColumnDef("client_ip", TypeDef.createVarchar(128), ColumnNullableType.NULLABLE));
        AUDIT_SCHEMA.add(new ColumnDef("user", TypeDef.createVarchar(128), ColumnNullableType.NULLABLE));
        AUDIT_SCHEMA.add(new ColumnDef("catalog", TypeDef.createVarchar(128), ColumnNullableType.NULLABLE));
        AUDIT_SCHEMA.add(new ColumnDef("db", TypeDef.createVarchar(128), ColumnNullableType.NULLABLE));
        AUDIT_SCHEMA.add(new ColumnDef("state", TypeDef.createVarchar(128), ColumnNullableType.NULLABLE));
        AUDIT_SCHEMA.add(new ColumnDef("error_code", TypeDef.create(PrimitiveType.INT), ColumnNullableType.NULLABLE));
        AUDIT_SCHEMA
                .add(new ColumnDef("error_message", TypeDef.create(PrimitiveType.STRING), ColumnNullableType.NULLABLE));
        AUDIT_SCHEMA
                .add(new ColumnDef("query_time", TypeDef.create(PrimitiveType.BIGINT), ColumnNullableType.NULLABLE));
        AUDIT_SCHEMA
                .add(new ColumnDef("scan_bytes", TypeDef.create(PrimitiveType.BIGINT), ColumnNullableType.NULLABLE));
        AUDIT_SCHEMA.add(new ColumnDef("scan_rows", TypeDef.create(PrimitiveType.BIGINT), ColumnNullableType.NULLABLE));
        AUDIT_SCHEMA
                .add(new ColumnDef("return_rows", TypeDef.create(PrimitiveType.BIGINT), ColumnNullableType.NULLABLE));
        AUDIT_SCHEMA.add(new ColumnDef("stmt_id", TypeDef.create(PrimitiveType.BIGINT), ColumnNullableType.NULLABLE));
        AUDIT_SCHEMA.add(new ColumnDef("stmt_type", TypeDef.createVarchar(48), ColumnNullableType.NULLABLE));
        AUDIT_SCHEMA.add(new ColumnDef("is_query", TypeDef.create(PrimitiveType.TINYINT), ColumnNullableType.NULLABLE));
        AUDIT_SCHEMA.add(new ColumnDef("frontend_ip", TypeDef.createVarchar(128), ColumnNullableType.NULLABLE));
        AUDIT_SCHEMA
                .add(new ColumnDef("cpu_time_ms", TypeDef.create(PrimitiveType.BIGINT), ColumnNullableType.NULLABLE));
        AUDIT_SCHEMA.add(new ColumnDef("sql_hash", TypeDef.createVarchar(128), ColumnNullableType.NULLABLE));
        AUDIT_SCHEMA.add(new ColumnDef("sql_digest", TypeDef.createVarchar(128), ColumnNullableType.NULLABLE));
        AUDIT_SCHEMA.add(
                new ColumnDef("peak_memory_bytes", TypeDef.create(PrimitiveType.BIGINT), ColumnNullableType.NULLABLE));
        AUDIT_SCHEMA.add(
                new ColumnDef("workload_group", TypeDef.create(PrimitiveType.STRING), ColumnNullableType.NULLABLE));
        AUDIT_SCHEMA.add(new ColumnDef("stmt", TypeDef.create(PrimitiveType.STRING), ColumnNullableType.NULLABLE));
    }

    // Get copied schema for statistic table
    // Do not use the original schema directly, because it may be modified by create table operation.
    public static List<ColumnDef> getCopiedSchema(String tblName) throws UserException {
        List<ColumnDef> schema;
        switch (tblName) {
            case StatisticConstants.TABLE_STATISTIC_TBL_NAME:
                schema = TABLE_STATS_SCHEMA;
                break;
            case StatisticConstants.PARTITION_STATISTIC_TBL_NAME:
                schema = PARTITION_STATS_SCHEMA;
                break;
            case StatisticConstants.HISTOGRAM_TBL_NAME:
                schema = HISTO_STATS_SCHEMA;
                break;
            case AuditLoader.AUDIT_LOG_TABLE:
                schema = AUDIT_SCHEMA;
                break;
            default:
                throw new UserException("Unknown internal table name: " + tblName);
        }
        List<ColumnDef> copiedSchema = Lists.newArrayList();
        for (ColumnDef columnDef : schema) {
            copiedSchema.add(new ColumnDef(columnDef.getName(), columnDef.getTypeDef(), columnDef.isAllowNull()));
        }
        return copiedSchema;
    }
}
