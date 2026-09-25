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
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.plugin.audit.AuditLoader;
import org.apache.doris.statistics.StatisticConstants;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;

public class InternalSchema {

    /** Name of the SPM baselines internal table (design doc 6.14.1). */
    public static final String SPM_BASELINES_TBL_NAME = "spm_baselines";

    /**
     * Name of the SPM plan-capture checkpoint internal table: the single durable row keeps
     * the truncated scan window, the total-order cursor and the retry state, so a leader
     * handoff / FE restart resumes the SAME window instead of excluding its unconsumed tail
     * forever.
     */
    public static final String SPM_CAPTURE_CHECKPOINT_TBL_NAME = "spm_capture_checkpoint";

    // Do not use the original schema directly, because it may be modified by create table operation.
    public static final List<ColumnDef> TABLE_STATS_SCHEMA;
    public static final List<ColumnDef> PARTITION_STATS_SCHEMA;
    public static final List<ColumnDef> HISTO_STATS_SCHEMA;
    public static final List<ColumnDef> AUDIT_SCHEMA;
    public static final List<ColumnDef> SPM_BASELINES_SCHEMA;
    public static final List<ColumnDef> SPM_CAPTURE_CHECKPOINT_SCHEMA;

    static {
        // table statistics table
        TABLE_STATS_SCHEMA = new ArrayList<>();
        TABLE_STATS_SCHEMA.add(
                new ColumnDef("id", ScalarType.createVarchar(StatisticConstants.ID_LEN),
                    ColumnNullableType.NOT_NULLABLE));
        TABLE_STATS_SCHEMA.add(new ColumnDef("catalog_id", ScalarType.createVarchar(StatisticConstants.MAX_NAME_LEN),
                ColumnNullableType.NOT_NULLABLE));
        TABLE_STATS_SCHEMA.add(new ColumnDef("db_id", ScalarType.createVarchar(StatisticConstants.MAX_NAME_LEN),
                ColumnNullableType.NOT_NULLABLE));
        TABLE_STATS_SCHEMA.add(new ColumnDef("tbl_id", ScalarType.createVarchar(StatisticConstants.MAX_NAME_LEN),
                ColumnNullableType.NOT_NULLABLE));
        TABLE_STATS_SCHEMA.add(new ColumnDef("idx_id", ScalarType.createVarchar(StatisticConstants.MAX_NAME_LEN),
                ColumnNullableType.NOT_NULLABLE));
        TABLE_STATS_SCHEMA.add(new ColumnDef("col_id", ScalarType.createVarchar(StatisticConstants.MAX_NAME_LEN),
                ColumnNullableType.NOT_NULLABLE));
        TABLE_STATS_SCHEMA.add(new ColumnDef("part_id", ScalarType.createVarchar(StatisticConstants.MAX_NAME_LEN),
                ColumnNullableType.NULLABLE));
        TABLE_STATS_SCHEMA
                .add(new ColumnDef("count", ScalarType.createType(PrimitiveType.BIGINT), ColumnNullableType.NULLABLE));
        TABLE_STATS_SCHEMA.add(new ColumnDef("ndv", ScalarType.createType(PrimitiveType.BIGINT),
                ColumnNullableType.NULLABLE));
        TABLE_STATS_SCHEMA
                .add(new ColumnDef("null_count", ScalarType.createType(PrimitiveType.BIGINT),
                    ColumnNullableType.NULLABLE));
        TABLE_STATS_SCHEMA.add(new ColumnDef("min", ScalarType.createVarchar(ScalarType.MAX_VARCHAR_LENGTH),
                ColumnNullableType.NULLABLE));
        TABLE_STATS_SCHEMA.add(new ColumnDef("max", ScalarType.createVarchar(ScalarType.MAX_VARCHAR_LENGTH),
                ColumnNullableType.NULLABLE));
        TABLE_STATS_SCHEMA.add(
                new ColumnDef("data_size_in_bytes", ScalarType.createType(PrimitiveType.BIGINT),
                    ColumnNullableType.NULLABLE));
        TABLE_STATS_SCHEMA.add(
                new ColumnDef("update_time", ScalarType.DATETIMEV2,
                    ColumnNullableType.NOT_NULLABLE));
        TABLE_STATS_SCHEMA.add(
                new ColumnDef("hot_value", ScalarType.createType(PrimitiveType.STRING), ColumnNullableType.NULLABLE));

        // partition statistics table
        PARTITION_STATS_SCHEMA = new ArrayList<>();
        PARTITION_STATS_SCHEMA.add(new ColumnDef("catalog_id",
                ScalarType.createVarchar(StatisticConstants.MAX_NAME_LEN),
                ColumnNullableType.NOT_NULLABLE));
        PARTITION_STATS_SCHEMA.add(new ColumnDef("db_id", ScalarType.createVarchar(StatisticConstants.MAX_NAME_LEN),
                ColumnNullableType.NOT_NULLABLE));
        PARTITION_STATS_SCHEMA.add(new ColumnDef("tbl_id", ScalarType.createVarchar(StatisticConstants.MAX_NAME_LEN),
                ColumnNullableType.NOT_NULLABLE));
        PARTITION_STATS_SCHEMA.add(new ColumnDef("idx_id", ScalarType.createVarchar(StatisticConstants.MAX_NAME_LEN),
                ColumnNullableType.NOT_NULLABLE));
        PARTITION_STATS_SCHEMA.add(new ColumnDef("part_name", ScalarType.createVarchar(StatisticConstants.MAX_NAME_LEN),
                ColumnNullableType.NOT_NULLABLE));
        PARTITION_STATS_SCHEMA.add(new ColumnDef("part_id", ScalarType.createType(PrimitiveType.BIGINT),
                ColumnNullableType.NOT_NULLABLE));
        PARTITION_STATS_SCHEMA.add(new ColumnDef("col_id", ScalarType.createVarchar(StatisticConstants.MAX_NAME_LEN),
                ColumnNullableType.NOT_NULLABLE));
        PARTITION_STATS_SCHEMA
                .add(new ColumnDef("count", ScalarType.createType(PrimitiveType.BIGINT), ColumnNullableType.NULLABLE));
        PARTITION_STATS_SCHEMA
                .add(new ColumnDef("ndv", ScalarType.createType(PrimitiveType.HLL), ColumnNullableType.NOT_NULLABLE));
        PARTITION_STATS_SCHEMA
                .add(new ColumnDef("null_count", ScalarType.createType(PrimitiveType.BIGINT),
                    ColumnNullableType.NULLABLE));
        PARTITION_STATS_SCHEMA.add(new ColumnDef("min", ScalarType.createVarchar(ScalarType.MAX_VARCHAR_LENGTH),
                ColumnNullableType.NULLABLE));
        PARTITION_STATS_SCHEMA.add(new ColumnDef("max", ScalarType.createVarchar(ScalarType.MAX_VARCHAR_LENGTH),
                ColumnNullableType.NULLABLE));
        PARTITION_STATS_SCHEMA.add(
                new ColumnDef("data_size_in_bytes", ScalarType.createType(PrimitiveType.BIGINT),
                    ColumnNullableType.NULLABLE));
        PARTITION_STATS_SCHEMA.add(
                new ColumnDef("update_time", ScalarType.DATETIMEV2,
                    ColumnNullableType.NOT_NULLABLE));

        // histogram_statistics table
        HISTO_STATS_SCHEMA = new ArrayList<>();
        HISTO_STATS_SCHEMA.add(
                new ColumnDef("id", ScalarType.createVarchar(StatisticConstants.ID_LEN),
                    ColumnNullableType.NOT_NULLABLE));
        HISTO_STATS_SCHEMA.add(new ColumnDef("catalog_id", ScalarType.createVarchar(StatisticConstants.MAX_NAME_LEN),
                ColumnNullableType.NOT_NULLABLE));
        HISTO_STATS_SCHEMA.add(new ColumnDef("db_id", ScalarType.createVarchar(StatisticConstants.MAX_NAME_LEN),
                ColumnNullableType.NOT_NULLABLE));
        HISTO_STATS_SCHEMA.add(new ColumnDef("tbl_id", ScalarType.createVarchar(StatisticConstants.MAX_NAME_LEN),
                ColumnNullableType.NOT_NULLABLE));
        HISTO_STATS_SCHEMA.add(new ColumnDef("idx_id", ScalarType.createVarchar(StatisticConstants.MAX_NAME_LEN),
                ColumnNullableType.NOT_NULLABLE));
        HISTO_STATS_SCHEMA.add(new ColumnDef("col_id", ScalarType.createVarchar(StatisticConstants.MAX_NAME_LEN),
                ColumnNullableType.NOT_NULLABLE));
        HISTO_STATS_SCHEMA.add(
                new ColumnDef("sample_rate", ScalarType.createType(PrimitiveType.DOUBLE),
                    ColumnNullableType.NOT_NULLABLE));
        HISTO_STATS_SCHEMA.add(new ColumnDef("buckets", ScalarType.createVarchar(ScalarType.MAX_VARCHAR_LENGTH),
                ColumnNullableType.NOT_NULLABLE));
        HISTO_STATS_SCHEMA.add(
                new ColumnDef("update_time", ScalarType.DATETIMEV2,
                    ColumnNullableType.NOT_NULLABLE));

        // audit table must all nullable because maybe remove some columns in feature
        AUDIT_SCHEMA = new ArrayList<>();
        // uuid and time
        AUDIT_SCHEMA.add(new ColumnDef("query_id",
                ScalarType.createVarchar(Config.label_regex_length), ColumnNullableType.NULLABLE));
        AUDIT_SCHEMA.add(new ColumnDef("time",
                ScalarType.createDatetimeV2Type(3), ColumnNullableType.NULLABLE));
        // cs info
        AUDIT_SCHEMA.add(new ColumnDef("client_ip",
                ScalarType.createVarchar(128), ColumnNullableType.NULLABLE));
        AUDIT_SCHEMA.add(new ColumnDef("user",
                ScalarType.createVarchar(128), ColumnNullableType.NULLABLE));
        AUDIT_SCHEMA.add(new ColumnDef("frontend_ip",
                ScalarType.createVarchar(1024), ColumnNullableType.NULLABLE));
        // default ctl and db
        AUDIT_SCHEMA.add(new ColumnDef("catalog",
                ScalarType.createVarchar(128), ColumnNullableType.NULLABLE));
        AUDIT_SCHEMA.add(new ColumnDef("db",
                ScalarType.createVarchar(128), ColumnNullableType.NULLABLE));
        // query state
        AUDIT_SCHEMA.add(new ColumnDef("state",
                ScalarType.createVarchar(128), ColumnNullableType.NULLABLE));
        AUDIT_SCHEMA.add(new ColumnDef("error_code",
                ScalarType.createType(PrimitiveType.INT), ColumnNullableType.NULLABLE));
        AUDIT_SCHEMA.add(new ColumnDef("error_message",
                ScalarType.createType(PrimitiveType.STRING), ColumnNullableType.NULLABLE));
        // execution info
        AUDIT_SCHEMA.add(new ColumnDef("query_time",
                ScalarType.createType(PrimitiveType.BIGINT), ColumnNullableType.NULLABLE));
        AUDIT_SCHEMA.add(new ColumnDef("queue_time_ms",
                ScalarType.createType(PrimitiveType.BIGINT), ColumnNullableType.NULLABLE));
        AUDIT_SCHEMA.add(new ColumnDef("cpu_time_ms",
                ScalarType.createType(PrimitiveType.BIGINT), ColumnNullableType.NULLABLE));
        AUDIT_SCHEMA.add(new ColumnDef("peak_memory_bytes",
                ScalarType.createType(PrimitiveType.BIGINT), ColumnNullableType.NULLABLE));
        AUDIT_SCHEMA.add(new ColumnDef("scan_bytes",
                ScalarType.createType(PrimitiveType.BIGINT), ColumnNullableType.NULLABLE));
        AUDIT_SCHEMA.add(new ColumnDef("scan_rows",
                ScalarType.createType(PrimitiveType.BIGINT), ColumnNullableType.NULLABLE));
        AUDIT_SCHEMA.add(new ColumnDef("return_rows",
                ScalarType.createType(PrimitiveType.BIGINT), ColumnNullableType.NULLABLE));
        AUDIT_SCHEMA.add(new ColumnDef("shuffle_send_rows",
                ScalarType.createType(PrimitiveType.BIGINT), ColumnNullableType.NULLABLE));
        AUDIT_SCHEMA.add(new ColumnDef("shuffle_send_bytes",
                ScalarType.createType(PrimitiveType.BIGINT), ColumnNullableType.NULLABLE));
        AUDIT_SCHEMA.add(new ColumnDef("spill_write_bytes_from_local_storage",
                ScalarType.createType(PrimitiveType.BIGINT), ColumnNullableType.NULLABLE));
        AUDIT_SCHEMA.add(new ColumnDef("spill_read_bytes_from_local_storage",
                ScalarType.createType(PrimitiveType.BIGINT), ColumnNullableType.NULLABLE));
        AUDIT_SCHEMA.add(new ColumnDef("scan_bytes_from_local_storage",
                ScalarType.createType(PrimitiveType.BIGINT), ColumnNullableType.NULLABLE));
        AUDIT_SCHEMA.add(new ColumnDef("scan_bytes_from_remote_storage",
                ScalarType.createType(PrimitiveType.BIGINT), ColumnNullableType.NULLABLE));
        // plan info
        AUDIT_SCHEMA.add(new ColumnDef("parse_time_ms",
                ScalarType.createType(PrimitiveType.INT), ColumnNullableType.NULLABLE));
        AUDIT_SCHEMA.add(new ColumnDef("plan_times_ms",
                new MapType(ScalarType.STRING, ScalarType.INT), ColumnNullableType.NULLABLE));
        AUDIT_SCHEMA.add(new ColumnDef("get_meta_times_ms",
                new MapType(ScalarType.STRING, ScalarType.INT), ColumnNullableType.NULLABLE));
        AUDIT_SCHEMA.add(new ColumnDef("schedule_times_ms",
                new MapType(ScalarType.STRING, ScalarType.INT), ColumnNullableType.NULLABLE));
        AUDIT_SCHEMA.add(new ColumnDef("hit_sql_cache",
                ScalarType.createType(PrimitiveType.TINYINT), ColumnNullableType.NULLABLE));
        AUDIT_SCHEMA.add(new ColumnDef("handled_in_fe",
                ScalarType.createType(PrimitiveType.TINYINT), ColumnNullableType.NULLABLE));
        // queried tables, views and m-views
        AUDIT_SCHEMA.add(new ColumnDef("queried_tables_and_views",
                new ArrayType(ScalarType.STRING), ColumnNullableType.NULLABLE));
        AUDIT_SCHEMA.add(new ColumnDef("chosen_m_views",
                new ArrayType(ScalarType.STRING), ColumnNullableType.NULLABLE));
        // variable and configs
        AUDIT_SCHEMA.add(new ColumnDef("changed_variables",
                new MapType(ScalarType.STRING, ScalarType.STRING), ColumnNullableType.NULLABLE));
        AUDIT_SCHEMA.add(new ColumnDef("sql_mode",
                ScalarType.createType(PrimitiveType.STRING), ColumnNullableType.NULLABLE));
        // type and digest
        AUDIT_SCHEMA.add(new ColumnDef("stmt_type",
                ScalarType.createVarchar(48), ColumnNullableType.NULLABLE));
        AUDIT_SCHEMA.add(new ColumnDef("stmt_id",
                ScalarType.createType(PrimitiveType.BIGINT), ColumnNullableType.NULLABLE));
        AUDIT_SCHEMA.add(new ColumnDef("sql_hash",
                ScalarType.createVarchar(128), ColumnNullableType.NULLABLE));
        AUDIT_SCHEMA.add(new ColumnDef("sql_digest",
                ScalarType.createVarchar(128), ColumnNullableType.NULLABLE));
        AUDIT_SCHEMA.add(new ColumnDef("is_query",
                ScalarType.createType(PrimitiveType.TINYINT), ColumnNullableType.NULLABLE));
        AUDIT_SCHEMA.add(new ColumnDef("is_nereids",
                ScalarType.createType(PrimitiveType.TINYINT), ColumnNullableType.NULLABLE));
        AUDIT_SCHEMA.add(new ColumnDef("is_internal",
                ScalarType.createType(PrimitiveType.TINYINT), ColumnNullableType.NULLABLE));
        // resource
        AUDIT_SCHEMA.add(new ColumnDef("workload_group",
                ScalarType.createType(PrimitiveType.STRING), ColumnNullableType.NULLABLE));
        AUDIT_SCHEMA.add(new ColumnDef("compute_group",
                ScalarType.createType(PrimitiveType.STRING), ColumnNullableType.NULLABLE));
        // Keep stmt as last column. So that in fe.audit.log, it will be easier to get sql string
        AUDIT_SCHEMA.add(new ColumnDef("stmt",
                ScalarType.createType(PrimitiveType.STRING), ColumnNullableType.NULLABLE));

        // ==================== SPM baselines internal table (design doc 6.14.1) ====================
        SPM_BASELINES_SCHEMA = new ArrayList<>();
        SPM_BASELINES_SCHEMA.add(new ColumnDef("id",
                ScalarType.createType(PrimitiveType.BIGINT), ColumnNullableType.NOT_NULLABLE));
        SPM_BASELINES_SCHEMA.add(new ColumnDef("bind_sql",
                ScalarType.createType(PrimitiveType.STRING), ColumnNullableType.NOT_NULLABLE));
        SPM_BASELINES_SCHEMA.add(new ColumnDef("bind_sql_digest",
                ScalarType.createType(PrimitiveType.STRING), ColumnNullableType.NOT_NULLABLE));
        SPM_BASELINES_SCHEMA.add(new ColumnDef("bind_sql_hash",
                ScalarType.createType(PrimitiveType.BIGINT), ColumnNullableType.NOT_NULLABLE));
        SPM_BASELINES_SCHEMA.add(new ColumnDef("plan_sql",
                ScalarType.createType(PrimitiveType.STRING), ColumnNullableType.NOT_NULLABLE));
        // audit_log correlation: the query id of the statement that produced the baseline
        // (CREATE BASELINE PLAN for USER baselines, the captured query for CAPTURE ones)
        SPM_BASELINES_SCHEMA.add(new ColumnDef("query_id",
                ScalarType.createVarchar(64), ColumnNullableType.NOT_NULLABLE));
        SPM_BASELINES_SCHEMA.add(new ColumnDef("cost",
                ScalarType.createType(PrimitiveType.DOUBLE), ColumnNullableType.NOT_NULLABLE));
        SPM_BASELINES_SCHEMA.add(new ColumnDef("query_time_ms",
                ScalarType.createType(PrimitiveType.BIGINT), ColumnNullableType.NOT_NULLABLE));
        SPM_BASELINES_SCHEMA.add(new ColumnDef("source",
                ScalarType.createVarchar(16), ColumnNullableType.NOT_NULLABLE));
        SPM_BASELINES_SCHEMA.add(new ColumnDef("status",
                ScalarType.createVarchar(16), ColumnNullableType.NOT_NULLABLE));
        SPM_BASELINES_SCHEMA.add(new ColumnDef("create_time",
                ScalarType.createType(PrimitiveType.DATETIME), ColumnNullableType.NOT_NULLABLE));
        SPM_BASELINES_SCHEMA.add(new ColumnDef("update_time",
                ScalarType.createType(PrimitiveType.DATETIME), ColumnNullableType.NOT_NULLABLE));

        // SPM plan-capture checkpoint (single row, id = 1): the truncated window bounds,
        // the (query_time, time, query_id) cursor and the retry state survive a leader
        // handoff / FE restart. JSON text for the two maps keeps the encoding trivial and
        // bounded (the writer caps the entry count).
        SPM_CAPTURE_CHECKPOINT_SCHEMA = new ArrayList<>();
        SPM_CAPTURE_CHECKPOINT_SCHEMA.add(new ColumnDef("id",
                ScalarType.createType(PrimitiveType.BIGINT), ColumnNullableType.NOT_NULLABLE));
        SPM_CAPTURE_CHECKPOINT_SCHEMA.add(new ColumnDef("last_scan_timestamp",
                ScalarType.createType(PrimitiveType.BIGINT), ColumnNullableType.NOT_NULLABLE));
        SPM_CAPTURE_CHECKPOINT_SCHEMA.add(new ColumnDef("pending_window_start",
                ScalarType.createType(PrimitiveType.BIGINT), ColumnNullableType.NOT_NULLABLE));
        SPM_CAPTURE_CHECKPOINT_SCHEMA.add(new ColumnDef("pending_window_end",
                ScalarType.createType(PrimitiveType.BIGINT), ColumnNullableType.NOT_NULLABLE));
        SPM_CAPTURE_CHECKPOINT_SCHEMA.add(new ColumnDef("cursor_query_time",
                ScalarType.createType(PrimitiveType.BIGINT), ColumnNullableType.NOT_NULLABLE));
        SPM_CAPTURE_CHECKPOINT_SCHEMA.add(new ColumnDef("cursor_time",
                ScalarType.createVarchar(4096), ColumnNullableType.NOT_NULLABLE));
        SPM_CAPTURE_CHECKPOINT_SCHEMA.add(new ColumnDef("cursor_query_id",
                ScalarType.createVarchar(1024), ColumnNullableType.NOT_NULLABLE));
        SPM_CAPTURE_CHECKPOINT_SCHEMA.add(new ColumnDef("failed_attempts",
                ScalarType.createType(PrimitiveType.STRING), ColumnNullableType.NOT_NULLABLE));
        SPM_CAPTURE_CHECKPOINT_SCHEMA.add(new ColumnDef("retry_queue",
                ScalarType.createType(PrimitiveType.STRING), ColumnNullableType.NOT_NULLABLE));
        SPM_CAPTURE_CHECKPOINT_SCHEMA.add(new ColumnDef("update_time",
                ScalarType.createType(PrimitiveType.DATETIME), ColumnNullableType.NOT_NULLABLE));
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
            case SPM_BASELINES_TBL_NAME:
                schema = SPM_BASELINES_SCHEMA;
                break;
            case SPM_CAPTURE_CHECKPOINT_TBL_NAME:
                schema = SPM_CAPTURE_CHECKPOINT_SCHEMA;
                break;
            default:
                throw new UserException("Unknown internal table name: " + tblName);
        }
        List<ColumnDef> copiedSchema = Lists.newArrayList();
        for (ColumnDef columnDef : schema) {
            copiedSchema.add(new ColumnDef(columnDef.getName(), columnDef.getType(), columnDef.isAllowNull()));
        }
        return copiedSchema;
    }
}
