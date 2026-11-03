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
     * Name of the SPM baseline id sequence internal table: append-only rows recording the
     * ids the create path has ALLOCATED. The id of a baseline must never be handed out
     * twice, but MAX(id) of the baselines table loses the highest id as soon as
     * its row is DROPped - a FE that never saw that id (a follower promoting after the
     * drop) would allocate it again for a DIFFERENT baseline, and a delayed
     * DROP BASELINE PLAN IF EXISTS N retry would then delete the new baseline.
     * The sequence survives the delete, so the watermark read is the MAX of BOTH.
     */
    public static final String SPM_BASELINES_SEQ_TBL_NAME = "spm_baselines_seq";

    /**
     * Name of the COMPACT SPM baseline id high-water-mark internal table:
     * a tiny append-only table whose surviving rows carry the newest allocated id. The
     * append-only sequence history grows by one row per create forever, so its
     * MAX(last_id) - the only unbounded read on the id-allocation path, and the
     * one that made GLOBAL CREATE fail once the scan outgrew its fixed timeout - is
     * answered from here: every allocation appends its id, superseded rows are pruned,
     * and a cluster upgraded from before this table pays the legacy history read ONCE.
     */
    public static final String SPM_BASELINES_HWM_TBL_NAME = "spm_baselines_hwm";

    /**
     * The scale of every SPM internal-table TIMESTAMPTZ column (microseconds).
     *
     * SECONDS (scale 0) were not enough: a status flip orders its row above the id's newest
     * stored instant, and at second precision two flips inside one second produced EQUAL values -
     * so the ordering had to be forced by advancing the stored instant by a WHOLE second, which in
     * turn made rapid alternating ALTERs push visible rows far ahead of wall time (see
     * BaselineManager#MAX_STORED_UPDATE_LEAD_MICROS).
     *
     * The stored instant is epoch MICROSECONDS end to end: the writers render it with
     * BaselineManager#toTs, the readers take it from the column itself with UNIX_TIMESTAMP (which
     * returns the SECOND value with six fractional digits), and nothing on the path scales it by
     * 1000 - so the column keeps every digit that was written instead of a rounded millisecond.
     */
    public static final int SPM_TIMESTAMP_SCALE = 6;

    // Do not use the original schema directly, because it may be modified by create table operation.
    public static final List<ColumnDef> TABLE_STATS_SCHEMA;
    public static final List<ColumnDef> PARTITION_STATS_SCHEMA;
    public static final List<ColumnDef> HISTO_STATS_SCHEMA;
    public static final List<ColumnDef> AUDIT_SCHEMA;
    public static final List<ColumnDef> SPM_BASELINES_SCHEMA;
    public static final List<ColumnDef> SPM_BASELINES_SEQ_SCHEMA;
    public static final List<ColumnDef> SPM_BASELINES_HWM_SCHEMA;

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
        AUDIT_SCHEMA.add(new ColumnDef("spill_write_bytes_to_remote_storage",
                ScalarType.createType(PrimitiveType.BIGINT), ColumnNullableType.NULLABLE));
        AUDIT_SCHEMA.add(new ColumnDef("spill_read_bytes_from_remote_storage",
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
        // the protocol the session runs: MySQL / ArrowFlightSQL
        AUDIT_SCHEMA.add(new ColumnDef("protocol",
                ScalarType.createVarchar(16), ColumnNullableType.NULLABLE));
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
                ScalarType.createTimeStampTzType(SPM_TIMESTAMP_SCALE),
                ColumnNullableType.NOT_NULLABLE));
        SPM_BASELINES_SCHEMA.add(new ColumnDef("update_time",
                ScalarType.createTimeStampTzType(SPM_TIMESTAMP_SCALE),
                ColumnNullableType.NOT_NULLABLE));
        // parser-relevant sql_mode bits of the CREATING session (PIPES_AS_CONCAT / ...):
        // the stored bindSql is user-authored text and must be re-parsed with the mode it
        // was created under.
        SPM_BASELINES_SCHEMA.add(new ColumnDef("sql_mode",
                ScalarType.createType(PrimitiveType.BIGINT), ColumnNullableType.NOT_NULLABLE));
        // parser mode of the STORED planSql: MODE_DEFAULT for the SPM decompiled frozen
        // rendering, the CREATOR's mode for the raw user planSql kept as the fallback when
        // the physical plan cannot be decompiled.
        SPM_BASELINES_SCHEMA.add(new ColumnDef("plan_sql_mode",
                ScalarType.createType(PrimitiveType.BIGINT), ColumnNullableType.NOT_NULLABLE));
        // explicit provenance of the stored planSql: TRUE = SPM decompiled,
        // placeholder-carrying frozen text replayed as text; FALSE = ordinary user text
        // (raw fallback / in-memory engine) whose parameterized tree must be rebuilt.
        SPM_BASELINES_SCHEMA.add(new ColumnDef("plan_frozen",
                ScalarType.createType(PrimitiveType.BOOLEAN), ColumnNullableType.NOT_NULLABLE));
        // CREATE-time schema identity of the referenced base tables (sorted
        // name|tableId|schemaHash entries): validated before every frozen replay so an
        // ALTER TABLE ... ADD COLUMN / DROP + CREATE cannot keep matching a frozen plan
        // that still emits the creator-time output columns. STRING, not VARCHAR(4096):
        // the entry list grows with every DISTINCT referenced table and has no length
        // cap (a valid UNION ALL over dozens of long-named tables exceeds 4096 chars),
        // and a persistence failure here fails the whole GLOBAL CREATE ( -
        // same precedent as the seq table's bind_sql_digest).
        SPM_BASELINES_SCHEMA.add(new ColumnDef("schema_fingerprint",
                ScalarType.createType(PrimitiveType.STRING), ColumnNullableType.NOT_NULLABLE));
        // canonical digest of the SUBMITTED plan SQL text (value-independent): two
        // baselines may share the bind digest while carrying DIFFERENT plan texts, so
        // the persisted plan-side identity is what lets a follower (and the forwarded
        // DELETE / UPDATE tombstone logic) attribute a row to the exact statement that
        // created it.
        SPM_BASELINES_SCHEMA.add(new ColumnDef("plan_sql_digest",
                ScalarType.createType(PrimitiveType.STRING), ColumnNullableType.NOT_NULLABLE));

        // SPM baseline id sequence (append-only, id = 1): every row records one id the
        // create path has reserved. The baseline table itself cannot be the watermark:
        // DROP removes rows, so MAX(id) of the table falls back and an id could be reused
        // for a different baseline - a delayed DROP-by-id retry for the old row would then
        // delete the new one. MAX(baseline_id) over these rows never decreases.
        //
        // The reservation ALSO carries the baseline's identity (bind_sql_digest +
        // plan_sql_hash, with reserve_time): a create that COMMITTED but is not readable
        // yet is invisible to the durable-key dedup, so a retry running on another FE
        // (leader handoff / restart, where the in-memory pending registry is empty) must
        // find and respect the reservation here instead of allocating a second id for the
        // same baseline.
        //
        // EVERY column is NOT NULL: this release publishes the schema for the first time,
        // so there is no pre-column row to decode - a missing value would be a writer bug,
        // not a legacy shape, and the NOT NULL declaration makes the engine reject it
        // instead of the reader guessing what NULL means.
        SPM_BASELINES_SEQ_SCHEMA = new ArrayList<>();
        SPM_BASELINES_SEQ_SCHEMA.add(new ColumnDef("id",
                ScalarType.createType(PrimitiveType.BIGINT), ColumnNullableType.NOT_NULLABLE));
        // the RESERVED baseline id (the column the watermark read takes MAX over)
        SPM_BASELINES_SEQ_SCHEMA.add(new ColumnDef("baseline_id",
                ScalarType.createType(PrimitiveType.BIGINT), ColumnNullableType.NOT_NULLABLE));
        // STRING, not VARCHAR(4096): the digest is the CANONICAL rendering of the
        // whole bind statement (SPMPlanner stores BaselinePlan#bindSqlDigest from
        // LogicalPlan#toSpmDigest without a length cap - spm_baselines.bind_sql_digest
        // is STRING for the same reason), and a valid SELECT with hundreds of projected
        // expressions exceeds 4096 chars. A fixed VARCHAR then failed the RESERVATION
        // INSERT (which runs before the baseline row), so a valid GLOBAL CREATE errored
        // out before writing anything.
        SPM_BASELINES_SEQ_SCHEMA.add(new ColumnDef("bind_sql_digest",
                ScalarType.createType(PrimitiveType.STRING), ColumnNullableType.NOT_NULLABLE));
        SPM_BASELINES_SEQ_SCHEMA.add(new ColumnDef("plan_sql_hash",
                ScalarType.createType(PrimitiveType.BIGINT), ColumnNullableType.NOT_NULLABLE));
        SPM_BASELINES_SEQ_SCHEMA.add(new ColumnDef("reserve_time",
                ScalarType.createTimeStampTzType(SPM_TIMESTAMP_SCALE),
                ColumnNullableType.NOT_NULLABLE));
        // 1 = the marker of an AMBIGUOUS create: only these rows drive the
        // durable pending-create fence - a plain reservation exists for every create
        // (successful ones included) and must never block a legitimate re-create.
        SPM_BASELINES_SEQ_SCHEMA.add(new ColumnDef("unconfirmed",
                ScalarType.createType(PrimitiveType.BIGINT), ColumnNullableType.NOT_NULLABLE));
        // 1 = a DROP TOMBSTONE: the identity of a baseline this FE deleted.
        // A demoted master's in-flight status INSERT can commit AFTER the DROP removed the
        // row (its conditional precondition ran against the pre-DROP snapshot and Doris
        // cannot re-check it at commit) - the revived row would look like an ACTIVE
        // baseline again. The append-only tombstone survives that commit, and every load
        // that sees a row matching (id, bind_sql_digest, plan_sql_hash) treats it as
        // deleted (ids are never reused, so a matching tombstone refers to this very
        // incarnation) and repairs it away.
        SPM_BASELINES_SEQ_SCHEMA.add(new ColumnDef("dropped",
                ScalarType.createType(PrimitiveType.BIGINT), ColumnNullableType.NOT_NULLABLE));

        // The COMPACT id high-water mark (see
        // SPM_BASELINES_HWM_TBL_NAME): append-only rows carrying the newest
        // allocated id, so the id watermark read stays bounded however many creates the
        // cluster has served. The rows are pruned after every write (best effort); the
        // read takes the MAX, so a delayed superseded row can never regress it.
        SPM_BASELINES_HWM_SCHEMA = new ArrayList<>();
        // the compact id high-water mark the create path resumes from
        SPM_BASELINES_HWM_SCHEMA.add(new ColumnDef("watermark",
                ScalarType.createType(PrimitiveType.BIGINT), ColumnNullableType.NOT_NULLABLE));
        SPM_BASELINES_HWM_SCHEMA.add(new ColumnDef("update_time",
                ScalarType.createTimeStampTzType(SPM_TIMESTAMP_SCALE),
                ColumnNullableType.NOT_NULLABLE));
        // The bounded mutation clock (see BaselineManager#bumpMutationClock): every
        // baseline mutation advances it BEFORE its row write, and a snapshot read compares
        // (MAX(tick), COUNT(*), SUM(tick)) around its page loop.
        SPM_BASELINES_HWM_SCHEMA.add(new ColumnDef("tick",
                ScalarType.createType(PrimitiveType.BIGINT), ColumnNullableType.NOT_NULLABLE));
        // The OPEN mutation window (see BaselineManager#beginRowMutation): the tick of the
        // mutation whose row statement is in flight; 0 when none. A snapshot read
        // overlapping an open window retries.
        SPM_BASELINES_HWM_SCHEMA.add(new ColumnDef("pending",
                ScalarType.createType(PrimitiveType.BIGINT), ColumnNullableType.NOT_NULLABLE));

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
            case SPM_BASELINES_SEQ_TBL_NAME:
                schema = SPM_BASELINES_SEQ_SCHEMA;
                break;
            case SPM_BASELINES_HWM_TBL_NAME:
                schema = SPM_BASELINES_HWM_SCHEMA;
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
