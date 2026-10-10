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
     * Name of the cluster-wide audit PUBLICATION horizon internal table: one row per FE
     * carrying the start time of the OLDEST audit event that FE has accepted but not yet
     * published (queued in its audit pipeline, or a batch whose stream load reported
     * Publish Timeout). The SPM capture runs on the leader only and scans the shared
     * audit table, so a row another FE still owes is invisible to it: the leader must
     * fence its scan-window floor with the MINIMUM over every alive FE's row, or a
     * follower's delayed event falls behind the advanced watermark and is never
     * captured. A row whose update_time is older than the reporter's freshness window is
     * ignored (its FE stopped reporting - the events are gone with it).
     */
    public static final String SPM_AUDIT_HORIZON_TBL_NAME = "spm_audit_horizon";

    // Do not use the original schema directly, because it may be modified by create table operation.
    public static final List<ColumnDef> TABLE_STATS_SCHEMA;
    public static final List<ColumnDef> PARTITION_STATS_SCHEMA;
    public static final List<ColumnDef> HISTO_STATS_SCHEMA;
    public static final List<ColumnDef> AUDIT_SCHEMA;
    public static final List<ColumnDef> SPM_BASELINES_SCHEMA;
    public static final List<ColumnDef> SPM_BASELINES_SEQ_SCHEMA;
    public static final List<ColumnDef> SPM_BASELINES_HWM_SCHEMA;
    public static final List<ColumnDef> SPM_CAPTURE_CHECKPOINT_SCHEMA;
    public static final List<ColumnDef> SPM_AUDIT_HORIZON_SCHEMA;

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
                ScalarType.createType(PrimitiveType.DATETIME), ColumnNullableType.NOT_NULLABLE));
        SPM_BASELINES_SCHEMA.add(new ColumnDef("update_time",
                ScalarType.createType(PrimitiveType.DATETIME), ColumnNullableType.NOT_NULLABLE));
        // parser-relevant sql_mode bits of the CREATING session (PIPES_AS_CONCAT / ...):
        // the stored bindSql is user-authored text and must be re-parsed with the mode it
        // was created under. NULLABLE so an upgraded cluster can add the column without
        // a default (a missing / NULL value means MODE_DEFAULT).
        SPM_BASELINES_SCHEMA.add(new ColumnDef("sql_mode",
                ScalarType.createType(PrimitiveType.BIGINT), ColumnNullableType.NULLABLE));
        // parser mode of the STORED planSql: MODE_DEFAULT for the SPM decompiled frozen
        // rendering, the CREATOR's mode for the raw user planSql kept as the fallback when
        // the physical plan cannot be decompiled. NULLABLE like sql_mode (NULL = default).
        SPM_BASELINES_SCHEMA.add(new ColumnDef("plan_sql_mode",
                ScalarType.createType(PrimitiveType.BIGINT), ColumnNullableType.NULLABLE));
        // explicit provenance of the stored planSql: TRUE = SPM decompiled,
        // placeholder-carrying frozen text replayed as text; FALSE = ordinary user text
        // (raw fallback / in-memory engine) whose parameterized tree must be rebuilt.
        // NULLABLE so a reload of pre-column rows falls back to parsing-based
        // classification (NULL = unknown).
        SPM_BASELINES_SCHEMA.add(new ColumnDef("plan_frozen",
                ScalarType.createType(PrimitiveType.BOOLEAN), ColumnNullableType.NULLABLE));
        // CREATE-time schema identity of the referenced base tables (sorted
        // name|tableId|schemaHash entries): validated before every frozen replay so an
        // ALTER TABLE ... ADD COLUMN / DROP + CREATE cannot keep matching a frozen plan
        // that still emits the creator-time output columns. STRING, not VARCHAR(4096):
        // the entry list grows with every DISTINCT referenced table and has no length
        // cap (a valid UNION ALL over dozens of long-named tables exceeds 4096 chars),
        // and a persistence failure here fails the whole GLOBAL CREATE ( -
        // same precedent as the seq table's bind_sql_digest). NULLABLE (NULL / empty =
        // a pre-column row: no validation possible).
        SPM_BASELINES_SCHEMA.add(new ColumnDef("schema_fingerprint",
                ScalarType.createType(PrimitiveType.STRING), ColumnNullableType.NULLABLE));
        // canonical digest of the SUBMITTED plan SQL text (value-independent): two
        // baselines may share the bind digest while carrying DIFFERENT plan texts, so
        // the persisted plan-side identity is what lets a follower (and the forwarded
        // DELETE / UPDATE tombstone logic) attribute a row to the exact statement that
        // created it. NULLABLE (NULL / empty = a pre-column row: the plan side of the
        // identity is not usable and only the bind digest can be compared).
        SPM_BASELINES_SCHEMA.add(new ColumnDef("plan_sql_digest",
                ScalarType.createType(PrimitiveType.STRING), ColumnNullableType.NULLABLE));

        // SPM baseline id sequence (append-only, id = 1): every row records one id the
        // create path has reserved. The baseline table itself cannot be the watermark:
        // DROP removes rows, so MAX(id) of the table falls back and an id could be reused
        // for a different baseline - a delayed DROP-by-id retry for the old row would then
        // delete the new one. MAX(last_id) over these rows never decreases.
        //
        // The reservation ALSO carries the baseline's identity (bind_sql_digest +
        // plan_sql_hash, with reserve_time): a create that COMMITTED but is not readable
        // yet is invisible to the durable-key dedup, so a retry running on another FE
        // (leader handoff / restart, where the in-memory pending registry is empty) must
        // find and respect the reservation here instead of allocating a second id for the
        // same baseline. Rows written before these columns existed carry
        // NULL and are skipped by the identity lookup; they reach no in-flight create
        // anyway (they are long published), and reserve_time NULL ages them out.
        SPM_BASELINES_SEQ_SCHEMA = new ArrayList<>();
        SPM_BASELINES_SEQ_SCHEMA.add(new ColumnDef("id",
                ScalarType.createType(PrimitiveType.BIGINT), ColumnNullableType.NOT_NULLABLE));
        SPM_BASELINES_SEQ_SCHEMA.add(new ColumnDef("last_id",
                ScalarType.createType(PrimitiveType.BIGINT), ColumnNullableType.NOT_NULLABLE));
        // STRING, not VARCHAR(4096): the digest is the CANONICAL rendering of the
        // whole bind statement (SPMPlanner stores BaselinePlan#bindSqlDigest from
        // LogicalPlan#toSpmDigest without a length cap - spm_baselines.bind_sql_digest
        // is STRING for the same reason), and a valid SELECT with hundreds of projected
        // expressions exceeds 4096 chars. A fixed VARCHAR then failed the RESERVATION
        // INSERT (which runs before the baseline row), so a valid GLOBAL CREATE errored
        // out before writing anything.
        SPM_BASELINES_SEQ_SCHEMA.add(new ColumnDef("bind_sql_digest",
                ScalarType.createType(PrimitiveType.STRING), ColumnNullableType.NULLABLE));
        SPM_BASELINES_SEQ_SCHEMA.add(new ColumnDef("plan_sql_hash",
                ScalarType.createType(PrimitiveType.BIGINT), ColumnNullableType.NULLABLE));
        SPM_BASELINES_SEQ_SCHEMA.add(new ColumnDef("reserve_time",
                ScalarType.createType(PrimitiveType.DATETIME), ColumnNullableType.NULLABLE));
        // 1 = the marker of an AMBIGUOUS create: only these rows drive the
        // durable pending-create fence - a plain reservation exists for every create
        // (successful ones included) and must never block a legitimate re-create.
        SPM_BASELINES_SEQ_SCHEMA.add(new ColumnDef("unconfirmed",
                ScalarType.createType(PrimitiveType.BIGINT), ColumnNullableType.NULLABLE));
        // 1 = a DROP TOMBSTONE: the identity of a baseline this FE deleted.
        // A demoted master's in-flight status INSERT can commit AFTER the DROP removed the
        // row (its conditional precondition ran against the pre-DROP snapshot and Doris
        // cannot re-check it at commit) - the revived row would look like an ACTIVE
        // baseline again. The append-only tombstone survives that commit, and every load
        // that sees a row matching (id, bind_sql_digest, plan_sql_hash) treats it as
        // deleted (ids are never reused, so a matching tombstone refers to this very
        // incarnation) and repairs it away.
        SPM_BASELINES_SEQ_SCHEMA.add(new ColumnDef("dropped",
                ScalarType.createType(PrimitiveType.BIGINT), ColumnNullableType.NULLABLE));

        // The COMPACT id high-water mark (see
        // SPM_BASELINES_HWM_TBL_NAME): append-only rows (id = 1) carrying the newest
        // allocated id, so the id watermark read stays bounded however many creates the
        // cluster has served. The rows are pruned after every write (best effort); the
        // read takes the MAX, so a delayed superseded row can never regress it.
        SPM_BASELINES_HWM_SCHEMA = new ArrayList<>();
        SPM_BASELINES_HWM_SCHEMA.add(new ColumnDef("id",
                ScalarType.createType(PrimitiveType.BIGINT), ColumnNullableType.NOT_NULLABLE));
        SPM_BASELINES_HWM_SCHEMA.add(new ColumnDef("last_id",
                ScalarType.createType(PrimitiveType.BIGINT), ColumnNullableType.NOT_NULLABLE));
        SPM_BASELINES_HWM_SCHEMA.add(new ColumnDef("update_time",
                ScalarType.createType(PrimitiveType.DATETIME), ColumnNullableType.NOT_NULLABLE));
        // The bounded mutation clock (see BaselineManager#bumpMutationClock): every
        // baseline mutation advances it BEFORE its row write, and a snapshot read compares
        // (MAX(tick), COUNT(*), SUM(tick)) around its page loop. NULLABLE: a slot written
        // before the column existed (an upgraded cluster) carries NULL and simply reads
        // as 0 - the next mutation re-writes the slot with a real tick.
        SPM_BASELINES_HWM_SCHEMA.add(new ColumnDef("tick",
                ScalarType.createType(PrimitiveType.BIGINT), ColumnNullableType.NULLABLE));
        // The OPEN mutation window (see BaselineManager#beginRowMutation): the tick of the
        // mutation whose row statement is in flight; 0 / NULL when none. A snapshot read
        // overlapping an open window retries. NULLABLE: rows written before the column
        // existed read as 0 (no window).
        SPM_BASELINES_HWM_SCHEMA.add(new ColumnDef("pending",
                ScalarType.createType(PrimitiveType.BIGINT), ColumnNullableType.NULLABLE));

        // SPM plan-capture checkpoint (APPEND-ONLY, every row carries the
        // fixed id = 1): the truncated window bounds, the FULL cursor (time, query_time,
        // query_id + the encoded tie-breaker tail) and the retry state survive a leader
        // handoff / FE restart. JSON text for the two maps keeps the encoding trivial
        // and bounded (the writer caps the entry count). cursor_tail is the JSON tail of
        // the ORDER BY key tuple (see AuditLogScanner#CursorTail): it keeps rows sharing
        // (time, query_time,
        // query_id) - e.g. a page of NULL query ids - from looping or being skipped
        // after a handoff; a legacy row without it re-scans its pending window.
        // min_query_time_ms / min_scan_rows are the capture thresholds the PENDING
        // window was OPENED with (-1 = no pending window): the audit SQL and the
        // in-memory filter must both use the window's own values, so a window truncated
        // before a `SET GLOBAL plan_capture_min_query_time_ms` keeps scanning with the
        // thresholds its already-consumed rows were judged by. include_pattern /
        // exclude_pattern are the table-name regexes of that same snapshot (empty = none):
        // a pattern change mid-window must not terminally filter away rows the window's
        // earlier pages had admitted. scan_zone is the zone ID of the last scan pass (the
        // zone its rendered bounds were written in): after `SET GLOBAL time_zone` the next
        // pass must render the window in the previous zone too, or rows already stored
        // under it become unreachable (see PlanCaptureManager).
        SPM_CAPTURE_CHECKPOINT_SCHEMA = new ArrayList<>();
        // (leader_epoch, write_seq) is the row's WRITE TOKEN and the table's key: the
        // writer's max journal id plus a per-process write counter. The
        // reader takes the GREATEST row. A demoted FE's write forwards to the new master
        // and executes there, where no statement precondition can stop it -
        // under the old single-row UNIQUE model it could only be REFUSED, and a refusal
        // the fenced writer did not expect (or a timeout AFTER the deferred commit) left
        // the store without its newest row. Append-only turns the same delayed write into
        // a harmless EXTRA row: it can never destroy the row that was already there, and
        // the next read (and the leader's next write) ignores it. leader_epoch is the
        // writer's max journal id, a cluster-wide monotonic token; write_seq orders the
        // writes of ONE epoch, so even two same-epoch writers cannot hide each other.
        SPM_CAPTURE_CHECKPOINT_SCHEMA.add(new ColumnDef("leader_epoch",
                ScalarType.createType(PrimitiveType.BIGINT), ColumnNullableType.NOT_NULLABLE));
        SPM_CAPTURE_CHECKPOINT_SCHEMA.add(new ColumnDef("write_seq",
                ScalarType.createType(PrimitiveType.BIGINT), ColumnNullableType.NOT_NULLABLE));
        // the fixed row payload id (always 1); no longer a uniqueness key
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
        SPM_CAPTURE_CHECKPOINT_SCHEMA.add(new ColumnDef("cursor_tail",
                ScalarType.createVarchar(4096), ColumnNullableType.NOT_NULLABLE));
        SPM_CAPTURE_CHECKPOINT_SCHEMA.add(new ColumnDef("failed_attempts",
                ScalarType.createType(PrimitiveType.STRING), ColumnNullableType.NOT_NULLABLE));
        SPM_CAPTURE_CHECKPOINT_SCHEMA.add(new ColumnDef("retry_queue",
                ScalarType.createType(PrimitiveType.STRING), ColumnNullableType.NOT_NULLABLE));
        SPM_CAPTURE_CHECKPOINT_SCHEMA.add(new ColumnDef("min_query_time_ms",
                ScalarType.createType(PrimitiveType.BIGINT), ColumnNullableType.NOT_NULLABLE));
        SPM_CAPTURE_CHECKPOINT_SCHEMA.add(new ColumnDef("min_scan_rows",
                ScalarType.createType(PrimitiveType.BIGINT), ColumnNullableType.NOT_NULLABLE));
        // STRING, not VARCHAR(4096): SET GLOBAL accepts ANY compiling regex, so a valid
        // pattern longer than a fixed VARCHAR silently failed at the reservation INSERT
        // and the capture cycle returned before scanning (a valid 4097-byte ASCII pattern
        // could never be stored). STRING matches the retry-queue columns' headroom.
        SPM_CAPTURE_CHECKPOINT_SCHEMA.add(new ColumnDef("include_pattern",
                ScalarType.createType(PrimitiveType.STRING), ColumnNullableType.NOT_NULLABLE));
        SPM_CAPTURE_CHECKPOINT_SCHEMA.add(new ColumnDef("exclude_pattern",
                ScalarType.createType(PrimitiveType.STRING), ColumnNullableType.NOT_NULLABLE));
        // scan_zone is the session time_zone (zone ID) the CURRENT scan pass renders its
        // bounds in - the zone the scanned rows' `time` columns were WRITTEN in. audit_log
        // stores local wall-clock DATETIMEs and the writer follows the global time_zone,
        // so after `SET GLOBAL time_zone` the rows of the OLD rendering are invisible to
        // bounds rendered in the new zone: the next pass must first render the window in
        // the OLD zone again (see PlanCaptureManager / AuditLogScanner#zoneOfTail). Empty
        // = never scanned (a fresh process follows the current global zone).
        SPM_CAPTURE_CHECKPOINT_SCHEMA.add(new ColumnDef("scan_zone",
                ScalarType.createType(PrimitiveType.STRING), ColumnNullableType.NOT_NULLABLE));
        SPM_CAPTURE_CHECKPOINT_SCHEMA.add(new ColumnDef("update_time",
                ScalarType.createType(PrimitiveType.DATETIME), ColumnNullableType.NOT_NULLABLE));

        // The cluster-wide audit publication horizon (see SPM_AUDIT_HORIZON_TBL_NAME):
        // fe_name is the FE identity reported by the audit events themselves
        // (feIp:port at the event's write time is FE-local, so the FE uses its own
        // name); horizon_ms is the start time of the oldest event that FE has accepted
        // but not published (0 = nothing outstanding), and update_time lets the reader
        // skip rows of an FE that stopped reporting. The row's size is constant, one
        // row per FE. writer_zones is the audit WRITER's zone history of that FE
        // (zoneId=lastRenderedMillis pairs, see AuditWriterZones): audit_log.time stores
        // the writer's local rendering, so the capture must scan a window in EVERY zone
        // that can own rows before completing it - a zone change is invisible to the
        // leader's own observations when it happens between two capture cycles
        // #3). committed_fence_ms is the oldest batch this FE sent whose outcome is
        // AMBIGUOUS (Publish Timeout / an error that may hide a commit): such a batch's
        // rows can still PUBLISH after the FE dies, so the reader must keep fencing for
        // it even when the FE is provably gone - only an ordinary (never sent) backlog
        // dies with its FE. committed_fence_labels lists the load
        // labels of those batches (oldest first, "-" for an unknown label): a DEAD FE
        // cannot re-report, so the reader resolves each transaction by its label and
        // keeps the fence until the LAST of them is terminal instead of releasing it
        // on the age bound alone.
        SPM_AUDIT_HORIZON_SCHEMA = new ArrayList<>();
        SPM_AUDIT_HORIZON_SCHEMA.add(new ColumnDef("fe_name",
                ScalarType.createVarchar(128), ColumnNullableType.NOT_NULLABLE));
        SPM_AUDIT_HORIZON_SCHEMA.add(new ColumnDef("horizon_ms",
                ScalarType.createType(PrimitiveType.BIGINT), ColumnNullableType.NOT_NULLABLE));
        SPM_AUDIT_HORIZON_SCHEMA.add(new ColumnDef("update_time",
                ScalarType.createType(PrimitiveType.DATETIME), ColumnNullableType.NOT_NULLABLE));
        SPM_AUDIT_HORIZON_SCHEMA.add(new ColumnDef("writer_zones",
                ScalarType.createType(PrimitiveType.STRING), ColumnNullableType.NULLABLE));
        SPM_AUDIT_HORIZON_SCHEMA.add(new ColumnDef("committed_fence_ms",
                ScalarType.createType(PrimitiveType.BIGINT), ColumnNullableType.NULLABLE));
        SPM_AUDIT_HORIZON_SCHEMA.add(new ColumnDef("committed_fence_labels",
                ScalarType.createType(PrimitiveType.STRING), ColumnNullableType.NULLABLE));
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
            case SPM_CAPTURE_CHECKPOINT_TBL_NAME:
                schema = SPM_CAPTURE_CHECKPOINT_SCHEMA;
                break;
            case SPM_AUDIT_HORIZON_TBL_NAME:
                schema = SPM_AUDIT_HORIZON_SCHEMA;
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
