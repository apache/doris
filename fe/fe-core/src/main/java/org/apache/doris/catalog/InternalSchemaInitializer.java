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
import org.apache.doris.analysis.DbName;
import org.apache.doris.catalog.info.ColumnPosition;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.ha.FrontendNodeType;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.AlterTableCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateDatabaseCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateTableCommand;
import org.apache.doris.nereids.trees.plans.commands.info.AddColumnOp;
import org.apache.doris.nereids.trees.plans.commands.info.AddColumnsOp;
import org.apache.doris.nereids.trees.plans.commands.info.AlterTableOp;
import org.apache.doris.nereids.trees.plans.commands.info.ColumnDefinition;
import org.apache.doris.nereids.trees.plans.commands.info.ModifyColumnOp;
import org.apache.doris.nereids.trees.plans.commands.info.ModifyPartitionOp;
import org.apache.doris.nereids.trees.plans.commands.info.ReorderColumnsOp;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.plugin.audit.AuditLoader;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SqlModeHelper;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.statistics.StatisticConstants;
import org.apache.doris.statistics.repository.ResultRow;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;


public class InternalSchemaInitializer extends Thread {

    private static final Logger LOG = LogManager.getLogger(InternalSchemaInitializer.class);
    private static boolean StatsTableSchemaValid = false;

    public InternalSchemaInitializer() {
        super("InternalSchemaInitializer");
    }

    public void run() {
        if (!FeConstants.enableInternalSchemaDb) {
            return;
        }
        modifyColumnStatsTblSchema();
        while (!created()) {
            try {
                FrontendNodeType feType = Env.getCurrentEnv().getFeType();
                if (feType.equals(FrontendNodeType.INIT) || feType.equals(FrontendNodeType.UNKNOWN)) {
                    LOG.warn("FE is not ready");
                    Thread.sleep(Config.resource_not_ready_sleep_seconds * 1000);
                    continue;
                }
                Thread.currentThread()
                        .join(Config.resource_not_ready_sleep_seconds * 1000L);
                createDb();
                createTbl();
            } catch (Throwable e) {
                LOG.warn("Statistics storage initiated failed, will try again later", e);
                try {
                    Thread.sleep(Config.resource_not_ready_sleep_seconds * 1000);
                } catch (InterruptedException ex) {
                    LOG.info("Sleep interrupted. {}", ex.getMessage());
                }
            }
        }
        LOG.info("Internal schema is initialized");
        Optional<Database> op
                = Env.getCurrentEnv().getInternalCatalog().getDb(StatisticConstants.DB_NAME);
        if (!op.isPresent()) {
            LOG.warn("Internal DB got deleted!");
            return;
        }
        Database database = op.get();
        // Runs even when every table already exists: an upgraded cluster must gain the
        // SPM provenance columns (sql_mode / plan_sql_mode / plan_frozen /
        // schema_fingerprint) although the completion gate no longer calls createTbl().
        // Waits until all columns are OBSERVED: run() reaches this point only once and the
        // replica-upgrade loop below never comes back, so a transient ALTER failure was
        // never retried in this process - the table stayed without the columns although
        // BaselineManager always reads / writes them, and baseline loading plus global
        // DDL stayed broken until a restart. Must precede the replica-upgrade loop below:
        // that loop WAITS for enough BEs (sleeping), so anything after it would be
        // deferred indefinitely on a small cluster.
        ensureSpmBaselinesColumnsExist();
        // Same reasoning for the capture checkpoint: an upgraded cluster gains the cursor
        // tail column here, and the capture daemon's checkpoint UPSERT writes it - a
        // missing column would make every checkpoint write fail, silently losing the
        // leader-handoff cursor.
        ensureSpmCaptureCheckpointColumnsExist();
        // ... and for the two SPM side tables: the id reservation rows carry the pending
        // create's identity and the horizon rows carry each FE's audit
        // writer zone history; the writers always fill them.
        ensureSpmBaselinesSeqColumnsExist();
        // ... and the HWM slot gains its bounded mutation-clock column (tick): every
        // baseline mutation advances it (see BaselineManager#bumpMutationClock) and the
        // paginated snapshot read compares it around its page loop.
        ensureSpmBaselinesHwmColumnsExist();
        ensureSpmAuditHorizonColumnsExist();
        for (String tblName : REPLICA_UPGRADED_INTERNAL_TABLES) {
            modifyTblReplicaCount(database, tblName);
        }
    }

    /**
     * Internal tables whose replica count is raised towards
     * StatisticConstants#STATISTIC_INTERNAL_TABLE_REPLICA_NUM. The SPM tables carry
     * cluster-wide state: with the default minimum replication of 1 they are created
     * single-replica, and losing the hosting BE would make every global baseline
     * unavailable, erase the only capture handoff cursor, make the id watermark / every
     * global CREATE BASELINE PLAN unreadable, or silently drop the audit publication fence
     * of a whole FE.
     */
    @VisibleForTesting
    static final List<String> REPLICA_UPGRADED_INTERNAL_TABLES = Lists.newArrayList(
            StatisticConstants.TABLE_STATISTIC_TBL_NAME,
            StatisticConstants.PARTITION_STATISTIC_TBL_NAME,
            AuditLoader.AUDIT_LOG_TABLE,
            InternalSchema.SPM_BASELINES_TBL_NAME,
            InternalSchema.SPM_BASELINES_SEQ_TBL_NAME,
            InternalSchema.SPM_BASELINES_HWM_TBL_NAME,
            InternalSchema.SPM_CAPTURE_CHECKPOINT_TBL_NAME,
            InternalSchema.SPM_AUDIT_HORIZON_TBL_NAME);

    public void modifyColumnStatsTblSchema() {
        while (true) {
            try {
                Table table = findStatsTable();
                if (table == null) {
                    break;
                }
                table.writeLock();
                try {
                    doSchemaChange(table);
                    break;
                } finally {
                    table.writeUnlock();
                }
            } catch (Throwable t) {
                LOG.warn("Failed to do schema change for stats table. Try again later.", t);
            }
            try {
                Thread.sleep(Config.resource_not_ready_sleep_seconds * 1000);
            } catch (InterruptedException t) {
                // IGNORE
            }
        }
        StatsTableSchemaValid = true;
    }

    public Table findStatsTable() {
        // 1. check database exist
        Optional<Database> dbOpt = Env.getCurrentEnv().getInternalCatalog().getDb(FeConstants.INTERNAL_DB_NAME);
        if (!dbOpt.isPresent()) {
            return null;
        }

        // 2. check table exist
        Database db = dbOpt.get();
        Optional<Table> tableOp = db.getTable(StatisticConstants.TABLE_STATISTIC_TBL_NAME);
        return tableOp.orElse(null);
    }

    public void doSchemaChange(Table table) throws Exception {
        List<AlterTableOp> ops = getModifyColumnOp(table);
        if (!ops.isEmpty()) {
            TableNameInfo tableNameInfo = new TableNameInfo(
                    InternalCatalog.INTERNAL_CATALOG_NAME,
                    StatisticConstants.DB_NAME,
                    table.getName());
            AlterTableCommand alterTableCommand = new AlterTableCommand(tableNameInfo, ops);
            Env.getCurrentEnv().alterTable(alterTableCommand);
        }
    }

    public List<AlterTableOp> getModifyColumnOp(Table table) throws UserException {
        List<AlterTableOp> alterTableOps = Lists.newArrayList();
        Set<String> currentColumnNames = table.getBaseSchema().stream()
                .map(Column::getName)
                .map(String::toLowerCase)
                .collect(Collectors.toSet());

        Set<String> clusterKeySet = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        Set<String> keysSet = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        boolean isEnableMergeOnWrite = ((OlapTable) table).getEnableUniqueKeyMergeOnWrite();

        if (!currentColumnNames.containsAll(InternalSchema.TABLE_STATS_SCHEMA.stream()
                .map(ColumnDef::getName)
                .map(String::toLowerCase)
                .collect(Collectors.toList()))) {
            for (ColumnDef expected : InternalSchema.TABLE_STATS_SCHEMA) {
                if (!currentColumnNames.contains(expected.getName().toLowerCase())) {
                    ColumnDefinition columnDefinition = expected.translateToColumnDefinition();

                    AddColumnOp addColumnOp = new AddColumnOp(
                            columnDefinition,
                            null,
                            null,
                            null);
                    addColumnOp.setColumn(columnDefinition.translateToCatalogStyleForSchemaChange());
                    alterTableOps.add(addColumnOp);
                }
            }
        }

        for (Column col : table.getFullSchema()) {
            if (col.isKey() && col.getType().isVarchar()
                    && col.getType().getLength() < StatisticConstants.MAX_NAME_LEN) {
                Type type = ScalarType.createVarchar(StatisticConstants.MAX_NAME_LEN);
                ColumnNullableType nullableType =
                        col.isAllowNull() ? ColumnNullableType.NULLABLE : ColumnNullableType.NOT_NULLABLE;

                ColumnDefinition columnDefinition = new ColumnDefinition(
                        col.getName(),
                        DataType.fromCatalogType(type),
                        true,
                        null,
                        nullableType,
                        -1,
                        Optional.empty(),
                        Optional.empty(),
                        "",
                        col.isVisible(),
                        Optional.empty());
                columnDefinition.validate(true, keysSet, clusterKeySet, isEnableMergeOnWrite, null);

                ModifyColumnOp modifyColumnOp = new ModifyColumnOp(
                        columnDefinition, null, null, Maps.newHashMap());
                modifyColumnOp.setColumn(columnDefinition.translateToCatalogStyleForSchemaChange());
                alterTableOps.add(modifyColumnOp);
            }
        }
        return alterTableOps;
    }

    @VisibleForTesting
    public static void modifyTblReplicaCount(Database database, String tblName) {
        if (Config.isCloudMode()
                || Config.min_replication_num_per_tablet >= StatisticConstants.STATISTIC_INTERNAL_TABLE_REPLICA_NUM
                || Config.max_replication_num_per_tablet < StatisticConstants.STATISTIC_INTERNAL_TABLE_REPLICA_NUM) {
            return;
        }
        while (true) {
            int backendNum = Env.getCurrentSystemInfo().getStorageBackendNumFromDiffHosts(true);
            if (FeConstants.runningUnitTest) {
                backendNum = Env.getCurrentSystemInfo().getAllBackendIds().size();
            }
            if (backendNum >= StatisticConstants.STATISTIC_INTERNAL_TABLE_REPLICA_NUM) {
                try {
                    OlapTable tbl = (OlapTable) StatisticsUtil.findTable(InternalCatalog.INTERNAL_CATALOG_NAME,
                            StatisticConstants.DB_NAME, tblName);
                    tbl.writeLock();
                    try {
                        if (tbl.getTableProperty().getReplicaAllocation().getTotalReplicaNum()
                                >= StatisticConstants.STATISTIC_INTERNAL_TABLE_REPLICA_NUM) {
                            return;
                        }
                        if (!tbl.isPartitionedTable()) {
                            Map<String, String> props = new HashMap<>();
                            props.put(PropertyAnalyzer.PROPERTIES_REPLICATION_ALLOCATION, "tag.location.default: "
                                    + StatisticConstants.STATISTIC_INTERNAL_TABLE_REPLICA_NUM);
                            Env.getCurrentEnv().modifyTableReplicaAllocation(database, tbl, props);
                        } else {
                            TableNameInfo tableNameInfo = new TableNameInfo(
                                    InternalCatalog.INTERNAL_CATALOG_NAME,
                                    StatisticConstants.DB_NAME,
                                    tbl.getName());
                            // 1. modify table's default replica allocation
                            Map<String, String> props = new HashMap<>();
                            ReplicaAllocation replicaAllocation = new ReplicaAllocation(
                                    (short) StatisticConstants.STATISTIC_INTERNAL_TABLE_REPLICA_NUM);
                            props.put("default." + PropertyAnalyzer.PROPERTIES_REPLICATION_ALLOCATION,
                                    replicaAllocation.toCreateStmt());
                            Env.getCurrentEnv().modifyTableDefaultReplicaAllocation(database, tbl, props);

                            // 2. modify each partition's replica num
                            List<AlterTableOp> ops = Lists.newArrayList();
                            props.clear();
                            props.put(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM,
                                    "" + StatisticConstants.STATISTIC_INTERNAL_TABLE_REPLICA_NUM);

                            ops.add(new ModifyPartitionOp(Lists.newArrayList(tbl.getPartitionNames()), props, false));
                            AlterTableCommand alterTableCommand = new AlterTableCommand(tableNameInfo, ops);
                            alterTableCommand.run(ConnectContext.get(), null);
                        }
                    } finally {
                        tbl.writeUnlock();
                    }
                    break;
                } catch (Throwable t) {
                    LOG.warn("Failed to scale replica of stats tbl:{} to 3", tblName, t);
                }
            }
            try {
                Thread.sleep(Config.resource_not_ready_sleep_seconds * 1000);
            } catch (InterruptedException t) {
                // IGNORE
            }
        }
    }

    @VisibleForTesting
    public static void createTbl() throws UserException {
        /**
         * CREATE TABLE IF NOT EXISTS `internal`.`__internal_schema`.`column_statistics` (
         *   `id` varchar(4096) NOT NULL COMMENT "",
         *   `catalog_id` varchar(1024) NOT NULL COMMENT "",
         *   `db_id` varchar(1024) NOT NULL COMMENT "",
         *   `tbl_id` varchar(1024) NOT NULL COMMENT "",
         *   `idx_id` varchar(1024) NOT NULL COMMENT "",
         *   `col_id` varchar(1024) NOT NULL COMMENT "",
         *   `part_id` varchar(1024) NULL COMMENT "",
         *   `count` bigint NULL COMMENT "",
         *   `ndv` bigint NULL COMMENT "",
         *   `null_count` bigint NULL COMMENT "",
         *   `min` varchar(65533) NULL COMMENT "",
         *   `max` varchar(65533) NULL COMMENT "",
         *   `data_size_in_bytes` bigint NULL COMMENT "",
         *   `update_time` datetime NOT NULL COMMENT "",
         *   `hot_value` text NULL COMMENT ""
         * ) ENGINE = olap
         * UNIQUE KEY(`id`, `catalog_id`, `db_id`, `tbl_id`, `idx_id`, `col_id`, `part_id`)
         * COMMENT "Doris internal statistics table, DO NOT MODIFY IT"
         * DISTRIBUTED BY HASH(`id`, `catalog_id`, `db_id`, `tbl_id`, `idx_id`, `col_id`, `part_id`)
         * BUCKETS 7
         * PROPERTIES ("replication_num"  =  "1")
         */
        createTable(getStatisticsCreateSql(StatisticConstants.TABLE_STATISTIC_TBL_NAME,
                Lists.newArrayList("id", "catalog_id", "db_id", "tbl_id", "idx_id", "col_id", "part_id")));
        /**
         *CREATE TABLE IF NOT EXISTS `internal`.`__internal_schema`.`partition_statistics` (
         *   `catalog_id` varchar(1024) NOT NULL COMMENT "",
         *   `db_id` varchar(1024) NOT NULL COMMENT "",
         *   `tbl_id` varchar(1024) NOT NULL COMMENT "",
         *   `idx_id` varchar(1024) NOT NULL COMMENT "",
         *   `part_name` varchar(1024) NOT NULL COMMENT "",
         *   `part_id` bigint NOT NULL COMMENT "",
         *   `col_id` varchar(1024) NOT NULL COMMENT "",
         *   `count` bigint NULL COMMENT "",
         *   `ndv` hll NOT NULL COMMENT "",
         *   `null_count` bigint NULL COMMENT "",
         *   `min` varchar(65533) NULL COMMENT "",
         *   `max` varchar(65533) NULL COMMENT "",
         *   `data_size_in_bytes` bigint NULL COMMENT "",
         *   `update_time` datetime NOT NULL COMMENT ""
         * ) ENGINE = olap
         * UNIQUE KEY(`catalog_id`, `db_id`, `tbl_id`, `idx_id`, `part_name`, `part_id`, `col_id`)
         * COMMENT "Doris internal statistics table, DO NOT MODIFY IT"
         * DISTRIBUTED BY HASH(`catalog_id`, `db_id`, `tbl_id`, `idx_id`, `part_name`, `part_id`, `col_id`)
         * BUCKETS 7
         * PROPERTIES ("replication_num" = "1")
         */
        createTable(getStatisticsCreateSql(StatisticConstants.PARTITION_STATISTIC_TBL_NAME,
                Lists.newArrayList("catalog_id", "db_id", "tbl_id", "idx_id", "part_name", "part_id", "col_id")));
        /**
         *CREATE TABLE IF NOT EXISTS `internal`.`__internal_schema`.`audit_log` (
         *   `query_id` varchar(48) NULL COMMENT "",
         *   `time` datetimev2(3) NULL COMMENT "",
         *   `client_ip` varchar(128) NULL COMMENT "",
         *   `user` varchar(128) NULL COMMENT "",
         *   `frontend_ip` varchar(1024) NULL COMMENT "",
         *   `catalog` varchar(128) NULL COMMENT "",
         *   `db` varchar(128) NULL COMMENT "",
         *   `state` varchar(128) NULL COMMENT "",
         *   `error_code` int NULL COMMENT "",
         *   `error_message` text NULL COMMENT "",
         *   `query_time` bigint NULL COMMENT "",
         *   `cpu_time_ms` bigint NULL COMMENT "",
         *   `peak_memory_bytes` bigint NULL COMMENT "",
         *   `scan_bytes` bigint NULL COMMENT "",
         *   `scan_rows` bigint NULL COMMENT "",
         *   `return_rows` bigint NULL COMMENT "",
         *   `shuffle_send_rows` bigint NULL COMMENT "",
         *   `shuffle_send_bytes` bigint NULL COMMENT "",
         *   `spill_write_bytes_from_local_storage` bigint NULL COMMENT "",
         *   `spill_read_bytes_from_local_storage` bigint NULL COMMENT "",
         *   `scan_bytes_from_local_storage` bigint NULL COMMENT "",
         *   `scan_bytes_from_remote_storage` bigint NULL COMMENT "",
         *   `parse_time_ms` int NULL COMMENT "",
         *   `plan_times_ms` map<text,int> NULL COMMENT "",
         *   `get_meta_times_ms` map<text,int> NULL COMMENT "",
         *   `schedule_times_ms` map<text,int> NULL COMMENT "",
         *   `hit_sql_cache` tinyint NULL COMMENT "",
         *   `handled_in_fe` tinyint NULL COMMENT "",
         *   `queried_tables_and_views` array<text> NULL COMMENT "",
         *   `chosen_m_views` array<text> NULL COMMENT "",
         *   `changed_variables` map<text,text> NULL COMMENT "",
         *   `sql_mode` text NULL COMMENT "",
         *   `stmt_type` varchar(48) NULL COMMENT "",
         *   `stmt_id` bigint NULL COMMENT "",
         *   `sql_hash` varchar(128) NULL COMMENT "",
         *   `sql_digest` varchar(128) NULL COMMENT "",
         *   `is_query` tinyint NULL COMMENT "",
         *   `is_nereids` tinyint NULL COMMENT "",
         *   `is_internal` tinyint NULL COMMENT "",
         *   `workload_group` text NULL COMMENT "",
         *   `compute_group` text NULL COMMENT "",
         *   `stmt` text NULL COMMENT ""
         * ) ENGINE = olap
         * DUPLICATE KEY(`query_id`, `time`, `client_ip`)
         * COMMENT "Doris internal audit table, DO NOT MODIFY IT"
         * PARTITION BY RANGE(`time`)
         * (
         *
         * )
         * DISTRIBUTED BY HASH(`query_id`)
         * BUCKETS 2
         * PROPERTIES (
         *   "dynamic_partition.time_unit" = "DAY",
         *   "dynamic_partition.buckets" = "2",
         *   "dynamic_partition.end" = "3",
         *   "dynamic_partition.enable" = "true",
         *   "replication_num" = "1",
         *   "dynamic_partition.start" = "-30",
         *   "dynamic_partition.prefix" = "p"
         * )
         */
        createTable(getAuditLogCreateSql());
        createTable(getSpmBaselinesCreateSql());
        createTable(getSpmBaselinesSeqCreateSql());
        createTable(getSpmBaselinesHwmCreateSql());
        createTable(getSpmCaptureCheckpointCreateSql());
        createTable(getSpmAuditHorizonCreateSql());
    }

    /**
     * The provenance / schema-identity columns an UPGRADED cluster must gain on the
     * pre-existing spm_baselines table (new clusters get them from the create SQL):
     *
     * - sql_mode: the parser mode of the creating session (without it a PIPES_AS_CONCAT
     *   baseline is re-parsed under the default mode after a restart, the stored digest
     *   still finds the row while the structural match rejects every CONCAT-mode query,
     *   and the baseline silently stops applying);
     * - plan_sql_mode: the parser mode of the stored planSql (the raw fallback text keeps
     *   the creator's mode, the decompiled rendering is MODE_DEFAULT);
     * - plan_frozen: the explicit decompiled / raw-fallback provenance, so a reload never
     *   guesses whether the text is the frozen placeholder rendering;
     * - schema_fingerprint: the referenced-table schema identity validated before a
     *   replay, so an ALTER TABLE ... ADD COLUMN cannot keep matching a frozen plan that
     *   still emits the creator-time columns.
     */
    private static final Map<String, ScalarType> SPM_BASELINES_UPGRADE_COLUMNS = new LinkedHashMap<>();

    static {
        SPM_BASELINES_UPGRADE_COLUMNS.put("sql_mode",
                ScalarType.createType(PrimitiveType.BIGINT));
        SPM_BASELINES_UPGRADE_COLUMNS.put("plan_sql_mode",
                ScalarType.createType(PrimitiveType.BIGINT));
        SPM_BASELINES_UPGRADE_COLUMNS.put("plan_frozen",
                ScalarType.createType(PrimitiveType.BOOLEAN));
        // STRING, not VARCHAR(4096): the fingerprint concatenates one entry per distinct
        // referenced table with no length cap, so a bounded column would
        // fail the baseline INSERT for wide multi-table queries.
        SPM_BASELINES_UPGRADE_COLUMNS.put("schema_fingerprint",
                ScalarType.createType(PrimitiveType.STRING));
        // STRING like schema_fingerprint (a hex digest, never bounded); NULL on
        // upgraded rows that predate the column.
        SPM_BASELINES_UPGRADE_COLUMNS.put("plan_sql_digest",
                ScalarType.createType(PrimitiveType.STRING));
    }

    /**
     * Waits until the spm_baselines table carries every column of
     * SPM_BASELINES_UPGRADE_COLUMNS: a transient ALTER failure (BE / tablet not
     * ready) must be retried HERE - run() calls this once and the replica-upgrade loop
     * never comes back, so a one-shot call left an upgraded cluster without the columns
     * until a restart although BaselineManager always reads / writes them (baseline load
     * and global DDL stayed broken).
     */
    static void ensureSpmBaselinesColumnsExist() {
        while (!spmBaselinesColumnsExist()) {
            try {
                upgradeSpmBaselinesSchema();
            } catch (Throwable t) {
                LOG.warn("SPM: failed to add the spm_baselines provenance columns, will retry", t);
            }
            if (spmBaselinesColumnsExist()) {
                return;
            }
            try {
                Thread.sleep(Config.resource_not_ready_sleep_seconds * 1000);
            } catch (InterruptedException e) {
                LOG.info("Sleep interrupted. {}", e.getMessage());
            }
        }
    }

    /**
     * Testable retry skeleton of ensureSpmBaselinesColumnsExist: waits until the
     * columns are observed, retrying a failed alter. A first ALTER failure followed by a
     * success must converge WITHOUT a restart.
     *
     * @param columnsExist whether every upgraded column is already observed
     * @param alter        the idempotent upgrade attempt (may throw)
     * @param sleeper      the wait between attempts
     */
    @VisibleForTesting
    static void ensureSpmBaselinesColumnsExist(BooleanSupplier columnsExist, Runnable alter,
            Runnable sleeper) {
        while (!columnsExist.getAsBoolean()) {
            try {
                alter.run();
            } catch (Throwable t) {
                LOG.warn("SPM: failed to add the spm_baselines provenance columns, will retry", t);
            }
            if (columnsExist.getAsBoolean()) {
                return;
            }
            sleeper.run();
        }
    }

    /** Whether the spm_baselines table already carries every upgraded column (false
     *  while the table itself is not there yet - the caller keeps retrying). */
    @VisibleForTesting
    static boolean spmBaselinesColumnsExist() {
        Optional<Database> dbOpt =
                Env.getCurrentEnv().getInternalCatalog().getDb(FeConstants.INTERNAL_DB_NAME);
        if (!dbOpt.isPresent()) {
            return false;
        }
        Table table = dbOpt.get().getTable(InternalSchema.SPM_BASELINES_TBL_NAME).orElse(null);
        if (table == null) {
            return false;
        }
        Set<String> existing = table.getBaseSchema().stream()
                .map(column -> column.getName().toLowerCase(Locale.ROOT))
                .collect(Collectors.toSet());
        return existing.containsAll(SPM_BASELINES_UPGRADE_COLUMNS.keySet());
    }

    /**
     * Adds every missing column of SPM_BASELINES_UPGRADE_COLUMNS to a
     * PRE-EXISTING spm_baselines table (one ALTER per column). Idempotent: columns that
     * already exist are left untouched, and a partially upgraded table gains only the
     * remainder. Throws on failure - the caller's retry loop owns the retry policy.
     */
    private static void upgradeSpmBaselinesSchema() throws UserException {
        Optional<Database> dbOpt =
                Env.getCurrentEnv().getInternalCatalog().getDb(FeConstants.INTERNAL_DB_NAME);
        if (!dbOpt.isPresent()) {
            LOG.warn("SPM: internal schema db not found yet, will retry the provenance upgrade");
            return;
        }
        Table table = dbOpt.get().getTable(InternalSchema.SPM_BASELINES_TBL_NAME).orElse(null);
        if (table == null) {
            LOG.warn("SPM: spm_baselines table not found yet, will retry the provenance upgrade");
            return;
        }
        // A column that was already issued is only visible in the base schema once its
        // schema change FINISHED; while the table is SCHEMA_CHANGE, its state also rejects
        // any further ALTER. Wait for the state instead of re-issuing the same column every
        // retry round (the retry loop runs every resource_not_ready_seconds and would
        // otherwise deadlock against its own pending schema change).
        if (!(table instanceof OlapTable)
                || ((OlapTable) table).getState() != OlapTable.OlapTableState.NORMAL) {
            LOG.info("SPM: spm_baselines is not in NORMAL state ({}), waiting for the pending"
                            + " schema change before the provenance upgrade",
                    table instanceof OlapTable ? ((OlapTable) table).getState() : "unknown");
            return;
        }
        Set<String> existing = table.getBaseSchema().stream()
                .map(column -> column.getName().toLowerCase(Locale.ROOT))
                .collect(Collectors.toSet());
        for (Map.Entry<String, ScalarType> entry : SPM_BASELINES_UPGRADE_COLUMNS.entrySet()) {
            if (existing.contains(entry.getKey())) {
                continue;
            }
            ColumnDefinition definition = spmUpgradeColumnDefinition(entry.getKey(), entry.getValue());
            AddColumnOp addColumnOp = new AddColumnOp(definition, null, null, null);
            addColumnOp.setColumn(definition.translateToCatalogStyleForSchemaChange());
            TableNameInfo tableNameInfo = new TableNameInfo(InternalCatalog.INTERNAL_CATALOG_NAME,
                    FeConstants.INTERNAL_DB_NAME, InternalSchema.SPM_BASELINES_TBL_NAME);
            Env.getCurrentEnv().alterTable(
                    new AlterTableCommand(tableNameInfo, Lists.newArrayList(addColumnOp)));
            LOG.info("SPM: added the {} column to {}", entry.getKey(),
                    InternalSchema.SPM_BASELINES_TBL_NAME);
            // ONE column per attempt: the table enters SCHEMA_CHANGE until this alter
            // finishes, and the wait loop's next round continues with the remainder
            return;
        }
    }

    /**
     * The definition of ONE upgraded SPM column (baselines / capture checkpoint): an added
     * column is always a VALUE column.
     *
     * The key flag must be FALSE. An ADD COLUMN flagged as KEY lands AFTER the existing
     * value columns in the altered schema, and the schema-change validator rejects any key
     * column that follows a value column ("Invalid column order. value should be after
     * key"): the ALTER threw, the initializer's wait loop retried the same column every
     * resource_not_ready_sleep_seconds forever, and an in-place upgraded cluster
     * never gained the columns its reader / writer already use - the baselines table stayed
     * broken until a restart with a modified table or a drop + fresh create.
     */
    @VisibleForTesting
    static ColumnDefinition spmUpgradeColumnDefinition(String name, ScalarType type) {
        return new ColumnDefinition(name, DataType.fromCatalogType(type),
                false, null, ColumnNullableType.NULLABLE, -1, Optional.empty(),
                Optional.empty(), "", true, Optional.empty());
    }

    /**
     * The columns an UPGRADED cluster must gain on a pre-existing
     * spm_capture_checkpoint table (new clusters get them from the create SQL):
     *
     * - cursor_tail: the encoded tail of the resume cursor (see
     *   PlanCaptureManager#CHECKPOINT_INSERT_SQL / AuditLogScanner#CursorTail). Without
     *   it a checkpointed truncated window can only resume on the (time, query_time,
     *   query_id) prefix: a page whose rows share those keys (e.g. NULL query ids)
     *   either loops on the same page forever or skips its remainder after a handoff.
     * - min_query_time_ms / min_scan_rows: the capture thresholds the pending window was
     *   opened with (-1 = no pending window). The takeover must scan with the SAME
     *   values, otherwise rows the cursor has already passed are judged again under a
     *   changed threshold and either become unreachable or get consumed while failing the
     *   stale in-memory filter.
     * - include_pattern / exclude_pattern: the table-name regexes of the same snapshot
     *   (empty = none). Restoring thresholds but not the patterns let a `SET GLOBAL
     *   plan_capture_include_pattern` applied mid-window terminally filter away rows the
     *   window's earlier pages had admitted: they are then neither captured nor inside
     *   the next window's overlap.
     * - scan_zone: the session time_zone (zone ID) the current scan pass rendered its
     *   bounds in. audit_log.time is the WRITER's local rendering; after `SET GLOBAL
     *   time_zone` the stored rows of the previous rendering fall outside bounds rendered
     *   in the new zone, so the next pass must re-render the same window in this zone
     *   (and only then in the new one).
     */
    @VisibleForTesting
    static final Map<String, ScalarType> SPM_CAPTURE_CHECKPOINT_UPGRADE_COLUMNS = new LinkedHashMap<>();

    static {
        SPM_CAPTURE_CHECKPOINT_UPGRADE_COLUMNS.put("cursor_tail", ScalarType.createVarchar(4096));
        SPM_CAPTURE_CHECKPOINT_UPGRADE_COLUMNS.put("min_query_time_ms",
                ScalarType.createType(PrimitiveType.BIGINT));
        SPM_CAPTURE_CHECKPOINT_UPGRADE_COLUMNS.put("min_scan_rows",
                ScalarType.createType(PrimitiveType.BIGINT));
        // STRING, not VARCHAR(4096): see InternalSchema#SPM_CAPTURE_CHECKPOINT_SCHEMA -
        // SET GLOBAL accepts any compiling regex, and a fixed VARCHAR made a valid long
        // pattern fail the checkpoint INSERT (the cycle then returned before scanning).
        SPM_CAPTURE_CHECKPOINT_UPGRADE_COLUMNS.put("include_pattern",
                ScalarType.createType(PrimitiveType.STRING));
        SPM_CAPTURE_CHECKPOINT_UPGRADE_COLUMNS.put("exclude_pattern",
                ScalarType.createType(PrimitiveType.STRING));
        // the zone ID of the last scan pass (the zone the scanned rows' timestamps were
        // rendered in): after a global time_zone change the next pass re-renders the window
        // in this zone, otherwise rows already stored under it are unreachable.
        SPM_CAPTURE_CHECKPOINT_UPGRADE_COLUMNS.put("scan_zone",
                ScalarType.createType(PrimitiveType.STRING));
        // leader_epoch / write_seq are NOT upgrade columns: they are the new APPEND-ONLY
        // key. A pre-append-only table (no write_seq) is DROPPED and
        // recreated by ensureSpmCaptureCheckpointColumnsExist - its UNIQUE-key(id)
        // merge-on-write layout is the very hazard the model removes, so it is never
        // ALTERed into the new one.
    }

    /**
     * The intended PHYSICAL position of every upgraded checkpoint column: the column of
     * InternalSchema#SPM_CAPTURE_CHECKPOINT_SCHEMA it must follow. cursor_tail
     * sits BEFORE failed_attempts / retry_queue / update_time in the canonical schema, so
     * a plain append leaves an upgraded table with an order no freshly created table ever
     * has. The name-addressed reader / writer survive that, but a positional
     * INSERT ... VALUES does not (see PlanCaptureManager#CHECKPOINT_INSERT_SQL):
     * restoring the canonical order keeps the two layouts identical.
     */
    @VisibleForTesting
    static final Map<String, String> SPM_CAPTURE_CHECKPOINT_UPGRADE_POSITIONS =
            new LinkedHashMap<>();

    static {
        SPM_CAPTURE_CHECKPOINT_UPGRADE_POSITIONS.put("cursor_tail", "cursor_query_id");
        // the thresholds plus their patterns sit between retry_queue and update_time in
        // the canonical schema
        SPM_CAPTURE_CHECKPOINT_UPGRADE_POSITIONS.put("min_query_time_ms", "retry_queue");
        SPM_CAPTURE_CHECKPOINT_UPGRADE_POSITIONS.put("min_scan_rows", "min_query_time_ms");
        SPM_CAPTURE_CHECKPOINT_UPGRADE_POSITIONS.put("include_pattern", "min_scan_rows");
        SPM_CAPTURE_CHECKPOINT_UPGRADE_POSITIONS.put("exclude_pattern", "include_pattern");
        SPM_CAPTURE_CHECKPOINT_UPGRADE_POSITIONS.put("scan_zone", "exclude_pattern");
    }

    /**
     * The ColumnPosition of one upgraded checkpoint column, or null for a plain
     * APPEND (the null-ColumnPosition semantics of AddColumnOp). The anchor of every
     * upgrade column has been part of the checkpoint table since BEFORE that column was
     * introduced, so an upgraded table always carries it; a table created by an even
     * older build (anchor missing) keeps the append order - its writes are name-addressed
     * anyway.
     *
     * @param column          the upgraded column (a key of
     *                        SPM_CAPTURE_CHECKPOINT_UPGRADE_COLUMNS)
     * @param existingColumns the LOWERCASE names already present in the table
     */
    @VisibleForTesting
    static ColumnPosition checkpointColumnPosition(String column, Set<String> existingColumns) {
        String anchor = SPM_CAPTURE_CHECKPOINT_UPGRADE_POSITIONS.get(column);
        if (anchor == null || !existingColumns.contains(anchor)) {
            return null;
        }
        return new ColumnPosition(anchor);
    }

    /**
     * The checkpoint payload columns that carry RESUME state, in the canonical schema
     * order minus the append-only key and update_time (see
     * InternalSchema#SPM_CAPTURE_CHECKPOINT_SCHEMA): a pre-append-only table's row is
     * copied over exactly these columns so the model rewrite cannot lose an unconsumed
     * window (see readCheckpointStateForModelUpgrade).
     */
    @VisibleForTesting
    static final List<String> SPM_CAPTURE_CHECKPOINT_PAYLOAD_COLUMNS = Arrays.asList(
            "last_scan_timestamp", "pending_window_start", "pending_window_end",
            "cursor_query_time", "cursor_time", "cursor_query_id", "cursor_tail",
            "failed_attempts", "retry_queue", "min_query_time_ms", "min_scan_rows",
            "include_pattern", "exclude_pattern", "scan_zone");

    /** The payload columns whose carried values are numeric (the rest are text). */
    private static final Set<String> SPM_CAPTURE_CHECKPOINT_NUMERIC_COLUMNS = Sets.newHashSet(
            "last_scan_timestamp", "pending_window_start", "pending_window_end",
            "cursor_query_time", "min_query_time_ms", "min_scan_rows");

    /**
     * The payload columns whose ABSENT historical value is the -1 UNPINNED sentinel
     * rather than the generic 0: min_query_time_ms / min_scan_rows describe the capture
     * filter the pending window was opened with, and the reader treats 0 / 0 as a PINNED
     * filter (see PlanCaptureManager#applyCheckpointRow) - a migrated pre-columns row
     * would then capture / skip queries under a filter the old build never had, ignoring
     * the configured global thresholds and patterns. -1 leaves the window unpinned, so
     * the cycle re-derives the filter from the globals.
     */
    @VisibleForTesting
    static final Set<String> SPM_CAPTURE_CHECKPOINT_UNPINNED_COLUMNS =
            Set.of("min_query_time_ms", "min_scan_rows");

    /** Bound of the model-upgrade carry read (one bounded row of the old table). */
    private static final int SPM_CHECKPOINT_CARRY_TIMEOUT_SECONDS = 10;

    /**
     * Staging table of the checkpoint model upgrade (see
     * ensureSpmCaptureCheckpointColumnsExist): the payload of the pre-append-only row is
     * parked HERE, durably, before the old table is dropped - from then on every crash
     * window leaves a readable copy somewhere, so the migration is RESUMABLE instead of
     * losing an unconsumed pending window. Dropped once the recreated table's copy is
     * confirmed readable.
     */
    @VisibleForTesting
    static final String SPM_CAPTURE_CHECKPOINT_STAGE_TBL_NAME =
            "spm_capture_checkpoint_stage";

    /**
     * Waits until the spm_capture_checkpoint table carries every column of
     * SPM_CAPTURE_CHECKPOINT_UPGRADE_COLUMNS: like the baselines upgrade, a
     * transient ALTER failure (BE / tablet not ready) must be retried HERE - run() calls
     * this once and the replica-upgrade loop never comes back. A pre-append-only table
     * is DROPPED and recreated (see checkpointTableModelOutdated), with its row carried
     * over first so the model rewrite cannot lose capture progress.
     */
    static void ensureSpmCaptureCheckpointColumnsExist() {
        // The payload of a pre-append-only row (a pending window's bounds, cursor, retry
        // queue and render zone) is copied DURABLY before the model drop below: first
        // into the staging table, and from there into the recreated table. The previous
        // in-memory handoff existed only in this process between the drop and its
        // replacement INSERT, so a crash there lost the row permanently, and a
        // transiently failed INSERT was never retried (the recreated table then
        // satisfied the loop condition). Every crash window now leaves the payload in a
        // readable table: the pre-append-only one until the staged copy is CONFIRMED,
        // the staging one until the recreated table's copy is confirmed.
        // The MODEL check is part of the gate: a pre-append-only table
        // carries every UPGRADE column (they were added by earlier builds), so the
        // column set alone would declare it ready and the drop/recreate below would
        // never run - every checkpoint INSERT then failed on the missing write_seq.
        while (!spmCaptureCheckpointColumnsExist() || checkpointTableModelOutdated()
                || stagedCheckpointCarryPending()) {
            try {
                if (spmCaptureCheckpointTableExists() && checkpointTableModelOutdated()) {
                    // Step one: get the old row into the staging table durably. The old
                    // table is dropped only once that copy is confirmed READABLE.
                    if (stageCheckpointStateForModelUpgrade()) {
                        dropSpmCaptureCheckpointTable();
                    }
                } else if (stagedCheckpointCarryPending()) {
                    // Step two: the payload's only copy is the staging table - recreate
                    // the append-only table and copy from there, dropping the staging
                    // table only once the recreated row is confirmed readable too.
                    if (!spmCaptureCheckpointTableExists()) {
                        // createTbl() ran BEFORE this method and never runs again, so the
                        // recreation happens here
                        createTable(getSpmCaptureCheckpointCreateSql());
                    }
                    if (restoreStagedCheckpointRow()) {
                        dropSpmCaptureCheckpointStageTable();
                    }
                } else if (!spmCaptureCheckpointTableExists()) {
                    // absent because this method just dropped the pre-append-only table
                    // (or the create gate never saw it): createTbl() ran BEFORE this
                    // method and never runs again, so the recreation happens here
                    createTable(getSpmCaptureCheckpointCreateSql());
                } else {
                    upgradeSpmCaptureCheckpointSchema();
                }
            } catch (Throwable t) {
                LOG.warn("SPM: failed to upgrade the spm_capture_checkpoint table,"
                        + " will retry", t);
            }
            // The loop may only end on a table that is BOTH complete and on the
            // append-only model, with nothing left to carry: an old UNIQUE-key table
            // already carries every UPGRADE column, so a columns-only check let one
            // failed drop stop this one-shot initializer with the old table - and every
            // checkpoint INSERT then failed on the missing write_seq until a restart.
            if (checkpointUpgradeComplete(spmCaptureCheckpointColumnsExist(),
                    checkpointTableModelOutdated(), stagedCheckpointCarryPending())) {
                return;
            }
            try {
                Thread.sleep(Config.resource_not_ready_sleep_seconds * 1000);
            } catch (InterruptedException e) {
                LOG.info("Sleep interrupted. {}", e.getMessage());
            }
        }
    }

    /**
     * Whether the checkpoint model rewrite is DONE: the table carries every upgraded
     * column, sits on the append-only model, and nothing is left to carry over (see
     * ensureSpmCaptureCheckpointColumnsExist). A columns-only completion check would end
     * the one-shot initializer on a pre-append-only table whose drop failed once - it
     * already carries every UPGRADE column - and every checkpoint INSERT then fails on
     * the missing write_seq until a restart.
     *
     * @param columnsExist  every SPM_CAPTURE_CHECKPOINT_UPGRADE_COLUMNS member is there
     * @param modelOutdated the table is still the old UNIQUE-key model
     * @param statePending  a staged copy is still waiting to be written into the
     *                      recreated table (see stagedCheckpointCarryPending)
     */
    @VisibleForTesting
    static boolean checkpointUpgradeComplete(boolean columnsExist, boolean modelOutdated,
            boolean statePending) {
        return columnsExist && !modelOutdated && !statePending;
    }

    /**
     * Reads the OLD checkpoint table's row before the model drop recreates it: the row
     * is the only copy of an UNCONSUMED pending window (bounds, cursor, retry queue,
     * thresholds and the zone its bounds were rendered in), and the capture resumes
     * from it after a restart. Dropping the table without carrying it made the new
     * process load no row and derive its window from the CURRENT interval, so the
     * pending window's unconsumed tail was never scanned.
     *
     * Only the payload columns the old table actually HAS are read (a model that
     * predates some upgrade columns carries whatever state exists); an idle / never
     * written checkpoint has no row and returns null.
     *
     * @return the carried payload values, or null when there is nothing to carry
     */
    private static Map<String, String> readCheckpointStateForModelUpgrade() throws Exception {
        Table table = internalSchemaTable(InternalSchema.SPM_CAPTURE_CHECKPOINT_TBL_NAME);
        if (table == null) {
            return null;
        }
        Set<String> existing = table.getBaseSchema().stream()
                .map(column -> column.getName().toLowerCase(Locale.ROOT))
                .collect(Collectors.toSet());
        List<String> available = new ArrayList<>();
        for (String column : SPM_CAPTURE_CHECKPOINT_PAYLOAD_COLUMNS) {
            if (existing.contains(column)) {
                available.add(column);
            }
        }
        if (available.isEmpty()) {
            return null;
        }
        StringBuilder select = new StringBuilder("SELECT ");
        for (String column : available) {
            if (select.length() > "SELECT ".length()) {
                select.append(", ");
            }
            select.append('`').append(column).append('`');
        }
        select.append(" FROM `").append(FeConstants.INTERNAL_DB_NAME).append("`.`")
                .append(InternalSchema.SPM_CAPTURE_CHECKPOINT_TBL_NAME)
                .append("` WHERE `id` = 1 LIMIT 1");
        List<ResultRow> rows = StatisticsUtil.executeQuery(select.toString(),
                Collections.emptyMap(), SPM_CHECKPOINT_CARRY_TIMEOUT_SECONDS);
        if (rows == null || rows.isEmpty()) {
            return null;
        }
        ResultRow row = rows.get(0);
        Map<String, String> state = new LinkedHashMap<>();
        for (int index = 0; index < available.size(); index++) {
            state.put(available.get(index), row.getWithDefault(index, ""));
        }
        LOG.info("SPM: reading the pre-append-only {} row (pending window [{}, {})) for the"
                        + " model migration",
                InternalSchema.SPM_CAPTURE_CHECKPOINT_TBL_NAME,
                state.get("pending_window_start"), state.get("pending_window_end"));
        return state;
    }

    /**
     * Whether the migration's staging table exists, i.e. a copy of the pre-append-only
     * row still has to be written into the recreated table (see
     * ensureSpmCaptureCheckpointColumnsExist). This is the loop's carry-pending flag: the
     * initializer never finishes with a staged copy left unrestored.
     */
    private static boolean stagedCheckpointCarryPending() {
        return internalSchemaTable(SPM_CAPTURE_CHECKPOINT_STAGE_TBL_NAME) != null;
    }

    /**
     * Guards the model drop: the pre-append-only row must be READABLE from the staging
     * table before the only other copy (the old table) is dropped. Reads the old row,
     * stages it, and confirms the staged copy by reading it back - an internal INSERT can
     * return SQL OK with the transaction merely COMMITTED, and a crash before publication
     * would otherwise lose the row together with the dropped table.
     *
     * @return whether the payload is durably staged (or there is nothing to stage);
     *         false = the old table must NOT be dropped yet, the caller retries
     */
    private static boolean stageCheckpointStateForModelUpgrade() {
        try {
            if (readStagedCheckpointRow() != null) {
                // the durable copy already exists (an earlier round of this process, or a
                // predecessor's): restaging would only append an equal row
                return true;
            }
            Map<String, String> state = readCheckpointStateForModelUpgrade();
            if (state == null) {
                // an idle / never written checkpoint has no row to carry
                return true;
            }
            createTable(getSpmCaptureCheckpointStageCreateSql());
            executeCarryStatement(buildCarriedCheckpointInsert(
                    SPM_CAPTURE_CHECKPOINT_STAGE_TBL_NAME, state, currentJournalEpoch()));
            if (readStagedCheckpointRow() == null) {
                LOG.warn("SPM: the staged checkpoint row is not readable yet, the"
                        + " pre-append-only table is kept");
                return false;
            }
            LOG.info("SPM: staged the pre-append-only {} row in {} (pending window [{}, {}))",
                    InternalSchema.SPM_CAPTURE_CHECKPOINT_TBL_NAME,
                    SPM_CAPTURE_CHECKPOINT_STAGE_TBL_NAME,
                    state.get("pending_window_start"), state.get("pending_window_end"));
            return true;
        } catch (Throwable t) {
            LOG.warn("SPM: failed to stage the pre-append-only checkpoint row, will retry: {}",
                    t.getMessage());
            return false;
        }
    }

    /**
     * Writes the staged payload into the recreated append-only table as its first
     * readable row and CONFIRMS it: until the capture writes its own (greater) token the
     * reader resolves to the carried state, so a takeover resumes the SAME pending window
     * instead of deriving a later one over its unconsumed tail. The staged token is
     * REUSED instead of a fresh epoch: a re-run after a crash before the staging table was
     * dropped then appends an EQUAL-token row, which can never supersede a row the
     * capture may already have written above it (a fresh epoch could).
     *
     * @return whether the recreated table's copy is readable (false = retryable; the
     *         staging table keeps the only copy until this returns true)
     */
    private static boolean restoreStagedCheckpointRow() {
        try {
            ResultRow staged = readStagedCheckpointRow();
            if (staged == null) {
                // an EMPTY staging table carries no state: the old table is only dropped
                // after the staged copy was confirmed, so there is nothing to restore
                return true;
            }
            Map<String, String> state = new LinkedHashMap<>();
            for (int index = 0; index < SPM_CAPTURE_CHECKPOINT_PAYLOAD_COLUMNS.size(); index++) {
                state.put(SPM_CAPTURE_CHECKPOINT_PAYLOAD_COLUMNS.get(index),
                        staged.getWithDefault(index + 2, ""));
            }
            long epoch = parseCarriedNumber(staged.getWithDefault(0, ""));
            executeCarryStatement(buildCarriedCheckpointInsert(
                    InternalSchema.SPM_CAPTURE_CHECKPOINT_TBL_NAME, state, epoch));
            if (!carriedCheckpointRowReadable(epoch, state.get("pending_window_start"))) {
                LOG.warn("SPM: the carried checkpoint row is not readable yet, the staging"
                        + " table is kept");
                return false;
            }
            LOG.info("SPM: restored the pre-append-only checkpoint row into the recreated"
                    + " append-only table");
            return true;
        } catch (Throwable t) {
            LOG.warn("SPM: failed to carry the staged checkpoint row into the recreated"
                    + " table, will retry: {}", t.getMessage());
            return false;
        }
    }

    /**
     * The staging table's row - the reused write token plus the payload (see
     * buildStagedCheckpointSelectSql) - or null while the staging table is absent / has
     * no row yet.
     */
    private static ResultRow readStagedCheckpointRow() throws Exception {
        if (!stagedCheckpointCarryPending()) {
            return null;
        }
        List<ResultRow> rows = StatisticsUtil.executeQuery(buildStagedCheckpointSelectSql(),
                Collections.emptyMap(), SPM_CHECKPOINT_CARRY_TIMEOUT_SECONDS);
        return rows == null || rows.isEmpty() ? null : rows.get(0);
    }

    /**
     * The read of the staged row: the write token the copy will reuse, then the payload
     * in the canonical order (see SPM_CAPTURE_CHECKPOINT_PAYLOAD_COLUMNS - the same
     * name-addressed list the carry INSERT writes).
     */
    @VisibleForTesting
    static String buildStagedCheckpointSelectSql() {
        StringBuilder select = new StringBuilder("SELECT `leader_epoch`, `write_seq`");
        for (String column : SPM_CAPTURE_CHECKPOINT_PAYLOAD_COLUMNS) {
            select.append(", `").append(column).append('`');
        }
        return select.append(" FROM `").append(FeConstants.INTERNAL_DB_NAME).append("`.`")
                .append(SPM_CAPTURE_CHECKPOINT_STAGE_TBL_NAME)
                .append("` WHERE `id` = 1 LIMIT 1").toString();
    }

    /**
     * Whether the carried row (the reused token plus the carried window start) is
     * READABLE in the recreated table: a successful INSERT only proves the transaction
     * committed, and the staging table - the other copy - may only be dropped once the
     * recreated row can actually be read.
     */
    private static boolean carriedCheckpointRowReadable(long epoch, String pendingWindowStart)
            throws Exception {
        List<ResultRow> rows = StatisticsUtil.executeQuery(
                buildCarriedCheckpointReadbackSql(epoch, pendingWindowStart),
                Collections.emptyMap(), SPM_CHECKPOINT_CARRY_TIMEOUT_SECONDS);
        return rows != null && !rows.isEmpty();
    }

    /**
     * The read-back of one carried row: the token it reuses plus the carried window start
     * disambiguate it from a capture write that could share the token (an exact token tie
     * is broken by update_time, so an equal-token row is not proof the carried VALUES
     * are there).
     */
    @VisibleForTesting
    static String buildCarriedCheckpointReadbackSql(long epoch, String pendingWindowStart) {
        return "SELECT `leader_epoch` FROM `" + FeConstants.INTERNAL_DB_NAME
                + "`.`" + InternalSchema.SPM_CAPTURE_CHECKPOINT_TBL_NAME
                + "` WHERE `id` = 1 AND `leader_epoch` = " + epoch
                + " AND `write_seq` = 0 AND `pending_window_start` = "
                + parseCarriedNumber(pendingWindowStart) + " LIMIT 1";
    }

    /**
     * Runs one carry statement pinned to the DEFAULT parser mode: the statement carries
     * user-visible TEXT (the pending filter SQL, the scan selectors) escaped for that
     * mode, and executing it under an ambient session mode (this initializer shares the
     * context with whatever connection booted the FE) would re-interpret the escapes - a
     * NO_BACKSLASH mode turns the escaped backslashes into literals and the restored row
     * would carry a MANGLED filter, scanning a different window forever. Persist /
     * capture writes pin the same mode (see PlanCaptureManager#persistCheckpoint).
     */
    private static void executeCarryStatement(String sql) {
        SqlModeHelper.withSqlMode(SqlModeHelper.MODE_DEFAULT, () -> {
            try {
                StatisticsUtil.execUpdate(sql, Collections.emptyMap(),
                        SPM_CHECKPOINT_CARRY_TIMEOUT_SECONDS);
            } catch (Exception e) {
                // rethrown as unchecked: the callers turn it into the retry verdict (the
                // Supplier cannot carry checked exceptions)
                throw new RuntimeException(e.getMessage(), e);
            }
            return null;
        });
    }

    /** Drops the staging table once the recreated table's copy is confirmed readable. */
    private static void dropSpmCaptureCheckpointStageTable() throws UserException {
        Env.getCurrentEnv().getInternalCatalog().dropTable(FeConstants.INTERNAL_DB_NAME,
                SPM_CAPTURE_CHECKPOINT_STAGE_TBL_NAME, false, false, false, true, false, true);
        LOG.info("SPM: dropped the {} staging table (the recreated checkpoint row is"
                + " readable)", SPM_CAPTURE_CHECKPOINT_STAGE_TBL_NAME);
    }

    /**
     * The journal id of the carried row's write token (0 when unreadable: an unreadable
     * id only LOWERS the token, and the capture's next write supersedes the carried row
     * either way).
     */
    private static long currentJournalEpoch() {
        try {
            Long journalId = Env.getCurrentEnv().getMaxJournalId();
            return journalId == null ? 0L : journalId;
        } catch (Throwable t) {
            LOG.debug("SPM: cannot read the journal id for the carried checkpoint row: {}",
                    t.getMessage());
            return 0L;
        }
    }

    /**
     * The INSERT that writes one carried payload into the recreated append-only table as
     * its first row: until the capture writes its own (greater) token, the reader
     * resolves to the carried state, so a takeover resumes the SAME pending window
     * instead of deriving a later one over its unconsumed tail. Name-addressed like the
     * capture's own write (the physical column order of an upgraded table may differ from
     * a freshly created one), numeric cells unquoted, text cells escaped. The same
     * builder stages the payload in the migration's staging table first (see
     * stageCheckpointStateForModelUpgrade), which is why the target table is a parameter.
     *
     * @param tableName the target table (the recreated checkpoint, or the staging table)
     * @param state     the carried payload (see readCheckpointStateForModelUpgrade)
     * @param epoch     the row's leader_epoch token
     * @return the INSERT statement
     */
    @VisibleForTesting
    static String buildCarriedCheckpointInsert(String tableName, Map<String, String> state,
            long epoch) {
        StringBuilder columns = new StringBuilder("(`id`, `leader_epoch`, `write_seq`,"
                + " `update_time`");
        StringBuilder values = new StringBuilder("(1, ").append(epoch).append(", 0, NOW()");
        for (String column : SPM_CAPTURE_CHECKPOINT_PAYLOAD_COLUMNS) {
            // A column the OLD table did not have yet (readCheckpointStateForModelUpgrade
            // only reads what exists there) is filled with its compatible historical
            // value: every payload column is NOT NULL in the recreated / staging schema
            // WITHOUT a default, so omitting it failed the carry INSERT with "Column has
            // no default value" on EVERY retry and the old table never reached the
            // append-only model - capture could not resume (round-52 #2). Numeric cells
            // carry 0, text cells the empty string: the values a row written by the
            // older build implicitly stands for (scan_zone empty = never scanned, the
            // reader then follows the current global zone).
            //
            // EXCEPTION - the THRESHOLD columns: a table predating min_query_time_ms /
            // min_scan_rows carries NO value for them, and 0 / 0 reads back as a PINNED
            // filter (see applyCheckpointRow: a non-negative pair pins the window), so
            // the resumed pending window would capture / skip queries under a filter it
            // never had - the CONFIGURED global thresholds and patterns would be ignored
            // and its carried empty patterns would terminally filter rows the global
            // filter admits. The absent threshold columns therefore carry the -1 UNPINNED
            // sentinel: the reader leaves the window unpinned and the cycle uses the
            // filters it refreshed from the globals, exactly like a row written before
            // the columns existed.
            String value = state.get(column);
            if (value == null) {
                if (SPM_CAPTURE_CHECKPOINT_UNPINNED_COLUMNS.contains(column)) {
                    value = "-1";
                } else {
                    value = SPM_CAPTURE_CHECKPOINT_NUMERIC_COLUMNS.contains(column) ? "0" : "";
                }
            }
            columns.append(", `").append(column).append('`');
            values.append(", ");
            if (SPM_CAPTURE_CHECKPOINT_NUMERIC_COLUMNS.contains(column)) {
                values.append(parseCarriedNumber(value));
            } else {
                values.append('\'').append(StatisticsUtil.escapeSQL(value)).append('\'');
            }
        }
        columns.append(')');
        values.append(')');
        return "INSERT INTO `" + FeConstants.INTERNAL_DB_NAME + "`."
                + "`" + tableName + "` "
                + columns + " VALUES " + values;
    }

    /**
     * The carry INSERT targeting the recreated checkpoint table (see the 3-arg builder) -
     * the table every non-staging caller writes.
     */
    @VisibleForTesting
    static String buildCarriedCheckpointInsert(Map<String, String> state, long epoch) {
        return buildCarriedCheckpointInsert(InternalSchema.SPM_CAPTURE_CHECKPOINT_TBL_NAME,
                state, epoch);
    }

    /** One numeric carried cell; a blank / unparsable value falls back to 0. */
    private static long parseCarriedNumber(String text) {
        if (text == null || text.trim().isEmpty()) {
            return 0L;
        }
        try {
            return Long.parseLong(text.trim());
        } catch (NumberFormatException e) {
            return 0L;
        }
    }

    /** Whether the spm_capture_checkpoint table is there at all (upgrade loop helper). */
    private static boolean spmCaptureCheckpointTableExists() {
        return internalSchemaTable(InternalSchema.SPM_CAPTURE_CHECKPOINT_TBL_NAME) != null;
    }

    /**
     * Whether the existing spm_capture_checkpoint table predates the APPEND-ONLY model
     * a table without the write_seq column is the old
     * UNIQUE-key(id) + merge-on-write layout, which is DROPPED and recreated instead of
     * ALTERed - keeping the unique key while adding write_seq would preserve the very
     * hazard the model removes (a delayed lower-epoch write REPLACED the newest row).
     * The checkpoint is a best-effort resume aid: its loss re-scans the pending window
     * from its top, which is idempotent (captures dedupe by query id and digest).
     */
    @VisibleForTesting
    static boolean checkpointTableModelOutdated() {
        Table table = internalSchemaTable(InternalSchema.SPM_CAPTURE_CHECKPOINT_TBL_NAME);
        if (table == null) {
            return false;
        }
        return table.getBaseSchema().stream()
                .noneMatch(column -> column.getName().equalsIgnoreCase("write_seq"));
    }

    /** Drops the pre-append-only checkpoint table; the caller recreates the new model. */
    private static void dropSpmCaptureCheckpointTable() throws UserException {
        LOG.warn("SPM: dropping the pre-append-only {} table (UNIQUE-key(id) model)",
                InternalSchema.SPM_CAPTURE_CHECKPOINT_TBL_NAME);
        Env.getCurrentEnv().getInternalCatalog().dropTable(FeConstants.INTERNAL_DB_NAME,
                InternalSchema.SPM_CAPTURE_CHECKPOINT_TBL_NAME, false, false, false, true,
                false, true);
    }

    /** The internal-schema table, or null while the db / table does not exist yet. */
    private static Table internalSchemaTable(String tableName) {
        Optional<Database> dbOpt =
                Env.getCurrentEnv().getInternalCatalog().getDb(FeConstants.INTERNAL_DB_NAME);
        if (!dbOpt.isPresent()) {
            return null;
        }
        return dbOpt.get().getTable(tableName).orElse(null);
    }

    /** Whether the spm_capture_checkpoint table already carries every upgraded column
     *  (false while the table itself is not there yet - the caller keeps retrying). */
    @VisibleForTesting
    static boolean spmCaptureCheckpointColumnsExist() {
        Optional<Database> dbOpt =
                Env.getCurrentEnv().getInternalCatalog().getDb(FeConstants.INTERNAL_DB_NAME);
        if (!dbOpt.isPresent()) {
            return false;
        }
        Table table = dbOpt.get().getTable(InternalSchema.SPM_CAPTURE_CHECKPOINT_TBL_NAME).orElse(null);
        if (table == null) {
            return false;
        }
        Set<String> existing = table.getBaseSchema().stream()
                .map(column -> column.getName().toLowerCase(Locale.ROOT))
                .collect(Collectors.toSet());
        return existing.containsAll(SPM_CAPTURE_CHECKPOINT_UPGRADE_COLUMNS.keySet());
    }

    /**
     * Adds the missing column of SPM_CAPTURE_CHECKPOINT_UPGRADE_COLUMNS to a
     * PRE-EXISTING spm_capture_checkpoint table (one ALTER per attempt). Idempotent:
     * a column that already exists is left untouched. Throws on failure - the caller's
     * retry loop owns the retry policy.
     */
    private static void upgradeSpmCaptureCheckpointSchema() throws UserException {
        Optional<Database> dbOpt =
                Env.getCurrentEnv().getInternalCatalog().getDb(FeConstants.INTERNAL_DB_NAME);
        if (!dbOpt.isPresent()) {
            LOG.warn("SPM: internal schema db not found yet, will retry the checkpoint upgrade");
            return;
        }
        Table table = dbOpt.get().getTable(InternalSchema.SPM_CAPTURE_CHECKPOINT_TBL_NAME).orElse(null);
        if (table == null) {
            LOG.warn("SPM: spm_capture_checkpoint table not found yet, will retry the upgrade");
            return;
        }
        // Same as the baselines upgrade: a column is observable only after its schema
        // change FINISHED, and a table in SCHEMA_CHANGE rejects further ALTERs - wait for
        // NORMAL instead of re-issuing the same column forever.
        if (!(table instanceof OlapTable)
                || ((OlapTable) table).getState() != OlapTable.OlapTableState.NORMAL) {
            LOG.info("SPM: spm_capture_checkpoint is not in NORMAL state ({}), waiting for the"
                            + " pending schema change before the upgrade",
                    table instanceof OlapTable ? ((OlapTable) table).getState() : "unknown");
            return;
        }
        Set<String> existing = table.getBaseSchema().stream()
                .map(column -> column.getName().toLowerCase(Locale.ROOT))
                .collect(Collectors.toSet());
        for (Map.Entry<String, ScalarType> entry : SPM_CAPTURE_CHECKPOINT_UPGRADE_COLUMNS.entrySet()) {
            if (existing.contains(entry.getKey())) {
                continue;
            }
            ColumnDefinition definition = spmUpgradeColumnDefinition(entry.getKey(), entry.getValue());
            // Restore the canonical schema order instead of appending: a positional
            // INSERT binds its values by POSITION, so an upgraded table must end up with
            // the layout of a freshly created one (see checkpointColumnPosition)
            AddColumnOp addColumnOp = new AddColumnOp(definition,
                    checkpointColumnPosition(entry.getKey(), existing), null, null);
            addColumnOp.setColumn(definition.translateToCatalogStyleForSchemaChange());
            TableNameInfo tableNameInfo = new TableNameInfo(InternalCatalog.INTERNAL_CATALOG_NAME,
                    FeConstants.INTERNAL_DB_NAME, InternalSchema.SPM_CAPTURE_CHECKPOINT_TBL_NAME);
            Env.getCurrentEnv().alterTable(
                    new AlterTableCommand(tableNameInfo, Lists.newArrayList(addColumnOp)));
            LOG.info("SPM: added the {} column to {}", entry.getKey(),
                    InternalSchema.SPM_CAPTURE_CHECKPOINT_TBL_NAME);
            // ONE column per attempt: the table enters SCHEMA_CHANGE until this alter
            // finishes, and the wait loop's next round continues with the remainder
            return;
        }
    }

    /**
     * The columns an UPGRADED cluster must gain on a pre-existing spm_baselines_seq
     * table (new clusters get them from the create SQL): bind_sql_digest / plan_sql_hash /
     * reserve_time let a create's id reservation carry the baseline's identity, which is
     * how the DURABLE unresolved-create fence of BaselineManager finds a committed but
     * unpublished write after a leader handoff / restart.
     */
    @VisibleForTesting
    /**
     * Upgrade column of the spm_baselines_hwm table: the bounded mutation clock (tick,
     * see BaselineManager#bumpMutationClock). A pre-existing slot without the column
     * reads as tick = 0 and is re-written by the next mutation.
     */
    static final Map<String, ScalarType> SPM_BASELINES_HWM_UPGRADE_COLUMNS = new LinkedHashMap<>();

    static {
        SPM_BASELINES_HWM_UPGRADE_COLUMNS.put("tick",
                ScalarType.createType(PrimitiveType.BIGINT));
    }

    static final Map<String, ScalarType> SPM_BASELINES_SEQ_UPGRADE_COLUMNS = new LinkedHashMap<>();

    static {
        // STRING, not a fixed VARCHAR: the digest is the canonical rendering of the WHOLE
        // bind statement without a length cap (see
        // InternalSchema#SPM_BASELINES_SEQ_SCHEMA); a >4096-char bind failed the
        // reservation INSERT although a fresh table's column accepts it.
        SPM_BASELINES_SEQ_UPGRADE_COLUMNS.put("bind_sql_digest",
                ScalarType.createType(PrimitiveType.STRING));
        SPM_BASELINES_SEQ_UPGRADE_COLUMNS.put("plan_sql_hash",
                ScalarType.createType(PrimitiveType.BIGINT));
        SPM_BASELINES_SEQ_UPGRADE_COLUMNS.put("reserve_time",
                ScalarType.createType(PrimitiveType.DATETIME));
        // 1 = the marker of an AMBIGUOUS create (the durable pending-create fence reads
        // only these rows); a plain reservation exists for every create and must never
        // fence. NULL (pre-marker rows) is treated as "not unconfirmed".
        SPM_BASELINES_SEQ_UPGRADE_COLUMNS.put("unconfirmed",
                ScalarType.createType(PrimitiveType.BIGINT));
        // 1 = a DROP TOMBSTONE: keeps a delayed status INSERT from
        // resurrecting a dropped baseline (see InternalSchema#SPM_BASELINES_SEQ_SCHEMA).
        // NULL (pre-marker rows) is treated as "not dropped".
        SPM_BASELINES_SEQ_UPGRADE_COLUMNS.put("dropped",
                ScalarType.createType(PrimitiveType.BIGINT));
    }

    /**
     * The columns an UPGRADED cluster must gain on a pre-existing spm_audit_horizon table
     * (new clusters get them from the create SQL): writer_zones carries the audit
     * writer's zone history of that FE, so the capture can require a window pass in every
     * zone that may own rows; committed_fence_ms marks the oldest batch
     * whose publication outcome is AMBIGUOUS, so the fence survives the FE's death - its
     * committed rows can still publish; committed_fence_labels lists the
     * labels of those batches, so a dead FE's fence is resolved per transaction instead
     * of expiring on the age bound alone.
     */
    @VisibleForTesting
    static final Map<String, ScalarType> SPM_AUDIT_HORIZON_UPGRADE_COLUMNS = new LinkedHashMap<>();

    static {
        SPM_AUDIT_HORIZON_UPGRADE_COLUMNS.put("writer_zones",
                ScalarType.createType(PrimitiveType.STRING));
        SPM_AUDIT_HORIZON_UPGRADE_COLUMNS.put("committed_fence_ms",
                ScalarType.createType(PrimitiveType.BIGINT));
        SPM_AUDIT_HORIZON_UPGRADE_COLUMNS.put("committed_fence_labels",
                ScalarType.createType(PrimitiveType.STRING));
    }

    /**
     * Waits until the spm_baselines_seq table carries every column of
     * SPM_BASELINES_SEQ_UPGRADE_COLUMNS (transient ALTER failures are retried
     * HERE - run() calls this once and never comes back).
     */
    static void ensureSpmBaselinesSeqColumnsExist() {
        ensureSpmUpgradeColumns(SPM_BASELINES_SEQ_UPGRADE_COLUMNS, "spm_baselines_seq",
                InternalSchema.SPM_BASELINES_SEQ_TBL_NAME);
    }

    /**
     * Waits until the spm_baselines_hwm table carries every column of
     * SPM_BASELINES_HWM_UPGRADE_COLUMNS.
     */
    static void ensureSpmBaselinesHwmColumnsExist() {
        ensureSpmUpgradeColumns(SPM_BASELINES_HWM_UPGRADE_COLUMNS, "spm_baselines_hwm",
                InternalSchema.SPM_BASELINES_HWM_TBL_NAME);
    }

    /**
     * Waits until the spm_audit_horizon table carries every column of
     * SPM_AUDIT_HORIZON_UPGRADE_COLUMNS.
     */
    static void ensureSpmAuditHorizonColumnsExist() {
        ensureSpmUpgradeColumns(SPM_AUDIT_HORIZON_UPGRADE_COLUMNS, "spm_audit_horizon",
                InternalSchema.SPM_AUDIT_HORIZON_TBL_NAME);
    }

    /** Shared retry skeleton of the two upgrade loops above (see the checkpoint one). */
    private static void ensureSpmUpgradeColumns(Map<String, ScalarType> upgradeColumns,
            String label, String tableName) {
        while (!spmUpgradeColumnsExist(upgradeColumns, tableName)) {
            try {
                upgradeSpmTableSchema(upgradeColumns, label, tableName);
            } catch (Throwable t) {
                LOG.warn("SPM: failed to add the {} columns, will retry", label, t);
            }
            if (spmUpgradeColumnsExist(upgradeColumns, tableName)) {
                return;
            }
            try {
                Thread.sleep(Config.resource_not_ready_sleep_seconds * 1000);
            } catch (InterruptedException e) {
                LOG.info("Sleep interrupted. {}", e.getMessage());
            }
        }
    }

    /** Whether a table already carries every upgraded column (false while it is absent). */
    private static boolean spmUpgradeColumnsExist(Map<String, ScalarType> upgradeColumns,
            String tableName) {
        Optional<Database> dbOpt =
                Env.getCurrentEnv().getInternalCatalog().getDb(FeConstants.INTERNAL_DB_NAME);
        if (!dbOpt.isPresent()) {
            return false;
        }
        Table table = dbOpt.get().getTable(tableName).orElse(null);
        if (table == null) {
            return false;
        }
        Set<String> existing = table.getBaseSchema().stream()
                .map(column -> column.getName().toLowerCase(Locale.ROOT))
                .collect(Collectors.toSet());
        return existing.containsAll(upgradeColumns.keySet());
    }

    /**
     * Adds the missing columns of an upgrade map to a PRE-EXISTING table (one ALTER per
     * attempt, plain APPEND: the two tables' canonical order puts the upgraded columns
     * last, and their writes are name-addressed). Idempotent; throws on failure - the
     * caller's retry loop owns the policy.
     */
    private static void upgradeSpmTableSchema(Map<String, ScalarType> upgradeColumns,
            String label, String tableName) throws UserException {
        Optional<Database> dbOpt =
                Env.getCurrentEnv().getInternalCatalog().getDb(FeConstants.INTERNAL_DB_NAME);
        if (!dbOpt.isPresent()) {
            LOG.warn("SPM: internal schema db not found yet, will retry the {} upgrade", label);
            return;
        }
        Table table = dbOpt.get().getTable(tableName).orElse(null);
        if (table == null) {
            LOG.warn("SPM: {} table not found yet, will retry the upgrade", label);
            return;
        }
        // a table in SCHEMA_CHANGE rejects further ALTERs - wait for NORMAL instead of
        // re-issuing the same column forever (see the checkpoint upgrade)
        if (!(table instanceof OlapTable)
                || ((OlapTable) table).getState() != OlapTable.OlapTableState.NORMAL) {
            LOG.info("SPM: {} is not in NORMAL state ({}), waiting for the pending schema"
                            + " change before the upgrade", label,
                    table instanceof OlapTable ? ((OlapTable) table).getState() : "unknown");
            return;
        }
        Set<String> existing = table.getBaseSchema().stream()
                .map(column -> column.getName().toLowerCase(Locale.ROOT))
                .collect(Collectors.toSet());
        for (Map.Entry<String, ScalarType> entry : upgradeColumns.entrySet()) {
            if (existing.contains(entry.getKey())) {
                continue;
            }
            ColumnDefinition definition = spmUpgradeColumnDefinition(entry.getKey(), entry.getValue());
            AddColumnOp addColumnOp = new AddColumnOp(definition, null, null, null);
            addColumnOp.setColumn(definition.translateToCatalogStyleForSchemaChange());
            TableNameInfo tableNameInfo = new TableNameInfo(InternalCatalog.INTERNAL_CATALOG_NAME,
                    FeConstants.INTERNAL_DB_NAME, tableName);
            Env.getCurrentEnv().alterTable(
                    new AlterTableCommand(tableNameInfo, Lists.newArrayList(addColumnOp)));
            LOG.info("SPM: added the {} column to {}", entry.getKey(), tableName);
            // ONE column per attempt: the table enters SCHEMA_CHANGE until this alter
            // finishes, and the wait loop's next round continues with the remainder
            return;
        }
    }

    private static String getStatisticsCreateSql(String tableName, List<String> uniqueKeys) throws UserException {
        String catalogName = InternalCatalog.INTERNAL_CATALOG_NAME;
        String dbName = FeConstants.INTERNAL_DB_NAME;
        int bucketNum = StatisticConstants.STATISTIC_TABLE_BUCKET_COUNT;
        Map<String, String> properties = new HashMap<String, String>() {
            {
                put(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, String.valueOf(
                        Math.max(1, Config.min_replication_num_per_tablet)));
            }
        };
        return getStatisticsCreateSql(catalogName, dbName, tableName, uniqueKeys, bucketNum, properties);
    }

    private static String getStatisticsCreateSql(String catalogName, String dbName, String tableName,
            List<String> uniqueKeys, int bucketNum,
            Map<String, String> properties) throws UserException {

        StringBuilder uniqueKeyStr = new StringBuilder();
        for (String key : uniqueKeys) {
            if (uniqueKeyStr.length() > 0) {
                uniqueKeyStr.append(", ");
            }
            uniqueKeyStr.append("`").append(key).append("`");
        }

        String template =
                "CREATE TABLE IF NOT EXISTS `%s`.`%s`.`%s` (\n"
                        + "%s\n"
                        + ") ENGINE = olap\n"
                        + "UNIQUE KEY(%s)\n"
                        + "COMMENT \"Doris internal statistics table, DO NOT MODIFY IT\"\n"
                        + "DISTRIBUTED BY HASH(%s)\n"
                        + "BUCKETS %d\n"
                        + "PROPERTIES (%s)";

        return String.format(template, catalogName, dbName, tableName,
                generateColumnDefinitions(InternalSchema.getCopiedSchema(tableName)), uniqueKeyStr, uniqueKeyStr,
                bucketNum, getPropertyStr(properties));
    }

    private static String getAuditLogCreateSql() throws UserException {
        String catalogName = InternalCatalog.INTERNAL_CATALOG_NAME;
        String dbName = FeConstants.INTERNAL_DB_NAME;
        String tableName = AuditLoader.AUDIT_LOG_TABLE;

        Map<String, String> properties = new HashMap<String, String>() {
            {
                put("dynamic_partition.time_unit", "DAY");
                put("dynamic_partition.start", "-30");
                put("dynamic_partition.end", "3");
                put("dynamic_partition.prefix", "p");
                put("dynamic_partition.buckets", "2");
                put("dynamic_partition.enable", "true");
                put("replication_num", String.valueOf(Math.max(1,
                        Config.min_replication_num_per_tablet)));
            }
        };

        String template =
                "CREATE TABLE IF NOT EXISTS `%s`.`%s`.`%s` (\n"
                        + "%s\n"
                        + ") ENGINE = olap\n"
                        + "DUPLICATE KEY(`query_id`, `time`, `client_ip`)\n"
                        + "COMMENT \"Doris internal audit table, DO NOT MODIFY IT\"\n"
                        + "PARTITION BY RANGE(`time`)\n"
                        + "(\n"
                        + "\n"
                        + ")\n"
                        + "DISTRIBUTED BY HASH(`query_id`)\n"
                        + "BUCKETS 2\n"
                        + "PROPERTIES (%s)";
        return String.format(template, catalogName, dbName, tableName,
                generateColumnDefinitions(InternalSchema.getCopiedSchema(tableName)), getPropertyStr(properties));
    }

    private static String getSpmBaselinesCreateSql() throws UserException {
        String catalogName = InternalCatalog.INTERNAL_CATALOG_NAME;
        String dbName = FeConstants.INTERNAL_DB_NAME;
        String tableName = InternalSchema.SPM_BASELINES_TBL_NAME;

        Map<String, String> properties = new HashMap<String, String>() {
            {
                put(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, String.valueOf(
                        Math.max(1, Config.min_replication_num_per_tablet)));
            }
        };

        String template =
                "CREATE TABLE IF NOT EXISTS `%s`.`%s`.`%s` (\n"
                        + "%s\n"
                        + ") ENGINE = olap\n"
                        + "DUPLICATE KEY(`id`)\n"
                        + "COMMENT \"Doris internal SPM baselines table, DO NOT MODIFY IT\"\n"
                        + "DISTRIBUTED BY HASH(`id`)\n"
                        + "BUCKETS 10\n"
                        + "PROPERTIES (%s)";
        return String.format(template, catalogName, dbName, tableName,
                generateColumnDefinitions(InternalSchema.getCopiedSchema(tableName)), getPropertyStr(properties));
    }

    /**
     * CREATE SQL of the SPM plan-capture checkpoint table: APPEND-ONLY rows keyed by
     * (leader_epoch, write_seq). Every write is a plain INSERT whose
     * row carries the writer's next token; the reader takes the GREATEST row, so a stale
     * writer's delayed row can only be IGNORED - it can never replace the newest
     * checkpoint (the old UNIQUE-key(id) merge-on-write UPSERT treated a late write from a
     * demoted leader as the newest row). The duplicate key cannot dedupe equal tokens,
     * which is fine: equal-token rows are equivalent writes, and the reader's update_time
     * tie-break prefers the newer commit.
     */
    private static String getSpmCaptureCheckpointCreateSql() throws UserException {
        String catalogName = InternalCatalog.INTERNAL_CATALOG_NAME;
        String dbName = FeConstants.INTERNAL_DB_NAME;
        String tableName = InternalSchema.SPM_CAPTURE_CHECKPOINT_TBL_NAME;

        Map<String, String> properties = new HashMap<String, String>() {
            {
                put(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, String.valueOf(
                        Math.max(1, Config.min_replication_num_per_tablet)));
            }
        };

        String template =
                "CREATE TABLE IF NOT EXISTS `%s`.`%s`.`%s` (\n"
                        + "%s\n"
                        + ") ENGINE = olap\n"
                        + "DUPLICATE KEY(`leader_epoch`, `write_seq`)\n"
                        + "COMMENT \"Doris internal SPM capture checkpoint table, DO NOT MODIFY IT\"\n"
                        + "DISTRIBUTED BY HASH(`leader_epoch`)\n"
                        + "BUCKETS 1\n"
                        + "PROPERTIES (%s)";
        return String.format(template, catalogName, dbName, tableName,
                generateColumnDefinitions(InternalSchema.getCopiedSchema(tableName)), getPropertyStr(properties));
    }

    /**
     * CREATE SQL of the checkpoint migration's staging table (see
     * ensureSpmCaptureCheckpointColumnsExist): the append-only checkpoint schema under
     * the staging name, so the payload of the pre-append-only row can be parked durably
     * while the model is rebuilt. Transient - dropped once the recreated table's copy is
     * confirmed readable - and deliberately NOT part of the InternalSchema registry: it
     * exists only while a migration is in flight.
     */
    @VisibleForTesting
    static String getSpmCaptureCheckpointStageCreateSql() throws UserException {
        String catalogName = InternalCatalog.INTERNAL_CATALOG_NAME;
        String dbName = FeConstants.INTERNAL_DB_NAME;
        String tableName = SPM_CAPTURE_CHECKPOINT_STAGE_TBL_NAME;

        Map<String, String> properties = new HashMap<String, String>() {
            {
                put(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, String.valueOf(
                        Math.max(1, Config.min_replication_num_per_tablet)));
            }
        };

        String template =
                "CREATE TABLE IF NOT EXISTS `%s`.`%s`.`%s` (\n"
                        + "%s\n"
                        + ") ENGINE = olap\n"
                        + "DUPLICATE KEY(`leader_epoch`, `write_seq`)\n"
                        + "COMMENT \"Doris internal SPM checkpoint migration staging table,"
                        + " DO NOT MODIFY IT\"\n"
                        + "DISTRIBUTED BY HASH(`leader_epoch`)\n"
                        + "BUCKETS 1\n"
                        + "PROPERTIES (%s)";
        return String.format(template, catalogName, dbName, tableName,
                generateColumnDefinitions(InternalSchema.getCopiedSchema(
                        InternalSchema.SPM_CAPTURE_CHECKPOINT_TBL_NAME)), getPropertyStr(properties));
    }

    /**
     * CREATE SQL of the SPM baseline id sequence table: append-only rows (id = 1) whose
     * MAX(last_id) is the id high-water mark of the create path. It must survive a DROP
     * of the baseline that held the highest id - the baselines table's own MAX(id) falls
     * back with the row, and a reused id would let a delayed DROP-by-id retry remove a
     * DIFFERENT baseline (see InternalSchema#SPM_BASELINES_SEQ_TBL_NAME).
     */
    private static String getSpmBaselinesSeqCreateSql() throws UserException {
        String catalogName = InternalCatalog.INTERNAL_CATALOG_NAME;
        String dbName = FeConstants.INTERNAL_DB_NAME;
        String tableName = InternalSchema.SPM_BASELINES_SEQ_TBL_NAME;

        Map<String, String> properties = new HashMap<String, String>() {
            {
                put(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, String.valueOf(
                        Math.max(1, Config.min_replication_num_per_tablet)));
            }
        };

        String template =
                "CREATE TABLE IF NOT EXISTS `%s`.`%s`.`%s` (\n"
                        + "%s\n"
                        + ") ENGINE = olap\n"
                        + "DUPLICATE KEY(`id`)\n"
                        + "COMMENT \"Doris internal SPM baseline id sequence table, DO NOT MODIFY IT\"\n"
                        + "DISTRIBUTED BY HASH(`id`)\n"
                        + "BUCKETS 1\n"
                        + "PROPERTIES (%s)";
        return String.format(template, catalogName, dbName, tableName,
                generateColumnDefinitions(InternalSchema.getCopiedSchema(tableName)), getPropertyStr(properties));
    }

    /**
     * CREATE SQL of the COMPACT SPM baseline id high-water-mark table:
     * append-only rows (id = 1) carrying the newest allocated id, pruned after every
     * write. It answers the id watermark read in one bounded scan however many creates
     * the cluster has served (see InternalSchema#SPM_BASELINES_HWM_TBL_NAME).
     */
    private static String getSpmBaselinesHwmCreateSql() throws UserException {
        String catalogName = InternalCatalog.INTERNAL_CATALOG_NAME;
        String dbName = FeConstants.INTERNAL_DB_NAME;
        String tableName = InternalSchema.SPM_BASELINES_HWM_TBL_NAME;

        Map<String, String> properties = new HashMap<String, String>() {
            {
                put(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, String.valueOf(
                        Math.max(1, Config.min_replication_num_per_tablet)));
            }
        };

        String template =
                "CREATE TABLE IF NOT EXISTS `%s`.`%s`.`%s` (\n"
                        + "%s\n"
                        + ") ENGINE = olap\n"
                        + "DUPLICATE KEY(`id`)\n"
                        + "COMMENT \"Doris internal SPM baseline id high-water mark table,"
                        + " DO NOT MODIFY IT\"\n"
                        + "DISTRIBUTED BY HASH(`id`)\n"
                        + "BUCKETS 1\n"
                        + "PROPERTIES (%s)";
        return String.format(template, catalogName, dbName, tableName,
                generateColumnDefinitions(InternalSchema.getCopiedSchema(tableName)), getPropertyStr(properties));
    }

    /**
     * CREATE SQL of the cluster-wide audit publication horizon table: one row per FE
     * (upserted by its own audit loader) holding the start time of the oldest audit event
     * that FE has accepted but not yet published. The SPM capture reads the MINIMUM over
     * the fresh rows so a follower's delayed event cannot fall behind the leader's
     * advanced scan watermark (see InternalSchema#SPM_AUDIT_HORIZON_TBL_NAME).
     */
    private static String getSpmAuditHorizonCreateSql() throws UserException {
        String catalogName = InternalCatalog.INTERNAL_CATALOG_NAME;
        String dbName = FeConstants.INTERNAL_DB_NAME;
        String tableName = InternalSchema.SPM_AUDIT_HORIZON_TBL_NAME;

        Map<String, String> properties = new HashMap<String, String>() {
            {
                put(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, String.valueOf(
                        Math.max(1, Config.min_replication_num_per_tablet)));
                // merge-on-write makes each FE's per-row upsert atomic, exactly like the
                // capture checkpoint: a reporter crash can never leave the fence of that
                // FE half-written
                put(PropertyAnalyzer.ENABLE_UNIQUE_KEY_MERGE_ON_WRITE, "true");
            }
        };

        String template =
                "CREATE TABLE IF NOT EXISTS `%s`.`%s`.`%s` (\n"
                        + "%s\n"
                        + ") ENGINE = olap\n"
                        + "UNIQUE KEY(`fe_name`)\n"
                        + "COMMENT \"Doris internal audit publication horizon table, DO NOT MODIFY IT\"\n"
                        + "DISTRIBUTED BY HASH(`fe_name`)\n"
                        + "BUCKETS 1\n"
                        + "PROPERTIES (%s)";
        return String.format(template, catalogName, dbName, tableName,
                generateColumnDefinitions(InternalSchema.getCopiedSchema(tableName)), getPropertyStr(properties));
    }

    private static String getPropertyStr(Map<String, String> properties) {
        StringBuilder propertiesStr = new StringBuilder();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (propertiesStr.length() > 0) {
                propertiesStr.append(", ");
            }
            propertiesStr.append("\"").append(entry.getKey()).append("\" = \"").append(entry.getValue()).append("\"");
        }
        return propertiesStr.toString();
    }

    private static String generateColumnDefinitions(List<ColumnDef> schema) {
        StringBuilder sb = new StringBuilder();
        for (ColumnDef column : schema) {
            sb.append("  `").append(column.getName()).append("` ")
                    .append(column.getType().toSql())
                    .append(column.isAllowNull() ? " NULL" : " NOT NULL")
                    .append(" COMMENT \"\",\n");
        }
        if (!schema.isEmpty()) {
            sb.setLength(sb.length() - 2);
        }
        return sb.toString();
    }

    private static void createTable(String sql) {
        try (AutoCloseConnectContext r = StatisticsUtil.buildConnectContext(false)) {
            NereidsParser nereidsParser = new NereidsParser();
            LogicalPlan parsed = nereidsParser.parseSingle(sql);
            StmtExecutor stmtExecutor = new StmtExecutor(r.connectContext, sql);
            if (parsed instanceof CreateTableCommand) {
                ((CreateTableCommand) parsed).run(r.connectContext, stmtExecutor);
            }
        } catch (Exception e) {
            LOG.info("Failed to create table {}. Reason {}", sql, e.getMessage());
        }
    }

    @VisibleForTesting
    public static void createDb() {
        CreateDatabaseCommand command = new CreateDatabaseCommand(true,
                new DbName("internal", FeConstants.INTERNAL_DB_NAME), null);
        try {
            Env.getCurrentEnv().createDb(command);
        } catch (DdlException e) {
            LOG.warn("Failed to create database: {}, will try again later",
                    FeConstants.INTERNAL_DB_NAME, e);
        }
    }


    private boolean created() {
        // 1. check database exist
        Optional<Database> optionalDatabase =
                Env.getCurrentEnv().getInternalCatalog()
                        .getDb(FeConstants.INTERNAL_DB_NAME);
        if (!optionalDatabase.isPresent()) {
            return false;
        }
        Database db = optionalDatabase.get();
        Optional<Table> optionalTable = db.getTable(StatisticConstants.TABLE_STATISTIC_TBL_NAME);
        if (!optionalTable.isPresent()) {
            return false;
        }

        // 2. check statistic tables
        Table statsTbl = optionalTable.get();
        Optional<Column> optionalColumn =
                statsTbl.fullSchema.stream().filter(c -> c.getName().equals("count")).findFirst();
        if (!optionalColumn.isPresent() || !optionalColumn.get().isAllowNull()) {
            try {
                Env.getCurrentEnv().getInternalCatalog()
                        .dropTable(StatisticConstants.DB_NAME, StatisticConstants.TABLE_STATISTIC_TBL_NAME,
                                false, false, false, true, false, true);
            } catch (Exception e) {
                LOG.warn("Failed to drop outdated table", e);
            }
            return false;
        }
        optionalTable = db.getTable(StatisticConstants.PARTITION_STATISTIC_TBL_NAME);
        if (!optionalTable.isPresent()) {
            return false;
        }

        // 3. check audit table
        optionalTable = db.getTable(AuditLoader.AUDIT_LOG_TABLE);
        if (!optionalTable.isPresent()) {
            return false;
        }

        // 4. check the SPM baselines table: an UPGRADED cluster already has every legacy
        // table above, so without this check created() returns true before run() ever
        // reaches createTbl() - the missing spm_baselines table would never be created,
        // every load attempt would fail, BaselineManager would stay unloaded and global
        // CREATE/ALTER/DROP BASELINE would keep reporting that the store is not ready.
        if (isSpmBaselinesTableMissing(db)) {
            return false;
        }

        // 4b. check the SPM plan-capture checkpoint table the same way: an upgraded cluster
        // has spm_baselines already, so without its own check the table would never be
        // created and the capture checkpoint could not be persisted / resumed.
        if (isSpmCaptureCheckpointTableMissing(db)) {
            return false;
        }

        // 4c. check the SPM baseline id sequence table the same way: without it the create
        // path cannot read / advance the id high-water mark and every CREATE BASELINE would
        // fail retryably.
        if (isSpmBaselinesSeqTableMissing(db)) {
            return false;
        }

        // 4c-2. check the compact id high-water-mark table the same way:
        // without it every create falls back to the unbounded history read.
        if (isSpmBaselinesHwmTableMissing(db)) {
            return false;
        }

        // 4d. check the audit publication horizon table the same way: the capture reads its
        // rows to fence a follower's still-unpublished backlog, so an upgraded cluster must
        // gain it too.
        if (isSpmAuditHorizonTableMissing(db)) {
            return false;
        }

        // 5. check and update audit table schema
        OlapTable auditTable = (OlapTable) optionalTable.get();

        // 6. check if we need to add new columns
        return alterAuditSchemaIfNeeded(auditTable);
    }

    /**
     * Whether the SPM baselines internal table is absent. Package-visible for the
     * upgrade test: a cluster where only this table is missing must NOT be considered
     * initialized.
     *
     * @param db the internal schema database
     * @return true when spm_baselines does not exist yet
     */
    @VisibleForTesting
    static boolean isSpmBaselinesTableMissing(Database db) {
        return !db.getTable(InternalSchema.SPM_BASELINES_TBL_NAME).isPresent();
    }

    /**
     * Whether the SPM plan-capture checkpoint internal table is absent. Package-visible for
     * the upgrade test, exactly like isSpmBaselinesTableMissing.
     *
     * @param db the internal schema database
     * @return true when spm_capture_checkpoint does not exist yet
     */
    @VisibleForTesting
    static boolean isSpmCaptureCheckpointTableMissing(Database db) {
        return !db.getTable(InternalSchema.SPM_CAPTURE_CHECKPOINT_TBL_NAME).isPresent();
    }

    /**
     * Whether the SPM baseline id sequence internal table is absent. Package-visible for
     * the upgrade test, exactly like isSpmBaselinesTableMissing.
     *
     * @param db the internal schema database
     * @return true when spm_baselines_seq does not exist yet
     */
    @VisibleForTesting
    static boolean isSpmBaselinesSeqTableMissing(Database db) {
        return !db.getTable(InternalSchema.SPM_BASELINES_SEQ_TBL_NAME).isPresent();
    }

    /**
     * Whether the compact SPM id high-water-mark internal table is absent. Package-visible
     * for the upgrade test, exactly like isSpmBaselinesTableMissing.
     *
     * @param db the internal schema database
     * @return true when spm_baselines_hwm does not exist yet
     */
    @VisibleForTesting
    static boolean isSpmBaselinesHwmTableMissing(Database db) {
        return !db.getTable(InternalSchema.SPM_BASELINES_HWM_TBL_NAME).isPresent();
    }

    /**
     * Whether the audit publication horizon internal table is absent. Package-visible for
     * the upgrade test, exactly like isSpmBaselinesTableMissing.
     *
     * @param db the internal schema database
     * @return true when spm_audit_horizon does not exist yet
     */
    @VisibleForTesting
    static boolean isSpmAuditHorizonTableMissing(Database db) {
        return !db.getTable(InternalSchema.SPM_AUDIT_HORIZON_TBL_NAME).isPresent();
    }

    private boolean alterAuditSchemaIfNeeded(OlapTable auditTable) {
        List<ColumnDef> expectedSchema = InternalSchema.AUDIT_SCHEMA;
        List<String> expectedColumnNames = expectedSchema.stream()
                .map(ColumnDef::getName)
                .map(String::toLowerCase)
                .collect(Collectors.toList());
        List<Column> currentColumns = auditTable.getBaseSchema();
        List<String> currentColumnNames = currentColumns.stream()
                .map(Column::getName)
                .map(String::toLowerCase)
                .collect(Collectors.toList());
        // check if all expected columns are exists and in the right order
        if (currentColumnNames.size() >= expectedColumnNames.size()
                && expectedColumnNames.equals(currentColumnNames.subList(0, expectedColumnNames.size()))) {
            return true;
        }

        List<AlterTableOp> alterClauses = Lists.newArrayList();
        // add new columns
        List<Column> addColumns = Lists.newArrayList();
        for (ColumnDef expected : expectedSchema) {
            if (!currentColumnNames.contains(expected.getName().toLowerCase())) {
                addColumns.add(new Column(expected.getName(), expected.getType(), expected.isAllowNull()));
            }
        }
        if (!addColumns.isEmpty()) {
            AddColumnsOp addColumnsOp = new AddColumnsOp(null, Maps.newHashMap(), addColumns);
            alterClauses.add(addColumnsOp);
        }
        // reorder columns
        List<String> removedColumnNames = Lists.newArrayList(currentColumnNames);
        removedColumnNames.removeAll(expectedColumnNames);
        List<String> newColumnOrders = Lists.newArrayList(expectedColumnNames);
        newColumnOrders.addAll(removedColumnNames);
        ReorderColumnsOp reorderColumnsOp = new ReorderColumnsOp(newColumnOrders, null, Maps.newHashMap());
        alterClauses.add(reorderColumnsOp);
        TableNameInfo auditTableName = new TableNameInfo(InternalCatalog.INTERNAL_CATALOG_NAME,
                FeConstants.INTERNAL_DB_NAME, AuditLoader.AUDIT_LOG_TABLE);
        AlterTableCommand alterTableCommand = new AlterTableCommand(auditTableName, alterClauses);
        try {
            Env.getCurrentEnv().alterTable(alterTableCommand);
        } catch (Exception e) {
            LOG.warn("Failed to alter audit table schema", e);
            return false;
        }
        return true;
    }

    public static boolean isStatsTableSchemaValid() {
        return StatsTableSchemaValid;
    }
}
