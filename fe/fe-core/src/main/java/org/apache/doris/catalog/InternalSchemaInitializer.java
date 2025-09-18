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
import org.apache.doris.analysis.TypeDef;
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
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.plugin.audit.AuditLoader;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.statistics.StatisticConstants;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
        modifyTblReplicaCount(database, StatisticConstants.TABLE_STATISTIC_TBL_NAME);
        modifyTblReplicaCount(database, StatisticConstants.PARTITION_STATISTIC_TBL_NAME);
        modifyTblReplicaCount(database, AuditLoader.AUDIT_LOG_TABLE);
    }

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
                TypeDef typeDef = new TypeDef(
                        ScalarType.createVarchar(StatisticConstants.MAX_NAME_LEN), col.isAllowNull());
                ColumnNullableType nullableType =
                        col.isAllowNull() ? ColumnNullableType.NULLABLE : ColumnNullableType.NOT_NULLABLE;

                ColumnDefinition columnDefinition = new ColumnDefinition(
                        col.getName(),
                        DataType.fromCatalogType(typeDef.getType()),
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
                            // 1. modify table's default replica num
                            Map<String, String> props = new HashMap<>();
                            props.put("default." + PropertyAnalyzer.PROPERTIES_REPLICATION_NUM,
                                    "" + StatisticConstants.STATISTIC_INTERNAL_TABLE_REPLICA_NUM);
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
                    .append(column.getTypeDef().toSql())
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
                                false, false, true, false, true);
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

        // 4. check and update audit table schema
        OlapTable auditTable = (OlapTable) optionalTable.get();

        // 5. check if we need to add new columns
        return alterAuditSchemaIfNeeded(auditTable);
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
