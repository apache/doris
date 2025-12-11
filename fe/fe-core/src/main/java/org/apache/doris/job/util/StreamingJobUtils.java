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

package org.apache.doris.job.util;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.datasource.jdbc.client.JdbcClient;
import org.apache.doris.datasource.jdbc.client.JdbcClientConfig;
import org.apache.doris.datasource.jdbc.client.JdbcMySQLClient;
import org.apache.doris.job.cdc.DataSourceConfigKeys;
import org.apache.doris.job.cdc.SnapshotSplit;
import org.apache.doris.job.common.DataSourceType;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.nereids.trees.plans.commands.CreateTableCommand;
import org.apache.doris.nereids.trees.plans.commands.info.ColumnDefinition;
import org.apache.doris.nereids.trees.plans.commands.info.CreateTableInfo;
import org.apache.doris.nereids.trees.plans.commands.info.DistributionDescriptor;
import org.apache.doris.nereids.trees.plans.commands.info.PartitionTableInfo;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.statistics.ResultRow;
import org.apache.doris.system.Backend;
import org.apache.doris.system.BeSelectionPolicy;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TUniqueId;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.text.StringSubstitutor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

@Log4j2
public class StreamingJobUtils {
    public static final String TABLE_PROPS_PREFIX = "table.create.properties.";
    public static final String INTERNAL_STREAMING_JOB_META_TABLE_NAME = "streaming_job_meta";
    public static final String FULL_QUALIFIED_META_TBL_NAME = InternalCatalog.INTERNAL_CATALOG_NAME
            + "." + FeConstants.INTERNAL_DB_NAME + "." + INTERNAL_STREAMING_JOB_META_TABLE_NAME;
    private static final String CREATE_META_TABLE = "CREATE TABLE %s(\n"
            + "id         int,\n"
            + "job_id     bigint,\n"
            + "table_name string,\n"
            + "chunk_list json\n"
            + ")\n"
            + "UNIQUE KEY(id, job_id)\n"
            + "DISTRIBUTED BY HASH(job_id)\n"
            + "BUCKETS 2\n"
            + "PROPERTIES ('replication_num' = '1')"; // todo: modify replication num like statistic sys tbl
    private static final String BATCH_INSERT_INTO_META_TABLE_TEMPLATE =
            "INSERT INTO " + FULL_QUALIFIED_META_TBL_NAME + " values";

    private static final String INSERT_INTO_META_TABLE_TEMPLATE =
            "('${id}', '${job_id}', '${table_name}', '${chunk_list}')";

    private static final String SELECT_SPLITS_TABLE_TEMPLATE =
            "SELECT table_name, chunk_list from " + FULL_QUALIFIED_META_TBL_NAME + " WHERE job_id='%s' ORDER BY id ASC";

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void createMetaTableIfNotExist() throws Exception {
        Optional<Database> optionalDatabase =
                Env.getCurrentEnv().getInternalCatalog()
                        .getDb(FeConstants.INTERNAL_DB_NAME);
        if (!optionalDatabase.isPresent()) {
            // should not happen
            throw new JobException("Internal database does not exist");
        }
        Database database = optionalDatabase.get();
        Table t = database.getTableNullable(FULL_QUALIFIED_META_TBL_NAME);
        if (t == null) {
            executeInsert(String.format(CREATE_META_TABLE, FULL_QUALIFIED_META_TBL_NAME));
        }

        // double check
        t = database.getTableNullable(INTERNAL_STREAMING_JOB_META_TABLE_NAME);
        if (t == null) {
            throw new JobException(String.format("Table %s doesn't exist", FULL_QUALIFIED_META_TBL_NAME));
        }
    }

    public static Map<String, List<SnapshotSplit>> restoreSplitsToJob(Long jobId) throws IOException {
        List<ResultRow> resultRows;
        String sql = String.format(SELECT_SPLITS_TABLE_TEMPLATE, jobId);
        try (AutoCloseConnectContext context
                = new AutoCloseConnectContext(buildConnectContext())) {
            StmtExecutor stmtExecutor = new StmtExecutor(context.connectContext, sql);
            resultRows = stmtExecutor.executeInternalQuery();
        }

        Map<String, List<SnapshotSplit>> tableSplits = new LinkedHashMap<>();
        for (ResultRow row : resultRows) {
            String tableName = row.get(0);
            String chunkListStr = row.get(1);
            List<SnapshotSplit> splits =
                    new ArrayList<>(Arrays.asList(objectMapper.readValue(chunkListStr, SnapshotSplit[].class)));
            tableSplits.put(tableName, splits);
        }
        return tableSplits;
    }

    public static void insertSplitsToMeta(Long jobId, Map<String, List<SnapshotSplit>> tableSplits) throws Exception {
        List<String> values = new ArrayList<>();
        int index = 1;
        for (Map.Entry<String, List<SnapshotSplit>> entry : tableSplits.entrySet()) {
            Map<String, String> params = new HashMap<>();
            params.put("id", index + "");
            params.put("job_id", jobId + "");
            params.put("table_name", entry.getKey());
            params.put("chunk_list", objectMapper.writeValueAsString(entry.getValue()));
            StringSubstitutor stringSubstitutor = new StringSubstitutor(params);
            String sql = stringSubstitutor.replace(INSERT_INTO_META_TABLE_TEMPLATE);
            values.add(sql);
            index++;
        }
        batchInsert(values);
    }

    private static void batchInsert(List<String> values) throws Exception {
        if (values.isEmpty()) {
            return;
        }
        StringBuilder query = new StringBuilder(BATCH_INSERT_INTO_META_TABLE_TEMPLATE);
        for (int i = 0; i < values.size(); i++) {
            query.append(values.get(i));
            if (i + 1 != values.size()) {
                query.append(",");
            } else {
                query.append(";");
            }
        }
        executeInsert(query.toString());
    }

    private static void executeInsert(String sql) throws Exception {
        try (AutoCloseConnectContext context
                = new AutoCloseConnectContext(buildConnectContext())) {
            StmtExecutor stmtExecutor = new StmtExecutor(context.connectContext, sql);
            stmtExecutor.execute();
        }
    }

    private static ConnectContext buildConnectContext() {
        ConnectContext ctx = new ConnectContext();
        ctx.setEnv(Env.getCurrentEnv());
        ctx.setCurrentUserIdentity(UserIdentity.ADMIN);
        ctx.getState().reset();
        ctx.getState().setInternal(true);
        ctx.getState().setNereids(true);
        ctx.setThreadLocalInfo();
        UUID uuid = UUID.randomUUID();
        TUniqueId queryId = new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
        ctx.setQueryId(queryId);
        return ctx;
    }

    private static JdbcClient getJdbcClient(DataSourceType sourceType, Map<String, String> properties)
            throws JobException {
        JdbcClientConfig config = new JdbcClientConfig();
        config.setCatalog(sourceType.name());
        config.setUser(properties.get(DataSourceConfigKeys.USER));
        config.setPassword(properties.get(DataSourceConfigKeys.PASSWORD));
        config.setDriverClass(properties.get(DataSourceConfigKeys.DRIVER_CLASS));
        config.setDriverUrl(properties.get(DataSourceConfigKeys.DRIVER_URL));
        config.setJdbcUrl(properties.get(DataSourceConfigKeys.JDBC_URL));
        switch (sourceType) {
            case MYSQL:
                JdbcClient client = JdbcMySQLClient.createJdbcClient(config);
                return client;
            default:
                throw new JobException("Unsupported source type " + sourceType);
        }
    }

    public static Backend selectBackend(Long jobId) throws JobException {
        Backend backend = null;
        BeSelectionPolicy policy = null;

        policy = new BeSelectionPolicy.Builder()
                .setEnableRoundRobin(true)
                .needLoadAvailable().build();
        List<Long> backendIds;
        backendIds = Env.getCurrentSystemInfo().selectBackendIdsByPolicy(policy, -1);
        if (backendIds.isEmpty()) {
            throw new JobException(SystemInfoService.NO_BACKEND_LOAD_AVAILABLE_MSG + ", policy: " + policy);
        }
        // jobid % backendSize
        long index = backendIds.get(jobId.intValue() % backendIds.size());
        backend = Env.getCurrentSystemInfo().getBackend(index);
        if (backend == null) {
            throw new JobException(SystemInfoService.NO_BACKEND_LOAD_AVAILABLE_MSG + ", policy: " + policy);
        }
        return backend;
    }

    public static List<CreateTableCommand> generateCreateTableCmds(String targetDb, DataSourceType sourceType,
            Map<String, String> properties, Map<String, String> targetProperties)
            throws JobException {
        List<CreateTableCommand> createtblCmds = new ArrayList<>();
        String includeTables = properties.get(DataSourceConfigKeys.INCLUDE_TABLES);
        String excludeTables = properties.get(DataSourceConfigKeys.EXCLUDE_TABLES);
        List<String> includeTablesList = new ArrayList<>();
        if (includeTables != null) {
            includeTablesList = Arrays.asList(includeTables.split(","));
        }

        String database = properties.get(DataSourceConfigKeys.DATABASE);
        JdbcClient jdbcClient = getJdbcClient(sourceType, properties);
        List<String> tablesNameList = jdbcClient.getTablesNameList(database);
        if (tablesNameList.isEmpty()) {
            throw new JobException("No tables found in database " + database);
        }
        Map<String, String> tableCreateProperties = getTableCreateProperties(targetProperties);
        for (String table : tablesNameList) {
            if (!includeTablesList.isEmpty() && !includeTablesList.contains(table)) {
                log.info("Skip table {} in database {} as it does not in include_tables {}", table, database,
                        includeTables);
                continue;
            }

            if (excludeTables != null && excludeTables.contains(table)) {
                log.info("Skip table {} in database {} as it in exclude_tables {}", table, database,
                        excludeTables);
                continue;
            }

            List<Column> columns = jdbcClient.getColumnsFromJdbc(database, table);
            List<String> primaryKeys = jdbcClient.getPrimaryKeys(database, table);
            if (primaryKeys.isEmpty()) {
                primaryKeys.add(columns.get(0).getName());
                log.info("table {} no primary key, use first column {} to primary key", table,
                        columns.get(0).getName());
            }
            // Convert Column to ColumnDefinition
            List<ColumnDefinition> columnDefinitions = columns.stream().map(col -> {
                DataType dataType = DataType.fromCatalogType(col.getType());
                return new ColumnDefinition(col.getName(), dataType, col.isAllowNull(), col.getComment());
            }).collect(Collectors.toList());

            // Create DistributionDescriptor
            DistributionDescriptor distribution = new DistributionDescriptor(
                    true, // isHash
                    true, // isAutoBucket
                    FeConstants.default_bucket_num,
                    primaryKeys
            );

            // Create CreateTableInfo
            CreateTableInfo createtblInfo = new CreateTableInfo(
                    true, // ifNotExists
                    false, // isExternal
                    false, // isTemp
                    InternalCatalog.INTERNAL_CATALOG_NAME, // ctlName
                    targetDb, // dbName
                    table, // tableName
                    columnDefinitions, // columns
                    ImmutableList.of(), // indexes
                    "olap", // engineName
                    KeysType.UNIQUE_KEYS, // keysType
                    primaryKeys, // keys
                    "", // comment
                    PartitionTableInfo.EMPTY, // partitionTableInfo
                    distribution, // distribution
                    ImmutableList.of(), // rollups
                    new HashMap<>(tableCreateProperties), // properties
                    ImmutableMap.of(), // extProperties
                    ImmutableList.of() // clusterKeyColumnNames
            );
            CreateTableCommand createtblCmd = new CreateTableCommand(Optional.empty(), createtblInfo);
            createtblCmds.add(createtblCmd);
        }
        return createtblCmds;
    }

    private static Map<String, String> getTableCreateProperties(Map<String, String> properties) {
        final Map<String, String> tableCreateProps = new HashMap<>();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().startsWith(TABLE_PROPS_PREFIX)) {
                String subKey = entry.getKey().substring(TABLE_PROPS_PREFIX.length());
                tableCreateProps.put(subKey, entry.getValue());
            }
        }
        return tableCreateProps;
    }
}
