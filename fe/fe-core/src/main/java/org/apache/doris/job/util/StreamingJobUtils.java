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
import org.apache.doris.job.common.DataSourceType;
import org.apache.doris.job.common.LoadConstants;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.offset.jdbc.split.SnapshotSplit;
import org.apache.doris.nereids.trees.plans.commands.info.ColumnDefinition;
import org.apache.doris.nereids.trees.plans.commands.info.CreateTableInfo;
import org.apache.doris.nereids.trees.plans.commands.info.DistributionDescriptor;
import org.apache.doris.nereids.trees.plans.commands.info.PartitionTableInfo;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

@Log4j2
public class StreamingJobUtils {
    public static final String INTERNAL_STREAMING_JOB_META_TABLE_NAME = "streaming_job_meta";
    public static final String FULL_QUALIFIED_META_TBL_NAME = InternalCatalog.INTERNAL_CATALOG_NAME
            + "." + FeConstants.INTERNAL_DB_NAME + "." + INTERNAL_STREAMING_JOB_META_TABLE_NAME;
    private static final String CREATE_META_TABLE = "CREATE TABLE %s(\n"
            + "id         varchar(32),\n"
            + "job_id     varchar(256),\n"
            + "table_name string,\n"
            + "chunk_list json\n"
            + ")\n"
            + "UNIQUE KEY(id)\n"
            + "DISTRIBUTED BY HASH(id)\n"
            + "BUCKETS 1\n"
            + "PROPERTIES ('replication_num' = '1')"; // todo: modify replication num like statistic sys tbl
    private static final String BATCH_INSERT_INTO_META_TABLE_TEMPLATE =
            "INSERT INTO " + FULL_QUALIFIED_META_TBL_NAME + " values";

    private static final String INSERT_INTO_META_TABLE_TEMPLATE =
            "('${id}', '${job_id}', '${table_name}', '${chunk_list}')";

    private static final String SELECT_SPLITS_TABLE_TEMPLATE =
            "SELECT table_name, chunk_list from " + FULL_QUALIFIED_META_TBL_NAME + " WHERE job_id='%s'";

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

    public static Map<String, List<SnapshotSplit>> restoreSplitsToJob(Long jobId) throws Exception {
        List<ResultRow> resultRows = new ArrayList<>();
        String sql = String.format(SELECT_SPLITS_TABLE_TEMPLATE, jobId);
        try (AutoCloseConnectContext r = buildConnectContext()) {
            StmtExecutor stmtExecutor = new StmtExecutor(r.connectContext, sql);
            resultRows = stmtExecutor.executeInternalQuery();
        }

        Map<String, List<SnapshotSplit>> tableSplits = new HashMap<>();
        for (ResultRow row : resultRows) {
            String tableName = row.get(0);
            String chunkListStr = row.get(1);
            List<SnapshotSplit> splits = Arrays.asList(objectMapper.readValue(chunkListStr, SnapshotSplit[].class));
            tableSplits.put(tableName, splits);
        }
        return tableSplits;
    }

    public static void insertSplitsToMeta(Long jobId, Map<String, List<SnapshotSplit>> tableSplits) throws Exception {
        List<String> values = new ArrayList<>();
        for (Map.Entry<String, List<SnapshotSplit>> entry : tableSplits.entrySet()) {
            Map<String, String> params = new HashMap<>();
            params.put("id", UUID.randomUUID().toString().replace("-", ""));
            params.put("job_id", jobId + "");
            params.put("table_name", entry.getKey());
            params.put("chunk_list", objectMapper.writeValueAsString(entry.getValue()));
            StringSubstitutor stringSubstitutor = new StringSubstitutor(params);
            String sql = stringSubstitutor.replace(INSERT_INTO_META_TABLE_TEMPLATE);
            values.add(sql);
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
        try (AutoCloseConnectContext r = buildConnectContext()) {
            StmtExecutor stmtExecutor = new StmtExecutor(r.connectContext, sql);
            stmtExecutor.execute();
        }
    }

    private static AutoCloseConnectContext buildConnectContext() {
        ConnectContext connectContext = new ConnectContext();
        connectContext.getState().setInternal(true);
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        sessionVariable.setEnableInsertStrict(true);
        sessionVariable.setInsertMaxFilterRatio(1);
        sessionVariable.enableProfile = false;
        connectContext.setEnv(Env.getCurrentEnv());
        connectContext.setDatabase(FeConstants.INTERNAL_DB_NAME);
        UUID uuid = UUID.randomUUID();
        TUniqueId queryId = new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
        connectContext.setQueryId(queryId);
        connectContext.setStartTime();
        connectContext.setCurrentUserIdentity(UserIdentity.ADMIN);
        return new AutoCloseConnectContext(connectContext);
    }

    private static JdbcClient getJdbcClient(DataSourceType sourceType, Map<String, String> properties)
            throws JobException {
        JdbcClientConfig config = new JdbcClientConfig();
        config.setCatalog(sourceType.name());
        config.setUser(properties.get(LoadConstants.USER));
        config.setPassword(properties.get(LoadConstants.PASSWORD));
        config.setDriverClass(properties.get(LoadConstants.DRIVER_CLASS));
        config.setDriverUrl(properties.get(LoadConstants.DRIVER_URL));
        config.setJdbcUrl(properties.get(LoadConstants.JDBC_URL));
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

    public static List<CreateTableInfo> generateCreateTableInfos(String targetDb, DataSourceType sourceType,
            Map<String, String> properties, Map<String, String> targetProperties)
            throws JobException {
        List<CreateTableInfo> createtblInfos = new ArrayList<>();
        String includeTables = properties.get(LoadConstants.INCLUDE_TABLES);
        String excludeTables = properties.get(LoadConstants.EXCLUDE_TABLES);
        List<String> includeTablesList = new ArrayList<>();
        if (includeTables != null) {
            includeTablesList = Arrays.asList(includeTables.split(","));
        }

        String database = properties.get(LoadConstants.DATABASE);
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

            List<String> primaryKeys = columns.stream().filter(Column::isKey).map(Column::getName)
                    .collect(Collectors.toList());
            if (primaryKeys.isEmpty()) {
                primaryKeys.add(columns.get(0).getName());
                log.info("table {} no primary key, use first column {} to primary key", table,
                        columns.get(0).getName());
            }
            // Convert Column to ColumnDefinition
            List<ColumnDefinition> columnDefinitions = columns.stream().map(col -> {
                DataType dataType = DataType.fromCatalogType(col.getType());
                return new ColumnDefinition(col.getName(), dataType, col.isKey(), col.isAllowNull(), col.getComment());
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
                    tableCreateProperties, // properties
                    ImmutableMap.of(), // extProperties
                    ImmutableList.of() // clusterKeyColumnNames
            );
            createtblInfo.analyzeEngine();
            createtblInfos.add(createtblInfo);
        }
        return createtblInfos;
    }

    private static Map<String, String> getTableCreateProperties(Map<String, String> properties) {
        final Map<String, String> tableCreateProps = new HashMap<>();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().startsWith(LoadConstants.TABLE_PROPS_PREFIX)) {
                String subKey = entry.getKey().substring(LoadConstants.TABLE_PROPS_PREFIX.length());
                tableCreateProps.put(subKey, entry.getValue());
            }
        }
        return tableCreateProps;
    }
}
