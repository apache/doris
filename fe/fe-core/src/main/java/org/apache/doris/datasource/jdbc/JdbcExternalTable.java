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

package org.apache.doris.datasource.jdbc;

import org.apache.doris.analysis.StatementBase;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.JdbcResource;
import org.apache.doris.catalog.JdbcTable;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.statistics.AnalysisInfo;
import org.apache.doris.statistics.BaseAnalysisTask;
import org.apache.doris.statistics.JdbcAnalysisTask;
import org.apache.doris.statistics.ResultRow;
import org.apache.doris.statistics.util.StatisticsUtil;
import org.apache.doris.thrift.TTableDescriptor;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.text.StringSubstitutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Jdbc external table.
 */
public class JdbcExternalTable extends ExternalTable {
    private static final Logger LOG = LogManager.getLogger(JdbcExternalTable.class);

    public static final String MYSQL_ROW_COUNT_SQL = "SELECT max(row_count) as `rows` FROM ("
            + "(SELECT TABLE_ROWS AS row_count FROM INFORMATION_SCHEMA.TABLES "
            + "WHERE TABLE_SCHEMA = '${dbName}' AND TABLE_NAME = '${tblName}' "
            + "AND TABLE_TYPE = 'BASE TABLE') "
            + "UNION ALL "
            + "(SELECT CARDINALITY AS row_count FROM INFORMATION_SCHEMA.STATISTICS "
            + "WHERE TABLE_SCHEMA = '${dbName}' AND TABLE_NAME = '${tblName}' "
            + "AND CARDINALITY IS NOT NULL)) t";

    public static final String PG_ROW_COUNT_SQL = "SELECT reltuples as rows FROM pg_class "
            + "WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = '${dbName}') "
            + "AND relname = '${tblName}'";

    public static final String SQLSERVER_ROW_COUNT_SQL = "SELECT sum(rows) as rows FROM sys.partitions "
            + "WHERE object_id = (SELECT object_id('${dbName}.${tblName}')) AND index_id IN (0, 1)";

    public static final String ORACLE_ROW_COUNT_SQL = "SELECT NUM_ROWS as \\\"rows\\\" FROM ALL_TABLES WHERE "
            + "OWNER = '${dbName}' and TABLE_NAME = '${tblName}'";

    public static final String FETCH_ROW_COUNT_TEMPLATE = "SELECT * FROM QUERY"
            + "(\"catalog\"=\"${ctlName}\", \"query\"=\"${sql}\");";

    private JdbcTable jdbcTable;

    /**
     * Create jdbc external table.
     *
     * @param id Table id.
     * @param name Table name.
     * @param remoteName Remote table name.
     * @param catalog JdbcExternalCatalog.
     * @param db JdbcExternalDatabase.
     */
    public JdbcExternalTable(long id, String name, String remoteName, JdbcExternalCatalog catalog,
            JdbcExternalDatabase db) {
        super(id, name, remoteName, catalog, db, TableType.JDBC_EXTERNAL_TABLE);
    }

    @Override
    protected synchronized void makeSureInitialized() {
        super.makeSureInitialized();
        if (!objectCreated) {
            jdbcTable = toJdbcTable();
            objectCreated = true;
        }
    }

    public JdbcTable getJdbcTable() {
        makeSureInitialized();
        return jdbcTable;
    }

    @Override
    public TTableDescriptor toThrift() {
        makeSureInitialized();
        return jdbcTable.toThrift();
    }

    @Override
    public Optional<SchemaCacheValue> initSchema() {
        String remoteDbName = ((ExternalDatabase<?>) this.getDatabase()).getRemoteName();

        // 1. Retrieve remote column information
        List<Column> columns = ((JdbcExternalCatalog) catalog).listColumns(remoteDbName, remoteName);
        if (columns == null || columns.isEmpty()) {
            return Optional.empty();
        }

        // 2. Generate local column names from remote names
        List<String> remoteColumnNames = columns.stream()
                .map(Column::getName)
                .collect(Collectors.toList());
        List<String> localColumnNames = Lists.newArrayListWithCapacity(remoteColumnNames.size());
        for (String remoteColName : remoteColumnNames) {
            String localName = ((JdbcExternalCatalog) catalog).getIdentifierMapping()
                    .fromRemoteColumnName(remoteDbName, remoteName, remoteColName);
            localColumnNames.add(localName);
        }

        // 3. Collect potential conflicts in a case-insensitive scenario
        Map<String, List<String>> lowerCaseToLocalNames = Maps.newHashMap();
        for (String localColName : localColumnNames) {
            String lowerName = localColName.toLowerCase();
            lowerCaseToLocalNames
                    .computeIfAbsent(lowerName, k -> Lists.newArrayList())
                    .add(localColName);
        }

        // 4. Check for conflicts
        List<String> conflicts = lowerCaseToLocalNames.values().stream()
                .filter(names -> names.size() > 1)
                .flatMap(List::stream)
                .distinct()
                .collect(Collectors.toList());

        if (!conflicts.isEmpty()) {
            throw new RuntimeException(String.format(
                    "Found conflicting column names under case-insensitive conditions. "
                            + "Conflicting column names: %s in remote table '%s.%s' under catalog '%s'. "
                            + "Please use meta_names_mapping to handle name mapping.",
                    String.join(", ", conflicts), remoteDbName, remoteName, catalog.getName()));
        }

        // 5. Update column objects with local names
        for (int i = 0; i < columns.size(); i++) {
            columns.get(i).setName(localColumnNames.get(i));
        }

        // 6. Build remote->local mapping
        Map<String, String> remoteColumnNamesMap = Maps.newHashMap();
        for (int i = 0; i < columns.size(); i++) {
            remoteColumnNamesMap.put(localColumnNames.get(i), remoteColumnNames.get(i));
        }

        // 7. Return the SchemaCacheValue
        return Optional.of(new JdbcSchemaCacheValue(columns, remoteColumnNamesMap));
    }

    private JdbcTable toJdbcTable() {
        List<Column> schema = getFullSchema();
        JdbcExternalCatalog jdbcCatalog = (JdbcExternalCatalog) catalog;
        String fullTableName = this.dbName + "." + this.name;
        JdbcTable jdbcTable = new JdbcTable(this.id, fullTableName, schema, TableType.JDBC_EXTERNAL_TABLE);
        jdbcCatalog.configureJdbcTable(jdbcTable, fullTableName);

        // Set remote properties
        jdbcTable.setRemoteDatabaseName(((ExternalDatabase<?>) this.getDatabase()).getRemoteName());
        jdbcTable.setRemoteTableName(this.getRemoteName());
        Map<String, String> remoteColumnNames = Maps.newHashMap();
        Optional<SchemaCacheValue> schemaCacheValue = getSchemaCacheValue();
        for (Column column : schema) {
            String remoteColumnName = schemaCacheValue.map(value -> ((JdbcSchemaCacheValue) value)
                    .getremoteColumnName(column.getName())).orElse(column.getName());
            remoteColumnNames.put(column.getName(), remoteColumnName);
        }
        jdbcTable.setRemoteColumnNames(remoteColumnNames);

        return jdbcTable;
    }

    @Override
    public BaseAnalysisTask createAnalysisTask(AnalysisInfo info) {
        makeSureInitialized();
        return new JdbcAnalysisTask(info);
    }

    @Override
    public long fetchRowCount() {
        Map<String, String> params = new HashMap<>();
        params.put("ctlName", catalog.getName());
        params.put("dbName", this.db.getRemoteName());
        params.put("tblName", this.remoteName);
        switch (((JdbcExternalCatalog) catalog).getDatabaseTypeName()) {
            case JdbcResource.MYSQL:
                params.put("sql", MYSQL_ROW_COUNT_SQL);
                return getRowCount(params);
            case JdbcResource.POSTGRESQL:
                params.put("sql", PG_ROW_COUNT_SQL);
                return getRowCount(params);
            case JdbcResource.SQLSERVER:
                params.put("sql", SQLSERVER_ROW_COUNT_SQL);
                return getRowCount(params);
            case JdbcResource.ORACLE:
                params.put("sql", ORACLE_ROW_COUNT_SQL);
                return getRowCount(params);
            default:
                break;
        }
        return UNKNOWN_ROW_COUNT;
    }

    protected long getRowCount(Map<String, String> params) {
        try (AutoCloseConnectContext r = StatisticsUtil.buildConnectContext(false)) {
            StringSubstitutor stringSubstitutor = new StringSubstitutor(params);
            String sql = stringSubstitutor.replace(FETCH_ROW_COUNT_TEMPLATE);
            StmtExecutor stmtExecutor = new StmtExecutor(r.connectContext, sql);
            List<ResultRow> resultRows = stmtExecutor.executeInternalQuery();
            if (resultRows == null || resultRows.size() != 1) {
                LOG.info("No status found for table {}.{}.{}", catalog.getName(), dbName, name);
                return UNKNOWN_ROW_COUNT;
            }
            StatementBase parsedStmt = stmtExecutor.getParsedStmt();
            if (parsedStmt == null || parsedStmt.getColLabels() == null) {
                LOG.info("No column label found for table {}.{}.{}", catalog.getName(), dbName, name);
                return UNKNOWN_ROW_COUNT;
            }
            ResultRow resultRow = resultRows.get(0);
            List<String> colLabels = parsedStmt.getColLabels();
            int index = colLabels.indexOf("rows");
            if (index == -1) {
                LOG.info("No TABLE_ROWS in status for table {}.{}.{}", catalog.getName(), dbName, name);
                return UNKNOWN_ROW_COUNT;
            }
            long rows = Long.parseLong(resultRow.get(index));
            if (rows <= 0) {
                LOG.info("Table {}.{}.{} row count is {}, discard it and use -1 instead",
                        catalog.getName(), dbName, name, rows);
                return UNKNOWN_ROW_COUNT;
            }
            LOG.info("Get table {}.{}.{} row count {}", catalog.getName(), dbName, name, rows);
            return rows;
        } catch (Exception e) {
            LOG.warn("Failed to fetch row count for table {}.{}.{}. Reason [{}]",
                    catalog.getName(), dbName, name, e.getMessage());
            return UNKNOWN_ROW_COUNT;
        }
    }
}
