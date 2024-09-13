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
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.statistics.AnalysisInfo;
import org.apache.doris.statistics.BaseAnalysisTask;
import org.apache.doris.statistics.ExternalAnalysisTask;
import org.apache.doris.statistics.ResultRow;
import org.apache.doris.statistics.util.StatisticsUtil;
import org.apache.doris.thrift.TTableDescriptor;

import org.apache.commons.text.StringSubstitutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Jdbc external table.
 */
public class JdbcExternalTable extends ExternalTable {
    private static final Logger LOG = LogManager.getLogger(JdbcExternalTable.class);

    public static final String MYSQL_ROW_COUNT_SQL = "SELECT * FROM QUERY"
            + "(\"catalog\"=\"${ctlName}\", \"query\"=\"show table status from `${dbName}` like '${tblName}'\");";

    private JdbcTable jdbcTable;

    /**
     * Create jdbc external table.
     *
     * @param id Table id.
     * @param name Table name.
     * @param dbName Database name.
     * @param catalog HMSExternalDataSource.
     */
    public JdbcExternalTable(long id, String name, String dbName, JdbcExternalCatalog catalog) {
        super(id, name, catalog, dbName, TableType.JDBC_EXTERNAL_TABLE);
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
        return Optional.of(new SchemaCacheValue(((JdbcExternalCatalog) catalog).getJdbcClient()
                .getColumnsFromJdbc(dbName, name)));
    }

    private JdbcTable toJdbcTable() {
        List<Column> schema = getFullSchema();
        JdbcExternalCatalog jdbcCatalog = (JdbcExternalCatalog) catalog;
        String fullDbName = this.dbName + "." + this.name;
        JdbcTable jdbcTable = new JdbcTable(this.id, fullDbName, schema, TableType.JDBC_EXTERNAL_TABLE);
        jdbcCatalog.configureJdbcTable(jdbcTable, fullDbName);

        // Set remote properties
        jdbcTable.setRemoteDatabaseName(jdbcCatalog.getJdbcClient().getRemoteDatabaseName(this.dbName));
        jdbcTable.setRemoteTableName(jdbcCatalog.getJdbcClient().getRemoteTableName(this.dbName, this.name));
        jdbcTable.setRemoteColumnNames(jdbcCatalog.getJdbcClient().getRemoteColumnNames(this.dbName, this.name));

        return jdbcTable;
    }

    @Override
    public BaseAnalysisTask createAnalysisTask(AnalysisInfo info) {
        makeSureInitialized();
        return new ExternalAnalysisTask(info);
    }

    @Override
    public long fetchRowCount() {
        Map<String, String> params = new HashMap<>();
        params.put("ctlName", catalog.getName());
        params.put("dbName", dbName);
        params.put("tblName", name);
        switch (((JdbcExternalCatalog) catalog).getDatabaseTypeName()) {
            case JdbcResource.MYSQL:
                try (AutoCloseConnectContext r = StatisticsUtil.buildConnectContext(false, false)) {
                    StringSubstitutor stringSubstitutor = new StringSubstitutor(params);
                    String sql = stringSubstitutor.replace(MYSQL_ROW_COUNT_SQL);
                    StmtExecutor stmtExecutor = new StmtExecutor(r.connectContext, sql);
                    List<ResultRow> resultRows = stmtExecutor.executeInternalQuery();
                    if (resultRows == null || resultRows.size() != 1) {
                        LOG.info("No mysql status found for table {}.{}.{}", catalog.getName(), dbName, name);
                        return -1;
                    }
                    StatementBase parsedStmt = stmtExecutor.getParsedStmt();
                    if (parsedStmt == null || parsedStmt.getColLabels() == null) {
                        LOG.info("No column label found for table {}.{}.{}", catalog.getName(), dbName, name);
                        return -1;
                    }
                    ResultRow resultRow = resultRows.get(0);
                    List<String> colLabels = parsedStmt.getColLabels();
                    int index = colLabels.indexOf("TABLE_ROWS");
                    if (index == -1) {
                        LOG.info("No TABLE_ROWS in status for table {}.{}.{}", catalog.getName(), dbName, name);
                        return -1;
                    }
                    long rows = Long.parseLong(resultRow.get(index));
                    LOG.info("Get mysql table {}.{}.{} row count {}", catalog.getName(), dbName, name, rows);
                    return rows;
                } catch (Exception e) {
                    LOG.warn("Failed to fetch mysql row count for table {}.{}.{}. Reason [{}]",
                            catalog.getName(), dbName, name, e.getMessage());
                    return -1;
                }
            case JdbcResource.ORACLE:
            case JdbcResource.POSTGRESQL:
            case JdbcResource.SQLSERVER:
            default:
                break;
        }
        return -1;
    }
}
