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

package org.apache.doris.job.extensions.cdc.utils;

import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.analysis.DistributionDesc;
import org.apache.doris.analysis.HashDistributionDesc;
import org.apache.doris.analysis.KeysDesc;
import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.datasource.jdbc.client.JdbcClient;
import org.apache.doris.datasource.jdbc.client.JdbcClientConfig;
import org.apache.doris.datasource.jdbc.client.JdbcMySQLClient;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.extensions.cdc.DataSourceType;

import static org.apache.doris.job.extensions.cdc.utils.CdcLoadConstants.DATABASE_NAME;
import static org.apache.doris.job.extensions.cdc.utils.CdcLoadConstants.DB_SOURCE_TYPE;
import static org.apache.doris.job.extensions.cdc.utils.CdcLoadConstants.HOST;
import static org.apache.doris.job.extensions.cdc.utils.CdcLoadConstants.INCLUDE_TABLES_LIST;
import static org.apache.doris.job.extensions.cdc.utils.CdcLoadConstants.PASSWORD;
import static org.apache.doris.job.extensions.cdc.utils.CdcLoadConstants.PORT;
import static org.apache.doris.job.extensions.cdc.utils.CdcLoadConstants.USERNAME;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CdcUtils {
    private static final Logger LOG = LogManager.getLogger(CdcUtils.class);

    public static JdbcClient getJdbcClient(Map<String, String> properties) throws JobException {
        DataSourceType sourceType = DataSourceType.getByType(properties.get(DB_SOURCE_TYPE));
        JdbcClientConfig config = new JdbcClientConfig();
        config.setCatalog(properties.get(DB_SOURCE_TYPE));
        config.setUser(properties.get(USERNAME));
        config.setPassword(properties.get(PASSWORD));
        config.setDriverClass(sourceType.getDriverClass());
        config.setDriverUrl(sourceType.getDriverUrl());
        config.setJdbcUrl(
                "jdbc:" + properties.get(DB_SOURCE_TYPE) + "://" + properties.get(HOST) + ":" + properties.get(PORT)
                        + "/");
        switch (sourceType) {
            case MYSQL:
                JdbcClient client = JdbcMySQLClient.createJdbcClient(config);
                return client;
            default:
                throw new JobException("Unsupported source type " + properties.get(DB_SOURCE_TYPE));
        }
    }

    public static List<CreateTableStmt> generateCreateTableStmts(String dorisDb, Map<String, String> properties)
            throws JobException {
        List<CreateTableStmt> stmts = new ArrayList<>();
        String tablesRegex = properties.get(INCLUDE_TABLES_LIST);
        String database = properties.get(DATABASE_NAME);
        JdbcClient jdbcClient = getJdbcClient(properties);
        List<String> tablesNameList = jdbcClient.getTablesNameList(database);
        if (tablesNameList.isEmpty()) {
            throw new JobException("No tables found in database " + database);
        }
        for (String table : tablesNameList) {
            if (!table.matches(tablesRegex)) {
                LOG.info("Skip table {} in database {} as it does not match the regex {}", table, database,
                        tablesRegex);
                continue;
            }
            List<Column> columns = jdbcClient.getColumnsFromJdbc(database, table);
            TableName tblName = new TableName(InternalCatalog.INTERNAL_CATALOG_NAME, dorisDb, table);
            List<String> primaryKeys = columns.stream().filter(Column::isKey).map(Column::getName)
                    .collect(Collectors.toList());
            if (primaryKeys.isEmpty()) {
                LOG.info("Skip table {} in database {} as it does not have primary keys", table, database);
                continue;
            }
            Map<String, String> tableCreateProperties = getTableCreateProperties(properties);
            KeysDesc keysDesc = new KeysDesc(KeysType.UNIQUE_KEYS, primaryKeys);
            DistributionDesc distributionDesc = new HashDistributionDesc(FeConstants.default_bucket_num, true,
                    primaryKeys);
            CreateTableStmt stmt = new CreateTableStmt(true, false, tblName, columns, new ArrayList<>(), "olap",
                    keysDesc, null, distributionDesc, tableCreateProperties, new HashMap<>(), "", new ArrayList<>(),
                    null);
            stmts.add(stmt);
        }
        return stmts;
    }

    private static Map<String, String> getTableCreateProperties(Map<String, String> properties) {
        final Map<String, String> tableCreateProps = new HashMap<>();

        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().startsWith(CdcLoadConstants.TABLE_PROPS_PREFIX)) {
                String subKey = entry.getKey().substring(CdcLoadConstants.TABLE_PROPS_PREFIX.length());
                tableCreateProps.put(subKey, entry.getValue());
            }
        }
        return tableCreateProps;
    }
}
