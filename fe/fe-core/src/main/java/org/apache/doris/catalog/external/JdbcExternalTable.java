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

package org.apache.doris.catalog.external;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.JdbcTable;
import org.apache.doris.datasource.JdbcExternalCatalog;
import org.apache.doris.thrift.TTableDescriptor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * Elasticsearch external table.
 */
public class JdbcExternalTable extends ExternalTable {
    private static final Logger LOG = LogManager.getLogger(JdbcExternalTable.class);

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

    protected synchronized void makeSureInitialized() {
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
    public String getMysqlType() {
        return type.name();
    }

    @Override
    public TTableDescriptor toThrift() {
        makeSureInitialized();
        return jdbcTable.toThrift();
    }

    private JdbcTable toJdbcTable() {
        List<Column> schema = getFullSchema();
        JdbcExternalCatalog jdbcCatalog = (JdbcExternalCatalog) catalog;
        String fullDbName = this.dbName + "." + this.name;
        JdbcTable jdbcTable = new JdbcTable(this.id, fullDbName, schema, TableType.JDBC_EXTERNAL_TABLE);
        jdbcTable.setExternalTableName(fullDbName);
        jdbcTable.setJdbcTypeName(jdbcCatalog.getDatabaseTypeName());
        jdbcTable.setJdbcUrl(jdbcCatalog.getJdbcUrl());
        jdbcTable.setJdbcUser(jdbcCatalog.getJdbcUser());
        jdbcTable.setJdbcPasswd(jdbcCatalog.getJdbcPasswd());
        jdbcTable.setDriverClass(jdbcCatalog.getDriverClass());
        jdbcTable.setDriverUrl(jdbcCatalog.getDriverUrl());
        jdbcTable.setCheckSum(jdbcCatalog.getCheckSum());
        return jdbcTable;
    }

}
