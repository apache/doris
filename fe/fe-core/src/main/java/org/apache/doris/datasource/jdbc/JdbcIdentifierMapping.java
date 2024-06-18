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

import org.apache.doris.datasource.jdbc.client.JdbcClient;
import org.apache.doris.datasource.mapping.IdentifierMapping;

public class JdbcIdentifierMapping extends IdentifierMapping {
    private final JdbcClient jdbcClient;

    public JdbcIdentifierMapping(boolean isLowerCaseMetaNames, String metaNamesMapping, JdbcClient jdbcClient) {
        super(isLowerCaseMetaNames, metaNamesMapping);
        this.jdbcClient = jdbcClient;
    }

    @Override
    protected void loadDatabaseNames() {
        jdbcClient.getDatabaseNameList();
    }

    @Override
    protected void loadTableNames(String localDbName) {
        jdbcClient.getTablesNameList(localDbName);
    }

    @Override
    protected void loadColumnNames(String localDbName, String localTableName) {
        jdbcClient.getColumnsFromJdbc(localDbName, localTableName);
    }
}
