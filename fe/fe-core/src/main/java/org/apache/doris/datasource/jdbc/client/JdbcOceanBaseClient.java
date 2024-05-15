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

package org.apache.doris.datasource.jdbc.client;

import org.apache.doris.catalog.JdbcResource;
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.jdbc.util.JdbcFieldSchema;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class JdbcOceanBaseClient extends JdbcClient {
    private JdbcClient currentClient;

    public JdbcOceanBaseClient(JdbcClientConfig jdbcClientConfig) {
        super(jdbcClientConfig);

        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;

        try {
            conn = super.getConnection();
            stmt = conn.createStatement();
            rs = stmt.executeQuery("SHOW VARIABLES LIKE 'ob_compatibility_mode'");
            if (rs.next()) {
                String compatibilityMode = rs.getString(2);
                if ("MYSQL".equalsIgnoreCase(compatibilityMode)) {
                    currentClient = new JdbcMySQLClient(jdbcClientConfig);
                } else if ("ORACLE".equalsIgnoreCase(compatibilityMode)) {
                    currentClient = new JdbcOracleClient(jdbcClientConfig);
                    setOracleMode();
                } else {
                    throw new JdbcClientException("Unsupported OceanBase compatibility mode: " + compatibilityMode);
                }
            }
        } catch (SQLException | JdbcClientException e) {
            closeClient();
            throw new JdbcClientException("Failed to initialize JdbcOceanBaseClient", e.getMessage());
        } finally {
            close(rs, stmt, conn);
        }
    }

    @Override
    protected Type jdbcTypeToDoris(JdbcFieldSchema fieldSchema) {
        return currentClient.jdbcTypeToDoris(fieldSchema);
    }

    public void setOracleMode() {
        this.dbType = JdbcResource.OCEANBASE_ORACLE;
    }
}
