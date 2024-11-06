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

package org.apache.doris.jdbc;

import org.apache.doris.thrift.TJdbcOperation;
import org.apache.doris.thrift.TOdbcTableType;

public class JdbcDataSourceConfig {
    private Long catalogId;
    private String jdbcUrl;
    private String jdbcUser;
    private String jdbcPassword;
    private String jdbcDriverUrl;
    private String jdbcDriverClass;
    private int batchSize;
    private TJdbcOperation op;
    private TOdbcTableType tableType;
    private int connectionPoolMinSize = 1;
    private int connectionPoolMaxSize = 30;
    private int connectionPoolMaxWaitTime = 5000;
    private int connectionPoolMaxLifeTime = 1800000;
    private boolean connectionPoolKeepAlive = false;

    public String createCacheKey() {
        return catalogId + jdbcUrl + jdbcUser + jdbcPassword + jdbcDriverUrl + jdbcDriverClass
                + connectionPoolMinSize + connectionPoolMaxSize + connectionPoolMaxLifeTime + connectionPoolMaxWaitTime
                + connectionPoolKeepAlive;
    }

    public long getCatalogId() {
        return catalogId;
    }

    public JdbcDataSourceConfig setCatalogId(long catalogId) {
        this.catalogId = catalogId;
        return this;
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public JdbcDataSourceConfig setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
        return this;
    }

    public String getJdbcUser() {
        return jdbcUser;
    }

    public JdbcDataSourceConfig setJdbcUser(String jdbcUser) {
        this.jdbcUser = jdbcUser;
        return this;
    }

    public String getJdbcPassword() {
        return jdbcPassword;
    }

    public JdbcDataSourceConfig setJdbcPassword(String jdbcPassword) {
        this.jdbcPassword = jdbcPassword;
        return this;
    }

    public String getJdbcDriverUrl() {
        return jdbcDriverUrl;
    }

    public JdbcDataSourceConfig setJdbcDriverUrl(String jdbcDriverUrl) {
        this.jdbcDriverUrl = jdbcDriverUrl;
        return this;
    }

    public String getJdbcDriverClass() {
        return jdbcDriverClass;
    }

    public JdbcDataSourceConfig setJdbcDriverClass(String jdbcDriverClass) {
        this.jdbcDriverClass = jdbcDriverClass;
        return this;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public JdbcDataSourceConfig setBatchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    public TJdbcOperation getOp() {
        return op;
    }

    public JdbcDataSourceConfig setOp(TJdbcOperation op) {
        this.op = op;
        return this;
    }

    public TOdbcTableType getTableType() {
        return tableType;
    }

    public JdbcDataSourceConfig setTableType(TOdbcTableType tableType) {
        this.tableType = tableType;
        return this;
    }

    public int getConnectionPoolMinSize() {
        return connectionPoolMinSize;
    }

    public JdbcDataSourceConfig setConnectionPoolMinSize(int connectionPoolMinSize) {
        this.connectionPoolMinSize = connectionPoolMinSize;
        return this;
    }

    public int getConnectionPoolMaxSize() {
        return connectionPoolMaxSize;
    }

    public JdbcDataSourceConfig setConnectionPoolMaxSize(int connectionPoolMaxSize) {
        this.connectionPoolMaxSize = connectionPoolMaxSize;
        return this;
    }

    public int getConnectionPoolMaxWaitTime() {
        return connectionPoolMaxWaitTime;
    }

    public JdbcDataSourceConfig setConnectionPoolMaxWaitTime(int connectionPoolMaxWaitTime) {
        this.connectionPoolMaxWaitTime = connectionPoolMaxWaitTime;
        return this;
    }

    public int getConnectionPoolMaxLifeTime() {
        return connectionPoolMaxLifeTime;
    }

    public JdbcDataSourceConfig setConnectionPoolMaxLifeTime(int connectionPoolMaxLifeTime) {
        this.connectionPoolMaxLifeTime = connectionPoolMaxLifeTime;
        return this;
    }

    public boolean isConnectionPoolKeepAlive() {
        return connectionPoolKeepAlive;
    }

    public JdbcDataSourceConfig setConnectionPoolKeepAlive(boolean connectionPoolKeepAlive) {
        this.connectionPoolKeepAlive = connectionPoolKeepAlive;
        return this;
    }
}
