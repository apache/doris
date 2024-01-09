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
    private String jdbcUrl;
    private String jdbcUser;
    private String jdbcPassword;
    private String jdbcDriverUrl;
    private String jdbcDriverClass;
    private int batchSize;
    private TJdbcOperation op;
    private TOdbcTableType tableType;
    private int minPoolSize;
    private int maxPoolSize;
    private int minIdleSize;
    private int maxIdleTime;
    private int maxWaitTime;
    private boolean keepAlive;

    public String createCacheKey() {
        return jdbcUrl + jdbcUser + jdbcPassword + jdbcDriverUrl + jdbcDriverClass
                + minPoolSize + maxPoolSize + minIdleSize + maxIdleTime + maxWaitTime + keepAlive;
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

    public int getMinPoolSize() {
        return minPoolSize;
    }

    public JdbcDataSourceConfig setMinPoolSize(int minPoolSize) {
        this.minPoolSize = minPoolSize;
        return this;
    }

    public int getMaxPoolSize() {
        return maxPoolSize;
    }

    public JdbcDataSourceConfig setMaxPoolSize(int maxPoolSize) {
        this.maxPoolSize = maxPoolSize;
        return this;
    }

    public int getMinIdleSize() {
        return minIdleSize;
    }

    public JdbcDataSourceConfig setMinIdleSize(int minIdleSize) {
        this.minIdleSize = minIdleSize;
        return this;
    }

    public int getMaxIdleTime() {
        return maxIdleTime;
    }

    public JdbcDataSourceConfig setMaxIdleTime(int maxIdleTime) {
        this.maxIdleTime = maxIdleTime;
        return this;
    }

    public int getMaxWaitTime() {
        return maxWaitTime;
    }

    public JdbcDataSourceConfig setMaxWaitTime(int maxWaitTime) {
        this.maxWaitTime = maxWaitTime;
        return this;
    }

    public boolean isKeepAlive() {
        return keepAlive;
    }

    public JdbcDataSourceConfig setKeepAlive(boolean keepAlive) {
        this.keepAlive = keepAlive;
        return this;
    }
}
