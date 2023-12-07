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

import java.util.Map;

public class JdbcClientConfig {
    private String catalog;
    private String user;
    private String password;
    private String jdbcUrl;
    private String driverUrl;
    private String driverClass;
    private String onlySpecifiedDatabase;
    private String isLowerCaseTableNames;
    private Map<String, Boolean> includeDatabaseMap;
    private Map<String, Boolean> excludeDatabaseMap;

    private String removeAbandonedTimeout;

    private String removeAbandoned;

    public String getCatalog() {
        return catalog;
    }

    public JdbcClientConfig setCatalog(String catalog) {
        this.catalog = catalog;
        return this;
    }

    public String getUser() {
        return user;
    }

    public JdbcClientConfig setUser(String user) {
        this.user = user;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public JdbcClientConfig setPassword(String password) {
        this.password = password;
        return this;
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public JdbcClientConfig setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
        return this;
    }

    public String getDriverUrl() {
        return driverUrl;
    }

    public JdbcClientConfig setDriverUrl(String driverUrl) {
        this.driverUrl = driverUrl;
        return this;
    }

    public String getDriverClass() {
        return driverClass;
    }

    public JdbcClientConfig setDriverClass(String driverClass) {
        this.driverClass = driverClass;
        return this;
    }

    public String getOnlySpecifiedDatabase() {
        return onlySpecifiedDatabase;
    }

    public JdbcClientConfig setOnlySpecifiedDatabase(String onlySpecifiedDatabase) {
        this.onlySpecifiedDatabase = onlySpecifiedDatabase;
        return this;
    }

    public String getIsLowerCaseTableNames() {
        return isLowerCaseTableNames;
    }

    public JdbcClientConfig setIsLowerCaseTableNames(String isLowerCaseTableNames) {
        this.isLowerCaseTableNames = isLowerCaseTableNames;
        return this;
    }

    public Map<String, Boolean> getIncludeDatabaseMap() {
        return includeDatabaseMap;
    }

    public JdbcClientConfig setIncludeDatabaseMap(Map<String, Boolean> includeDatabaseMap) {
        this.includeDatabaseMap = includeDatabaseMap;
        return this;
    }

    public Map<String, Boolean> getExcludeDatabaseMap() {
        return excludeDatabaseMap;
    }

    public JdbcClientConfig setExcludeDatabaseMap(Map<String, Boolean> excludeDatabaseMap) {
        this.excludeDatabaseMap = excludeDatabaseMap;
        return this;
    }

    public String getRemoveAbandonedTimeout() {
        return removeAbandonedTimeout;
    }

    public JdbcClientConfig setRemoveAbandonedTimeout(String removeAbandonedTimeout) {
        this.removeAbandonedTimeout = removeAbandonedTimeout;
        return this;
    }

    public String getRemoveAbandoned() {
        return removeAbandoned;
    }

    public JdbcClientConfig setRemoveAbandoned(String removeAbandoned) {
        this.removeAbandoned = removeAbandoned;
        return this;
    }
}
