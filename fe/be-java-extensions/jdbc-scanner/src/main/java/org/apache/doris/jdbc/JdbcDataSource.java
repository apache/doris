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

import com.alibaba.druid.pool.DruidDataSource;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class JdbcDataSource {
    private static final JdbcDataSource jdbcDataSource = new JdbcDataSource();
    private final Map<String, DruidDataSource> sourcesMap = new ConcurrentHashMap<>();

    public static JdbcDataSource getDataSource() {
        return jdbcDataSource;
    }

    public DruidDataSource getSource(String cacheKey) {
        return sourcesMap.get(cacheKey);
    }

    public void putSource(String cacheKey, DruidDataSource ds) {
        sourcesMap.put(cacheKey, ds);
    }

    public Map<String, DruidDataSource> getSourcesMap() {
        return sourcesMap;
    }

    public String createCacheKey(String jdbcUrl, String jdbcUser, String jdbcPassword, String jdbcDriverUrl,
            String jdbcDriverClass) {
        return jdbcUrl + jdbcUser + jdbcPassword + jdbcDriverUrl + jdbcDriverClass;
    }
}
