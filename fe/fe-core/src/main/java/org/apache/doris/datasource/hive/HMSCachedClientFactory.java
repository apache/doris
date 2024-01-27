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

package org.apache.doris.datasource.hive;

import org.apache.doris.catalog.JdbcResource;
import org.apache.doris.datasource.jdbc.client.JdbcClient;
import org.apache.doris.datasource.jdbc.client.JdbcClientConfig;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hive.conf.HiveConf;

public class HMSCachedClientFactory {
    public static HMSCachedClient createCachedClient(HiveConf hiveConf, int thriftClientPoolSize,
            JdbcClientConfig jdbcClientConfig) {
        if (hiveConf != null) {
            return new ThriftHMSCachedClient(hiveConf, thriftClientPoolSize);
        }
        Preconditions.checkNotNull(jdbcClientConfig, "hiveConf and jdbcClientConfig are both null");
        String dbType = JdbcClient.parseDbType(jdbcClientConfig.getJdbcUrl());
        switch (dbType) {
            case JdbcResource.POSTGRESQL:
                return new PostgreSQLJdbcHMSCachedClient(jdbcClientConfig);
            default:
                throw new IllegalArgumentException("Unsupported DB type: " + dbType);
        }
    }
}
