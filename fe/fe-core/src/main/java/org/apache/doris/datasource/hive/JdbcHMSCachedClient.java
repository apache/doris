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

import org.apache.doris.datasource.jdbc.client.JdbcClient;
import org.apache.doris.datasource.jdbc.client.JdbcClientConfig;

import com.google.common.base.Preconditions;

/**
 * This class uses the JDBC protocol to directly access the relational databases under HMS
 * to obtain Hive metadata information
 */
public abstract class JdbcHMSCachedClient extends JdbcClient implements HMSCachedClient {
    protected JdbcClientConfig jdbcClientConfig;

    protected JdbcHMSCachedClient(JdbcClientConfig jdbcClientConfig) {
        super(jdbcClientConfig);
        Preconditions.checkNotNull(jdbcClientConfig, "JdbcClientConfig can not be null");
        this.jdbcClientConfig = jdbcClientConfig;
    }
}
