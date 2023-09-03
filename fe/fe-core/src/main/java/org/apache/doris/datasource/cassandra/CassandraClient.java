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

package org.apache.doris.datasource.cassandra;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class CassandraClient {
    public static CqlSessionBuilder getCqlSessionBuilder(
        String nodeAddress,
        String username,
        String password,
        String dataCenter) {

        List<InetSocketAddress> inetSocketAddresses = Arrays.stream(nodeAddress.split(",")).map(address -> {
            String[] split = address.split(":", 2);
            return new InetSocketAddress(split[0], Integer.parseInt(split[1]));
        }).collect(Collectors.toList());

        return CqlSession.builder()
            .addContactPoints(inetSocketAddresses)
            .withLocalDatacenter(dataCenter)
            .withAuthCredentials(username, password);
    }

    public static SimpleStatement createSimpleStatement(
        String cql, ConsistencyLevel consistencyLevel) {
        return SimpleStatement.builder(cql).setConsistencyLevel(consistencyLevel).build();
    }
}
