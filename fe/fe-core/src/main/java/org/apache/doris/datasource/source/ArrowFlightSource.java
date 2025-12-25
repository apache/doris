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

package org.apache.doris.datasource.source;

import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.arrowflight.FlightSqlClientLoadBalancer;

import java.util.Map;

public class ArrowFlightSource implements ExternalSource {

    private final ExternalTable externalTable;
    private final String user;
    private final String password;
    private final Map<String, String> properties;
    private final int queryRetryCount;
    private final boolean enableParallelResultSink;
    private final FlightSqlClientLoadBalancer clientLoadBalancer;
    private final boolean propagateSession;

    public ArrowFlightSource(ExternalTable externalTable, String user, String password,
                             Map<String, String> properties, int queryRetryCount,
                             boolean enableParallelResultSink, FlightSqlClientLoadBalancer clientLoadBalancer,
                             boolean propagateSession) {
        this.externalTable = externalTable;
        this.user = user;
        this.password = password;
        this.properties = properties;
        this.queryRetryCount = queryRetryCount;
        this.enableParallelResultSink = enableParallelResultSink;
        this.clientLoadBalancer = clientLoadBalancer;
        this.propagateSession = propagateSession;
    }

    public String getUsername() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public String getTargetTableName() {
        return externalTable.getDbName() + "." + externalTable.getName();
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public int getQueryRetryCount() {
        return queryRetryCount;
    }

    public boolean enableParallelResultSink() {
        return enableParallelResultSink;
    }

    public FlightSqlClientLoadBalancer getSqlClientLoadBalancer() {
        return clientLoadBalancer;
    }

    public boolean isPropagateSession() {
        return propagateSession;
    }
}
