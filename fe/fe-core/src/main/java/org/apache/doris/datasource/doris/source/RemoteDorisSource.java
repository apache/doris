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

package org.apache.doris.datasource.doris.source;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.arrowflight.FlightSqlClientLoadBalancer;
import org.apache.doris.datasource.doris.RemoteDorisExternalCatalog;
import org.apache.doris.datasource.doris.RemoteDorisExternalTable;

import java.util.List;

public class RemoteDorisSource {
    private final TupleDescriptor desc;
    private final RemoteDorisExternalCatalog remoteDorisExternalCatalog;
    private final RemoteDorisExternalTable remoteDorisExtTable;
    private final FlightSqlClientLoadBalancer flightSqlClientLoadBalancer;

    public RemoteDorisSource(TupleDescriptor desc) {
        this.desc = desc;
        this.remoteDorisExtTable = (RemoteDorisExternalTable) desc.getTable();
        this.remoteDorisExternalCatalog = (RemoteDorisExternalCatalog) remoteDorisExtTable.getCatalog();
        this.flightSqlClientLoadBalancer = remoteDorisExternalCatalog.getSqlClientLoadBalancer();
    }

    public TupleDescriptor getDesc() {
        return desc;
    }

    public RemoteDorisExternalTable getTargetTable() {
        return remoteDorisExtTable;
    }

    public RemoteDorisExternalCatalog getCatalog() {
        return remoteDorisExternalCatalog;
    }

    @Deprecated
    public Pair<String, Integer> getHostAndArrowPort() {
        List<String> feArrowNodes = remoteDorisExternalCatalog.getFeArrowNodes();
        String[] arrowFlightHost = feArrowNodes.get(0).trim().split(":");
        if (arrowFlightHost.length != 2) {
            throw new IllegalArgumentException("Invalid arrow flight host: " + feArrowNodes.get(0));
        }
        return Pair.of(arrowFlightHost[0].trim(), Integer.parseInt(arrowFlightHost[1].trim()));
    }

    public FlightSqlClientLoadBalancer getSqlClientLoadBalancer() {
        return flightSqlClientLoadBalancer;
    }
}
