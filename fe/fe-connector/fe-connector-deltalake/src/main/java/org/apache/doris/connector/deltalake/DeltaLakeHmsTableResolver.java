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


package org.apache.doris.connector.deltalake;

import org.apache.doris.connector.hms.HmsClient;
import org.apache.doris.connector.hms.HmsTableInfo;
import org.apache.doris.connector.spi.DorisConnectorException;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** HMS provides discovery and location only, never Delta schema or partition metadata. */
final class DeltaLakeHmsTableResolver implements DeltaLakeTableResolver {
    private final HmsClient client;

    DeltaLakeHmsTableResolver(HmsClient client) {
        this.client = client;
    }

    @Override
    public List<String> listDatabases() {
        return client.listDatabases();
    }

    @Override
    public List<String> listTables(String database) {
        List<String> tables = new ArrayList<>();
        for (String table : client.listTables(database)) {
            if (isDelta(client.getTable(database, table))) {
                tables.add(table);
            }
        }
        return tables;
    }

    @Override
    public Optional<String> tableLocation(String database, String table) {
        if (!client.tableExists(database, table)) {
            return Optional.empty();
        }
        HmsTableInfo info = client.getTable(database, table);
        if (!isDelta(info)) {
            return Optional.empty();
        }
        String location = info.getLocation();
        if (location == null || location.isEmpty()) {
            throw new DorisConnectorException("Delta table " + database + "." + table + " has no HMS location");
        }
        return Optional.of(location);
    }

    static boolean isDelta(HmsTableInfo table) {
        return "delta".equalsIgnoreCase(table.getParameters().get("spark.sql.sources.provider"));
    }
}
