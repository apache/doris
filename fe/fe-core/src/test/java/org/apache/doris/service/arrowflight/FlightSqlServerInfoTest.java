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

package org.apache.doris.service.arrowflight;

import org.apache.doris.common.Version;
import org.apache.doris.service.arrowflight.sessions.FlightSessionsManager;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.flight.sql.FlightSqlProducer.Schemas;
import org.apache.arrow.flight.sql.SqlInfoBuilder;
import org.apache.arrow.flight.sql.impl.FlightSql.SqlInfo;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

public class FlightSqlServerInfoTest {
    @Test
    public void reportsBuildVersionsInAllSqlInfo() throws Exception {
        Map<Integer, Object> info = readSqlInfo();
        Assert.assertEquals("DorisFE", info.get(SqlInfo.FLIGHT_SQL_SERVER_NAME_VALUE));
        Assert.assertEquals(Version.DORIS_BUILD_VERSION + "-" + Version.DORIS_BUILD_SHORT_HASH,
                info.get(SqlInfo.FLIGHT_SQL_SERVER_VERSION_VALUE));
        assertArrowVersion(info);
        Assert.assertEquals(false, info.get(SqlInfo.FLIGHT_SQL_SERVER_READ_ONLY_VALUE));
    }

    @Test
    public void reportsOnlyRequestedServerVersion() throws Exception {
        Map<Integer, Object> info = readSqlInfo(SqlInfo.FLIGHT_SQL_SERVER_VERSION);
        Assert.assertEquals(1, info.size());
        Assert.assertEquals(Version.DORIS_BUILD_VERSION + "-" + Version.DORIS_BUILD_SHORT_HASH,
                info.get(SqlInfo.FLIGHT_SQL_SERVER_VERSION_VALUE));
    }

    @Test
    public void reportsOnlyRequestedArrowVersion() throws Exception {
        Map<Integer, Object> info = readSqlInfo(SqlInfo.FLIGHT_SQL_SERVER_ARROW_VERSION);
        assertArrowVersion(info);
        Assert.assertEquals(1, info.size());
    }

    private static void assertArrowVersion(Map<Integer, Object> info) {
        String version = SqlInfoBuilder.class.getPackage().getImplementationVersion();
        if (version == null || version.isEmpty()) {
            // Exploded dependencies may omit the manifest; unknown metadata must not be fabricated.
            Assert.assertEquals("unknown", info.get(SqlInfo.FLIGHT_SQL_SERVER_ARROW_VERSION_VALUE));
        } else {
            Assert.assertEquals(version, info.get(SqlInfo.FLIGHT_SQL_SERVER_ARROW_VERSION_VALUE));
        }
    }

    private static Map<Integer, Object> readSqlInfo(SqlInfo... requestedInfo) throws Exception {
        Location location = Location.forGrpcInsecure("127.0.0.1", 0);
        FlightSessionsManager sessions = Mockito.mock(FlightSessionsManager.class);
        try (DorisFlightSqlProducer producer = new DorisFlightSqlProducer(location, sessions);
                RootAllocator allocator = new RootAllocator();
                FlightServer server = FlightServer.builder(allocator, location, producer).build().start();
                FlightClient client = FlightClient.builder(allocator,
                        Location.forGrpcInsecure("127.0.0.1", server.getPort())).build()) {
            FlightSqlClient sql = new FlightSqlClient(client);
            FlightInfo info = sql.getSqlInfo(requestedInfo);
            Assert.assertEquals(Schemas.GET_SQL_INFO_SCHEMA, info.getSchema());
            Assert.assertEquals(1, info.getEndpoints().size());
            Map<Integer, Object> values = new HashMap<>();
            try (FlightStream stream = client.getStream(info.getEndpoints().get(0).getTicket())) {
                while (stream.next()) {
                    VectorSchemaRoot root = stream.getRoot();
                    for (int row = 0; row < root.getRowCount(); ++row) {
                        int code = ((Number) root.getVector("info_name").getObject(row)).intValue();
                        Object value = root.getVector("value").getObject(row);
                        values.put(code, value instanceof Boolean ? value : value.toString());
                    }
                }
            }
            Mockito.verifyNoInteractions(sessions);
            return values;
        }
    }
}
