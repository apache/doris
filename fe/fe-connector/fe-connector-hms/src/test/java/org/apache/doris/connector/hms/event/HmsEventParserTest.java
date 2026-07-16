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

package org.apache.doris.connector.hms.event;

import org.apache.doris.connector.api.event.MetastoreChangeDescriptor;
import org.apache.doris.connector.api.event.MetastoreChangeDescriptor.Op;
import org.apache.doris.connector.hms.HmsNotificationEvent;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Dormant unit coverage of the neutral (body-free) event mappings and the lazy-decompress guarantee.
 * The body-parsing table/partition paths (which need captured Hive JSON/GZIP message fixtures) are
 * covered by the heterogeneous-HMS e2e matrix owed at the flip.
 */
public class HmsEventParserTest {

    private static HmsNotificationEvent event(long id, String type, String db, String table,
            String message, String format, long timeSec) {
        return new HmsNotificationEvent(id, type, db, table, message, format, timeSec);
    }

    @Test
    public void createDatabaseMapsToRegisterDb() {
        List<MetastoreChangeDescriptor> out = HmsEventParser.parse(
                event(7L, "CREATE_DATABASE", "MyDb", null, null, "json-2.0", 100L));
        Assertions.assertEquals(1, out.size());
        MetastoreChangeDescriptor d = out.get(0);
        Assertions.assertEquals(Op.REGISTER_DATABASE, d.getOp());
        // db name is lowercased, mirroring the legacy base MetastoreEvent
        Assertions.assertEquals("mydb", d.getDbName());
        Assertions.assertEquals(7L, d.getEventId());
        // event time (seconds) is surfaced as millis
        Assertions.assertEquals(100L * 1000L, d.getUpdateTime());
    }

    @Test
    public void dropDatabaseMapsToUnregisterDb() {
        List<MetastoreChangeDescriptor> out = HmsEventParser.parse(
                event(8L, "DROP_DATABASE", "db1", null, null, "json-2.0", 0L));
        Assertions.assertEquals(1, out.size());
        Assertions.assertEquals(Op.UNREGISTER_DATABASE, out.get(0).getOp());
        Assertions.assertEquals("db1", out.get(0).getDbName());
    }

    @Test
    public void insertMapsToRefreshTable() {
        List<MetastoreChangeDescriptor> out = HmsEventParser.parse(
                event(9L, "INSERT", "db1", "t1", null, "json-2.0", 0L));
        Assertions.assertEquals(1, out.size());
        Assertions.assertEquals(Op.REFRESH_TABLE, out.get(0).getOp());
        Assertions.assertEquals("db1", out.get(0).getDbName());
        Assertions.assertEquals("t1", out.get(0).getTableName());
    }

    @Test
    public void unsupportedTypeProducesNoDescriptor() {
        Assertions.assertTrue(HmsEventParser.parse(
                event(10L, "COMMIT_TXN", "db1", null, null, "json-2.0", 0L)).isEmpty());
    }

    @Test
    public void nullEventTypeIsIgnored() {
        Assertions.assertTrue(HmsEventParser.parse(
                event(13L, null, "db1", null, null, "json-2.0", 0L)).isEmpty());
    }

    @Test
    public void bodyFreeEventNeverDecompressesTheMessage() {
        // A db-level / ignored event must not touch the payload: even a gzip-tagged, un-decompressible body
        // (or a null body) cannot throw, because prepareBody is gated on needsBody(type). This is the
        // regression the lazy-decompress fix closed.
        Assertions.assertDoesNotThrow(() -> HmsEventParser.parse(
                event(11L, "CREATE_DATABASE", "db1", null, "not-valid-gzip", "gzip(json-2.0)", 0L)));
        Assertions.assertDoesNotThrow(() -> HmsEventParser.parse(
                event(12L, "COMMIT_TXN", "db1", null, null, "gzip(json-2.0)", 0L)));
    }
}
