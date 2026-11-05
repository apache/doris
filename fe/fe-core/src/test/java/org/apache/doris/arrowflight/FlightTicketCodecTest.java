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

package org.apache.doris.arrowflight;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class FlightTicketCodecTest {
    // Keep this complete vector identical to BE FlightTicketTest in
    // be/test/service/arrow_flight/flight_sql_service_test.cpp.
    private static final String TOKEN = "flight-test-token";
    private static final int CLUSTER_ID = 42;
    private static final long NOW_MS = 1700000000000L;
    private static final long LIFETIME_MS = 300000L;
    private static final String TICKET =
            "0000000000000001-0000000000000002&127.0.0.1&8060&c2VsZWN0IDE=";
    private static final String SIGNED_TICKET =
            "0000000000000001-0000000000000002&127.0.0.1&8060&c2VsZWN0IDE="
            + "&v1&42&1700000000000&1700000300000&"
            + "4t0KqCaYZ4VyWvng0cNCURonABuOxYtJZ/HMWdpbTXc=";

    @Test
    public void testMatchesBackendVector() throws Exception {
        String signed = FlightTicketCodec.sign(TOKEN, CLUSTER_ID, TICKET, NOW_MS, LIFETIME_MS);
        Assertions.assertEquals(SIGNED_TICKET, signed);
        Assertions.assertNotEquals(SIGNED_TICKET,
                FlightTicketCodec.sign("rotated-token", CLUSTER_ID, TICKET, NOW_MS, LIFETIME_MS));
    }

    @Test
    public void testPreservesLargeAndEmptySql() throws Exception {
        // Legacy tickets carry base64 SQL and can legitimately exceed the removed 4096-byte limit.
        String prefix = "1-2&::1&8060&";
        for (String ticket : new String[] {prefix, prefix + "YWFh".repeat(2000)}) {
            String signed = FlightTicketCodec.sign("token", 42, ticket, 1000, 300000);
            Assertions.assertTrue(signed.startsWith(ticket + "&v1&42&1000&301000&"));
            Assertions.assertEquals(9, signed.split("&", -1).length);
        }
    }

    @Test
    public void testRejectsMissingKeyAndInvalidLifetime() {
        for (String token : new String[] {null, ""}) {
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> FlightTicketCodec.sign(token, 42, TICKET, 1000, 300000));
        }
        for (long lifetime : new long[] {0, -1, 3600001}) {
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> FlightTicketCodec.sign("token", 42, TICKET, 1000, lifetime));
        }
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> FlightTicketCodec.sign("token", 42, TICKET, -1, 300000));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> FlightTicketCodec.sign("token", 42, TICKET, Long.MAX_VALUE, 300000));
    }
}
