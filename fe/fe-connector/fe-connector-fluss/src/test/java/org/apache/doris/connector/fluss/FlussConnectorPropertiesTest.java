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

package org.apache.doris.connector.fluss;

import org.apache.doris.connector.fluss.FlussConnectorProperties.UnionReadMode;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Pins the catalog-property contract of a fluss catalog: what CREATE CATALOG must reject, and exactly
 * which properties reach the fluss client.
 */
public class FlussConnectorPropertiesTest {

    private static Map<String, String> props(String... keyValues) {
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            map.put(keyValues[i], keyValues[i + 1]);
        }
        return map;
    }

    @Test
    public void bootstrapServersIsRequired() {
        // A catalog with no bootstrap servers can never answer a query, so it must fail at CREATE
        // CATALOG rather than at the user's first SELECT.
        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> FlussConnectorProperties.validate(props()));
        Assertions.assertTrue(e.getMessage().contains(FlussConnectorProperties.BOOTSTRAP_SERVERS),
                "message should name the missing property, was: " + e.getMessage());

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> FlussConnectorProperties.validate(props(FlussConnectorProperties.BOOTSTRAP_SERVERS, "   ")));
    }

    @Test
    public void bootstrapServersMustBeHostPortPairs() {
        // Each rejected form is one a user actually writes: a bare host, a non-numeric port, a port out
        // of range, and a trailing empty element from a stray comma.
        for (String bad : new String[] {"localhost", "localhost:", "localhost:abc", "localhost:0",
                "localhost:65536", "host1:9123,"}) {
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> FlussConnectorProperties.validate(
                            props(FlussConnectorProperties.BOOTSTRAP_SERVERS, bad)),
                    "expected '" + bad + "' to be rejected");
        }
    }

    @Test
    public void bootstrapServersAcceptsListsAndIpv6() {
        // The IPv6 case is why the port is split at the LAST colon, not the first.
        for (String good : new String[] {"localhost:9123", " host1:9123 , host2:9124 ", "[::1]:9123"}) {
            FlussConnectorProperties.validate(props(FlussConnectorProperties.BOOTSTRAP_SERVERS, good));
        }
    }

    @Test
    public void unionReadModeDefaultsToAutoAndIsCaseInsensitive() {
        Assertions.assertEquals(UnionReadMode.AUTO, FlussConnectorProperties.unionReadMode(props()));
        Assertions.assertEquals(UnionReadMode.REQUIRED,
                FlussConnectorProperties.unionReadMode(props(FlussConnectorProperties.UNION_READ_MODE, "ReQuIrEd")));
        Assertions.assertEquals(UnionReadMode.DISABLED,
                FlussConnectorProperties.unionReadMode(props(FlussConnectorProperties.UNION_READ_MODE, " disabled ")));
    }

    @Test
    public void unionReadModeRejectsUnknownValueAtCreateCatalog() {
        // A typo here would otherwise degrade silently to whatever the default is, and the difference
        // between auto and required is only visible as "the query returned fewer rows than it should".
        Map<String, String> properties = props(
                FlussConnectorProperties.BOOTSTRAP_SERVERS, "localhost:9123",
                FlussConnectorProperties.UNION_READ_MODE, "enabled");
        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> FlussConnectorProperties.validate(properties));
        Assertions.assertTrue(e.getMessage().contains("auto, required, disabled"),
                "message should list the accepted values, was: " + e.getMessage());
    }

    @Test
    public void clientConfigIsThePrefixedPropertiesMinusTheDorisOnlyOnes() {
        Map<String, String> properties = props(
                FlussConnectorProperties.BOOTSTRAP_SERVERS, "localhost:9123",
                "fluss.client.security.protocol", "sasl",
                FlussConnectorProperties.UNION_READ_MODE, "required",
                "type", "fluss",
                "warehouse", "s3://ignored");

        Map<String, String> config = FlussConnectorProperties.toFlussClientConfig(properties);

        // bootstrap.servers and client.* arrive under fluss's own names; the Doris-only union-read
        // switch and every non-fluss catalog property stay behind. The engine's own keys ("type") and
        // other connectors' keys ("warehouse") are not fluss options and must not be handed over as if
        // they were — the fluss config is not a place to dump whatever the catalog happened to carry.
        Map<String, String> expected = new HashMap<>();
        expected.put("bootstrap.servers", "localhost:9123");
        expected.put("client.security.protocol", "sasl");
        Assertions.assertEquals(expected, config);
    }
}
