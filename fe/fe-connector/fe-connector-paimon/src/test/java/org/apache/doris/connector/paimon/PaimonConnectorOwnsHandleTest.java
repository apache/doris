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

package org.apache.doris.connector.paimon;

import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

/**
 * Whether this connector claims the handles it produces, which is what lets a <em>gateway</em> connector
 * embed it as a sibling: the gateway cannot name this module's handle type across the plugin classloader
 * split, so it routes a handle by asking each sibling to test its own in-loader type.
 *
 * <p>Asserted rather than left to the SPI default, because the default is {@code false} and the failure it
 * causes points away from here. A sibling that disowns its own handles makes every guard on the gateway
 * side fail open, and the first cast throws a ClassCastException naming the GATEWAY's handle type and two
 * class loaders — with nothing to suggest that the missing piece is a method this connector never
 * overrode. That is not hypothetical: it is what happened the first time the fluss connector read a lake
 * table through this one end to end.
 */
public class PaimonConnectorOwnsHandleTest {

    @Test
    public void claimsItsOwnTableHandle() {
        PaimonConnector connector = new PaimonConnector(Collections.emptyMap(), context());

        Assertions.assertTrue(connector.ownsHandle(new PaimonTableHandle(
                "db1", "t1", Collections.emptyList(), Collections.emptyList())));
    }

    @Test
    public void disownsAnotherConnectorsHandle() {
        // The gateway asks its siblings in turn, so answering yes to a foreign handle would route it to the
        // wrong connector instead of leaving the gateway to keep looking.
        PaimonConnector connector = new PaimonConnector(Collections.emptyMap(), context());

        Assertions.assertFalse(connector.ownsHandle(new ForeignHandle()));
    }

    /** Stands in for whatever another connector's handle happens to be; only its type matters here. */
    private static final class ForeignHandle implements ConnectorTableHandle {
        private static final long serialVersionUID = 1L;
    }

    /** The connector wraps whatever context it is given, so it cannot be null; nothing here reads it. */
    private static ConnectorContext context() {
        return new ConnectorContext() {
            @Override
            public String getCatalogName() {
                return "test_catalog";
            }

            @Override
            public long getCatalogId() {
                return 1L;
            }
        };
    }
}
