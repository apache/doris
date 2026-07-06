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

package org.apache.doris.connector.api;

import org.apache.doris.connector.api.handle.ConnectorColumnHandle;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.connector.api.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.api.scan.ConnectorScanRange;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Pins the per-table scan-provider selection seam
 * {@link Connector#getScanPlanProvider(ConnectorTableHandle)}.
 *
 * <p><b>WHY this matters (Rule 9):</b> after the hive/hms cut-over a single catalog is heterogeneous
 * (plain-hive + iceberg-on-HMS + hudi-on-HMS under one gateway connector). The engine must pick a
 * DIFFERENT scan provider per table without ever inspecting the format itself. The selection must happen
 * here — at provider-acquisition time — rather than inside a single dispatching provider, because
 * {@link ConnectorScanPlanProvider} has handle-less methods (e.g. {@code appendExplainInfo}) and providers
 * are built fresh/stateless per call, so a returned provider must already be bound to the right backing
 * scanner for the handle. The default must stay inert for every single-format connector.</p>
 */
public class ConnectorScanProviderSelectionTest {

    /** A scan provider identified by name so tests can assert WHICH provider was selected. */
    private static final class NamedProvider implements ConnectorScanPlanProvider {
        private final String name;

        private NamedProvider(String name) {
            this.name = name;
        }

        @Override
        public List<ConnectorScanRange> planScan(ConnectorSession session, ConnectorTableHandle handle,
                List<ConnectorColumnHandle> columns, Optional<ConnectorExpression> filter) {
            return Collections.emptyList();
        }

        @Override
        public String toString() {
            return name;
        }
    }

    /** Bare connector implementing only the abstract methods; nothing scan-related is overridden. */
    private abstract static class FakeConnector implements Connector {
        @Override
        public ConnectorMetadata getMetadata(ConnectorSession session) {
            return null;
        }

        @Override
        public void close() {
        }
    }

    private static ConnectorTableHandle handle() {
        return new ConnectorTableHandle() {
        };
    }

    @Test
    public void defaultDelegatesToNoArgProvider() {
        // A single-format connector overrides only the no-arg getter. The per-handle default must delegate
        // to it (NOT return null), so every existing connector routes unchanged after the seam is added.
        // MUTATION: making the default return null instead of getScanPlanProvider() -> non-null assert red.
        NamedProvider only = new NamedProvider("only");
        Connector connector = new FakeConnector() {
            @Override
            public ConnectorScanPlanProvider getScanPlanProvider() {
                return only;
            }
        };

        Assertions.assertSame(only, connector.getScanPlanProvider(handle()),
                "the per-handle default must delegate to the connector-level no-arg provider");
    }

    @Test
    public void defaultReturnsNullWhenConnectorHasNoScanCapability() {
        // A connector with no scan capability (no-arg default returns null) must keep returning null through
        // the per-handle seam, preserving the null-tolerant scan path in PluginDrivenScanNode.
        Connector connector = new FakeConnector() {
        };

        Assertions.assertNull(connector.getScanPlanProvider(handle()),
                "with no scan provider at all the per-handle seam stays null");
    }

    @Test
    public void overrideSelectsProviderPerHandle() {
        // A heterogeneous gateway overrides the per-handle seam and returns a DIFFERENT provider per table,
        // and must NOT fall back to the no-arg provider once it has an answer for the handle.
        // MUTATION: keying the override on the no-arg getter (ignoring the handle) -> per-handle assert red.
        ConnectorTableHandle icebergHandle = handle();
        ConnectorTableHandle hiveHandle = handle();
        NamedProvider icebergProvider = new NamedProvider("iceberg");
        NamedProvider hiveProvider = new NamedProvider("hive");
        NamedProvider fallback = new NamedProvider("fallback");
        Connector gateway = new FakeConnector() {
            @Override
            public ConnectorScanPlanProvider getScanPlanProvider() {
                return fallback;
            }

            @Override
            public ConnectorScanPlanProvider getScanPlanProvider(ConnectorTableHandle handle) {
                return handle == icebergHandle ? icebergProvider : hiveProvider;
            }
        };

        Assertions.assertSame(icebergProvider, gateway.getScanPlanProvider(icebergHandle),
                "gateway routes the iceberg-on-HMS handle to the iceberg provider");
        Assertions.assertSame(hiveProvider, gateway.getScanPlanProvider(hiveHandle),
                "gateway routes the plain-hive handle to the hive provider");
        Assertions.assertNotSame(fallback, gateway.getScanPlanProvider(icebergHandle),
                "an overriding gateway does not fall back to the connector-level no-arg provider");
    }
}
