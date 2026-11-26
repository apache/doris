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

package org.apache.doris.connector.spi.scan;

import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Guards the additive {@code getMustReadColumns} SPI default on {@link ConnectorScanPlanProvider}.
 *
 * <p>WHY: the engine consults this on EVERY plugin-table scan that has a projection above it, and widens the
 * scan's tuple by whatever comes back. The default must therefore be empty, or every connector that never
 * asked for anything would start reading extra columns — and, worse, would fail the query loud when a name
 * it never returned matches no slot. This is the zero-break guard for es/jdbc/paimon/iceberg/hive/maxcompute,
 * none of which override it.</p>
 */
public class ConnectorScanPlanProviderMustReadColumnsTest {

    /** Bare provider: only the abstract planScan implemented; everything else inherits SPI defaults. */
    private static final class BareProvider implements ConnectorScanPlanProvider {
        @Override
        public List<ConnectorScanRange> planScan(ConnectorSession session, ConnectorScanRequest request) {
            return Collections.emptyList();
        }
    }

    /** A connector whose BE-side reader needs a merge key the query may not have selected. */
    private static final class KeyReadingProvider implements ConnectorScanPlanProvider {
        @Override
        public List<ConnectorScanRange> planScan(ConnectorSession session, ConnectorScanRequest request) {
            return Collections.emptyList();
        }

        @Override
        public Set<String> getMustReadColumns(ConnectorSession session, ConnectorTableHandle handle) {
            return Collections.singleton("id");
        }
    }

    @Test
    public void defaultAsksForNoExtraColumns() {
        ConnectorScanPlanProvider provider = new BareProvider();

        // MUTATION: a default returning anything non-empty would widen every connector's scans and fail
        // loud on the first name that matches no slot -> red here first.
        Assertions.assertEquals(Collections.emptySet(), provider.getMustReadColumns(null, null),
                "a connector that never opted in must ask for no extra columns");
    }

    @Test
    public void connectorThatOptsInIsObeyed() {
        ConnectorScanPlanProvider provider = new KeyReadingProvider();

        Assertions.assertEquals(Collections.singleton("id"), provider.getMustReadColumns(null, null),
                "the engine must read back exactly what the connector asked for");
    }
}
