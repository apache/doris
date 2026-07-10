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

package org.apache.doris.connector.hudi;

import org.apache.doris.connector.api.ConnectorCapability;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.ConnectorContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Set;

/**
 * Pins {@link HudiConnector#ownsHandle} for the hms 3-way sibling routing: a flipped hms gateway embeds this
 * connector as a sibling and asks it "is this foreign handle yours?" to route a scan/metadata call, because the
 * concrete handle type is invisible across the plugin classloader split.
 *
 * <p>ownsHandle must be TRUE for this connector's own {@link HudiTableHandle} and FALSE for any other connector's
 * handle (e.g. an iceberg sibling's), so the gateway routes correctly. Dormant until hms enters
 * {@code SPI_READY_TYPES}: no production path asks this connector yet, so this is a Rule-9 routing guard.
 */
public class HudiConnectorOwnsHandleTest {

    private static HudiConnector connector() {
        return new HudiConnector(Collections.emptyMap(), new ConnectorContext() {
            @Override
            public String getCatalogName() {
                return "test_catalog";
            }

            @Override
            public long getCatalogId() {
                return 1L;
            }
        });
    }

    @Test
    public void ownsHandleOnlyForHudiTableHandle() {
        // MUTATION: returning true unconditionally -> the gateway sends iceberg handles here -> red.
        HudiConnector connector = connector();
        Assertions.assertTrue(connector.ownsHandle(new HudiTableHandle("db", "t", "s3://b/t", "COPY_ON_WRITE")),
                "a HudiTableHandle is owned by the hudi connector");
        Assertions.assertFalse(connector.ownsHandle(new ConnectorTableHandle() {
        }), "a foreign (non-hudi) handle is NOT owned by the hudi connector");
    }

    @Test
    public void declaresMetadataTableCapabilityForTimeline() {
        // WHY: the hudi_meta() / TIMELINE TVF's plugin-driven arm delegates only when the connector declares
        // SUPPORTS_METADATA_TABLE (read via PluginDrivenExternalTable.hasScanCapability), and the hive gateway
        // reflects it onto a hudi-on-HMS table's schema so hudi_meta keeps working through the sibling delegation.
        // Every hudi table has a commit timeline, so it is connector-wide. MUTATION: dropping the capability -> a
        // flipped hudi table's hudi_meta() returns "not a hudi table".
        Set<ConnectorCapability> caps = connector().getCapabilities();
        Assertions.assertTrue(caps.contains(ConnectorCapability.SUPPORTS_METADATA_TABLE),
                "hudi declares the metadata-table capability so hudi_meta() works post-flip");
    }
}
