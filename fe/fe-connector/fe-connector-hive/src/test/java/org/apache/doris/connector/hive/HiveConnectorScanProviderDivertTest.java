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

package org.apache.doris.connector.hive;

import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.handle.ConnectorColumnHandle;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.connector.api.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.api.scan.ConnectorScanRange;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Pins the HMS-cutover §4.4 S5 scan-provider gateway seam: {@link HiveConnector#getScanPlanProvider(
 * ConnectorTableHandle)} routes by the concrete handle type — a hive handle runs the hive scan provider, any
 * foreign (iceberg-on-HMS) handle is delegated to the embedded iceberg sibling's scan provider — and NEVER casts
 * the foreign handle.
 *
 * <p>WHY (Rule 9): this pairs with the getTableHandle iceberg divert. Once getTableHandle returns a foreign
 * iceberg handle, the scan of that table must pick the SIBLING's scan provider (iceberg-loader-built, so
 * {@code PluginDrivenScanNode.onPluginClassLoader} auto-pins the scan-thread TCCL to the iceberg loader). A hive
 * (or hudi-stamped hive) handle must NOT be diverted — a hive-only deployment has no iceberg plugin, and hudi's
 * delegation is a later substep. The selection MUST happen at provider-acquisition time, so a wrong route here
 * would hand iceberg splits to the hive scanner (or vice versa).</p>
 *
 * <p>Dormant until hms enters {@code SPI_READY_TYPES}: no production path selects a scan provider for this
 * connector yet, so these assertions are a guard, not a live-path test.</p>
 */
public class HiveConnectorScanProviderDivertTest {

    private static final String METASTORE_URI = "thrift://host:9083";

    /** The foreign (non-hive) handle the iceberg sibling's getTableHandle produces post-flip. */
    private static final class ForeignHandle implements ConnectorTableHandle {
    }

    /**
     * The connector-level hive scan provider, stubbed so the divert test does not build a real HmsClient. A hive
     * handle must resolve to exactly THIS (what the no-arg {@link HiveConnector#getScanPlanProvider()} returns);
     * the real no-arg provider construction (which needs a HiveConf, off the unit-test classpath) is covered by
     * the scan-planning suites. Distinct instance from any sibling provider so {@code assertSame} is meaningful.
     */
    private final ConnectorScanPlanProvider stubbedHiveProvider = new MarkerScanProvider();

    /** A gateway whose no-arg hive provider is the stub above — isolates the per-handle routing from HmsClient. */
    private HiveConnector gatewayWithStubbedHiveProvider(RecordingSiblingContext context) {
        return new HiveConnector(props(), context) {
            @Override
            public ConnectorScanPlanProvider getScanPlanProvider() {
                return stubbedHiveProvider;
            }
        };
    }

    @Test
    public void foreignHandleDelegatesToSiblingScanProviderUnmodified() {
        RecordingSibling sibling = new RecordingSibling();
        RecordingSiblingContext context = new RecordingSiblingContext(sibling);
        HiveConnector connector = new HiveConnector(props(), context);
        ForeignHandle foreign = new ForeignHandle();

        ConnectorScanPlanProvider provider = connector.getScanPlanProvider(foreign);

        Assertions.assertSame(sibling.provider, provider,
                "a foreign handle must return the iceberg sibling's OWN scan provider");
        Assertions.assertSame(foreign, sibling.lastHandle,
                "the foreign handle must reach the sibling's per-handle selector UNMODIFIED (a rewrap would "
                        + "poison the downstream iceberg cast)");
        Assertions.assertEquals(1, context.buildCount, "the sibling must be built exactly once and consulted");
    }

    @Test
    public void hiveHandleUsesHiveProviderWithoutConsultingSibling() {
        RecordingSibling sibling = new RecordingSibling();
        RecordingSiblingContext context = new RecordingSiblingContext(sibling);
        HiveConnector connector = gatewayWithStubbedHiveProvider(context);
        HiveTableHandle hive = new HiveTableHandle.Builder("db", "t", HiveTableType.HIVE).build();

        ConnectorScanPlanProvider provider = connector.getScanPlanProvider(hive);

        Assertions.assertSame(stubbedHiveProvider, provider,
                "a hive handle resolves to the connector-level hive provider (the no-arg getScanPlanProvider)");
        Assertions.assertEquals(0, context.buildCount, "a hive handle must never build or consult the sibling");
        Assertions.assertNull(sibling.lastHandle, "the sibling's scan provider must not be consulted for a hive handle");
    }

    @Test
    public void hudiStampedHiveHandleStaysOnHiveProvider() {
        // The route keys on the JVM handle TYPE (HiveTableHandle), not the format enum: a HUDI-stamped hive handle
        // is still a HiveTableHandle, so it stays on the hive scan path (hudi delegation is a later substep).
        RecordingSibling sibling = new RecordingSibling();
        RecordingSiblingContext context = new RecordingSiblingContext(sibling);
        HiveConnector connector = gatewayWithStubbedHiveProvider(context);
        HiveTableHandle hudi = new HiveTableHandle.Builder("db", "t", HiveTableType.HUDI).build();

        ConnectorScanPlanProvider provider = connector.getScanPlanProvider(hudi);

        Assertions.assertSame(stubbedHiveProvider, provider, "a hudi-stamped hive handle stays on the hive provider");
        Assertions.assertEquals(0, context.buildCount, "a hudi table must NOT be diverted to the iceberg sibling");
    }

    @Test
    public void foreignHandleFailsLoudWhenIcebergPluginAbsent() {
        // The seam returns a null sibling when the iceberg plugin is absent; selecting a scan provider for a
        // foreign handle must fail loud (naming the catalog), not NPE.
        RecordingSiblingContext context = new RecordingSiblingContext(null);
        HiveConnector connector = new HiveConnector(props(), context);

        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> connector.getScanPlanProvider(new ForeignHandle()),
                "a foreign handle with no iceberg plugin must fail loud");
        Assertions.assertTrue(ex.getMessage().contains("test_catalog"), ex.getMessage());
    }

    private static Map<String, String> props() {
        // A metastore uri so the hive-handle branch can build its (offline, lazy-pool) HmsClient; the foreign
        // branch never touches it.
        Map<String, String> props = new HashMap<>();
        props.put("hive.metastore.uris", METASTORE_URI);
        return props;
    }

    /** Records the {@code createSiblingConnector} call and returns a configurable (possibly null) sibling. */
    private static final class RecordingSiblingContext extends FakeConnectorContext {
        int buildCount;
        Connector siblingToReturn;

        RecordingSiblingContext(Connector siblingToReturn) {
            this.siblingToReturn = siblingToReturn;
        }

        @Override
        public Connector createSiblingConnector(String catalogType, Map<String, String> properties) {
            buildCount++;
            return siblingToReturn;
        }
    }

    /** A sibling {@link Connector} whose per-handle scan provider is a distinguishable marker, recording the handle. */
    private static final class RecordingSibling implements Connector {
        final ConnectorScanPlanProvider provider = new MarkerScanProvider();
        ConnectorTableHandle lastHandle;

        @Override
        public ConnectorMetadata getMetadata(ConnectorSession session) {
            return null;
        }

        @Override
        public ConnectorScanPlanProvider getScanPlanProvider(ConnectorTableHandle handle) {
            lastHandle = handle;
            return provider;
        }

        @Override
        public void close() {
        }
    }

    /** A bare scan provider stand-in; only its identity matters here. */
    private static final class MarkerScanProvider implements ConnectorScanPlanProvider {
        @Override
        public List<ConnectorScanRange> planScan(ConnectorSession session, ConnectorTableHandle handle,
                List<ConnectorColumnHandle> columns, Optional<ConnectorExpression> filter) {
            return Collections.emptyList();
        }
    }
}
