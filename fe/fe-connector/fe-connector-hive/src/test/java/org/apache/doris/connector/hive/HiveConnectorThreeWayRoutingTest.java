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
import org.apache.doris.connector.api.handle.ConnectorWriteHandle;
import org.apache.doris.connector.api.procedure.ConnectorProcedureOps;
import org.apache.doris.connector.api.procedure.ConnectorProcedureResult;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.connector.api.pushdown.ConnectorPredicate;
import org.apache.doris.connector.api.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.api.scan.ConnectorScanRange;
import org.apache.doris.connector.api.write.ConnectorSinkPlan;
import org.apache.doris.connector.api.write.ConnectorWritePlanProvider;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Pins the hms-cutover THREE-WAY foreign-handle routing: with TWO embedded siblings (iceberg + hudi) under one
 * gateway, the binary {@code else -> iceberg} discriminator no longer works — a hudi handle must not be
 * wrong-routed to the iceberg sibling. {@link HiveConnector} routes every per-handle seam by asking each
 * ALREADY-BUILT sibling {@link Connector#ownsHandle} (a hive handle stays hive; else the owning sibling by
 * {@code ownsHandle}; else fail loud). The concrete foreign handle type is invisible across the plugin
 * classloader split, so the gateway can never {@code instanceof} it itself.
 *
 * <p>The same {@code HiveConnector.resolveSiblingOwner} brain backs both the connector-level
 * {@code get*Provider(handle)} seams exercised here AND the ~34 per-handle guard-and-forward methods in
 * {@link HiveConnectorMetadata} (which forward via the injected resolver — see
 * {@link HiveConnectorMetadataSiblingDelegationTest}).</p>
 *
 * <p><b>WHY the PEEK design (Rule 9):</b> routing consults only siblings that are already built (the owning
 * sibling is always built first, by getTableHandle, before it can produce the handle). It never force-builds an
 * unrelated plugin merely to classify a handle — so a catalog serving only hudi (no iceberg plugin installed)
 * still routes its hudi handles, and the iceberg arm stays byte-behaviour-identical (an iceberg handle routes to
 * iceberg without ever touching the hudi holder). Dormant until hms enters {@code SPI_READY_TYPES}.</p>
 */
public class HiveConnectorThreeWayRoutingTest {

    private static final String METASTORE_URI = "thrift://host:9083";

    /** Stand-in for the raw iceberg-loader handle the iceberg sibling produces (loader-invisible to the gateway). */
    private static final class IcebergLikeHandle implements ConnectorTableHandle {
    }

    /** Stand-in for the raw hudi-loader handle the hudi sibling produces. */
    private static final class HudiLikeHandle implements ConnectorTableHandle {
    }

    @Test
    public void icebergHandleRoutesToIcebergSiblingAcrossAllSeams() {
        RoutingSibling iceberg = new RoutingSibling("iceberg", IcebergLikeHandle.class);
        RoutingSibling hudi = new RoutingSibling("hudi", HudiLikeHandle.class);
        TwoSiblingContext ctx = new TwoSiblingContext(iceberg, hudi);
        HiveConnector connector = new HiveConnector(props(), ctx);
        connector.getOrCreateIcebergSibling();
        connector.getOrCreateHudiSibling();
        IcebergLikeHandle handle = new IcebergLikeHandle();

        Assertions.assertSame(iceberg.scanProvider, connector.getScanPlanProvider(handle),
                "an iceberg handle must route to the iceberg sibling's scan provider");
        Assertions.assertSame(iceberg.writeProvider, connector.getWritePlanProvider(handle),
                "an iceberg handle must route to the iceberg sibling's write provider");
        Assertions.assertSame(iceberg.procedureOps, connector.getProcedureOps(handle),
                "an iceberg handle must route to the iceberg sibling's procedure ops");
        Assertions.assertNull(hudi.lastScanHandle, "the hudi sibling must never be consulted for an iceberg handle");
    }

    @Test
    public void hudiHandleRoutesToHudiSiblingAcrossAllSeams() {
        RoutingSibling iceberg = new RoutingSibling("iceberg", IcebergLikeHandle.class);
        RoutingSibling hudi = new RoutingSibling("hudi", HudiLikeHandle.class);
        TwoSiblingContext ctx = new TwoSiblingContext(iceberg, hudi);
        HiveConnector connector = new HiveConnector(props(), ctx);
        connector.getOrCreateIcebergSibling();
        connector.getOrCreateHudiSibling();
        HudiLikeHandle handle = new HudiLikeHandle();

        Assertions.assertSame(hudi.scanProvider, connector.getScanPlanProvider(handle),
                "a hudi handle must route to the hudi sibling's scan provider, NOT the iceberg sibling's");
        Assertions.assertSame(hudi.writeProvider, connector.getWritePlanProvider(handle),
                "a hudi handle must route to the hudi sibling's write provider");
        Assertions.assertSame(hudi.procedureOps, connector.getProcedureOps(handle),
                "a hudi handle must route to the hudi sibling's procedure ops");
        Assertions.assertNull(iceberg.lastScanHandle, "the iceberg sibling must never be consulted for a hudi handle");
    }

    @Test
    public void hiveHandleStaysHiveAndConsultsNeitherSibling() {
        RoutingSibling iceberg = new RoutingSibling("iceberg", IcebergLikeHandle.class);
        RoutingSibling hudi = new RoutingSibling("hudi", HudiLikeHandle.class);
        TwoSiblingContext ctx = new TwoSiblingContext(iceberg, hudi);
        HiveConnector connector = new HiveConnector(props(), ctx);
        connector.getOrCreateIcebergSibling();
        connector.getOrCreateHudiSibling();
        HiveTableHandle hive = new HiveTableHandle.Builder("db", "t", HiveTableType.HIVE).build();

        // getProcedureOps needs no HmsClient: a hive handle inherits the connector-level null (plain-hive has none).
        Assertions.assertNull(connector.getProcedureOps(hive), "a hive handle has no procedures (connector-level null)");
        Assertions.assertNull(iceberg.lastProcedureHandle, "a hive handle must not consult the iceberg sibling");
        Assertions.assertNull(hudi.lastProcedureHandle, "a hive handle must not consult the hudi sibling");
    }

    @Test
    public void unknownForeignHandleFailsLoud() {
        RoutingSibling iceberg = new RoutingSibling("iceberg", IcebergLikeHandle.class);
        RoutingSibling hudi = new RoutingSibling("hudi", HudiLikeHandle.class);
        TwoSiblingContext ctx = new TwoSiblingContext(iceberg, hudi);
        HiveConnector connector = new HiveConnector(props(), ctx);
        connector.getOrCreateIcebergSibling();
        connector.getOrCreateHudiSibling();

        // A foreign handle owned by NEITHER built sibling is an orphan (a bug, not a route) — fail loud, do not
        // silently hand it to an arbitrary sibling.
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> connector.getScanPlanProvider(new ConnectorTableHandle() {
                }), "a foreign handle no sibling owns must fail loud");
        Assertions.assertTrue(ex.getMessage().contains("test_catalog"), ex.getMessage());
    }

    @Test
    public void hudiHandleRoutesWithoutBuildingIcebergSibling() {
        // The PEEK no-regression guarantee: a catalog serving only hudi tables (no iceberg plugin installed) must
        // route its hudi handles WITHOUT the router force-building an iceberg sibling. Iceberg is absent (null);
        // only the hudi sibling is built.
        RoutingSibling hudi = new RoutingSibling("hudi", HudiLikeHandle.class);
        TwoSiblingContext ctx = new TwoSiblingContext(null, hudi);
        HiveConnector connector = new HiveConnector(props(), ctx);
        connector.getOrCreateHudiSibling();

        Assertions.assertSame(hudi.scanProvider, connector.getScanPlanProvider(new HudiLikeHandle()),
                "a hudi handle must route to the hudi sibling even with no iceberg plugin present");
        Assertions.assertEquals(0, ctx.icebergBuilds,
                "routing a hudi handle must NOT build (or require) the iceberg sibling");
    }

    @Test
    public void icebergHandleRoutesWithoutBuildingHudiSibling() {
        // Symmetric guarantee — the iceberg arm stays byte-behaviour-identical after adding the hudi holder: an
        // iceberg handle routes to iceberg without the router touching the hudi holder (no hudi plugin required).
        RoutingSibling iceberg = new RoutingSibling("iceberg", IcebergLikeHandle.class);
        TwoSiblingContext ctx = new TwoSiblingContext(iceberg, null);
        HiveConnector connector = new HiveConnector(props(), ctx);
        connector.getOrCreateIcebergSibling();

        Assertions.assertSame(iceberg.scanProvider, connector.getScanPlanProvider(new IcebergLikeHandle()),
                "an iceberg handle must route to the iceberg sibling with no hudi plugin present");
        Assertions.assertEquals(0, ctx.hudiBuilds,
                "routing an iceberg handle must NOT build (or require) the hudi sibling");
    }

    @Test
    public void getMetadataWiresEachByTypeSupplierToItsOwnSibling() {
        // The by-HANDLE routing above is one brain (resolveSiblingOwner). getTableHandle instead diverts BY TYPE
        // (no handle exists yet), and HiveConnector.newMetadata (extracted from getMetadata) is the SOLE production
        // point that wires those two by-TYPE suppliers: `this::getOrCreateIcebergSibling,
        // this::getOrCreateHudiSibling`. They share the static type Supplier<Connector>, so a transposition compiles
        // clean and would silently send getTableHandle's ICEBERG arm to the hudi sibling (and vice versa) at flip.
        // The per-handle DivertTest builds the metadata DIRECTLY with hand-labeled lambdas and cannot observe this
        // order — guard it here, at the real wiring. newMetadata(null) exercises that wiring without getMetadata's
        // eager ThriftHmsClient build (whose Hadoop stack is absent from unit tests); the null client is never
        // dereferenced because only the by-TYPE sibling arms are driven.
        RoutingSibling iceberg = new RoutingSibling("iceberg", IcebergLikeHandle.class);
        RoutingSibling hudi = new RoutingSibling("hudi", HudiLikeHandle.class);
        TwoSiblingContext ctx = new TwoSiblingContext(iceberg, hudi);
        HiveConnector connector = new HiveConnector(props(), ctx);

        HiveConnectorMetadata md = connector.newMetadata(null);

        // The getTableHandle ICEBERG arm resolves BY TYPE via icebergSiblingMetadata, which force-builds the
        // iceberg sibling (createSiblingConnector("iceberg")). A transposed getMetadata would build hudi here.
        md.icebergSiblingMetadata(null);
        Assertions.assertEquals(1, ctx.icebergBuilds, "the iceberg by-TYPE arm must force-build the iceberg sibling");
        Assertions.assertEquals(0, ctx.hudiBuilds, "the iceberg by-TYPE arm must NOT build the hudi sibling");

        // Symmetrically the HUDI arm must resolve the hudi sibling, not rebuild iceberg.
        md.hudiSiblingMetadata(null);
        Assertions.assertEquals(1, ctx.hudiBuilds, "the hudi by-TYPE arm must force-build the hudi sibling");
        Assertions.assertEquals(1, ctx.icebergBuilds, "the hudi by-TYPE arm must not rebuild the iceberg sibling");
    }

    private static Map<String, String> props() {
        Map<String, String> props = new HashMap<>();
        props.put("hive.metastore.uris", METASTORE_URI);
        return props;
    }

    /** A context that builds a distinct sibling per type (iceberg / hudi), each possibly null (absent plugin). */
    private static final class TwoSiblingContext extends FakeConnectorContext {
        private final Connector iceberg;
        private final Connector hudi;
        int icebergBuilds;
        int hudiBuilds;

        TwoSiblingContext(Connector iceberg, Connector hudi) {
            this.iceberg = iceberg;
            this.hudi = hudi;
        }

        @Override
        public Connector createSiblingConnector(String catalogType, Map<String, String> properties) {
            if ("iceberg".equals(catalogType)) {
                icebergBuilds++;
                return iceberg;
            }
            if ("hudi".equals(catalogType)) {
                hudiBuilds++;
                return hudi;
            }
            return null;
        }
    }

    /** A sibling that owns exactly one handle type and returns identity-marked, call-recording providers. */
    private static final class RoutingSibling implements Connector {
        private final Class<? extends ConnectorTableHandle> ownedType;
        final ConnectorScanPlanProvider scanProvider;
        final ConnectorWritePlanProvider writeProvider = new MarkerWriteProvider();
        final ConnectorProcedureOps procedureOps = new MarkerProcedureOps();
        ConnectorTableHandle lastScanHandle;
        ConnectorTableHandle lastProcedureHandle;

        RoutingSibling(String name, Class<? extends ConnectorTableHandle> ownedType) {
            this.ownedType = ownedType;
            this.scanProvider = new MarkerScanProvider();
        }

        @Override
        public boolean ownsHandle(ConnectorTableHandle handle) {
            return ownedType.isInstance(handle);
        }

        @Override
        public ConnectorMetadata getMetadata(ConnectorSession session) {
            return null;
        }

        @Override
        public ConnectorScanPlanProvider getScanPlanProvider(ConnectorTableHandle handle) {
            lastScanHandle = handle;
            return scanProvider;
        }

        @Override
        public ConnectorWritePlanProvider getWritePlanProvider(ConnectorTableHandle handle) {
            return writeProvider;
        }

        @Override
        public ConnectorProcedureOps getProcedureOps(ConnectorTableHandle handle) {
            lastProcedureHandle = handle;
            return procedureOps;
        }

        @Override
        public void close() {
        }
    }

    private static final class MarkerScanProvider implements ConnectorScanPlanProvider {
        @Override
        public List<ConnectorScanRange> planScan(ConnectorSession session, ConnectorTableHandle handle,
                List<ConnectorColumnHandle> columns, Optional<ConnectorExpression> filter) {
            return Collections.emptyList();
        }
    }

    private static final class MarkerWriteProvider implements ConnectorWritePlanProvider {
        @Override
        public ConnectorSinkPlan planWrite(ConnectorSession session, ConnectorWriteHandle handle) {
            return null;
        }
    }

    private static final class MarkerProcedureOps implements ConnectorProcedureOps {
        @Override
        public List<String> getSupportedProcedures() {
            return Collections.emptyList();
        }

        @Override
        public ConnectorProcedureResult execute(ConnectorSession session, ConnectorTableHandle table,
                String procedureName, Map<String, String> properties, ConnectorPredicate whereCondition,
                List<String> partitionNames) {
            return null;
        }
    }
}
