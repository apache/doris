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
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.procedure.ConnectorProcedureOps;
import org.apache.doris.connector.api.procedure.ConnectorProcedureResult;
import org.apache.doris.connector.api.pushdown.ConnectorPredicate;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Pins the HMS-cutover §4.4 write-delegation W5 seam: {@link HiveConnector#getProcedureOps(ConnectorTableHandle)}
 * routes {@code ALTER TABLE ... EXECUTE} procedure-ops selection by the concrete handle type — a hive handle has
 * no procedures (inherits the connector-level {@code null}), any foreign (iceberg-on-HMS) handle is delegated to
 * the embedded iceberg sibling's per-handle procedure ops — and NEVER casts the foreign handle. The
 * procedure-side twin of {@link HiveConnectorWriteProviderDivertTest} / {@link HiveConnectorScanProviderDivertTest}.
 *
 * <p>WHY (Rule 9): post-flip an iceberg-on-HMS table gains the native iceberg procedures (rollback_to_snapshot,
 * rewrite_data_files, ...), so a foreign handle must pick the SIBLING's ops; a plain-hive (or hudi-stamped hive)
 * handle has none and must keep the null, so EXECUTE on it is fail-loud rejected (plain-hive exposes no
 * procedures, and hudi's delegation is a later substep). The foreign handle is passed through UNMODIFIED (a
 * rewrap would poison the sibling's downstream iceberg cast).</p>
 *
 * <p>Dormant until hms enters {@code SPI_READY_TYPES}: no production path selects procedure ops for this
 * connector yet, so these assertions are a guard, not a live-path test.</p>
 */
public class HiveConnectorProcedureOpsDivertTest {

    private static final String METASTORE_URI = "thrift://host:9083";

    /** The foreign (non-hive) handle the iceberg sibling's getTableHandle produces post-flip. */
    private static final class ForeignHandle implements ConnectorTableHandle {
    }

    @Test
    public void foreignHandleDelegatesToSiblingProcedureOpsUnmodified() {
        RecordingSibling sibling = new RecordingSibling();
        RecordingSiblingContext context = new RecordingSiblingContext(sibling);
        HiveConnector connector = new HiveConnector(props(), context);
        ForeignHandle foreign = new ForeignHandle();

        ConnectorProcedureOps ops = connector.getProcedureOps(foreign);

        Assertions.assertSame(sibling.ops, ops,
                "a foreign handle must return the iceberg sibling's OWN procedure ops");
        Assertions.assertSame(foreign, sibling.lastHandle,
                "the foreign handle must reach the sibling's per-handle selector UNMODIFIED (a rewrap would "
                        + "poison the downstream iceberg cast)");
        Assertions.assertEquals(1, context.buildCount, "the sibling must be built exactly once and consulted");
    }

    @Test
    public void hiveHandleHasNoProceduresWithoutConsultingSibling() {
        // A hive handle inherits the connector-level null (plain-hive has no procedures). It must NOT build or
        // consult the sibling, and — unlike the write/scan seams — it needs no HmsClient (no-arg getProcedureOps
        // returns null directly).
        RecordingSibling sibling = new RecordingSibling();
        RecordingSiblingContext context = new RecordingSiblingContext(sibling);
        HiveConnector connector = new HiveConnector(props(), context);
        HiveTableHandle hive = new HiveTableHandle.Builder("db", "t", HiveTableType.HIVE).build();

        Assertions.assertNull(connector.getProcedureOps(hive),
                "a hive handle has no procedures — it inherits the connector-level null");
        Assertions.assertEquals(0, context.buildCount, "a hive handle must never build or consult the sibling");
        Assertions.assertNull(sibling.lastHandle, "the sibling's procedure ops must not be consulted for a hive handle");
    }

    @Test
    public void hudiStampedHiveHandleHasNoProcedures() {
        // The route keys on the JVM handle TYPE (HiveTableHandle), not the format enum: a HUDI-stamped hive handle
        // is still a HiveTableHandle, so it inherits the null (no procedures) — hudi delegation is a later substep.
        RecordingSibling sibling = new RecordingSibling();
        RecordingSiblingContext context = new RecordingSiblingContext(sibling);
        HiveConnector connector = new HiveConnector(props(), context);
        HiveTableHandle hudi = new HiveTableHandle.Builder("db", "t", HiveTableType.HUDI).build();

        Assertions.assertNull(connector.getProcedureOps(hudi),
                "a hudi-stamped hive handle inherits the connector-level null (no procedures)");
        Assertions.assertEquals(0, context.buildCount, "a hudi table must NOT be diverted to the iceberg sibling");
    }

    @Test
    public void foreignHandleFailsLoudWhenIcebergPluginAbsent() {
        // The seam returns a null sibling when the iceberg plugin is absent; selecting procedure ops for a
        // foreign handle must fail loud (naming the catalog), not NPE.
        RecordingSiblingContext context = new RecordingSiblingContext(null);
        HiveConnector connector = new HiveConnector(props(), context);

        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> connector.getProcedureOps(new ForeignHandle()),
                "a foreign handle with no iceberg plugin must fail loud");
        Assertions.assertTrue(ex.getMessage().contains("test_catalog"), ex.getMessage());
    }

    private static Map<String, String> props() {
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

    /** A sibling {@link Connector} whose per-handle procedure ops are a distinguishable marker, recording the handle. */
    private static final class RecordingSibling implements Connector {
        final ConnectorProcedureOps ops = new MarkerProcedureOps();
        ConnectorTableHandle lastHandle;

        @Override
        public ConnectorMetadata getMetadata(ConnectorSession session) {
            return null;
        }

        @Override
        public ConnectorProcedureOps getProcedureOps(ConnectorTableHandle handle) {
            lastHandle = handle;
            return ops;
        }

        @Override
        public void close() {
        }
    }

    /** A bare procedure-ops stand-in; only its identity matters here. */
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
