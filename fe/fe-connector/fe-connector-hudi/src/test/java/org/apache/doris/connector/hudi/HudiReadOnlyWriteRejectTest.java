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

import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.ConnectorContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

/**
 * Locks the READ-ONLY safety net for hudi-on-HMS tables: once a flipped hms gateway diverts a hudi table to a
 * foreign handle, the gateway's 3-way routing sends that handle to THIS (the hudi sibling) connector, so this
 * connector — not iceberg — answers every write / procedure / DDL / transaction call. Hudi data is written by
 * Spark/Flink; Doris only reads. This test pins that the hudi connector rejects EVERY mutation fail-loud, so a
 * future change cannot silently make a hudi table writable or DDL-mutable, and a hudi write is never
 * mis-executed as an iceberg write.
 *
 * <p>Dormant until hms enters {@code SPI_READY_TYPES} and hudi handles are diverted (no production path reaches
 * this connector yet); this is a Rule-9 behavior lock. The correctness at flip is asserted end-to-end (a
 * heterogeneous HMS catalog where hudi INSERT/DELETE/MERGE/EXECUTE all fail loud read-only).
 */
public class HudiReadOnlyWriteRejectTest {

    private static final ConnectorTableHandle HUDI_HANDLE =
            new HudiTableHandle("db", "t", "s3://b/t", "COPY_ON_WRITE");

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

    /** The write/DDL methods throw before touching the client/executor, so null collaborators are sufficient. */
    private static HudiConnectorMetadata metadata() {
        return new HudiConnectorMetadata(null, Collections.emptyMap(), null);
    }

    @Test
    public void noWriterSoDataWritesRejectedByAdmissionGate() {
        // Read-only: the hudi connector supplies NO write plan provider, so supportedWriteOperations is empty and
        // the engine's PhysicalPlanTranslator INSERT / row-level-DML gates throw a clean AnalysisException up
        // front (before opening any transaction). Keeping the provider null (NOT throwing) is deliberate: the
        // admission gate CALLS getWritePlanProvider to decide, so a throw there would make the gate raise a
        // DorisConnectorException the engine misclassifies as an internal error instead of "does not support
        // INSERT". MUTATION: return a write plan provider / a non-empty op set -> a hudi INSERT/DELETE would be
        // admitted and mis-executed -> red.
        HudiConnector connector = connector();
        Assertions.assertNull(connector.getWritePlanProvider(HUDI_HANDLE));
        Assertions.assertTrue(connector.supportedWriteOperations(HUDI_HANDLE).isEmpty());
    }

    @Test
    public void noProceduresSoExecuteRejected() {
        // No procedure ops -> EXECUTE <proc> on a hudi table is rejected ("does not support EXECUTE"), same as
        // plain-hive. MUTATION: return a non-null ProcedureOps -> EXECUTE admitted -> red.
        Assertions.assertNull(connector().getProcedureOps(HUDI_HANDLE));
    }

    @Test
    public void beginTransactionThrowsExplicitReadOnly() {
        // The explicit last-line defense: opening a write transaction on a hudi table fails loud with a
        // HUDI-SPECIFIC read-only message rather than the generic "Transactions not supported" default, so any
        // write path that reaches transaction-open (bypassing the admission gate) reports the right reason.
        // Overriding the no-arg form covers the per-handle overload (its SPI default delegates to it). MUTATION:
        // drop the override -> the message is the generic default, not read-only -> red.
        HudiConnectorMetadata metadata = metadata();
        DorisConnectorException noArg = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata.beginTransaction(null));
        Assertions.assertTrue(noArg.getMessage().toLowerCase().contains("read-only"),
                "expected an explicit hudi read-only message, got: " + noArg.getMessage());
        // The production-routed path is the PER-HANDLE overload (the gateway forwards
        // siblingMetadata(session, handle).beginTransaction(session, handle)); its SPI default delegates to the
        // no-arg override, so it too fails with the explicit read-only message. Lock BOTH so the delegation the
        // real path relies on cannot silently break.
        DorisConnectorException perHandle = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata.beginTransaction(null, HUDI_HANDLE));
        Assertions.assertTrue(perHandle.getMessage().toLowerCase().contains("read-only"),
                "the per-handle overload the gateway routes to must also be explicit read-only");
    }

    @Test
    public void allDdlRejected() {
        // Hudi tables are externally managed, so Doris rejects ALL DDL — catalog metadata ops (DROP TABLE /
        // RENAME TABLE / TRUNCATE, a deliberate strict-read-only choice: a signed-off behavior change vs legacy
        // HMS, which allowed them) AND the full ALTER family (add/drop/rename/modify/reorder column, create/drop
        // branch/tag, add/drop/replace partition field). These are the SPI defaults; this test LOCKS the WHOLE
        // mutation surface so a future override cannot silently make a hudi table DDL-mutable. MUTATION: implement
        // any of these on the hudi metadata -> that one assertion goes red.
        HudiConnectorMetadata m = metadata();
        // Table-level catalog DDL.
        Assertions.assertThrows(DorisConnectorException.class, () -> m.dropTable(null, HUDI_HANDLE));
        Assertions.assertThrows(DorisConnectorException.class, () -> m.renameTable(null, HUDI_HANDLE, "new_name"));
        Assertions.assertThrows(DorisConnectorException.class,
                () -> m.truncateTable(null, HUDI_HANDLE, Collections.emptyList()));
        // Column ALTER.
        Assertions.assertThrows(DorisConnectorException.class, () -> m.addColumn(null, HUDI_HANDLE, null, null));
        Assertions.assertThrows(DorisConnectorException.class, () -> m.addColumns(null, HUDI_HANDLE, null));
        Assertions.assertThrows(DorisConnectorException.class, () -> m.dropColumn(null, HUDI_HANDLE, null));
        Assertions.assertThrows(DorisConnectorException.class, () -> m.renameColumn(null, HUDI_HANDLE, null, null));
        Assertions.assertThrows(DorisConnectorException.class, () -> m.modifyColumn(null, HUDI_HANDLE, null, null));
        Assertions.assertThrows(DorisConnectorException.class, () -> m.reorderColumns(null, HUDI_HANDLE, null));
        // Branch / tag / partition-field ALTER.
        Assertions.assertThrows(DorisConnectorException.class,
                () -> m.createOrReplaceBranch(null, HUDI_HANDLE, null));
        Assertions.assertThrows(DorisConnectorException.class, () -> m.createOrReplaceTag(null, HUDI_HANDLE, null));
        Assertions.assertThrows(DorisConnectorException.class, () -> m.dropBranch(null, HUDI_HANDLE, null));
        Assertions.assertThrows(DorisConnectorException.class, () -> m.dropTag(null, HUDI_HANDLE, null));
        Assertions.assertThrows(DorisConnectorException.class, () -> m.addPartitionField(null, HUDI_HANDLE, null));
        Assertions.assertThrows(DorisConnectorException.class, () -> m.dropPartitionField(null, HUDI_HANDLE, null));
        Assertions.assertThrows(DorisConnectorException.class,
                () -> m.replacePartitionField(null, HUDI_HANDLE, null));
    }
}
