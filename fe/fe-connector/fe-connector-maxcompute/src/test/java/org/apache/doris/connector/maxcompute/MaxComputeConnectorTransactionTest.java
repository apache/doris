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

package org.apache.doris.connector.maxcompute;

import org.apache.doris.connector.api.DorisConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Guards the write block-id cap (GC1 / FIX-BLOCKID-CAP-CONFIG). The cap mirrors legacy
 * {@code MCTransaction.allocateBlockIdRange}, which reads the tunable
 * {@code Config.max_compute_write_max_block_count}. The connector cannot import fe-core
 * {@code Config}, so the live value is surfaced through {@code ConnectorSession.getSessionProperties()}
 * (injected by fe-core's {@code ConnectorSessionBuilder}, the same channel as
 * {@code lower_case_table_names}) and threaded into the transaction via its constructor.
 *
 * <p><b>Why this matters.</b> The previous hardcoded {@code MAX_BLOCK_COUNT = 20000L} (DV-011)
 * silently ignored a tuned fe.conf: a deployment that raised the cap could no longer run the large
 * writes legacy allowed. These tests pin that the cap is now driven by the constructor argument
 * (not a constant) and that resolution falls back to the legacy default when the session carries
 * no value. The transaction is fe-core-free, so it is exercised directly — no network / live ODPS.</p>
 */
public class MaxComputeConnectorTransactionTest {

    private static MaxComputeConnectorTransaction txnWithCap(long maxBlockCount) {
        MaxComputeConnectorTransaction txn = new MaxComputeConnectorTransaction(1L, maxBlockCount);
        // Only writeSessionId is consulted by allocateWriteBlockRange; identifier/settings (commit-only) may be null.
        txn.setWriteSession("sess-1", null, null);
        return txn;
    }

    // ---- the cap is enforced at exactly maxBlockCount ----

    @Test
    public void testAllocationUpToCapSucceedsAndBeyondThrows() {
        MaxComputeConnectorTransaction txn = txnWithCap(5L);
        Assertions.assertEquals(0L, txn.allocateWriteBlockRange("sess-1", 3)); // [0,3)
        Assertions.assertEquals(3L, txn.allocateWriteBlockRange("sess-1", 2)); // [3,5) -> endExclusive == cap, allowed
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> txn.allocateWriteBlockRange("sess-1", 1)); // 5+1 > 5
        Assertions.assertTrue(ex.getMessage().contains("maxBlockCount=5"),
                "the limit error must report the configured cap; got: " + ex.getMessage());
    }

    // ---- the limit is driven by the constructor arg, NOT a hardcoded 20000 ----

    @Test
    public void testCapIsConfigurableNotHardcoded() {
        // 8 blocks: rejected under cap 5, allowed under cap 10. A hardcoded 20000 would allow both,
        // so this would fail if the cap were still a constant.
        MaxComputeConnectorTransaction small = txnWithCap(5L);
        Assertions.assertThrows(DorisConnectorException.class,
                () -> small.allocateWriteBlockRange("sess-1", 8));

        MaxComputeConnectorTransaction large = txnWithCap(10L);
        Assertions.assertEquals(0L, large.allocateWriteBlockRange("sess-1", 8));
    }

    // ---- resolveMaxBlockCount: present -> parsed; absent / unparseable -> legacy default ----

    @Test
    public void testResolveMaxBlockCountParsesInjectedValue() {
        Map<String, String> props = new HashMap<>();
        props.put("max_compute_write_max_block_count", "50000");
        Assertions.assertEquals(50000L, MaxComputeConnectorMetadata.resolveMaxBlockCount(props));
    }

    @Test
    public void testResolveMaxBlockCountFallsBackWhenAbsent() {
        Assertions.assertEquals(MaxComputeConnectorTransaction.DEFAULT_MAX_BLOCK_COUNT,
                MaxComputeConnectorMetadata.resolveMaxBlockCount(new HashMap<>()));
        Assertions.assertEquals(20000L, MaxComputeConnectorTransaction.DEFAULT_MAX_BLOCK_COUNT);
    }

    @Test
    public void testResolveMaxBlockCountFallsBackWhenUnparseable() {
        Map<String, String> props = new HashMap<>();
        props.put("max_compute_write_max_block_count", "not-a-number");
        Assertions.assertEquals(MaxComputeConnectorTransaction.DEFAULT_MAX_BLOCK_COUNT,
                MaxComputeConnectorMetadata.resolveMaxBlockCount(props));
    }

    // ---- reject writing to ODPS external tables / logical views ----
    // Migrated from MCTransaction.beginInsert / MCTransactionTest (PR apache/doris#64119). The write
    // path now gates in MaxComputeWritePlanProvider.planWrite via
    // MaxComputeTableHandle.checkOperationSupported("Writing") before opening a write session; the
    // ODPS Storage API cannot write to external tables or logical views. The guard is exercised
    // directly here (the connector test module has no Mockito to fake an ODPS Table).

    @Test
    public void testWriteRejectsOdpsExternalTable() {
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> MaxComputeTableHandle.checkOperationSupported(
                        true, false, "Writing", "default", "mc_external_table"));
        Assertions.assertTrue(ex.getMessage().contains(
                "Writing MaxCompute external table or logical view is not supported: "
                        + "default.mc_external_table"),
                "got: " + ex.getMessage());
    }

    @Test
    public void testWriteRejectsOdpsLogicalView() {
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> MaxComputeTableHandle.checkOperationSupported(
                        false, true, "Writing", "default", "mc_logical_view"));
        Assertions.assertTrue(ex.getMessage().contains(
                "Writing MaxCompute external table or logical view is not supported: "
                        + "default.mc_logical_view"),
                "got: " + ex.getMessage());
    }

    @Test
    public void testWriteAllowsManagedTable() {
        // a normal (non-external, non-view) table must not be rejected (guards against over-rejection)
        Assertions.assertDoesNotThrow(() -> MaxComputeTableHandle.checkOperationSupported(
                false, false, "Writing", "default", "mc_managed_table"));
    }
}
