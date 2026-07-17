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

package org.apache.doris.datasource.scan;

import org.apache.doris.analysis.ColumnAccessPath;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.connector.api.Connector;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TPushAggOp;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

/**
 * FIX-R3-RESIDUAL — guards that the VERBOSE per-backend {@code backends:} block in
 * {@link PluginDrivenScanNode#getNodeExplainString} is emitted for EVERY plugin connector, NOT gated to a
 * hardcoded source name.
 *
 * <p><b>Why this matters (Rule 9 — tests encode WHY):</b> the {@code backends:} block (the per-backend
 * scan-range detail with {@code dataFileNum/deleteFileNum/deleteSplitNum}) is universal {@link FileScanNode}
 * behavior: the parent emits it unconditionally under {@code VERBOSE && !isBatchMode()}
 * ({@code FileScanNode#getNodeExplainString}). This override does not call super, and previously re-emitted
 * the block only when {@code "paimon".equals(catalog.getType())}. That source-name gate (a) regressed
 * cut-over MaxCompute VERBOSE EXPLAIN (legacy {@code MaxComputeScanNode extends FileQueryScanNode} inherited
 * the unconditional block) and (b) violated the project rule that the generic SPI node must not branch on a
 * connector source name. After cut-over, jdbc/es/trino-connector/max_compute/paimon all route through this
 * node ({@code SPI_READY_TYPES}); the block must appear for all of them.</p>
 *
 * <p><b>MUTATION killed:</b> re-introducing {@code && "paimon".equals(desc.getTable().getDatabase()
 * .getCatalog().getType())} makes a non-paimon catalog (here {@code max_compute}) skip the block, so
 * {@code "backends:"} disappears from VERBOSE EXPLAIN &rarr; {@link #verboseEmitsBackendsBlockForNonPaimonConnector}
 * goes red. {@link #nonVerboseOmitsBackendsBlock} pins the surviving {@code VERBOSE} gate so a mutant that
 * drops the level check (always emitting the block) is also killed.</p>
 *
 * <p>Driven on a {@code CALLS_REAL_METHODS} mock with only the fields the explain path reads injected (the
 * same partial-node technique as {@code PluginDrivenScanNodeDeleteFilesTest}; full {@code create(...)}
 * construction is unnecessary). {@code scanRangeLocations} is left EMPTY: the per-backend loop is then
 * skipped and only the unconditional bare {@code backends:} header is emitted, so no synthetic
 * {@code FileScanRange} plumbing is needed and the deref chain inside the loop never runs.</p>
 */
public class PluginDrivenScanNodeVerboseExplainTest {

    /**
     * A {@code CALLS_REAL_METHODS} node whose {@code desc} resolves to a table on a catalog of
     * {@code catalogType}, with empty scan ranges / conjuncts and {@code isBatchMode()==false}, so
     * {@code getNodeExplainString} runs its full table-scan (else) branch without I/O or NPE.
     */
    private static PluginDrivenScanNode nodeForCatalogType(String catalogType) {
        PluginDrivenScanNode node = Mockito.mock(PluginDrivenScanNode.class, Mockito.CALLS_REAL_METHODS);

        TableIf table = Mockito.mock(TableIf.class);
        DatabaseIf db = Mockito.mock(DatabaseIf.class);
        CatalogIf<?> catalog = Mockito.mock(CatalogIf.class);
        Mockito.when(table.getNameWithFullQualifiers()).thenReturn(catalogType + "_ctl.db.tbl");
        Mockito.when(table.getDatabase()).thenReturn(db);
        Mockito.when(db.getCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getType()).thenReturn(catalogType);
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        desc.setTable(table);

        Deencapsulation.setField(node, "desc", desc);
        // Mockito skips the constructor, so field initializers do not run -> set the non-null fields the
        // explain path reads. Empty scanRangeLocations => the per-backend loop body is skipped.
        Deencapsulation.setField(node, "conjuncts", new ArrayList<>());
        Deencapsulation.setField(node, "scanRangeLocations", new ArrayList<>());
        // useTopnFilter() runs at the method tail (common to both EXPLAIN paths) and derefs this list.
        Deencapsulation.setField(node, "topnFilterSortNodes", new ArrayList<>());
        // Pre-seed the cache so getOrLoadScanNodeProperties() returns it without contacting the connector.
        Deencapsulation.setField(node, "scanNodeProperties", Collections.<String, String>emptyMap());
        // Pre-seed the isBatchMode cache so the gate's !isBatchMode() is deterministic (no computeBatchMode).
        Deencapsulation.setField(node, "isBatchModeCache", Boolean.FALSE);
        Deencapsulation.setField(node, "connector", Mockito.mock(Connector.class));
        Deencapsulation.setField(node, "pushDownAggNoGroupingOp", TPushAggOp.NONE);
        return node;
    }

    @Test
    public void verboseEmitsBackendsBlockForNonPaimonConnector() {
        // max_compute is the connector that actually regressed at cut-over (legacy MaxComputeScanNode
        // inherited the unconditional FileScanNode block); the same holds for es/jdbc/trino-connector.
        PluginDrivenScanNode node = nodeForCatalogType("max_compute");

        String explain = node.getNodeExplainString("", TExplainLevel.VERBOSE);

        Assertions.assertTrue(explain.contains("backends:"),
                "VERBOSE EXPLAIN must emit the universal FileScanNode backends: block for a non-paimon "
                        + "plugin connector (no source-name gate). Actual:\n" + explain);
    }

    @Test
    public void verboseEmitsBackendsBlockForPaimon() {
        // Parity guard: removing the gate must NOT drop the block for paimon (it stays emitted).
        PluginDrivenScanNode node = nodeForCatalogType("paimon");

        String explain = node.getNodeExplainString("", TExplainLevel.VERBOSE);

        Assertions.assertTrue(explain.contains("backends:"),
                "VERBOSE EXPLAIN must still emit the backends: block for paimon. Actual:\n" + explain);
    }

    @Test
    public void emitsNestedColumnsBlockForPluginConnector() {
        // F6/F7: the parent FileScanNode emits the "nested columns:" block (pruned type / sub path / all +
        // predicate access paths) via printNestedColumns; this override drops it by not calling super, so the
        // WHOLE block vanished for EVERY plugin FileScan connector (broader than iceberg). Attach a slot
        // carrying nested-pruned access paths and assert the block re-appears. MUTATION: removing the
        // printNestedColumns(...) call -> "nested columns:" disappears -> red. The node is a
        // PluginDrivenScanNode (never an IcebergScanNode), so the GENERIC name-join path renders "a.b"
        // (the dead iceberg field-id merge arms PlanNode:949/965 -> "a(3).b(5)" are NOT reached).
        PluginDrivenScanNode node = nodeForCatalogType("max_compute");
        TupleDescriptor desc = Deencapsulation.getField(node, "desc");
        SlotDescriptor slot = new SlotDescriptor(new SlotId(1), desc.getId());
        Column col = new Column("c1", Type.INT);
        slot.setColumn(col);
        slot.setType(Type.INT);
        // printNestedColumns gates the "all access paths" line on getDisplayAllAccessPaths but the generic
        // branch renders getDisplayAllAccessPaths; it gates the "predicate access paths" line on
        // getDisplayPredicateAccessPaths but the generic branch renders getPredicateAccessPaths. Set BOTH the
        // display and non-display forms so each line renders its value regardless of that asymmetry.
        slot.setAllAccessPaths(Collections.singletonList(ColumnAccessPath.data(Arrays.asList("a", "b"))));
        slot.setDisplayAllAccessPaths(
                Collections.singletonList(ColumnAccessPath.data(Arrays.asList("a", "b"))));
        slot.setPredicateAccessPaths(
                Collections.singletonList(ColumnAccessPath.data(Collections.singletonList("x"))));
        slot.setDisplayPredicateAccessPaths(
                Collections.singletonList(ColumnAccessPath.data(Collections.singletonList("x"))));
        desc.addSlot(slot);

        String explain = node.getNodeExplainString("", TExplainLevel.VERBOSE);

        Assertions.assertTrue(explain.contains("nested columns:"),
                "the nested columns block must be re-emitted for a plugin FileScan connector. Actual:\n"
                        + explain);
        Assertions.assertTrue(explain.contains("all access paths: [a.b]"),
                "F6: the all-access-paths line must re-appear (generic name-join). Actual:\n" + explain);
        Assertions.assertTrue(explain.contains("predicate access paths: [x]"),
                "F7: the predicate-access-paths line must re-appear. Actual:\n" + explain);
    }

    @Test
    public void nonVerboseOmitsBackendsBlock() {
        // Pins the surviving level gate: the block is VERBOSE-only. A mutant that emits it unconditionally
        // (drops the TExplainLevel.VERBOSE check) would leak the block into NORMAL EXPLAIN -> red.
        PluginDrivenScanNode node = nodeForCatalogType("max_compute");

        String explain = node.getNodeExplainString("", TExplainLevel.NORMAL);

        Assertions.assertFalse(explain.contains("backends:"),
                "NORMAL EXPLAIN must NOT emit the VERBOSE-only backends: block. Actual:\n" + explain);
    }
}
