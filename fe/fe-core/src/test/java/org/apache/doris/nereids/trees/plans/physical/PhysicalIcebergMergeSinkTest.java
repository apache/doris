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

package org.apache.doris.nereids.trees.plans.physical;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.write.ConnectorWritePartitionField;
import org.apache.doris.connector.api.write.ConnectorWritePartitionSpec;
import org.apache.doris.connector.api.write.ConnectorWritePlanProvider;
import org.apache.doris.datasource.PluginDrivenExternalCatalog;
import org.apache.doris.datasource.PluginDrivenExternalTable;
import org.apache.doris.nereids.properties.DistributionSpecHash;
import org.apache.doris.nereids.properties.DistributionSpecMerge;
import org.apache.doris.nereids.properties.DistributionSpecMerge.IcebergPartitionField;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.merge.MergeOperation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalIcebergMergeSink.InsertPartitionFieldResult;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/**
 * Tests for the dual-mode partition-field resolution added to {@link PhysicalIcebergMergeSink} for the
 * iceberg SPI cutover (P6.6 C3b-core step 2). Pre-flip the merge-write distribution walks the native
 * iceberg {@code PartitionSpec}; post-flip it must reproduce the <em>byte-identical</em> distribution
 * from the connector's engine-neutral {@link ConnectorWritePartitionSpec}.
 *
 * <p>The parity core is {@link PhysicalIcebergMergeSink#reconstructPartitionFields} — a pure function
 * tested here directly (no mocks) to pin the three legacy parities that a silent divergence would break:
 * <ul>
 *   <li><b>P1 hard-fail clear</b> — an unresolvable source column (null name, or a name absent from the
 *       bound expr-id map) clears the accumulated fields and fails the whole spec, short-circuited before
 *       the {@link IcebergPartitionField} ctor's non-null expr-id requirement;</li>
 *   <li><b>P2 non-identity pre-pass</b> — {@code hasNonIdentity} is computed over <em>all</em> fields
 *       from the transform string, independent of resolvability and of where the build loop short-circuits;</li>
 *   <li><b>spec-id carry</b> — the spec id rides every partitioned outcome, null only when unpartitioned.</li>
 * </ul>
 * Two mocked-chain tests on {@link PhysicalIcebergMergeSink#getRequirePhysicalProperties()} pin the
 * dual-mode dispatch (a {@link PluginDrivenExternalTable} routes to the connector branch and its spec
 * flows into the {@link DistributionSpecMerge}) and the {@code enableIcebergMergePartitioning} gate
 * (off → no connector consultation).</p>
 */
public class PhysicalIcebergMergeSinkTest {

    private ConnectContext connectContext;

    @BeforeEach
    public void setUp() {
        connectContext = new ConnectContext();
        connectContext.setSessionVariable(new SessionVariable());
        connectContext.setThreadLocalInfo();
    }

    @AfterEach
    public void tearDown() {
        ConnectContext.remove();
    }

    // ==================== pure reconstructPartitionFields parity tests ====================

    @Test
    public void reconstructResolvedIdentityFieldsSucceedAndCarryTuples() {
        ExprId a = exprId("a");
        ExprId b = exprId("b");
        Map<String, ExprId> map = map("a", a, "b", b);
        List<IcebergPartitionField> out = new ArrayList<>();

        InsertPartitionFieldResult result = PhysicalIcebergMergeSink.reconstructPartitionFields(out,
                spec(9, field("identity", null, "a", "a", 1), field("identity", null, "b", "b", 2)),
                map);

        Assertions.assertTrue(result.success, "all-resolvable spec must succeed");
        Assertions.assertFalse(result.hasNonIdentity, "two identity transforms => no non-identity");
        Assertions.assertEquals(Integer.valueOf(9), result.partitionSpecId, "spec id must be carried");
        Assertions.assertEquals(
                ImmutableList.of(new IcebergPartitionField("identity", a, null, "a", 1),
                        new IcebergPartitionField("identity", b, null, "b", 2)),
                out,
                "fields must carry transform/exprId/param/name/sourceId verbatim and in order");
    }

    @Test
    public void reconstructCarriesNonIdentityTransformAndParam() {
        ExprId id = exprId("id");
        List<IcebergPartitionField> out = new ArrayList<>();

        InsertPartitionFieldResult result = PhysicalIcebergMergeSink.reconstructPartitionFields(out,
                spec(3, field("bucket[16]", 16, "id", "id_bucket", 5)), map("id", id));

        Assertions.assertTrue(result.success);
        Assertions.assertTrue(result.hasNonIdentity, "a non-identity transform must set hasNonIdentity");
        Assertions.assertEquals(Integer.valueOf(3), result.partitionSpecId);
        Assertions.assertEquals(
                ImmutableList.of(new IcebergPartitionField("bucket[16]", id, 16, "id_bucket", 5)), out,
                "transform param/name/sourceId must be carried verbatim from the connector field");
    }

    @Test
    public void reconstructNullSourceColumnNameHardFailsAndClears() {
        // PARITY-1a: a null source-column-name field hard-fails the whole spec; the already-added prior
        // field is cleared (so the result is EMPTY, not a partial list) and no IcebergPartitionField is
        // constructed with a null expr id (which would NPE).
        ExprId a = exprId("a");
        List<IcebergPartitionField> out = new ArrayList<>();

        InsertPartitionFieldResult result = PhysicalIcebergMergeSink.reconstructPartitionFields(out,
                spec(4, field("identity", null, "a", "a", 1), field("bucket[8]", 8, null, "x_bucket", 9)),
                map("a", a));

        Assertions.assertFalse(result.success, "an unresolvable (null-name) field must fail the spec");
        Assertions.assertTrue(out.isEmpty(), "the prior resolved field must be cleared on hard fail");
        Assertions.assertTrue(result.hasNonIdentity, "bucket[8] keeps hasNonIdentity true through the fail");
        Assertions.assertEquals(Integer.valueOf(4), result.partitionSpecId, "spec id is carried even on fail");
    }

    @Test
    public void reconstructUnresolvedExprIdHardFailsAndClears() {
        // PARITY-1b: a source column name that is not in the bound expr-id map hard-fails and clears.
        ExprId a = exprId("a");
        List<IcebergPartitionField> out = new ArrayList<>();

        InsertPartitionFieldResult result = PhysicalIcebergMergeSink.reconstructPartitionFields(out,
                spec(7, field("identity", null, "a", "a", 1), field("identity", null, "ghost", "ghost", 2)),
                map("a", a));

        Assertions.assertFalse(result.success, "a name absent from the expr-id map must fail the spec");
        Assertions.assertTrue(out.isEmpty(), "the prior resolved field must be cleared on hard fail");
        Assertions.assertFalse(result.hasNonIdentity, "all identity transforms => no non-identity");
        Assertions.assertEquals(Integer.valueOf(7), result.partitionSpecId);
    }

    @Test
    public void reconstructNonIdentityPrePassSeesFieldsAfterHardFail() {
        // PARITY-2 independence: the build loop short-circuits on field 0 (null name), but the
        // hasNonIdentity pre-pass over ALL fields must still see the bucket[16] at field 1. A mutation
        // computing hasNonIdentity inside the build loop would exit before field 1 and wrongly report false.
        List<IcebergPartitionField> out = new ArrayList<>();

        InsertPartitionFieldResult result = PhysicalIcebergMergeSink.reconstructPartitionFields(out,
                spec(2, field("identity", null, null, "a", 1), field("bucket[16]", 16, "b", "b_bucket", 2)),
                map("b", exprId("b")));

        Assertions.assertFalse(result.success);
        Assertions.assertTrue(out.isEmpty());
        Assertions.assertTrue(result.hasNonIdentity,
                "the non-identity pre-pass must scan all fields, not stop where the build loop hard-fails");
        Assertions.assertEquals(Integer.valueOf(2), result.partitionSpecId);
    }

    @Test
    public void reconstructNullSpecIsUnpartitioned() {
        // A null connector spec == unpartitioned target (legacy spec().isPartitioned() gate).
        List<IcebergPartitionField> out = new ArrayList<>();

        InsertPartitionFieldResult result = PhysicalIcebergMergeSink.reconstructPartitionFields(out, null,
                map("a", exprId("a")));

        Assertions.assertFalse(result.success);
        Assertions.assertFalse(result.hasNonIdentity);
        Assertions.assertNull(result.partitionSpecId, "unpartitioned target carries a null spec id");
        Assertions.assertTrue(out.isEmpty());
    }

    // ==================== getRequirePhysicalProperties dual-mode dispatch + gate ====================

    @Test
    public void postFlipMergeBuildsDistributionFromConnectorSpec() {
        // A PluginDrivenExternalTable must route through the connector branch and its write partitioning
        // must flow into the DistributionSpecMerge: one identity partition column 'id' resolved to the
        // child's id slot, insertRandom=false, spec id carried.
        Column id = new Column("id", PrimitiveType.INT);
        SlotReference idSlot = new SlotReference("id", IntegerType.INSTANCE);
        SlotReference opSlot = new SlotReference(MergeOperation.OPERATION_COLUMN, IntegerType.INSTANCE);
        SlotReference rowidSlot = new SlotReference(Column.ICEBERG_ROWID_COL, IntegerType.INSTANCE);

        PhysicalIcebergMergeSink<Plan> sink = pluginSink(
                spec(11, field("identity", null, "id", "id", 1)),
                ImmutableList.of(id),                                  // partition columns
                ImmutableList.of(id),                                  // visible cols
                ImmutableList.of(idSlot, opSlot, rowidSlot));          // child output

        PhysicalProperties props = sink.getRequirePhysicalProperties();

        Assertions.assertTrue(props.getDistributionSpec() instanceof DistributionSpecMerge,
                "a post-flip merge write must build a merge distribution from the connector spec");
        DistributionSpecMerge dist = (DistributionSpecMerge) props.getDistributionSpec();
        Assertions.assertFalse(dist.isInsertRandom(), "a resolvable connector partitioning must not be random");
        Assertions.assertEquals(Integer.valueOf(11), dist.getPartitionSpecId(), "connector spec id must be carried");
        Assertions.assertEquals(
                ImmutableList.of(new IcebergPartitionField("identity", idSlot.getExprId(), null, "id", 1)),
                dist.getInsertPartitionFields(),
                "the connector partition field must be reconstructed against the bound id slot");
        Assertions.assertEquals(ImmutableList.of(idSlot.getExprId()), dist.getInsertPartitionExprIds(),
                "the identity partition column must thread into the merge distribution insert-partition expr ids");
    }

    @Test
    public void mergePartitioningGateOffDoesNotConsultConnector() {
        // PARITY-3 (sink gate): with enableIcebergMergePartitioning off, the row-id hash path is taken
        // and the connector is never consulted (no getCatalog() -> getWritePartitioning()).
        connectContext.getSessionVariable().enableIcebergMergePartitioning = false;
        Column id = new Column("id", PrimitiveType.INT);
        SlotReference idSlot = new SlotReference("id", IntegerType.INSTANCE);
        SlotReference opSlot = new SlotReference(MergeOperation.OPERATION_COLUMN, IntegerType.INSTANCE);
        SlotReference rowidSlot = new SlotReference(Column.ICEBERG_ROWID_COL, IntegerType.INSTANCE);
        PluginDrivenExternalTable table = pluginTable(spec(11, field("identity", null, "id", "id", 1)),
                ImmutableList.of(id), true);
        PhysicalIcebergMergeSink<Plan> sink = sinkWith(table, ImmutableList.of(id),
                ImmutableList.of(idSlot, opSlot, rowidSlot));

        PhysicalProperties props = sink.getRequirePhysicalProperties();

        Assertions.assertTrue(props.getDistributionSpec() instanceof DistributionSpecHash,
                "gate off with a row id present must hash by the row id, not merge-distribute");
        DistributionSpecHash hash = (DistributionSpecHash) props.getDistributionSpec();
        Assertions.assertEquals(ImmutableList.of(rowidSlot.getExprId()), hash.getOrderedShuffledColumns(),
                "the row-id hash path must shuffle by the row-id slot");
        Mockito.verify(table, Mockito.never()).getCatalog();
    }

    @Test
    public void postFlipAbsentTableHandleDegradesToUnpartitioned() {
        // getRequirePhysicalProperties runs in CBO distribution derivation and must NEVER throw. If the
        // connector cannot resolve the target's write handle, the connector partitioning degrades to a random
        // (unpartitioned) merge distribution rather than an error. The provider here WOULD return a valid spec,
        // so this also pins that the absent-handle guard short-circuits before getWritePartitioning is trusted.
        Column id = new Column("id", PrimitiveType.INT);
        SlotReference idSlot = new SlotReference("id", IntegerType.INSTANCE);
        SlotReference opSlot = new SlotReference(MergeOperation.OPERATION_COLUMN, IntegerType.INSTANCE);
        SlotReference rowidSlot = new SlotReference(Column.ICEBERG_ROWID_COL, IntegerType.INSTANCE);
        // No partition columns -> the insertPartitionExprIds path also yields nothing, so the connector spec is
        // the only partitioning signal, and the absent handle must suppress it.
        PluginDrivenExternalTable table = pluginTable(spec(11, field("identity", null, "id", "id", 1)),
                ImmutableList.of(), false);
        PhysicalIcebergMergeSink<Plan> sink = sinkWith(table, ImmutableList.of(id),
                ImmutableList.of(idSlot, opSlot, rowidSlot));

        PhysicalProperties props = sink.getRequirePhysicalProperties();

        Assertions.assertTrue(props.getDistributionSpec() instanceof DistributionSpecMerge,
                "a merge write still produces a merge distribution");
        DistributionSpecMerge dist = (DistributionSpecMerge) props.getDistributionSpec();
        Assertions.assertTrue(dist.isInsertRandom(),
                "an unresolvable connector write handle must degrade to a random (unpartitioned) distribution");
        Assertions.assertNull(dist.getPartitionSpecId(), "no partitioning resolved -> null spec id");
        Assertions.assertTrue(dist.getInsertPartitionFields().isEmpty(), "no partition fields when unresolved");
    }

    // ==================== helpers ====================

    private static ExprId exprId(String name) {
        return new SlotReference(name, IntegerType.INSTANCE).getExprId();
    }

    private static Map<String, ExprId> map(Object... kv) {
        Map<String, ExprId> m = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (int i = 0; i < kv.length; i += 2) {
            m.put((String) kv[i], (ExprId) kv[i + 1]);
        }
        return m;
    }

    private static ConnectorWritePartitionField field(String transform, Integer param, String src,
            String name, int sourceId) {
        return new ConnectorWritePartitionField(transform, param, src, name, sourceId);
    }

    private static ConnectorWritePartitionSpec spec(int specId, ConnectorWritePartitionField... fields) {
        return new ConnectorWritePartitionSpec(specId, ImmutableList.copyOf(fields));
    }

    /** A plugin-driven table whose connector returns {@code writeSpec} from getWritePartitioning. */
    private static PluginDrivenExternalTable pluginTable(ConnectorWritePartitionSpec writeSpec,
            List<Column> partitionColumns, boolean handlePresent) {
        ConnectorWritePlanProvider provider = Mockito.mock(ConnectorWritePlanProvider.class);
        ConnectorTableHandle handle = Mockito.mock(ConnectorTableHandle.class);
        ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
        ConnectorSession session = Mockito.mock(ConnectorSession.class);
        Connector connector = Mockito.mock(Connector.class);
        Mockito.when(connector.getWritePlanProvider()).thenReturn(provider);
        Mockito.when(connector.getMetadata(Mockito.any())).thenReturn(metadata);
        Mockito.when(metadata.getTableHandle(Mockito.any(), Mockito.any(), Mockito.any()))
                .thenReturn(handlePresent ? Optional.of(handle) : Optional.empty());
        Mockito.when(provider.getWritePartitioning(Mockito.any(), Mockito.any())).thenReturn(writeSpec);
        PluginDrivenExternalCatalog catalog = Mockito.mock(PluginDrivenExternalCatalog.class);
        Mockito.when(catalog.getConnector()).thenReturn(connector);
        Mockito.when(catalog.buildConnectorSession()).thenReturn(session);
        PluginDrivenExternalTable table = Mockito.mock(PluginDrivenExternalTable.class);
        Mockito.when(table.getCatalog()).thenReturn(catalog);
        Mockito.when(table.getPartitionColumns(Mockito.any())).thenReturn(partitionColumns);
        Mockito.when(table.getRemoteDbName()).thenReturn("db");
        Mockito.when(table.getRemoteName()).thenReturn("t");
        return table;
    }

    private static PhysicalIcebergMergeSink<Plan> pluginSink(ConnectorWritePartitionSpec writeSpec,
            List<Column> partitionColumns, List<Column> cols, List<Slot> childOutput) {
        return sinkWith(pluginTable(writeSpec, partitionColumns, true), cols, childOutput);
    }

    /**
     * Builds a {@link PhysicalIcebergMergeSink} exercising only {@code getRequirePhysicalProperties()}.
     * CALLS_REAL_METHODS skips the heavyweight ctor and injects the three read fields ({@code targetTable},
     * {@code cols}, and the single child via {@code children}), mirroring {@code PhysicalConnectorTableSinkTest}.
     */
    private static PhysicalIcebergMergeSink<Plan> sinkWith(PluginDrivenExternalTable table,
            List<Column> cols, List<Slot> childOutput) {
        Plan child = Mockito.mock(Plan.class);
        Mockito.when(child.getOutput()).thenReturn(childOutput);
        @SuppressWarnings("unchecked")
        PhysicalIcebergMergeSink<Plan> sink =
                Mockito.mock(PhysicalIcebergMergeSink.class, Mockito.CALLS_REAL_METHODS);
        Deencapsulation.setField(sink, "targetTable", table);
        Deencapsulation.setField(sink, "cols", cols);
        Deencapsulation.setField(sink, "children", ImmutableList.of(child));
        return sink;
    }
}
