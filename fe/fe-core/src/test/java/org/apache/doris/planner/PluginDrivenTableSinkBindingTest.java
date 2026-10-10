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

package org.apache.doris.planner;

import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.connector.ConnectorSessionBuilder;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.handle.ConnectorWriteHandle;
import org.apache.doris.connector.spi.mvcc.ConnectorMvccSnapshot;
import org.apache.doris.connector.spi.write.ConnectorSinkPlan;
import org.apache.doris.connector.spi.write.ConnectorWritePlanProvider;
import org.apache.doris.datasource.mvcc.MvccUtil;
import org.apache.doris.datasource.mvcc.PluginDrivenMvccSnapshot;
import org.apache.doris.datasource.plugin.PluginDrivenExternalTable;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DecimalV3Literal;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.trees.plans.commands.insert.PluginDrivenInsertCommandContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TDataSink;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Binding-context consumption test (P4-T06a §4.2 / gaps G4+G5).
 *
 * <p>After the cutover, INSERT OVERWRITE and INSERT ... PARTITION(col=val) against a
 * plugin-driven MaxCompute table must keep working. The commands carry the overwrite
 * flag and the static partition spec on a {@link PluginDrivenInsertCommandContext};
 * this test pins the <em>consumption</em> seam — that
 * {@link PluginDrivenTableSink#bindDataSink} forwards both into the
 * {@link ConnectorWriteHandle} handed to the connector's
 * {@link ConnectorWritePlanProvider#planWrite}. If this regresses, INSERT OVERWRITE
 * silently degrades to append and partition pinning is lost.</p>
 *
 * <p>(The production side — the command populating the context from the unbound sink —
 * is covered by post-cutover manual smoke, per the T06a test-scope decision.)</p>
 */
public class PluginDrivenTableSinkBindingTest {

    @Test
    public void overwriteAndStaticPartitionFlowToWriteHandle() throws AnalysisException {
        RecordingWritePlanProvider provider = new RecordingWritePlanProvider();
        PluginDrivenTableSink sink = newPlanProviderSink(provider);

        PluginDrivenInsertCommandContext ctx = new PluginDrivenInsertCommandContext();
        ctx.setOverwrite(true);
        ctx.setStaticPartitionSpecFromExpressions(Collections.singletonMap("pt", new StringLiteral("20240101")));

        sink.bindDataSink(Optional.of(ctx));

        ConnectorWriteHandle handle = provider.capturedHandle;
        Assertions.assertNotNull(handle, "planWrite must be invoked with a bound write handle");
        Assertions.assertTrue(handle.isOverwrite(),
                "INSERT OVERWRITE must propagate ctx.isOverwrite()=true to the connector write handle");
        Assertions.assertEquals(Collections.singletonMap("pt", "20240101"), handle.getStaticPartitionSpec(),
                "PARTITION(col=val) must propagate the static partition spec to the write handle");
    }

    @Test
    public void staticPartitionValuesCastToTheirColumnTypesFlowToWriteHandle() throws AnalysisException {
        // The rows carry each PARTITION literal cast to its column type; a connector that names the partition
        // by value (Paimon's static overwrite) must get that value, not the literal as written. MUTATION:
        // returning the spec as written from getCastStaticPartitionSpec -> red.
        RecordingWritePlanProvider provider = new RecordingWritePlanProvider();
        PluginDrivenTableSink sink = newPlanProviderSink(provider);
        PluginDrivenInsertCommandContext ctx = new PluginDrivenInsertCommandContext();
        Map<String, Expression> partition = new LinkedHashMap<>();
        partition.put("P", BooleanLiteral.TRUE);
        partition.put("dt", new IntegerLiteral(20240101));
        partition.put("amount", new DecimalV3Literal(new BigDecimal("1.5")));
        partition.put("region", new NullLiteral());
        partition.put("p_bucket", new StringLiteral("x"));
        ctx.setStaticPartitionSpecFromExpressions(partition);
        ctx.setBoundTargetSchema(Arrays.asList(new Column("p", Type.INT), new Column("dt", Type.DATEV2),
                new Column("amount", ScalarType.createDecimalV3Type(10, 2)), new Column("region", Type.STRING)));

        sink.bindDataSink(Optional.of(ctx));

        ConnectorWriteHandle handle = provider.capturedHandle;
        Assertions.assertEquals("20240101", handle.getStaticPartitionSpec().get("dt"));
        Map<String, String> expected = new LinkedHashMap<>();
        expected.put("P", "1");
        expected.put("dt", "2024-01-01");
        expected.put("amount", "1.50");
        // A key that names no column, such as an Iceberg partition field, keeps its literal; SQL NULL stays a null key.
        expected.put("p_bucket", "x");
        Assertions.assertEquals(expected, handle.getCastStaticPartitionSpec());
        Assertions.assertEquals(Collections.singleton("region"), handle.getStaticPartitionNullKeys());
    }

    @Test
    public void staticPartitionDateTimeValuesCastInTheSessionZone() throws AnalysisException {
        // A DATETIMEV2 value stays local time in the session zone, and a TIMESTAMPTZ value becomes the instant
        // in UTC with its offset; Paimon moves either one to the zone its overwrite parser uses.
        ConnectContext previous = ConnectContext.get();
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().setTimeZone("Asia/Shanghai");
        context.setThreadLocalInfo();
        try {
            RecordingWritePlanProvider provider = new RecordingWritePlanProvider();
            PluginDrivenTableSink sink = newPlanProviderSink(provider);
            PluginDrivenInsertCommandContext ctx = new PluginDrivenInsertCommandContext();
            Map<String, Expression> partition = new LinkedHashMap<>();
            partition.put("ts", new StringLiteral("2024-01-15 08:30:45.123456"));
            partition.put("tz", new StringLiteral("2024-01-15 08:30:45.123456"));
            ctx.setStaticPartitionSpecFromExpressions(partition);
            ctx.setBoundTargetSchema(Arrays.asList(new Column("ts", ScalarType.createDatetimeV2Type(6)),
                    new Column("tz", ScalarType.createTimeStampTzType(6))));

            sink.bindDataSink(Optional.of(ctx));

            Map<String, String> expected = new LinkedHashMap<>();
            expected.put("ts", "2024-01-15 08:30:45.123456");
            expected.put("tz", "2024-01-15 00:30:45.123456+00:00");
            Assertions.assertEquals(expected, provider.capturedHandle.getCastStaticPartitionSpec());
        } finally {
            if (previous == null) {
                ConnectContext.remove();
            } else {
                previous.setThreadLocalInfo();
            }
        }
    }

    @Test
    public void staticPartitionStringValueIsCutLikeTheWrittenRows() throws AnalysisException {
        // BindSink writes a string into a shorter CHAR / VARCHAR column cut to the column length in code points,
        // not cast: a cast rejects the CHAR value and splits the emoji's surrogate pair, so the overwrite would
        // fail or name another partition. MUTATION: casting string values like the others -> red.
        ConnectContext previous = ConnectContext.get();
        ConnectContext context = new ConnectContext();
        context.setThreadLocalInfo();
        try {
            // One code point, two UTF-16 units.
            String emoji = new String(Character.toChars(0x1F600));
            Map<String, Expression> partition = new LinkedHashMap<>();
            partition.put("c", new VarcharLiteral("abcd"));
            partition.put("v", new VarcharLiteral(emoji + "x"));
            partition.put("s", new VarcharLiteral("abcd"));
            List<Column> schema = Arrays.asList(new Column("c", ScalarType.createCharType(2)),
                    new Column("v", ScalarType.createVarcharType(1)), new Column("s", Type.STRING));

            Map<String, String> expected = new LinkedHashMap<>();
            expected.put("c", "ab");
            expected.put("v", emoji);
            expected.put("s", "abcd");
            Assertions.assertEquals(expected, castStaticPartitionSpec(partition, schema));

            // A session that does not truncate writes the strings as they are, and they stay so here.
            context.getSessionVariable().enableInsertValueAutoCast = false;
            Map<String, String> asWritten = new LinkedHashMap<>();
            asWritten.put("c", "abcd");
            asWritten.put("v", emoji + "x");
            asWritten.put("s", "abcd");
            Assertions.assertEquals(asWritten, castStaticPartitionSpec(partition, schema));
        } finally {
            if (previous == null) {
                ConnectContext.remove();
            } else {
                previous.setThreadLocalInfo();
            }
        }
    }

    @Test
    public void staticPartitionValueThatDoesNotFitItsTypeFailsOnlyWhenCast() throws AnalysisException {
        // Only a connector that reads the cast values may fail on them; the others keep reading the literal.
        RecordingWritePlanProvider provider = new RecordingWritePlanProvider();
        PluginDrivenTableSink sink = newPlanProviderSink(provider);
        PluginDrivenInsertCommandContext ctx = new PluginDrivenInsertCommandContext();
        ctx.setStaticPartitionSpecFromExpressions(Collections.singletonMap("dt", new StringLiteral("2024-13-45")));
        ctx.setBoundTargetSchema(Collections.singletonList(new Column("dt", Type.DATEV2)));

        sink.bindDataSink(Optional.of(ctx));

        ConnectorWriteHandle handle = provider.capturedHandle;
        Assertions.assertEquals(Collections.singletonMap("dt", "2024-13-45"), handle.getStaticPartitionSpec());
        Assertions.assertThrows(RuntimeException.class, handle::getCastStaticPartitionSpec);
    }

    @Test
    public void absentContextDefaultsToNonOverwriteEmptySpec() throws AnalysisException {
        RecordingWritePlanProvider provider = new RecordingWritePlanProvider();
        PluginDrivenTableSink sink = newPlanProviderSink(provider);

        sink.bindDataSink(Optional.empty());

        ConnectorWriteHandle handle = provider.capturedHandle;
        Assertions.assertNotNull(handle);
        Assertions.assertFalse(handle.isOverwrite(),
                "a plain INSERT must default the connector write handle to non-overwrite");
        Assertions.assertTrue(handle.getStaticPartitionSpec().isEmpty(),
                "a plain INSERT must pass an empty static partition spec");
    }

    @Test
    public void branchTargetUsesItsExactVersionAwareSnapshotPin() throws AnalysisException {
        RecordingWritePlanProvider provider = new RecordingWritePlanProvider();
        ConnectorSession session = ConnectorSessionBuilder.create().withCatalogName("iceberg").build();
        ConnectorTableHandle baseHandle = Mockito.mock(ConnectorTableHandle.class);
        ConnectorTableHandle pinnedHandle = Mockito.mock(ConnectorTableHandle.class);
        ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
        PluginDrivenExternalTable table = Mockito.mock(PluginDrivenExternalTable.class);
        ConnectorMvccSnapshot connectorSnapshot = Mockito.mock(ConnectorMvccSnapshot.class);
        PluginDrivenMvccSnapshot snapshot = new PluginDrivenMvccSnapshot(
                connectorSnapshot, Collections.emptyMap(), Collections.emptyMap());
        Mockito.when(metadata.applySnapshot(session, baseHandle, connectorSnapshot)).thenReturn(pinnedHandle);
        PluginDrivenTableSink sink = new PluginDrivenTableSink(table, provider, session, baseHandle,
                Collections.emptyList(), null, null, false, metadata);
        PluginDrivenInsertCommandContext ctx = new PluginDrivenInsertCommandContext();
        ctx.setBranchName(Optional.of("audit"));

        try (MockedStatic<MvccUtil> mvcc = Mockito.mockStatic(MvccUtil.class)) {
            mvcc.when(() -> MvccUtil.getSnapshotFromContext(
                    Mockito.eq(table), Mockito.eq(Optional.empty()), Mockito.any()))
                    .thenAnswer(invocation -> {
                        Optional<TableScanParams> selector = invocation.getArgument(2);
                        Assertions.assertEquals(TableScanParams.BRANCH,
                                selector.orElseThrow().getParamType());
                        Assertions.assertEquals(Collections.singletonList("audit"),
                                selector.orElseThrow().getListParams());
                        return Optional.of(snapshot);
                    });
            sink.bindDataSink(Optional.of(ctx));
        }

        Assertions.assertSame(pinnedHandle, provider.capturedHandle.getTableHandle());
    }

    private static Map<String, String> castStaticPartitionSpec(Map<String, Expression> partition,
            List<Column> boundTargetSchema) throws AnalysisException {
        RecordingWritePlanProvider provider = new RecordingWritePlanProvider();
        PluginDrivenTableSink sink = newPlanProviderSink(provider);
        PluginDrivenInsertCommandContext ctx = new PluginDrivenInsertCommandContext();
        ctx.setStaticPartitionSpecFromExpressions(partition);
        ctx.setBoundTargetSchema(boundTargetSchema);
        sink.bindDataSink(Optional.of(ctx));
        return provider.capturedHandle.getCastStaticPartitionSpec();
    }

    private static PluginDrivenTableSink newPlanProviderSink(ConnectorWritePlanProvider provider) {
        ConnectorSession session = ConnectorSessionBuilder.create().withCatalogName("mc_cat").build();
        ConnectorTableHandle tableHandle = new ConnectorTableHandle() { };
        // targetTable is unused on the plan-provider bind path; pass null to avoid building a
        // full PluginDrivenExternalTable (which would require a catalog + database).
        return new PluginDrivenTableSink(null, provider, session, tableHandle, Collections.emptyList());
    }

    /** Records the bound {@link ConnectorWriteHandle} that the sink hands to {@code planWrite}. */
    private static final class RecordingWritePlanProvider implements ConnectorWritePlanProvider {
        private ConnectorWriteHandle capturedHandle;

        @Override
        public ConnectorSinkPlan planWrite(ConnectorSession session, ConnectorWriteHandle handle) {
            this.capturedHandle = handle;
            return new ConnectorSinkPlan(new TDataSink());
        }
    }
}
