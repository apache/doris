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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.datasource.plugin.PluginDrivenExternalTable;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Tests for {@link BindSink#selectConnectorSinkBindColumns} — the bind-time column selection for the
 * generic connector table sink (FIX-BIND-STATIC-PARTITION, P0-3).
 *
 * <p>Root cause this guards: before the fix, the no-column-list path bound the full base schema
 * (including partition columns), so {@code INSERT INTO mc PARTITION(pt='x') SELECT <non-partition cols>}
 * produced more bound columns than the query output and threw "insert into cols should be corresponding
 * to the query output" at bind. The static partition columns carry their value via the static partition
 * spec (not the query), so they must be excluded from the bound columns — mirroring legacy
 * {@code bindMaxComputeTableSink}.</p>
 */
public class BindConnectorSinkStaticPartitionTest {

    private static final Column ID = new Column("id", PrimitiveType.INT);
    private static final Column VAL = new Column("val", PrimitiveType.INT);
    private static final Column DS = new Column("ds", PrimitiveType.INT);
    private static final Column REGION = new Column("region", PrimitiveType.INT);
    // Base schema appends partition columns after the data columns (as the connector reports it).
    private static final List<Column> BASE_SCHEMA = ImmutableList.of(ID, VAL, DS, REGION);

    private static PluginDrivenExternalTable partitionedTable() {
        PluginDrivenExternalTable table = Mockito.mock(PluginDrivenExternalTable.class);
        Mockito.when(table.getBaseSchema(true)).thenReturn(BASE_SCHEMA);
        stubWriteSchemaSnapshot(table, BASE_SCHEMA, ImmutableList.of(DS, REGION));
        // Model ExternalTable.getColumn, which resolves case-INSENSITIVELY (equalsIgnoreCase) for every
        // external table. Stubbing only the exact spelling would hide the very behavior under test.
        Mockito.when(table.getColumn(Mockito.anyString())).thenAnswer(inv -> {
            String wanted = inv.getArgument(0);
            return BASE_SCHEMA.stream()
                    .filter(c -> c.getName().equalsIgnoreCase(wanted))
                    .findFirst().orElse(null);
        });
        return table;
    }

    /** A {@code PARTITION(...)} spec; values are irrelevant to name canonicalization. */
    private static Map<String, Expression> partitionSpec(String... colNames) {
        Map<String, Expression> spec = Maps.newLinkedHashMap();
        for (String name : colNames) {
            spec.put(name, new StringLiteral("v"));
        }
        return spec;
    }

    private static PluginDrivenExternalTable tableWithRewriteColumns(boolean includeLineage) {
        Column rowLocator = new Column("__DORIS_ICEBERG_ROWID_COL__", PrimitiveType.STRING);
        rowLocator.setIsVisible(false);
        ImmutableList.Builder<Column> schemaBuilder = ImmutableList.builder();
        schemaBuilder.add(ID, VAL);
        if (includeLineage) {
            Column rowId = new Column("_row_id", PrimitiveType.BIGINT);
            rowId.setIsVisible(false);
            rowId.setReservedPassthrough(true);
            Column sequenceNumber = new Column("_last_updated_sequence_number", PrimitiveType.BIGINT);
            sequenceNumber.setIsVisible(false);
            sequenceNumber.setReservedPassthrough(true);
            schemaBuilder.add(rowId, sequenceNumber);
        }
        schemaBuilder.add(rowLocator);
        List<Column> schema = schemaBuilder.build();
        PluginDrivenExternalTable table = Mockito.mock(PluginDrivenExternalTable.class);
        Mockito.when(table.getBaseSchema(true)).thenReturn(schema);
        stubWriteSchemaSnapshot(table, schema, Collections.emptyList());
        for (Column c : schema) {
            Mockito.when(table.getColumn(c.getName())).thenReturn(c);
        }
        return table;
    }

    private static void stubWriteSchemaSnapshot(PluginDrivenExternalTable table,
            List<Column> schema, List<Column> partitionColumns) {
        PluginDrivenExternalTable.WriteSchemaSnapshot snapshot =
                Mockito.mock(PluginDrivenExternalTable.WriteSchemaSnapshot.class);
        Mockito.when(snapshot.getFullSchema()).thenReturn(schema);
        Mockito.when(snapshot.getPartitionColumns()).thenReturn(partitionColumns);
        Mockito.when(table.getWriteSchemaSnapshot()).thenReturn(snapshot);
    }

    private static List<String> names(List<Column> columns) {
        return columns.stream().map(Column::getName).collect(Collectors.toList());
    }

    /**
     * No column list, all-static {@code PARTITION(ds='x', region='y')}: both partition columns are
     * statically specified and must be excluded from the bound columns, leaving only the data columns
     * so the count matches the query output (the original blocker).
     */
    @Test
    public void noColumnListAllStaticExcludesPartitionColumns() {
        List<Column> bound = BindSink.selectConnectorSinkBindColumns(
                partitionedTable(), Collections.emptyList(), ImmutableSet.of("ds", "region"), false);
        Assertions.assertEquals(ImmutableList.of("id", "val"), names(bound),
                "static partition columns must be excluded from the bound columns");
    }

    /**
     * No column list, partial-static {@code PARTITION(ds='x') SELECT id, val, region}: only the static
     * 'ds' is excluded; the dynamic 'region' stays (its value comes from the query).
     */
    @Test
    public void noColumnListPartialStaticExcludesOnlyStaticColumn() {
        List<Column> bound = BindSink.selectConnectorSinkBindColumns(
                partitionedTable(), Collections.emptyList(), ImmutableSet.of("ds"), false);
        Assertions.assertEquals(ImmutableList.of("id", "val", "region"), names(bound),
                "only the statically-specified partition column must be excluded");
    }

    /**
     * No column list, no static partition (pure dynamic, e.g. {@code INSERT ... SELECT id,val,ds,region}):
     * nothing is excluded — the full base schema is bound, so the existing dynamic/JDBC path is
     * unchanged.
     */
    @Test
    public void noColumnListNoStaticPartitionBindsFullSchema() {
        List<Column> bound = BindSink.selectConnectorSinkBindColumns(
                partitionedTable(), Collections.emptyList(), Collections.emptySet(), false);
        Assertions.assertEquals(ImmutableList.of("id", "val", "ds", "region"), names(bound),
                "without a static partition spec the full base schema is bound");
    }

    /**
     * Explicit column list: bound columns follow the user-specified list verbatim, in user order. Columns
     * that do not collide with the static partition spec are bound unchanged (the colliding case is
     * rejected — see {@link #explicitColumnListNamingStaticPartitionColumnThrows}).
     */
    @Test
    public void explicitColumnListUsesUserColumnsVerbatim() {
        List<Column> bound = BindSink.selectConnectorSinkBindColumns(
                partitionedTable(), ImmutableList.of("val", "id"), ImmutableSet.of("ds"), false);
        Assertions.assertEquals(ImmutableList.of("val", "id"), names(bound),
                "explicit column list is bound in user order");
    }

    @Test
    public void explicitColumnListUsesLatestTargetSchemaInsteadOfAmbientSourceSnapshot() {
        Column oldName = new Column("old_name", PrimitiveType.INT);
        Column newName = new Column("new_name", PrimitiveType.INT);
        PluginDrivenExternalTable table = Mockito.mock(PluginDrivenExternalTable.class);
        // Model a statement whose historical source is the only pinned relation: the no-arg table lookup
        // therefore sees old_name, while an explicit empty pin means the latest write-target schema.
        Mockito.when(table.getColumn("id")).thenReturn(ID);
        Mockito.when(table.getColumn("new_name")).thenReturn(null);
        stubWriteSchemaSnapshot(table, ImmutableList.of(ID, newName), Collections.emptyList());
        Mockito.when(table.getBaseSchema(true)).thenReturn(ImmutableList.of(ID, oldName));

        List<Column> bound = BindSink.selectConnectorSinkBindColumns(
                table, ImmutableList.of("id", "new_name"), Collections.emptySet(), false);
        Assertions.assertEquals(ImmutableList.of("id", "new_name"), names(bound),
                "a historical source pin must not replace the latest write-target schema");
    }

    @Test
    public void explicitColumnListLoadsLatestTargetSchemaOnce() {
        PluginDrivenExternalTable table = partitionedTable();

        List<Column> bound = BindSink.selectConnectorSinkBindColumns(
                table, ImmutableList.of("id", "val", "region"), Collections.emptySet(), false);

        Assertions.assertEquals(ImmutableList.of("id", "val", "region"), names(bound));
        Mockito.verify(table, Mockito.times(1)).getWriteSchemaSnapshot();
    }

    /**
     * A column whose value comes from the PARTITION clause must not ALSO be listed in the insert column
     * list. Encodes WHY: the materialize block re-projects the PARTITION literal over that column, so the
     * value the query supplies for it would be silently discarded — the user would see neither their value
     * nor an error. Upstream #65991 case 5.2.
     */
    @Test
    public void explicitColumnListNamingStaticPartitionColumnThrows() {
        AnalysisException ex = Assertions.assertThrows(AnalysisException.class, () ->
                BindSink.selectConnectorSinkBindColumns(
                        partitionedTable(), ImmutableList.of("id", "val", "ds"),
                        ImmutableSet.of("ds"), false));
        Assertions.assertTrue(ex.getMessage().contains("is a static partition column"),
                "expected the static-partition-column reject, got: " + ex.getMessage());
        Assertions.assertTrue(ex.getMessage().contains("ds"), "error must name the offending column");
    }

    /**
     * Same reject when the insert column list spells the column in a different case than the PARTITION
     * clause. Reachable because {@code canonicalStaticPartitionColNames} has already folded the spec name
     * to the schema name, so both sides compare as schema names. Upstream #65991 case 5.5.
     */
    @Test
    public void explicitColumnListNamingStaticPartitionColumnCaseInsensitiveThrows() {
        // PARTITION(DS='x') canonicalizes to "ds"; the column list spells it "Ds".
        Set<String> canonical = BindSink.canonicalStaticPartitionColNames(
                partitionedTable(), partitionSpec("DS"));
        AnalysisException ex = Assertions.assertThrows(AnalysisException.class, () ->
                BindSink.selectConnectorSinkBindColumns(
                        partitionedTable(), ImmutableList.of("id", "val", "Ds"), canonical, false));
        Assertions.assertTrue(ex.getMessage().contains("is a static partition column"),
                "case-mismatched column list must be rejected too, got: " + ex.getMessage());
    }

    /**
     * {@code PARTITION(DS='x')} on a column stored as {@code ds} resolves to the schema name, so the
     * exclusion filter below actually drops it. Encodes WHY: without the fold the column stays bound, the
     * bound-column count exceeds the query output, and the user gets a bogus "insert into cols should be
     * corresponding to the query output" instead of a working insert. Upstream #65991 success case
     * {@code PARTITION(TS_DATE=...)}.
     */
    @Test
    public void canonicalizationFoldsCaseSoStaticColumnIsExcluded() {
        Set<String> canonical = BindSink.canonicalStaticPartitionColNames(
                partitionedTable(), partitionSpec("DS"));
        Assertions.assertEquals(ImmutableSet.of("ds"), canonical,
                "the user-typed name must resolve to the schema name");
        List<Column> bound = BindSink.selectConnectorSinkBindColumns(
                partitionedTable(), Collections.emptyList(), canonical, false);
        Assertions.assertEquals(ImmutableList.of("id", "val", "region"), names(bound),
                "a case-mismatched static partition column must still be excluded");
    }

    /**
     * A name that matches no table column is kept VERBATIM. Encodes WHY: on iceberg the PARTITION clause
     * names a partition FIELD (e.g. {@code category_bucket} for {@code bucket(4, category)}), which is not
     * a table column and which the connector has already validated — fe-core must not rewrite it.
     */
    @Test
    public void canonicalizationKeepsUnresolvableNameVerbatim() {
        Assertions.assertEquals(ImmutableSet.of("ds_bucket"),
                BindSink.canonicalStaticPartitionColNames(partitionedTable(), partitionSpec("ds_bucket")),
                "a partition-field name that is not a table column must pass through unchanged");
    }

    /**
     * The same column named twice with different casing is a duplicate. Encodes WHY: {@code columnToOutput}
     * is a CASE_INSENSITIVE_ORDER map, so both entries collapse onto one key and the second PARTITION value
     * would silently overwrite the first — the row would land in a partition the user never asked for.
     * Upstream #65991 case 5.6.
     */
    @Test
    public void canonicalizationRejectsCaseInsensitiveDuplicate() {
        AnalysisException ex = Assertions.assertThrows(AnalysisException.class, () ->
                BindSink.canonicalStaticPartitionColNames(partitionedTable(), partitionSpec("ds", "DS")));
        Assertions.assertTrue(ex.getMessage().contains("Duplicate partition column"),
                "expected the duplicate reject, got: " + ex.getMessage());
    }

    /**
     * Two DISTINCT partition columns are not a duplicate — guards the reject above from over-firing.
     */
    @Test
    public void canonicalizationAcceptsDistinctColumns() {
        Assertions.assertEquals(ImmutableSet.of("ds", "region"),
                BindSink.canonicalStaticPartitionColNames(partitionedTable(), partitionSpec("DS", "Region")),
                "distinct partition columns must both survive canonicalization");
    }

    /**
     * No static partition spec: canonicalization is a no-op, so a plain {@code INSERT ... SELECT} is
     * unchanged for every connector.
     */
    @Test
    public void canonicalizationOfEmptySpecIsEmpty() {
        Assertions.assertTrue(
                BindSink.canonicalStaticPartitionColNames(partitionedTable(), Collections.emptyMap()).isEmpty());
        Assertions.assertTrue(
                BindSink.canonicalStaticPartitionColNames(partitionedTable(), null).isEmpty());
    }

    /**
     * Explicit column list naming an unknown column fails loud with a clear message (unchanged behavior).
     */
    @Test
    public void explicitColumnListUnknownColumnThrows() {
        AnalysisException ex = Assertions.assertThrows(AnalysisException.class, () ->
                BindSink.selectConnectorSinkBindColumns(
                        partitionedTable(), ImmutableList.of("nope"), Collections.emptySet(), false));
        Assertions.assertTrue(ex.getMessage().contains("nope"), "error must name the missing column");
    }

    @Test
    public void pinnedDataSchemaStillRejectsExplicitInvisibleColumn() {
        PluginDrivenExternalTable table = tableWithRowLineage();
        AnalysisException ex = Assertions.assertThrows(AnalysisException.class, () ->
                BindSink.selectConnectorSinkBindColumns(
                        table, ImmutableList.of(ID, VAL), ImmutableList.of("_row_id"),
                        Collections.emptySet(), false));
        Assertions.assertEquals(
                "Cannot specify invisible column '_row_id' in INSERT statement", ex.getMessage());
    }

    /**
     * No column list, ordinary write (not a rewrite): invisible columns (e.g. iceberg v3 row-lineage
     * {@code _row_id} / {@code _last_updated_sequence_number}) must be EXCLUDED from the default bound
     * columns — the user never supplies their values, so including them would make the bound-column
     * count exceed the query output and throw "insert into cols should be corresponding to the query
     * output". Guards the v3 row-lineage INSERT regression (test_iceberg_v2_to_v3_doris_spark_compare).
     */
    @Test
    public void noColumnListOrdinaryWriteExcludesInvisibleColumns() {
        List<Column> bound = BindSink.selectConnectorSinkBindColumns(
                tableWithRewriteColumns(true), Collections.emptyList(), Collections.emptySet(), false);
        Assertions.assertEquals(ImmutableList.of("id", "val"), names(bound),
                "invisible row-lineage columns must be excluded from an ordinary write target");
    }

    /**
     * A v2 rewrite under show-hidden carries the request-scoped row locator in the table's full schema, but
     * the rewrite sink has no physical field for it.
     */
    @Test
    public void noColumnListV2RewriteExcludesRequestScopedRowLocator() {
        List<Column> bound = BindSink.selectConnectorSinkBindColumns(
                tableWithRewriteColumns(false), Collections.emptyList(), Collections.emptySet(), true);
        Assertions.assertEquals(ImmutableList.of("id", "val"), names(bound),
                "a v2 rewrite must not emit the request-scoped row locator");
    }

    /**
     * A v3 rewrite preserves persistent lineage fields, while excluding the unrelated request-scoped locator.
     */
    @Test
    public void noColumnListV3RewriteRetainsLineageButExcludesRequestScopedRowLocator() {
        List<Column> bound = BindSink.selectConnectorSinkBindColumns(
                tableWithRewriteColumns(true), Collections.emptyList(), Collections.emptySet(), true);
        Assertions.assertEquals(ImmutableList.of("id", "val", "_row_id", "_last_updated_sequence_number"),
                names(bound), "a v3 rewrite must retain only persistent lineage metadata");
    }

    @Test
    public void rewriteSourceOutputExcludesRequestScopedRowLocator() {
        NamedExpression id = namedExpression("id");
        NamedExpression val = namedExpression("val");
        NamedExpression rowId = namedExpression("_row_id");
        NamedExpression sequenceNumber = namedExpression("_last_updated_sequence_number");
        NamedExpression locator = namedExpression("__DORIS_ICEBERG_ROWID_COL__");

        List<NamedExpression> v2Selected = BindSink.selectConnectorRewriteOutputs(
                ImmutableList.of(ID, VAL), ImmutableList.of(id, val, locator));
        List<Column> v3WriteSchema = BindSink.selectConnectorSinkBindColumns(
                tableWithRewriteColumns(true), Collections.emptyList(), Collections.emptySet(), true);
        List<NamedExpression> v3Selected = BindSink.selectConnectorRewriteOutputs(
                v3WriteSchema, ImmutableList.of(id, val, rowId, sequenceNumber, locator));

        Assertions.assertEquals(ImmutableList.of(id, val), v2Selected);
        Assertions.assertEquals(ImmutableList.of(id, val, rowId, sequenceNumber), v3Selected,
                "v2/v3 rewrite input must use the same physical column set as its sink schema");
    }

    private static NamedExpression namedExpression(String name) {
        NamedExpression expression = Mockito.mock(NamedExpression.class);
        Mockito.when(expression.getName()).thenReturn(name);
        return expression;
    }
}
