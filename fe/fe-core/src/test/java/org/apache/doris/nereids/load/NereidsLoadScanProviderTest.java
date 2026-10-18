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

package org.apache.doris.nereids.load;

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.property.fileformat.ArrowFileFormatProperties;
import org.apache.doris.datasource.property.fileformat.CsvFileFormatProperties;
import org.apache.doris.datasource.property.fileformat.FileFormatProperties;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TFileCompressType;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TStreamLoadPutRequest;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.thrift.TUniqueKeyUpdateMode;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class NereidsLoadScanProviderTest {

    @Test
    public void testArrowStreamLoadKeepsExplicitColumnCase() throws Exception {
        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        request.setLoadId(new TUniqueId(1, 2));
        request.setTxnId(3);
        request.setFileType(TFileType.FILE_STREAM);
        request.setFormatType(TFileFormatType.FORMAT_ARROW);
        request.setCompressType(TFileCompressType.UNKNOWN);
        request.setColumns("time,securityid,EV");

        NereidsStreamLoadTask task = NereidsStreamLoadTask.fromTStreamLoadPutRequest(request);
        NereidsDataDescription dataDescription = new NereidsDataDescription("t_upper", task);

        Assertions.assertEquals(Lists.newArrayList("time", "securityid", "EV"),
                dataDescription.getFileFieldNames());
    }

    @Test
    public void testArrowBrokerLoadKeepsExplicitColumnCaseForNonLowercaseFormat() {
        NereidsDataDescription dataDescription = new NereidsDataDescription("t_upper", null,
                Lists.newArrayList("dummy"), Lists.newArrayList("time", "securityid", "EV"),
                null, null, "ARROW", null, null, false, Collections.emptyList(), null, null,
                LoadTask.MergeType.APPEND, null, null, null);

        Assertions.assertEquals(Lists.newArrayList("time", "securityid", "EV"),
                dataDescription.getFileFieldNames());
    }

    @Test
    public void testArrowBrokerLoadLowersColumnsFromPath() {
        NereidsDataDescription dataDescription = new NereidsDataDescription("t_upper", null,
                Lists.newArrayList("dummy"), Lists.newArrayList("time", "securityid", "EV"),
                null, null, "ARROW", null, Lists.newArrayList("DT"), false, Collections.emptyList(), null, null,
                LoadTask.MergeType.APPEND, null, null, null);

        Assertions.assertEquals(Lists.newArrayList("time", "securityid", "EV"),
                dataDescription.getFileFieldNames());
        Assertions.assertEquals(Lists.newArrayList("dt"), dataDescription.getColumnsFromPath());
    }

    @Test
    public void testArrowAutoGeneratedSlotsKeepUppercaseTableColumn() throws Exception {
        OlapTable table = mockTable();
        NereidsParamCreateContext context = createArrowLoadContext(table, ImmutableList.of());

        assertSlot(context, "time", PrimitiveType.DATETIME);
        assertSlot(context, "securityid", PrimitiveType.INT);
        assertSlot(context, "EV", PrimitiveType.DOUBLE);
        Assertions.assertFalse(context.scanSlots.stream().anyMatch(slot -> slot.getName().equals("ev")));
    }

    @Test
    public void testArrowSourceColumnUsesCaseInsensitiveTableType() throws Exception {
        OlapTable table = mockTable();
        NereidsParamCreateContext context = createArrowLoadContext(table,
                ImmutableList.of(new NereidsImportColumnDesc("time"),
                        new NereidsImportColumnDesc("securityid"),
                        new NereidsImportColumnDesc("ev")));

        assertSlot(context, "ev", PrimitiveType.DOUBLE);
    }

    @Test
    public void testTimestampNsSequenceDefaultWithPrecisionMayBeOmitted() {
        OlapTable table = mockTable();
        Column sequenceColumn = new Column("time", org.apache.doris.catalog.Type.TIMESTAMP_NS,
                false, null, true, "CURRENT_TIMESTAMP(9)", "");
        Mockito.when(table.hasSequenceCol()).thenReturn(true);
        Mockito.when(table.getSequenceMapCol()).thenReturn("time");
        Mockito.when(table.getFullSchema()).thenReturn(ImmutableList.of(sequenceColumn));

        Assertions.assertDoesNotThrow(() -> createLoadContext(table,
                ImmutableList.of(new NereidsImportColumnDesc("securityid")),
                new ArrowFileFormatProperties()));
    }

    @Test
    public void testDateTimeV2SequenceDefaultBehaviorIsUnchanged() {
        OlapTable table = mockTable();
        Column sequenceColumn = new Column("time", ScalarType.createDatetimeV2Type(6),
                false, null, true, "CURRENT_TIMESTAMP(6)", "");
        Mockito.when(table.hasSequenceCol()).thenReturn(true);
        Mockito.when(table.getSequenceMapCol()).thenReturn("time");
        Mockito.when(table.getFullSchema()).thenReturn(ImmutableList.of(sequenceColumn));

        Assertions.assertThrows(UserException.class, () -> createLoadContext(table,
                ImmutableList.of(new NereidsImportColumnDesc("securityid")),
                new ArrowFileFormatProperties()));
    }

    @Test
    public void testImportColumnMatchesTableColumnCaseInsensitive() {
        Column tableColumn = new Column("EV", PrimitiveType.DOUBLE, true);

        Assertions.assertTrue(NereidsLoadUtils.hasImportColumn(
                ImmutableList.of(new NereidsImportColumnDesc("ev")), tableColumn));
        Assertions.assertTrue(NereidsLoadUtils.hasImportColumn(
                ImmutableList.of(new NereidsImportColumnDesc("EV")), tableColumn));
        Assertions.assertFalse(NereidsLoadUtils.hasImportColumn(
                ImmutableList.of(new NereidsImportColumnDesc("other")), tableColumn));
    }

    @Test
    public void testComplexColumnMappingValueIsCanonicalized() throws Exception {
        OlapTable table = mockComplexTable();
        StringLiteral canonical = new StringLiteral("[1000, 7]");

        // v = default_value('[1e3, "7"]')
        NereidsParamCreateContext context = createMappingLoadContext(table,
                new UnboundFunction("default_value", ImmutableList.of(new StringLiteral("[1e3, \"7\"]"))),
                Pair.of("default_value", Lists.newArrayList("[1e3, \"7\"]")));
        Assertions.assertEquals(canonical, context.exprMap.get("v"));

        // v = replace_value(null, '[1e3, "7"]') becomes if(v is not null, v, cast('[1000, 7]' as ARRAY<INT>))
        context = createMappingLoadContext(table,
                new UnboundFunction("replace_value", ImmutableList.of(new NullLiteral(),
                        new StringLiteral("[1e3, \"7\"]"))),
                Pair.of("replace_value", Lists.newArrayList(null, "[1e3, \"7\"]")));
        Expression replaced = context.exprMap.get("v");
        Assertions.assertTrue(replaced instanceof UnboundFunction, replaced.toSql());
        Assertions.assertEquals(canonical, ((Cast) replaced.child(2)).child());

        // v = replace_value('[]', '[1e3, "7"]') becomes
        // if(v is not null, if(v != '[]', v, cast('[1000, 7]' as ARRAY<INT>)), null)
        context = createMappingLoadContext(table,
                new UnboundFunction("replace_value", ImmutableList.of(new StringLiteral("[]"),
                        new StringLiteral("[1e3, \"7\"]"))),
                Pair.of("replace_value", Lists.newArrayList("[]", "[1e3, \"7\"]")));
        replaced = context.exprMap.get("v");
        Assertions.assertTrue(replaced instanceof UnboundFunction, replaced.toSql());
        Assertions.assertEquals(canonical, ((Cast) replaced.child(1).child(2)).child());

        // the value is validated as a literal of the column type before it is rewritten
        UserException exception = Assertions.assertThrows(UserException.class, () -> createMappingLoadContext(table,
                new UnboundFunction("default_value", ImmutableList.of(new StringLiteral("[\"bad\"]"))),
                Pair.of("default_value", Lists.newArrayList("[\"bad\"]"))));
        Assertions.assertTrue(exception.getMessage().contains("Invalid default value"), exception.getMessage());

        // a scalar column keeps its mapping value verbatim
        context = createMappingLoadContext(table, "k",
                new UnboundFunction("default_value", ImmutableList.of(new StringLiteral("007"))),
                Pair.of("default_value", Lists.newArrayList("007")));
        Assertions.assertEquals(new StringLiteral("007"), context.exprMap.get("k"));
    }

    @Test
    public void testComplexColumnMappingOnTextSourceKeepsStringSpace() throws Exception {
        OlapTable table = mockComplexTable();
        StringLiteral canonical = new StringLiteral("[1000, 7]");
        CsvFileFormatProperties textFormat = new CsvFileFormatProperties(FileFormatProperties.FORMAT_CSV);

        // the text formats read the column as VARCHAR, so the replacement stays a string
        NereidsParamCreateContext context = createMappingLoadContext(table,
                new UnboundFunction("replace_value", ImmutableList.of(new NullLiteral(),
                        new StringLiteral("[1e3, \"7\"]"))),
                Pair.of("replace_value", Lists.newArrayList(null, "[1e3, \"7\"]")), textFormat);
        Expression replaced = context.exprMap.get("v");
        Assertions.assertEquals(canonical, replaced.child(2));

        // and the search value is compared with the raw text
        context = createMappingLoadContext(table,
                new UnboundFunction("replace_value", ImmutableList.of(new StringLiteral("[]"),
                        new StringLiteral("[1e3, \"7\"]"))),
                Pair.of("replace_value", Lists.newArrayList("[]", "[1e3, \"7\"]")), textFormat);
        replaced = context.exprMap.get("v");
        EqualTo comparison = (EqualTo) ((Not) replaced.child(1).child(0)).child();
        Assertions.assertTrue(comparison.left() instanceof UnboundSlot, comparison.left().toSql());
        Assertions.assertEquals(new StringLiteral("[]"), comparison.right());
        Assertions.assertEquals(canonical, replaced.child(1).child(2));
        Assertions.assertTrue(replaced.child(2) instanceof NullLiteral);
        Assertions.assertEquals(VarcharType.SYSTEM_DEFAULT, replaced.child(2).getDataType());
    }

    @Test
    public void testComplexColumnMappingOnArrowSourceUsesComplexType() throws Exception {
        OlapTable table = mockComplexTable();
        DataType complexType = DataType.fromCatalogType(table.getColumn("v").getType());
        StringLiteral canonical = new StringLiteral("[1000, 7]");

        // Arrow keeps the native complex type in the scan slot, so the replacement is parsed into it
        // and the IF arms share one type under both coercion behaviors
        NereidsParamCreateContext context = createMappingLoadContext(table,
                new UnboundFunction("replace_value", ImmutableList.of(new NullLiteral(),
                        new StringLiteral("[1e3, \"7\"]"))),
                Pair.of("replace_value", Lists.newArrayList(null, "[1e3, \"7\"]")));
        Expression replaced = context.exprMap.get("v");
        Cast replacement = (Cast) replaced.child(2);
        Assertions.assertEquals(complexType, replacement.getDataType());
        Assertions.assertEquals(canonical, replacement.child());

        // ARRAY comparisons are structural: the search is cast to the column type, not rendered as
        // the display text whose nested strings are not escaped
        context = createMappingLoadContext(table,
                new UnboundFunction("replace_value", ImmutableList.of(new StringLiteral("[]"),
                        new StringLiteral("[1e3, \"7\"]"))),
                Pair.of("replace_value", Lists.newArrayList("[]", "[1e3, \"7\"]")));
        replaced = context.exprMap.get("v");
        EqualTo comparison = (EqualTo) ((Not) replaced.child(1).child(0)).child();
        Assertions.assertTrue(comparison.left() instanceof UnboundSlot, comparison.left().toSql());
        Cast search = (Cast) comparison.right();
        Assertions.assertEquals(complexType, search.getDataType());
        Assertions.assertEquals(new StringLiteral("[]"), search.child());
        Cast innerReplacement = (Cast) replaced.child(1).child(2);
        Assertions.assertEquals(complexType, innerReplacement.getDataType());
        Assertions.assertEquals(canonical, innerReplacement.child());
        // the NULL branch must stay untyped, a VARCHAR NULL has no common type with a complex branch
        Assertions.assertTrue(replaced.child(2) instanceof NullLiteral);
        Assertions.assertTrue(replaced.child(2).getDataType().isNullType());
    }

    @Test
    public void testArrowComplexSearchSlotIsCastToColumnType() throws Exception {
        OlapTable table = mockComplexTable();
        DataType complexType = DataType.fromCatalogType(table.getColumn("v").getType());
        UnboundSlot searchSlot = new UnboundSlot("w");

        // v = replace_value(w, NULL): every search expression, not only a literal, is parsed into the
        // column type so the comparison is structural. The cast is strict so that a malformed search
        // fails instead of silently becoming NULL and replacing every row.
        NereidsParamCreateContext context = createMappingLoadContext(table,
                new UnboundFunction("replace_value", ImmutableList.of(searchSlot, new NullLiteral())),
                Pair.of("replace_value", Lists.newArrayList("w", null)));
        Expression replaced = context.exprMap.get("v");
        EqualTo comparison = (EqualTo) ((Not) replaced.child(1).child(0)).child();
        Assertions.assertTrue(comparison.left() instanceof UnboundSlot, comparison.left().toSql());
        Cast search = (Cast) comparison.right();
        Assertions.assertEquals(complexType, search.getDataType());
        Assertions.assertTrue(search.isStrict(), search.toSql());
        Assertions.assertEquals(searchSlot, search.child());
    }

    @Test
    public void testArrowComplexSearchIsNotCanonicalizedAsDefault() throws Exception {
        OlapTable table = mockTable(Arrays.asList(
                new Column("k", PrimitiveType.INT, true),
                new Column("v", ArrayType.create(ScalarType.createStringType(), true), true)));
        DataType complexType = DataType.fromCatalogType(table.getColumn("v").getType());
        StringLiteral search = new StringLiteral("[\"it's\"]");

        // A search is a transient comparison value. The default value format rejects quotes, but the
        // complex cast that parses the search accepts them, so the search must stay verbatim.
        NereidsParamCreateContext context = createMappingLoadContext(table,
                new UnboundFunction("replace_value", ImmutableList.of(search, new NullLiteral())),
                Pair.of("replace_value", Lists.newArrayList("[\"it's\"]", null)));
        Expression replaced = context.exprMap.get("v");
        EqualTo comparison = (EqualTo) ((Not) replaced.child(1).child(0)).child();
        Cast searchCast = (Cast) comparison.right();
        Assertions.assertEquals(complexType, searchCast.getDataType());
        Assertions.assertEquals(search, searchCast.child(0));
    }

    @Test
    public void testArrowMapSearchComparesKeysAndValues() throws Exception {
        OlapTable table = mockTable(Arrays.asList(
                new Column("k", PrimitiveType.INT, true),
                new Column("v", new MapType(ScalarType.createStringType(), ScalarType.INT, true, true), true)));
        DataType complexType = DataType.fromCatalogType(table.getColumn("v").getType());
        StringLiteral search = new StringLiteral("{\"a\": 1}");

        // MAP is rejected by ComparisonPredicate, so its keys and values are compared as arrays. A key
        // array keeps the key NULL bitmap and has no 255 byte key limit like a JSON object key, and a
        // NULL search keeps the text format three-valued result through the guard.
        NereidsParamCreateContext context = createMappingLoadContext(table,
                new UnboundFunction("replace_value", ImmutableList.of(search, new NullLiteral())),
                Pair.of("replace_value", Lists.newArrayList("{\"a\": 1}", null)));
        Expression replaced = context.exprMap.get("v");
        UnboundFunction condition = (UnboundFunction) ((Not) replaced.child(1).child(0)).child();
        Assertions.assertEquals("if", condition.getName());
        Assertions.assertTrue(condition.child(0) instanceof IsNull, condition.toSql());
        Assertions.assertTrue(condition.child(1) instanceof NullLiteral, condition.toSql());
        And equality = (And) condition.child(2);
        Assertions.assertEquals(2, equality.children().size(), equality.toSql());
        assertMapArrayComparison(equality.child(0), "map_keys", complexType, search);
        assertMapArrayComparison(equality.child(1), "map_values", complexType, search);
    }

    @Test
    public void testArrowStructSearchComparesFields() throws Exception {
        OlapTable table = mockTable(Arrays.asList(
                new Column("k", PrimitiveType.INT, true),
                new Column("v", new org.apache.doris.catalog.StructType(
                        new org.apache.doris.catalog.StructField("f1", ScalarType.INT),
                        new org.apache.doris.catalog.StructField("f2", ScalarType.createStringType())), true)));
        DataType complexType = DataType.fromCatalogType(table.getColumn("v").getType());
        StringLiteral search = new StringLiteral("{7, \"x\"}");

        // STRUCT is rejected by ComparisonPredicate, so every field is compared by its 1-based
        // constant index; a nested NULL field matches another NULL field through <=>.
        NereidsParamCreateContext context = createMappingLoadContext(table,
                new UnboundFunction("replace_value", ImmutableList.of(search, new NullLiteral())),
                Pair.of("replace_value", Lists.newArrayList("{7, \"x\"}", null)));
        Expression replaced = context.exprMap.get("v");
        UnboundFunction condition = (UnboundFunction) ((Not) replaced.child(1).child(0)).child();
        Assertions.assertEquals("if", condition.getName());
        And equality = (And) condition.child(2);
        Assertions.assertEquals(2, equality.children().size(), equality.toSql());
        assertStructFieldComparison(equality.child(0), 1, complexType, search);
        assertStructFieldComparison(equality.child(1), 2, complexType, search);
    }

    @Test
    public void testArrowMapSearchSupportsNonStringKeys() throws Exception {
        OlapTable table = mockTable(Arrays.asList(
                new Column("k", PrimitiveType.INT, true),
                new Column("v", new MapType(ScalarType.INT, ScalarType.INT, true, true), true)));
        DataType complexType = DataType.fromCatalogType(table.getColumn("v").getType());
        StringLiteral search = new StringLiteral("{1: 2}");

        // map_keys is compared as an array, so a key that is not string-like no longer needs a JSON
        // object key and is supported instead of rejected.
        NereidsParamCreateContext context = createMappingLoadContext(table,
                new UnboundFunction("replace_value", ImmutableList.of(search, new NullLiteral())),
                Pair.of("replace_value", Lists.newArrayList("{1: 2}", null)));
        Expression replaced = context.exprMap.get("v");
        UnboundFunction condition = (UnboundFunction) ((Not) replaced.child(1).child(0)).child();
        And equality = (And) condition.child(2);
        assertMapArrayComparison(equality.child(0), "map_keys", complexType, search);
        assertMapArrayComparison(equality.child(1), "map_values", complexType, search);
    }

    @Test
    public void testArrowMapSearchWithNestedMapValueIsRejected() {
        OlapTable table = mockTable(Arrays.asList(
                new Column("k", PrimitiveType.INT, true),
                new Column("v", new MapType(ScalarType.createStringType(),
                        new MapType(ScalarType.createStringType(), ScalarType.INT, true, true), true, true), true)));

        // JSONB object keys are limited to 255 bytes and lose the key NULL flag, so a MAP value
        // cannot be compared through to_json; a nested MAP is rejected instead of compared wrongly.
        UserException exception = Assertions.assertThrows(UserException.class, () -> createMappingLoadContext(table,
                new UnboundFunction("replace_value", ImmutableList.of(new StringLiteral("{}"), new NullLiteral())),
                Pair.of("replace_value", Lists.newArrayList("{}", null))));
        Assertions.assertTrue(exception.getMessage().contains("no lossless comparison"),
                exception.getMessage());
    }

    @Test
    public void testArrowStructArraySearchRejectsUnsupportedJsonLeaf() {
        OlapTable table = mockTable(Arrays.asList(
                new Column("k", PrimitiveType.INT, true),
                new Column("v", ArrayType.create(new org.apache.doris.catalog.StructType(
                        new org.apache.doris.catalog.StructField("f", ScalarType.createHllType())),
                        true), true)));

        // The engine cannot compare ARRAY<STRUCT> as an array, and HLL has no JSONB serde, so the
        // type is rejected at plan time instead of failing later during the load.
        UserException exception = Assertions.assertThrows(UserException.class, () -> createMappingLoadContext(table,
                new UnboundFunction("replace_value", ImmutableList.of(new StringLiteral("[]"), new NullLiteral())),
                Pair.of("replace_value", Lists.newArrayList("[]", null))));
        Assertions.assertTrue(exception.getMessage().contains("no lossless comparison"),
                exception.getMessage());
    }

    private static void assertMapArrayComparison(Expression equality, String function, DataType complexType,
            StringLiteral search) {
        Assertions.assertTrue(equality instanceof NullSafeEqual, equality.toSql());
        Expression left = equality.child(0);
        Expression right = equality.child(1);
        Assertions.assertTrue(left instanceof UnboundFunction, left.toSql());
        Assertions.assertTrue(right instanceof UnboundFunction, right.toSql());
        Assertions.assertEquals(function, ((UnboundFunction) left).getName());
        Assertions.assertEquals(function, ((UnboundFunction) right).getName());
        Assertions.assertTrue(left.child(0) instanceof UnboundSlot, left.toSql());
        Expression searchCast = right.child(0);
        Assertions.assertTrue(searchCast instanceof Cast, searchCast.toSql());
        Assertions.assertEquals(complexType, searchCast.getDataType());
        Assertions.assertTrue(((Cast) searchCast).isStrict(), searchCast.toSql());
        Assertions.assertEquals(search, searchCast.child(0));
    }

    private static void assertStructFieldComparison(Expression equality, int index, DataType complexType,
            StringLiteral search) {
        Assertions.assertTrue(equality instanceof NullSafeEqual, equality.toSql());
        Expression left = equality.child(0);
        Expression right = equality.child(1);
        Assertions.assertEquals("element_at", ((UnboundFunction) left).getName());
        Assertions.assertEquals("element_at", ((UnboundFunction) right).getName());
        Assertions.assertTrue(left.child(0) instanceof UnboundSlot, left.toSql());
        Assertions.assertEquals(new IntegerLiteral(index), left.child(1));
        Expression searchCast = right.child(0);
        Assertions.assertTrue(searchCast instanceof Cast, searchCast.toSql());
        Assertions.assertEquals(complexType, searchCast.getDataType());
        Assertions.assertTrue(((Cast) searchCast).isStrict(), searchCast.toSql());
        Assertions.assertEquals(search, searchCast.child(0));
        Assertions.assertEquals(new IntegerLiteral(index), right.child(1));
    }

    private NereidsParamCreateContext createMappingLoadContext(OlapTable table, Expression mapping,
            Pair<String, List<String>> hadoopFunction) throws UserException {
        return createMappingLoadContext(table, "v", mapping, hadoopFunction);
    }

    private NereidsParamCreateContext createMappingLoadContext(OlapTable table, Expression mapping,
            Pair<String, List<String>> hadoopFunction, FileFormatProperties fileFormatProperties)
            throws UserException {
        return createMappingLoadContext(table, "v", mapping, hadoopFunction, fileFormatProperties);
    }

    private NereidsParamCreateContext createMappingLoadContext(OlapTable table, String column, Expression mapping,
            Pair<String, List<String>> hadoopFunction) throws UserException {
        return createMappingLoadContext(table, column, mapping, hadoopFunction, new ArrowFileFormatProperties());
    }

    private NereidsParamCreateContext createMappingLoadContext(OlapTable table, String column, Expression mapping,
            Pair<String, List<String>> hadoopFunction, FileFormatProperties fileFormatProperties)
            throws UserException {
        Map<String, Pair<String, List<String>>> columnToHadoopFunction = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        columnToHadoopFunction.put(column, hadoopFunction);
        return createLoadContext(table,
                ImmutableList.of(new NereidsImportColumnDesc("k"), new NereidsImportColumnDesc("v"),
                        new NereidsImportColumnDesc(column, mapping)),
                columnToHadoopFunction, fileFormatProperties);
    }

    private NereidsParamCreateContext createArrowLoadContext(
            OlapTable table, List<NereidsImportColumnDesc> columnExprList) throws UserException {
        return createLoadContext(table, columnExprList, new ArrowFileFormatProperties());
    }

    private NereidsParamCreateContext createLoadContext(OlapTable table, List<NereidsImportColumnDesc> columnExprList,
            FileFormatProperties fileFormatProperties)
            throws UserException {
        return createLoadContext(table, columnExprList, null, fileFormatProperties);
    }

    private NereidsParamCreateContext createLoadContext(OlapTable table, List<NereidsImportColumnDesc> columnExprList,
            Map<String, Pair<String, List<String>>> columnToHadoopFunction, FileFormatProperties fileFormatProperties)
            throws UserException {
        NereidsBrokerFileGroup fileGroup = new NereidsBrokerFileGroup(1L, false, null,
                Lists.newArrayList("dummy"), null, null, null, columnExprList, columnToHadoopFunction, null, null,
                null, LoadTask.MergeType.APPEND, null, -1L, false, false, fileFormatProperties);
        TBrokerFileStatus fileStatus = new TBrokerFileStatus();
        fileStatus.setPath("");
        fileStatus.setIsDir(false);
        fileStatus.setSize(-1);
        NereidsFileGroupInfo fileGroupInfo = new NereidsFileGroupInfo(new TUniqueId(1, 2), 3L, table,
                BrokerDesc.createForStreamLoad(), fileGroup, fileStatus, false, TFileType.FILE_STREAM, null,
                TUniqueKeyUpdateMode.UPSERT, null);
        return new NereidsLoadScanProvider(fileGroupInfo, Collections.emptySet()).createLoadContext();
    }

    private OlapTable mockTable() {
        return mockTable(Arrays.asList(
                new Column("time", PrimitiveType.DATETIME, true),
                new Column("securityid", PrimitiveType.INT, true),
                new Column("EV", PrimitiveType.DOUBLE, true)));
    }

    private OlapTable mockTable(List<Column> schema) {
        Map<String, Column> nameToColumn = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (Column column : schema) {
            nameToColumn.put(column.getName(), column);
        }

        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.getName()).thenReturn("t_upper");
        Mockito.when(table.getBaseSchema()).thenReturn(schema);
        Mockito.when(table.getBaseSchema(false)).thenReturn(schema);
        Mockito.when(table.getBaseSchema(true)).thenReturn(schema);
        Mockito.when(table.getColumn(Mockito.anyString()))
                .thenAnswer(invocation -> nameToColumn.get(invocation.getArgument(0)));
        Mockito.when(table.hasSequenceCol()).thenReturn(false);
        Mockito.when(table.getSequenceMapCol()).thenReturn(null);
        Mockito.when(table.hasSkipBitmapColumn()).thenReturn(false);
        Mockito.when(table.getKeysType()).thenReturn(KeysType.UNIQUE_KEYS);
        Mockito.when(table.getFullQualifiers()).thenReturn(ImmutableList.of("internal", "db", "t_upper"));
        return table;
    }

    private OlapTable mockComplexTable() {
        return mockTable(Arrays.asList(
                new Column("k", PrimitiveType.INT, true),
                new Column("v", ArrayType.create(ScalarType.INT, true), true)));
    }

    private void assertSlot(NereidsParamCreateContext context, String name, PrimitiveType type) {
        SlotReference slot = context.scanSlots.stream()
                .filter(scanSlot -> scanSlot.getName().equals(name))
                .findFirst()
                .orElseThrow(() -> new AssertionError("missing scan slot " + name));
        Assertions.assertEquals(type, slot.getOriginalColumn().get().getDataType());
    }
}
