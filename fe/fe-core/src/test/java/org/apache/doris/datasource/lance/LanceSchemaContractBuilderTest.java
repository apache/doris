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

package org.apache.doris.datasource.lance;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.lance.job.LanceIndexSchemaContract;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.UnionMode;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.lance.schema.LanceField;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Pins the Java-side canonical normalizedType vocabulary of schema contract v1. The Rust
 * worker's golden fixtures align to these exact strings, so every ArrowType mapping and the
 * generic fallback grammar are asserted literally here.
 */
public class LanceSchemaContractBuilderTest {

    @Test
    public void testPrimitiveCanonicalTypes() throws Exception {
        Assertions.assertEquals("bool", normalizedType(new ArrowType.Bool()));
        Assertions.assertEquals("int<8>", normalizedType(new ArrowType.Int(8, true)));
        Assertions.assertEquals("int<16>", normalizedType(new ArrowType.Int(16, true)));
        Assertions.assertEquals("int<32>", normalizedType(new ArrowType.Int(32, true)));
        Assertions.assertEquals("int<64>", normalizedType(new ArrowType.Int(64, true)));
        Assertions.assertEquals("uint<8>", normalizedType(new ArrowType.Int(8, false)));
        Assertions.assertEquals("uint<16>", normalizedType(new ArrowType.Int(16, false)));
        Assertions.assertEquals("uint<32>", normalizedType(new ArrowType.Int(32, false)));
        Assertions.assertEquals("uint<64>", normalizedType(new ArrowType.Int(64, false)));
        Assertions.assertEquals("float16",
                normalizedType(new ArrowType.FloatingPoint(FloatingPointPrecision.HALF)));
        Assertions.assertEquals("float32",
                normalizedType(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)));
        Assertions.assertEquals("float64",
                normalizedType(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)));
        Assertions.assertEquals("utf8", normalizedType(new ArrowType.Utf8()));
        Assertions.assertEquals("large_utf8", normalizedType(new ArrowType.LargeUtf8()));
    }

    @Test
    public void testDecimalCarriesPrecisionScaleAndBitWidth() throws Exception {
        Assertions.assertEquals("decimal<128>(10,2)",
                normalizedType(new ArrowType.Decimal(10, 2, 128)));
        Assertions.assertEquals("decimal<256>(38,8)",
                normalizedType(new ArrowType.Decimal(38, 8, 256)));
        Assertions.assertEquals("decimal<128>(1,0)",
                normalizedType(new ArrowType.Decimal(1, 0, 128)));
    }

    @Test
    public void testDateCanonicalTypes() throws Exception {
        Assertions.assertEquals("date<day>", normalizedType(new ArrowType.Date(DateUnit.DAY)));
        Assertions.assertEquals("date<ms>",
                normalizedType(new ArrowType.Date(DateUnit.MILLISECOND)));
    }

    @Test
    public void testTimestampCanonicalTypesCoverEveryUnitAndTimezone() throws Exception {
        Assertions.assertEquals("timestamp<sec,tz=\"\">",
                normalizedType(new ArrowType.Timestamp(TimeUnit.SECOND, null)));
        Assertions.assertEquals("timestamp<ms,tz=\"\">",
                normalizedType(new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)));
        Assertions.assertEquals("timestamp<us,tz=\"\">",
                normalizedType(new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)));
        Assertions.assertEquals("timestamp<ns,tz=\"\">",
                normalizedType(new ArrowType.Timestamp(TimeUnit.NANOSECOND, null)));
        Assertions.assertEquals("timestamp<ms,tz=\"UTC\">",
                normalizedType(new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC")));
        Assertions.assertEquals("timestamp<us,tz=\"Asia/Shanghai\">",
                normalizedType(new ArrowType.Timestamp(TimeUnit.MICROSECOND, "Asia/Shanghai")));
        Assertions.assertEquals("timestamp<ns,tz=\"America/Los_Angeles\">",
                normalizedType(new ArrowType.Timestamp(TimeUnit.NANOSECOND, "America/Los_Angeles")));
        Assertions.assertEquals("timestamp<sec,tz=\"Etc/GMT+8\">",
                normalizedType(new ArrowType.Timestamp(TimeUnit.SECOND, "Etc/GMT+8")));
    }

    @Test
    public void testTimestampWithUnsafeTimezoneFallsBackToGenericForm() throws Exception {
        // The canonical tz slot has no escaping, so a timezone outside the IANA character set
        // (letters, digits, '+', '-', '_', '/') must degrade to the generic recursive format.
        // That includes Arrow offset spellings such as "+08:00", whose colon is not in the set.
        Assertions.assertEquals("timestamp(unit=MILLISECOND,timezone=bad,tz)",
                normalizedType(new ArrowType.Timestamp(TimeUnit.MILLISECOND, "bad,tz")));
        Assertions.assertEquals("timestamp(unit=SECOND,timezone=Bad\"Tz)",
                normalizedType(new ArrowType.Timestamp(TimeUnit.SECOND, "Bad\"Tz")));
        Assertions.assertEquals("timestamp(unit=MICROSECOND,timezone=gt>)",
                normalizedType(new ArrowType.Timestamp(TimeUnit.MICROSECOND, "gt>")));
        Assertions.assertEquals("timestamp(unit=SECOND,timezone=+08:00)",
                normalizedType(new ArrowType.Timestamp(TimeUnit.SECOND, "+08:00")));
        Assertions.assertEquals("timestamp(unit=NANOSECOND,timezone=)",
                normalizedType(new ArrowType.Timestamp(TimeUnit.NANOSECOND, "")));
    }

    @Test
    public void testUnusualIntWidthUsesGenericForm() throws Exception {
        Assertions.assertEquals("int(bitWidth=1,signed=true)",
                normalizedType(new ArrowType.Int(1, true)));
        Assertions.assertEquals("int(bitWidth=7,signed=false)",
                normalizedType(new ArrowType.Int(7, false)));
        Assertions.assertEquals("int(bitWidth=128,signed=true)",
                normalizedType(new ArrowType.Int(128, true)));
    }

    @Test
    public void testOtherTypesUseGenericRecursiveForm() throws Exception {
        Assertions.assertEquals("time(unit=MILLISECOND,bitWidth=64)",
                normalizedType(new ArrowType.Time(TimeUnit.MILLISECOND, 64)));
        Assertions.assertEquals("time(unit=SECOND,bitWidth=32)",
                normalizedType(new ArrowType.Time(TimeUnit.SECOND, 32)));
        Assertions.assertEquals("duration(unit=SECOND)",
                normalizedType(new ArrowType.Duration(TimeUnit.SECOND)));
        Assertions.assertEquals("interval(unit=YEAR_MONTH)",
                normalizedType(new ArrowType.Interval(IntervalUnit.YEAR_MONTH)));
        Assertions.assertEquals("binary()", normalizedType(new ArrowType.Binary()));
        Assertions.assertEquals("largebinary()", normalizedType(new ArrowType.LargeBinary()));
        Assertions.assertEquals("binaryview()", normalizedType(new ArrowType.BinaryView()));
        Assertions.assertEquals("fixedsizebinary(byteWidth=16)",
                normalizedType(new ArrowType.FixedSizeBinary(16)));
        Assertions.assertEquals("utf8view()", normalizedType(new ArrowType.Utf8View()));
        Assertions.assertEquals("list()", normalizedType(new ArrowType.List()));
        Assertions.assertEquals("largelist()", normalizedType(new ArrowType.LargeList()));
        Assertions.assertEquals("listview()", normalizedType(new ArrowType.ListView()));
        Assertions.assertEquals("largelistview()", normalizedType(new ArrowType.LargeListView()));
        Assertions.assertEquals("struct()", normalizedType(new ArrowType.Struct()));
        Assertions.assertEquals("null()", normalizedType(new ArrowType.Null()));
        Assertions.assertEquals("map(keysSorted=false)",
                normalizedType(new ArrowType.Map(false)));
        Assertions.assertEquals("map(keysSorted=true)",
                normalizedType(new ArrowType.Map(true)));
        Assertions.assertEquals("union(mode=Dense,typeIds=[0, 1])",
                normalizedType(new ArrowType.Union(UnionMode.Dense, new int[]{0, 1})));
        Assertions.assertEquals("runendencoded()",
                normalizedType(new ArrowType.RunEndEncoded()));
    }

    @Test
    public void testFixedSizeListHasPinnedLiteralAndExtractsVectorElement() throws Exception {
        // Mirrors the pinned SDK: the LanceField tree carries no children for a fixed-size
        // list (the element is collapsed into the manifest logical-type string), so element
        // facts come from the synthesized child of the reconstructed Arrow view.
        LanceField vector = vectorField(5, "embedding", 768, FloatingPointPrecision.SINGLE, false);

        LanceIndexSchemaContract.IndexedField indexed = buildSingleField(
                Collections.singletonList(vector), "embedding");

        Assertions.assertEquals(5, indexed.getFieldId());
        Assertions.assertEquals("embedding", indexed.getNormalizedName());
        Assertions.assertEquals("fixed_size_list", indexed.getNormalizedType());
        Assertions.assertFalse(indexed.isNullable());
        Assertions.assertEquals(768, indexed.getFixedSizeListDimension());
        Assertions.assertEquals("float32", indexed.getVectorElementType());
        // The reconstruction synthesizes the element child as nullable — even when the data
        // was written with a not-null element — because the manifest has no slot for it.
        Assertions.assertEquals(Boolean.TRUE, indexed.getVectorElementNullable());

        LanceField halfVector = vectorField(7, "half_embedding", 3, FloatingPointPrecision.HALF,
                true);
        LanceIndexSchemaContract.IndexedField halfIndexed = buildSingleField(
                Collections.singletonList(halfVector), "half_embedding");
        Assertions.assertEquals("fixed_size_list", halfIndexed.getNormalizedType());
        Assertions.assertEquals(3, halfIndexed.getFixedSizeListDimension());
        Assertions.assertEquals("float16", halfIndexed.getVectorElementType());
        Assertions.assertEquals(Boolean.TRUE, halfIndexed.getVectorElementNullable());
        Assertions.assertTrue(halfIndexed.isNullable());
    }

    @Test
    public void testNonVectorFieldsLeaveVectorSlotsEmpty() throws Exception {
        LanceIndexSchemaContract.IndexedField indexed = buildSingleField(
                Collections.singletonList(field(1, "name", new ArrowType.Utf8(), true)), "name");
        Assertions.assertNull(indexed.getFixedSizeListDimension());
        Assertions.assertNull(indexed.getVectorElementType());
        Assertions.assertNull(indexed.getVectorElementNullable());
    }

    @Test
    public void testContractIsSingleElementAndVersionOne() throws Exception {
        LanceIndexSchemaContract contract = LanceSchemaContractBuilder.build(Arrays.asList(
                field(1, "a", new ArrowType.Int(32, true), false),
                field(2, "b", new ArrowType.Utf8(), true)), "b");

        Assertions.assertEquals(LanceIndexSchemaContract.SCHEMA_CONTRACT_VERSION_V1,
                contract.getSchemaContractVersion());
        Assertions.assertEquals(1, contract.getFields().size());
        Assertions.assertEquals(2, contract.getFields().get(0).getFieldId());
        Assertions.assertEquals("b", contract.getFields().get(0).getNormalizedName());
    }

    @Test
    public void testMixedCaseStoredNameIsNormalizedInContract() throws Exception {
        LanceIndexSchemaContract.IndexedField indexed = buildSingleField(
                Collections.singletonList(field(1, "Embedding", new ArrowType.Utf8(), true)),
                "Embedding");
        Assertions.assertEquals("embedding", indexed.getNormalizedName());
    }

    @Test
    public void testFieldMatchingIsExactBytes() {
        // The builder input is the stored column name (byte-identical to the LanceField name);
        // a differently-cased name means the field is absent and must fail closed.
        List<LanceField> fields = Collections.singletonList(
                field(1, "embedding", new ArrowType.Utf8(), true));
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> LanceSchemaContractBuilder.build(fields, "Embedding"));
        Assertions.assertTrue(exception.getMessage().contains(
                "unsupported schema contract: indexed field not found"));
    }

    @Test
    public void testMissingFieldFailsClosed() {
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> LanceSchemaContractBuilder.build(Collections.singletonList(
                        field(1, "a", new ArrowType.Bool(), true)), "missing"));
        Assertions.assertTrue(exception.getMessage().contains(
                "unsupported schema contract: indexed field not found"));

        Assertions.assertThrows(AnalysisException.class,
                () -> LanceSchemaContractBuilder.build(Collections.emptyList(), "missing"));
    }

    @Test
    public void testNegativeFieldIdFailsClosed() {
        // A negative provider field id is a malformed schema fact: bounded error, no provider
        // string echoed, same shape as the missing-field rejection.
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> LanceSchemaContractBuilder.build(Collections.singletonList(
                        field(-1, "a", new ArrowType.Utf8(), true)), "a"));
        Assertions.assertTrue(exception.getMessage().contains(
                "unsupported schema contract: indexed field id must not be negative"));
    }

    @Test
    public void testZeroFieldIdIsLegal() throws Exception {
        // Field id 0 is a legitimate provider id; only negatives are rejected.
        LanceIndexSchemaContract.IndexedField indexed = buildSingleField(
                Collections.singletonList(field(0, "a", new ArrowType.Utf8(), true)), "a");
        Assertions.assertEquals(0, indexed.getFieldId());
    }

    @Test
    public void testNonPositiveFixedSizeListDimensionFailsClosed() {
        for (int dimension : new int[] {0, -3}) {
            LanceField vector = field(1, "embedding", new ArrowType.FixedSizeList(dimension),
                    false);
            AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                    () -> LanceSchemaContractBuilder.build(
                            Collections.singletonList(vector), "embedding"));
            Assertions.assertTrue(exception.getMessage().contains(
                    "unsupported schema contract: fixed-size list dimension must be positive"));
        }
    }

    @Test
    public void testNestedChildrenNeverEnterTheContract() throws Exception {
        // Only the indexed top-level field is represented; nested subfields of a struct stay
        // out of the contract even though they share the same LanceField tree.
        LanceField child = field(2, "nested", new ArrowType.Int(32, true), true);
        LanceField parent = field(1, "payload", new ArrowType.Struct(), true,
                Collections.singletonList(child));

        LanceIndexSchemaContract contract = LanceSchemaContractBuilder.build(
                Collections.singletonList(parent), "payload");

        Assertions.assertEquals(1, contract.getFields().size());
        Assertions.assertEquals("payload", contract.getFields().get(0).getNormalizedName());
        Assertions.assertEquals("struct()", contract.getFields().get(0).getNormalizedType());
    }

    @Test
    public void testRejectsMalformedInputs() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> LanceSchemaContractBuilder.build(null, "a"));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> LanceSchemaContractBuilder.build(Collections.singletonList(
                        field(1, "a", new ArrowType.Bool(), true)), null));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> LanceSchemaContractBuilder.build(Collections.singletonList(
                        field(1, "a", new ArrowType.Bool(), true)), ""));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> LanceSchemaContractBuilder.build(
                        Collections.singletonList((LanceField) null), "a"));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> LanceSchemaContractBuilder.build(Collections.singletonList(
                        field(1, "a", null, true)), "a"));
    }

    @Test
    public void testFixedSizeListWithoutRecoverableElementFailsClosed() {
        // The reconstructed Arrow view is the only element source: a missing view, an empty
        // child list or a null first child is a malformed provider fact and fails closed with
        // the same bounded error as a missing field.
        String expected = "unsupported schema contract: fixed-size list element must be present";
        LanceField missingView = Mockito.mock(LanceField.class);
        Mockito.when(missingView.getId()).thenReturn(1);
        Mockito.when(missingView.getName()).thenReturn("embedding");
        Mockito.when(missingView.getType()).thenReturn(new ArrowType.FixedSizeList(4));
        Mockito.when(missingView.isNullable()).thenReturn(false);
        Mockito.when(missingView.getChildren()).thenReturn(Collections.emptyList());
        // asArrowField() left unstubbed returns null from the mock.
        AnalysisException missing = Assertions.assertThrows(AnalysisException.class,
                () -> LanceSchemaContractBuilder.build(
                        Collections.singletonList(missingView), "embedding"));
        Assertions.assertTrue(missing.getMessage().contains(expected));

        LanceField emptyChildren = vectorField(2, "embedding", 4, FloatingPointPrecision.SINGLE,
                false);
        Mockito.when(emptyChildren.asArrowField()).thenReturn(new Field("embedding",
                FieldType.notNullable(new ArrowType.FixedSizeList(4)), Collections.emptyList()));
        AnalysisException empty = Assertions.assertThrows(AnalysisException.class,
                () -> LanceSchemaContractBuilder.build(
                        Collections.singletonList(emptyChildren), "embedding"));
        Assertions.assertTrue(empty.getMessage().contains(expected));

        LanceField nullChild = vectorField(3, "embedding", 4, FloatingPointPrecision.SINGLE,
                false);
        Mockito.when(nullChild.asArrowField()).thenReturn(new Field("embedding",
                FieldType.notNullable(new ArrowType.FixedSizeList(4)),
                Collections.singletonList(null)));
        AnalysisException nullElement = Assertions.assertThrows(AnalysisException.class,
                () -> LanceSchemaContractBuilder.build(
                        Collections.singletonList(nullChild), "embedding"));
        Assertions.assertTrue(nullElement.getMessage().contains(expected));
    }

    private static String normalizedType(ArrowType type) throws AnalysisException {
        LanceIndexSchemaContract contract = LanceSchemaContractBuilder.build(
                Collections.singletonList(field(1, "target", type, true)), "target");
        return contract.getFields().get(0).getNormalizedType();
    }

    private static LanceIndexSchemaContract.IndexedField buildSingleField(
            List<LanceField> fields, String storedColumnName) throws AnalysisException {
        LanceIndexSchemaContract contract = LanceSchemaContractBuilder.build(fields, storedColumnName);
        Assertions.assertEquals(1, contract.getFields().size());
        return contract.getFields().get(0);
    }

    private static LanceField field(int id, String name, ArrowType type, boolean nullable) {
        return field(id, name, type, nullable, Collections.emptyList());
    }

    private static LanceField field(int id, String name, ArrowType type, boolean nullable,
            List<LanceField> children) {
        LanceField field = Mockito.mock(LanceField.class);
        Mockito.when(field.getId()).thenReturn(id);
        Mockito.when(field.getName()).thenReturn(name);
        Mockito.when(field.getType()).thenReturn(type);
        Mockito.when(field.isNullable()).thenReturn(nullable);
        Mockito.when(field.getChildren()).thenReturn(children);
        return field;
    }

    /**
     * A fixed-size-list LanceField shaped exactly as the pinned SDK reconstructs it: no
     * children on the LanceField tree, and a synthesized (always-nullable) element child on
     * the Arrow view.
     */
    private static LanceField vectorField(int id, String name, int dimension,
            FloatingPointPrecision elementPrecision, boolean nullable) {
        Field synthesizedElement = Field.nullable("item",
                new ArrowType.FloatingPoint(elementPrecision));
        Field arrowView = new Field(name,
                nullable ? FieldType.nullable(new ArrowType.FixedSizeList(dimension))
                        : FieldType.notNullable(new ArrowType.FixedSizeList(dimension)),
                Collections.singletonList(synthesizedElement));
        LanceField field = Mockito.mock(LanceField.class);
        Mockito.when(field.getId()).thenReturn(id);
        Mockito.when(field.getName()).thenReturn(name);
        Mockito.when(field.getType()).thenReturn(new ArrowType.FixedSizeList(dimension));
        Mockito.when(field.isNullable()).thenReturn(nullable);
        Mockito.when(field.getChildren()).thenReturn(Collections.emptyList());
        Mockito.when(field.asArrowField()).thenReturn(arrowView);
        return field;
    }
}
