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

package org.apache.doris.datasource.paimon;

import org.apache.doris.catalog.Column;
import org.apache.doris.common.Config;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.TimeV2Type;
import org.apache.doris.nereids.types.VariantType;

import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

public class PaimonVariantWriteAnalyzerTest {
    private boolean originalEnableVariantV2;

    @Before
    public void saveVariantV2Config() {
        originalEnableVariantV2 = Config.enable_variant_v2;
    }

    @After
    public void restoreVariantV2Config() {
        Config.enable_variant_v2 = originalEnableVariantV2;
    }

    @Test
    public void testDisabledVariantV2IsRejectedDuringAnalysis() throws Exception {
        PaimonWriteTarget target = createTarget(
                DataTypes.FIELD(0, "payload", DataTypes.VARIANT()));

        Config.enable_variant_v2 = false;
        AnalysisException exception = Assert.assertThrows(
                AnalysisException.class,
                () -> validate(target, VariantType.COMPUTE_V2_INSTANCE));
        Assert.assertTrue(exception.getMessage().contains("enable_variant_v2=true"));
    }

    @Test
    public void testNativeVariantInputUsesGlobalV2Policy() throws Exception {
        PaimonWriteTarget target = createTarget(
                DataTypes.FIELD(0, "payload", DataTypes.VARIANT()));

        Config.enable_variant_v2 = true;
        validate(target, VariantType.INSTANCE);
    }

    @Test
    public void testComputeV2InputIsAccepted() throws Exception {
        PaimonWriteTarget target = createTarget(
                DataTypes.FIELD(0, "payload", DataTypes.VARIANT()));

        Config.enable_variant_v2 = true;
        PaimonVariantWriteAnalyzer.validate(
                target,
                Collections.singletonList(target.getColumn("payload")),
                outputs("payload", VariantType.COMPUTE_V2_INSTANCE));
    }

    @Test
    public void testInlineCoercionPreservesValuesBeforeCommonTypeResolution() {
        Alias integerValue = new Alias(new IntegerLiteral(1), "payload");
        Config.enable_variant_v2 = true;
        Assert.assertEquals(
                VariantType.COMPUTE_V2_INSTANCE,
                PaimonVariantWriteAnalyzer.resolveInlineCoercionTarget(
                        VariantType.INSTANCE, integerValue).get());
        Config.enable_variant_v2 = false;
        Assert.assertFalse(PaimonVariantWriteAnalyzer.resolveInlineCoercionTarget(
                VariantType.INSTANCE, integerValue).isPresent());

        Config.enable_variant_v2 = true;
        Alias variantValue = new Alias(
                new Cast(new StringLiteral("{}"), VariantType.INSTANCE), "payload");
        Assert.assertFalse(PaimonVariantWriteAnalyzer.resolveInlineCoercionTarget(
                VariantType.INSTANCE, variantValue).isPresent());

        Assert.assertEquals(
                ArrayType.of(VariantType.COMPUTE_V2_INSTANCE),
                PaimonVariantWriteAnalyzer.resolveInlineCoercionTarget(
                        ArrayType.of(VariantType.INSTANCE), integerValue).get());
    }

    @Test
    public void testNestedNativeVariantInputUsesGlobalV2Policy() throws Exception {
        PaimonWriteTarget target = createTarget(
                DataTypes.FIELD(0, "payload", DataTypes.ARRAY(DataTypes.VARIANT())));

        Config.enable_variant_v2 = true;
        validate(target, ArrayType.of(VariantType.INSTANCE));
    }

    @Test
    public void testNonVariantTableDoesNotRequireVariantV2() throws Exception {
        PaimonWriteTarget target = createTarget(
                DataTypes.FIELD(0, "payload", DataTypes.STRING()));

        Config.enable_variant_v2 = false;
        validate(target, org.apache.doris.nereids.types.StringType.INSTANCE);
    }

    @Test
    public void testOmittedVariantColumnDoesNotRequireVariantV2() throws Exception {
        PaimonWriteTarget target = createTarget(
                DataTypes.FIELD(0, "id", DataTypes.INT()),
                DataTypes.FIELD(1, "payload", DataTypes.VARIANT()));
        Column id = target.getColumn("id");

        Config.enable_variant_v2 = false;
        PaimonVariantWriteAnalyzer.validate(
                target,
                Collections.singletonList(id),
                outputs("id", IntegerType.INSTANCE));
    }

    @Test
    public void testSupportedV2ShapeChangingSourcesAreAccepted() throws Exception {
        Config.enable_variant_v2 = true;
        PaimonWriteTarget scalarTarget = createTarget(
                DataTypes.FIELD(0, "payload", DataTypes.VARIANT()));
        validate(scalarTarget, ArrayType.of(VariantType.INSTANCE));

        PaimonWriteTarget arrayTarget = createTarget(
                DataTypes.FIELD(0, "payload", DataTypes.ARRAY(DataTypes.VARIANT())));
        validate(arrayTarget, VariantType.INSTANCE);
    }

    @Test
    public void testUnsupportedComputeV2SourcesAreRejectedDuringAnalysis() throws Exception {
        PaimonWriteTarget target = createTarget(
                DataTypes.FIELD(0, "payload", DataTypes.VARIANT()));

        Config.enable_variant_v2 = true;
        AnalysisException mapException = Assert.assertThrows(
                AnalysisException.class,
                () -> validate(
                        target, MapType.of(StringType.INSTANCE, IntegerType.INSTANCE)));
        Assert.assertTrue(mapException.getMessage().contains("MAP"));

        AnalysisException timeException = Assert.assertThrows(
                AnalysisException.class,
                () -> validate(target, TimeV2Type.MAX));
        Assert.assertTrue(timeException.getMessage().contains("TIME"));
    }

    private static void validate(PaimonWriteTarget target, DataType sourceType)
            throws AnalysisException {
        Column column = target.getSchema().get(0);
        PaimonVariantWriteAnalyzer.validate(
                target,
                Collections.singletonList(column),
                outputs(column.getName(), sourceType));
    }

    private static Map<String, NamedExpression> outputs(String name, DataType dataType) {
        NamedExpression expression = Mockito.mock(NamedExpression.class);
        Mockito.when(expression.getDataType()).thenReturn(dataType);
        Map<String, NamedExpression> outputs = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        outputs.put(name, expression);
        return outputs;
    }

    private static PaimonWriteTarget createTarget(DataField... fields) throws Exception {
        PaimonExternalTable dorisTable = Mockito.mock(PaimonExternalTable.class);
        PaimonExternalCatalog catalog = Mockito.mock(PaimonExternalCatalog.class);
        FileStoreTable table = Mockito.mock(FileStoreTable.class);
        Mockito.when(dorisTable.getCatalog()).thenReturn(catalog);
        Mockito.when(dorisTable.getPaimonTableForWrite()).thenReturn(table);
        Mockito.when(table.rowType()).thenReturn(DataTypes.ROW(fields));
        Mockito.when(table.partitionKeys()).thenReturn(Collections.emptyList());
        return PaimonWriteTarget.create(dorisTable);
    }
}
