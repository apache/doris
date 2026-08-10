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

package org.apache.doris.datasource.iceberg;

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.VariantType;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

public class IcebergVariantWriteAnalyzerTest {

    @Test
    public void testNestedComputeV2InputIsAccepted() {
        Type leafStruct = new StructType(new ArrayList<>(ImmutableList.of(
                new StructField("payload", icebergVariantType()))));
        Type events = ArrayType.create(new MapType(Type.STRING, leafStruct), true);
        Type targetType = new StructType(new ArrayList<>(ImmutableList.of(
                new StructField("events", events))));
        DataType sourceType = DataType.fromCatalogType(targetType);

        validate(targetType, sourceType);
    }

    @Test
    public void testLegacyVariantLeavesAreRejectedWithNestedPath() {
        assertLegacyRejected(
                ArrayType.create(icebergVariantType(), true),
                org.apache.doris.nereids.types.ArrayType.of(VariantType.INSTANCE),
                "payload[]");

        assertLegacyRejected(
                new MapType(Type.STRING, icebergVariantType()),
                org.apache.doris.nereids.types.MapType.of(StringType.INSTANCE, VariantType.INSTANCE),
                "payload.value");

        Type structTarget = new StructType(new ArrayList<>(ImmutableList.of(
                new StructField("leaf", icebergVariantType()))));
        DataType structSource = new org.apache.doris.nereids.types.StructType(ImmutableList.of(
                new org.apache.doris.nereids.types.StructField(
                        "leaf", VariantType.INSTANCE, true, "")));
        assertLegacyRejected(structTarget, structSource, "payload.leaf");
    }

    @Test
    public void testPrimitiveAndNullSourcesCanBecomeNestedVariantV2() {
        Type targetType = ArrayType.create(icebergVariantType(), true);
        validate(targetType, org.apache.doris.nereids.types.ArrayType.of(StringType.INSTANCE));
        validate(targetType, org.apache.doris.nereids.types.NullType.INSTANCE);
    }

    @Test
    public void testInlineCoercionPreservesVariantRepresentation() {
        DataType targetType = DataType.fromCatalogType(
                ArrayType.create(icebergVariantType(), true));
        Alias primitiveValue = new Alias(new IntegerLiteral(1), "payload");
        Assert.assertEquals(targetType, IcebergVariantWriteAnalyzer.resolveInlineCoercionTarget(
                targetType, primitiveValue).get());

        Alias legacyVariantValue = new Alias(
                new Cast(new StringLiteral("{}"), VariantType.INSTANCE), "payload");
        Assert.assertFalse(IcebergVariantWriteAnalyzer.resolveInlineCoercionTarget(
                targetType, legacyVariantValue).isPresent());
    }

    private static void assertLegacyRejected(Type targetType, DataType sourceType, String path) {
        AnalysisException exception = Assert.assertThrows(
                AnalysisException.class, () -> validate(targetType, sourceType));
        Assert.assertTrue(exception.getMessage(), exception.getMessage().contains(path));
        Assert.assertTrue(exception.getMessage(), exception.getMessage().contains("legacy Doris VARIANT"));
    }

    private static void validate(Type targetType, DataType sourceType) {
        Column targetColumn = new Column("payload", targetType);
        NamedExpression source = Mockito.mock(NamedExpression.class);
        Mockito.when(source.getDataType()).thenReturn(sourceType);
        List<Column> targets = ImmutableList.of(targetColumn);
        List<NamedExpression> sources = ImmutableList.of(source);
        IcebergVariantWriteAnalyzer.validate(targets, sources);
    }

    private static Type icebergVariantType() {
        return IcebergUtils.icebergTypeToDorisType(
                org.apache.iceberg.types.Types.VariantType.get(), false, false);
    }
}
