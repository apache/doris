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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.types.DataType;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

public class IcebergSpatialWriteAnalyzerTest {
    @Test
    public void testMatchingSpatialParametersAreAccepted() {
        validate(ScalarType.createGeometryType("EPSG:4326"),
                ScalarType.createGeometryType("EPSG:4326"));
        validate(ScalarType.createGeographyType("OGC:CRS84", "spherical"),
                ScalarType.createGeographyType("OGC:CRS84", "spherical"));
    }

    @Test
    public void testSpatialKindMismatchIsRejected() {
        assertRejected(ScalarType.createGeometryType("OGC:CRS84"),
                ScalarType.createGeographyType("OGC:CRS84", "spherical"), "type mismatch");
    }

    @Test
    public void testSpatialCrsMismatchIsRejected() {
        assertRejected(ScalarType.createGeometryType("EPSG:3857"),
                ScalarType.createGeometryType("EPSG:4326"), "CRS mismatch");
    }

    @Test
    public void testSpatialAlgorithmMismatchIsRejected() {
        assertRejected(ScalarType.createGeographyType("OGC:CRS84", "vincenty"),
                ScalarType.createGeographyType("OGC:CRS84", "spherical"), "algorithm mismatch");
    }

    @Test
    public void testNonSpatialSourceIsRejected() {
        assertRejected(Type.STRING, ScalarType.createGeometryType("OGC:CRS84"),
                "cannot convert input column");
    }

    @Test
    public void testMergeValidationInspectsSourceBeforeTargetCast() {
        ScalarType targetType = ScalarType.createGeometryType("EPSG:4326");
        DataType targetDataType = DataType.fromCatalogType(targetType);
        NamedExpression source = new Alias(new If(
                BooleanLiteral.TRUE,
                new Cast(new SlotReference("matching", targetDataType), targetDataType),
                new Cast(new SlotReference("mismatched", DataType.fromCatalogType(
                        ScalarType.createGeometryType("EPSG:3857"))), targetDataType)), "payload");
        AnalysisException exception = Assert.assertThrows(AnalysisException.class,
                () -> IcebergSpatialWriteAnalyzer.validateMergeActions(
                        ImmutableList.of(new Column("payload", targetType)), ImmutableList.of(source)));
        Assert.assertTrue(exception.getMessage(), exception.getMessage().contains("CRS mismatch"));
        Assert.assertTrue(exception.getMessage(), exception.getMessage().contains("payload"));
    }

    private static void assertRejected(Type sourceType, ScalarType targetType, String message) {
        AnalysisException exception = Assert.assertThrows(AnalysisException.class,
                () -> validate(sourceType, targetType));
        Assert.assertTrue(exception.getMessage(), exception.getMessage().contains("payload"));
        Assert.assertTrue(exception.getMessage(), exception.getMessage().contains(message));
    }

    private static void validate(Type sourceType, ScalarType targetType) {
        NamedExpression source = new Alias(
                new SlotReference("source", DataType.fromCatalogType(sourceType)), "payload");
        IcebergSpatialWriteAnalyzer.validate(ImmutableList.of(new Column("payload", targetType)),
                ImmutableList.of(source));
    }
}
