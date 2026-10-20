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

package org.apache.doris.nereids.trees.expressions.functions;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AnyValue;
import org.apache.doris.nereids.trees.expressions.functions.agg.ArrayAgg;
import org.apache.doris.nereids.trees.expressions.functions.agg.CollectList;
import org.apache.doris.nereids.trees.expressions.functions.agg.CollectSet;
import org.apache.doris.nereids.trees.expressions.functions.agg.Histogram;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.functions.agg.Ndv;
import org.apache.doris.nereids.trees.expressions.functions.combinator.CombineCombinator;
import org.apache.doris.nereids.trees.expressions.functions.combinator.StateCombinator;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.GeographyType;
import org.apache.doris.nereids.types.GeometryType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class SpatialAggregateLegalityTest {
    @Test
    public void nestedSpatialArrayAggIsRejectedBeforeStateSerialization() {
        for (DataType spatial : List.of(new GeometryType("EPSG:3857"),
                new GeographyType("OGC:CRS84", "spherical"))) {
            for (DataType nested : List.of(ArrayType.of(spatial), ArrayType.of(ArrayType.of(spatial)),
                    MapType.of(IntegerType.INSTANCE, spatial),
                    new StructType(List.of(new StructField("shape", spatial, true, ""))))) {
                for (boolean nullable : List.of(false, true)) {
                    Expression value = new SlotReference("shapes", nested, nullable);
                    ArrayAgg function = new ArrayAgg(value);
                    for (BoundFunction entry : List.of(function, new ArrayAgg(true, value),
                            StateCombinator.create(function),
                            new CombineCombinator(function.getArguments(), function))) {
                        AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                                entry::checkLegalityBeforeTypeCoercion, entry.getName());
                        Assertions.assertEquals("array_agg does not support nested spatial arguments",
                                error.getMessage());
                    }
                }
            }
        }
        // Scalar WKB and ordinary nested values retain their existing admission.
        for (DataType type : List.of(ArrayType.of(IntegerType.INSTANCE),
                new GeometryType("OGC:CRS84"), new GeographyType("OGC:CRS84", "spherical"))) {
            for (boolean nullable : List.of(false, true)) {
                ArrayAgg function = new ArrayAgg(new SlotReference("value", type, nullable));
                for (BoundFunction entry : List.of(function, StateCombinator.create(function),
                        new CombineCombinator(function.getArguments(), function))) {
                    Assertions.assertDoesNotThrow(entry::checkLegalityBeforeTypeCoercion);
                }
            }
        }
    }

    @Test
    public void nestedSpatialCollectListIsRejectedBeforeStateSerialization() {
        for (DataType spatial : List.of(new GeometryType("EPSG:3857"),
                new GeographyType("OGC:CRS84", "spherical"))) {
            for (DataType nested : List.of(ArrayType.of(spatial), ArrayType.of(ArrayType.of(spatial)),
                    MapType.of(IntegerType.INSTANCE, spatial),
                    new StructType(List.of(new StructField("shape", spatial, true, ""))))) {
                Expression value = new SlotReference("shapes", nested);
                for (BoundFunction function : List.of(new CollectList(value),
                        new CollectList(value, new IntegerLiteral(2)), new CollectList(true, value))) {
                    AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                            function::checkLegalityBeforeTypeCoercion);
                    Assertions.assertEquals("collect_list does not support nested spatial arguments",
                            error.getMessage());
                }
            }
        }
        // Ordinary complex values and nonnested spatial cells keep their existing admission.
        for (DataType type : List.of(ArrayType.of(IntegerType.INSTANCE),
                new GeometryType("OGC:CRS84"), new GeographyType("OGC:CRS84", "spherical"))) {
            CollectList function = new CollectList(new SlotReference("value", type));
            Assertions.assertDoesNotThrow(function::checkLegalityBeforeTypeCoercion);
        }
    }

    @Test
    public void unsupportedSpatialAggregateKernelsAreRejectedBeforeCoercion() {
        for (DataType type : List.of(new GeometryType("OGC:CRS84"),
                new GeographyType("OGC:CRS84", "spherical"))) {
            Expression shape = new SlotReference("shape", type);
            for (BoundFunction function : List.of(new Ndv(shape), new Histogram(shape),
                    new Histogram(shape, new IntegerLiteral(2)), new CollectSet(shape),
                    new CollectSet(shape, new IntegerLiteral(2)))) {
                AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                        function::checkLegalityBeforeTypeCoercion, function.getName());
                Assertions.assertEquals(function.getName() + " does not support spatial arguments",
                        error.getMessage());
            }
        }
    }

    @Test
    public void singleValueSpatialAndOrdinaryAggregatesRemainAdmitted() {
        for (DataType type : List.of(new GeometryType("EPSG:3857"),
                new GeographyType("OGC:CRS84", "spherical"), IntegerType.INSTANCE)) {
            Expression value = new SlotReference("value", type);
            for (BoundFunction function : List.of(new AnyValue(value), new Min(value), new Max(value))) {
                Assertions.assertDoesNotThrow(function::checkLegalityBeforeTypeCoercion);
                Assertions.assertEquals(type, function.getDataType());
            }
        }
        Expression integer = new SlotReference("value", IntegerType.INSTANCE);
        for (BoundFunction function : List.of(new Ndv(integer), new Histogram(integer), new CollectSet(integer))) {
            Assertions.assertDoesNotThrow(function::checkLegalityBeforeTypeCoercion);
        }
    }
}
