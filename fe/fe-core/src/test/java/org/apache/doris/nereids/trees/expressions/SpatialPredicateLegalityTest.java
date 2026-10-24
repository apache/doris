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

package org.apache.doris.nereids.trees.expressions;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Greatest;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Least;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MapContainsEntry;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MapContainsKey;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MapContainsValue;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ScalarFunction;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.GeographyType;
import org.apache.doris.nereids.types.GeometryType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.MapType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class SpatialPredicateLegalityTest {
    @Test
    public void mapValueAndEntryRejectSpatialComparisonsBeforeCoercion() {
        Expression integer = new SlotReference("id", IntegerType.INSTANCE);
        Expression ordinaryMap = new SlotReference("ordinary_map",
                MapType.of(IntegerType.INSTANCE, IntegerType.INSTANCE));
        for (DataType spatial : List.of(new GeometryType("EPSG:3857"),
                new GeographyType("OGC:CRS84", "spherical"),
                new GeographyType("OGC:CRS84", "vincenty"))) {
            for (DataType type : List.of(spatial, ArrayType.of(spatial))) {
                MapType mapType = MapType.of(IntegerType.INSTANCE, type);
                for (boolean nullable : List.of(false, true)) {
                    Expression map = new SlotReference("spatial_values", mapType, nullable);
                    Expression value = new SlotReference("shape", type, nullable);
                    for (ScalarFunction function : List.of(new MapContainsValue(map, value),
                            new MapContainsValue(map, NullLiteral.INSTANCE),
                            new MapContainsValue(new NullLiteral(mapType), value),
                            new MapContainsValue(ordinaryMap, value),
                            new MapContainsEntry(map, integer, value),
                            new MapContainsEntry(map, integer, NullLiteral.INSTANCE),
                            new MapContainsEntry(new NullLiteral(mapType), integer, value),
                            new MapContainsEntry(ordinaryMap, integer, value),
                            new MapContainsEntry(ordinaryMap, value, integer))) {
                        assertUnsupportedSpatialComparison(function);
                    }
                }
            }
        }
    }

    @Test
    public void mapKeyRejectSpatialComparisonsBeforeCoercion() {
        Expression integer = new SlotReference("id", IntegerType.INSTANCE);
        Expression ordinaryMap = new SlotReference("ordinary_map",
                MapType.of(IntegerType.INSTANCE, IntegerType.INSTANCE));
        for (DataType type : List.of(new GeometryType("EPSG:3857"),
                new GeographyType("OGC:CRS84", "spherical"))) {
            MapType mapType = MapType.of(type, IntegerType.INSTANCE);
            for (boolean nullable : List.of(false, true)) {
                Expression map = new SlotReference("spatial_keys", mapType, nullable);
                Expression key = new SlotReference("shape", type, nullable);
                for (ScalarFunction function : List.of(new MapContainsKey(map, key),
                        new MapContainsKey(map, NullLiteral.INSTANCE),
                        new MapContainsKey(new NullLiteral(mapType), key),
                        new MapContainsKey(ordinaryMap, key),
                        new MapContainsEntry(map, key, integer),
                        new MapContainsEntry(map, NullLiteral.INSTANCE, integer))) {
                    assertUnsupportedSpatialComparison(function);
                }
            }
        }
    }

    @Test
    public void mapKeyRetainsOrdinaryKeysWithSpatialValues() {
        Expression key = new SlotReference("id", IntegerType.INSTANCE);
        for (DataType spatial : List.of(new GeometryType("EPSG:3857"),
                new GeographyType("OGC:CRS84", "spherical"))) {
            for (DataType valueType : List.of(spatial, ArrayType.of(spatial))) {
                MapType mapType = MapType.of(IntegerType.INSTANCE, valueType);
                for (boolean nullable : List.of(false, true)) {
                    Expression map = new SlotReference("spatial_values", mapType, nullable);
                    for (Expression search : List.of(key, NullLiteral.INSTANCE)) {
                        ScalarFunction function = new MapContainsKey(map, search);
                        Assertions.assertDoesNotThrow(function::checkLegalityBeforeTypeCoercion);
                        Assertions.assertEquals(BooleanType.INSTANCE, function.getDataType());
                    }
                }
                Assertions.assertDoesNotThrow(new MapContainsKey(new NullLiteral(mapType), key)
                        ::checkLegalityBeforeTypeCoercion);
            }
        }
    }

    @Test
    public void mapContainsRetainsOrdinaryNullableComparisons() {
        for (boolean nullable : List.of(false, true)) {
            Expression integer = new SlotReference("id", IntegerType.INSTANCE, nullable);
            Expression map = new SlotReference("ordinary_map",
                    MapType.of(IntegerType.INSTANCE, IntegerType.INSTANCE), nullable);
            for (ScalarFunction function : List.of(new MapContainsKey(map, integer),
                    new MapContainsValue(map, integer), new MapContainsEntry(map, integer, integer),
                    new MapContainsValue(map, NullLiteral.INSTANCE),
                    new MapContainsEntry(map, integer, NullLiteral.INSTANCE))) {
                Assertions.assertDoesNotThrow(function::checkLegalityBeforeTypeCoercion);
                Assertions.assertEquals(BooleanType.INSTANCE, function.getDataType());
            }
        }
    }

    private static void assertUnsupportedSpatialComparison(ScalarFunction function) {
        AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                function::checkLegalityBeforeTypeCoercion);
        Assertions.assertEquals(function.getName() + " does not support spatial comparison", error.getMessage());
    }

    @Test
    public void greatestLeastRejectSpatialSlotsBeforeCoercion() {
        Expression integer = new SlotReference("id", IntegerType.INSTANCE);
        for (DataType spatial : List.of(new GeometryType("EPSG:3857"),
                new GeographyType("OGC:CRS84", "spherical"),
                new GeographyType("OGC:CRS84", "vincenty"))) {
            for (DataType type : List.of(spatial, ArrayType.of(spatial))) {
                for (boolean nullable : List.of(false, true)) {
                    Expression left = new SlotReference("left", type, nullable);
                    Expression right = new SlotReference("right", type, nullable);
                    for (ScalarFunction function : List.of(new Greatest(left, right), new Least(left, right),
                            new Greatest(integer, left), new Least(left, integer))) {
                        AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                                function::checkLegalityBeforeTypeCoercion);
                        Assertions.assertEquals(function.getName() + " does not support spatial arguments",
                                error.getMessage());
                    }
                }
            }
        }
    }

    @Test
    public void greatestLeastRetainOrdinaryNullableArguments() {
        for (boolean nullable : List.of(false, true)) {
            Expression left = new SlotReference("left", IntegerType.INSTANCE, nullable);
            Expression right = new SlotReference("right", IntegerType.INSTANCE, nullable);
            for (ScalarFunction function : List.of(new Greatest(left, right), new Least(left, right))) {
                Assertions.assertDoesNotThrow(function::checkLegalityBeforeTypeCoercion);
                Assertions.assertEquals(IntegerType.INSTANCE, function.getDataType());
            }
        }
    }

    @Test
    public void spatialInArgumentsAreRejectedBeforeCoercion() {
        IntegerLiteral integer = new IntegerLiteral(1);
        for (DataType type : List.of(new GeometryType("OGC:CRS84"),
                new GeographyType("OGC:CRS84", "spherical"))) {
            SlotReference shape = new SlotReference("shape", type);
            for (InPredicate predicate : List.of(new InPredicate(shape, List.of(integer)),
                    new InPredicate(integer, List.of(shape)))) {
                AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                        predicate::checkLegalityBeforeTypeCoercion);
                Assertions.assertTrue(error.getMessage().contains("in predicate does not support spatial arguments"));
            }
        }
        Assertions.assertDoesNotThrow(new InPredicate(integer, List.of(integer))::checkLegalityBeforeTypeCoercion);
    }
}
