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
import org.apache.doris.nereids.trees.expressions.functions.scalar.ScalarFunction;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.GeographyType;
import org.apache.doris.nereids.types.GeometryType;
import org.apache.doris.nereids.types.IntegerType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class SpatialPredicateLegalityTest {
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
