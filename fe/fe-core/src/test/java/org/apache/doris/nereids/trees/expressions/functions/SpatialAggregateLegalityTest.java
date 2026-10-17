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
import org.apache.doris.nereids.trees.expressions.functions.agg.CollectSet;
import org.apache.doris.nereids.trees.expressions.functions.agg.Histogram;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.functions.agg.Ndv;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.GeographyType;
import org.apache.doris.nereids.types.GeometryType;
import org.apache.doris.nereids.types.IntegerType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class SpatialAggregateLegalityTest {
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
