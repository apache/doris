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

package org.apache.doris.nereids.trees.expressions.functions.scalar;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.BitmapType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.HllType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.QuantileStateType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

public class ComplexEqualsTest {
    @Test
    public void testRejectUnsupportedLeafTypes() {
        for (DataType type : Arrays.asList(BitmapType.INSTANCE, HllType.INSTANCE,
                QuantileStateType.INSTANCE, ArrayType.of(BitmapType.INSTANCE),
                MapType.of(IntegerType.INSTANCE, HllType.INSTANCE),
                new StructType(ImmutableList.of(new StructField("v", BitmapType.INSTANCE, true, ""))))) {
            Expression unsupported = new SlotReference("unsupported", type);
            Expression supported = new SlotReference("supported", IntegerType.INSTANCE);
            ComplexEquals function = new ComplexEquals(supported, unsupported);
            AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                    function::checkLegalityBeforeTypeCoercion);
            Assertions.assertTrue(error.getMessage().contains("complex_equals does not support argument type"));
            Assertions.assertThrows(AnalysisException.class, function::checkLegalityAfterRewrite);
        }
    }

    @Test
    public void testComparableNestedTypesRemainLegal() {
        DataType type = new StructType(ImmutableList.of(
                new StructField("a", ArrayType.of(IntegerType.INSTANCE), true, ""),
                new StructField("m", MapType.of(IntegerType.INSTANCE, IntegerType.INSTANCE), true, "")));
        ComplexEquals function = new ComplexEquals(new SlotReference("left", type), new SlotReference("right", type));
        Assertions.assertDoesNotThrow(function::checkLegalityBeforeTypeCoercion);
        Assertions.assertDoesNotThrow(function::checkLegalityAfterRewrite);
    }
}
