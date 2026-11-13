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

import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.FloatType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ApproximateDistanceTest {
    @Test
    public void testEmptyArraysCanProduceNullFromNonNullableArguments() {
        SlotReference vector = new SlotReference("vector", ArrayType.of(FloatType.INSTANCE), false);
        SlotReference query = new SlotReference("query", ArrayType.of(FloatType.INSTANCE), false);
        Assertions.assertTrue(new L2DistanceApproximate(vector, query).nullable());
        Assertions.assertTrue(new InnerProductApproximate(vector, query).nullable());
        Assertions.assertFalse(new L2Distance(vector, query).nullable());
    }
}
