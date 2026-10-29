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

package org.apache.doris.alter;

import org.apache.doris.catalog.AggStateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.DdlException;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class SchemaChangeTypeConversionTest {
    @Test
    void testAggregateStateResultSignature() throws DdlException {
        for (String function : ImmutableList.of("sum", "avg", "sum_map", "avg_map")) {
            Type input = ScalarType.createDecimalV3Type(18, 2);
            Type result128 = ScalarType.createDecimalV3Type(38, 2);
            Type result256 = ScalarType.createDecimalV3Type(76, 2);
            if (function.endsWith("_map")) {
                input = new MapType(Type.INT, input);
                result128 = new MapType(Type.INT, result128);
                result256 = new MapType(Type.INT, result256);
            }
            Column state128 = new Column("s", new AggStateType(function, true,
                    ImmutableList.of(input), ImmutableList.of(true), result128));
            Column state256 = new Column("s", new AggStateType(function, true,
                    ImmutableList.of(input), ImmutableList.of(true), result256));
            SchemaChangeHandler.checkColumnTypeConversion(state128, state128);
            SchemaChangeHandler.checkColumnTypeConversion(state256, state256);
            Assertions.assertThrows(DdlException.class,
                    () -> SchemaChangeHandler.checkColumnTypeConversion(state128, state256));
            Assertions.assertThrows(DdlException.class,
                    () -> SchemaChangeHandler.checkColumnTypeConversion(state256, state128));
        }
    }

    @Test
    void testOrdinaryConversions() throws DdlException {
        SchemaChangeHandler.checkColumnTypeConversion(new Column("v", Type.INT), new Column("v", Type.BIGINT));
        SchemaChangeHandler.checkColumnTypeConversion(new Column("v", ScalarType.createVarcharType(10)),
                new Column("v", ScalarType.createVarcharType(20)));
    }
}
