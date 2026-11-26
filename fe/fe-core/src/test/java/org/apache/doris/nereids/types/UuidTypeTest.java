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

package org.apache.doris.nereids.types;

import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.util.TypeCoercionUtils;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.statistics.util.StatisticsUtil;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

class UuidTypeTest {
    private final NereidsParser parser = new NereidsParser();

    @Test
    void rejectParametersInEveryTypePosition() {
        Assertions.assertSame(UuidType.INSTANCE, parser.parseDataType("UUID"));
        for (String type : List.of("UUID(16)", "UUID(0)", "UUID(16, 0)",
                "ARRAY<UUID(36)>", "MAP<UUID(16), UUID>", "STRUCT<u:UUID(16)>")) {
            AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                    () -> parser.parseDataType(type), type);
            Assertions.assertTrue(error.getMessage().contains("UUID does not support"));
        }
    }

    @Test
    void preserveIdentityThroughCatalogThriftAndGson() {
        for (String sql : List.of("UUID", "ARRAY<UUID>", "MAP<UUID, UUID>",
                "STRUCT<u:UUID, nested:ARRAY<MAP<UUID, UUID>>>")) {
            DataType nereids = parser.parseDataType(sql);
            Type catalog = nereids.toCatalogDataType();
            Assertions.assertEquals(nereids, DataType.fromCatalogType(catalog), sql);
            Assertions.assertEquals(catalog, Type.fromThrift(catalog.toThrift()), sql);
            Assertions.assertEquals(catalog,
                    GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(catalog, Type.class), Type.class), sql);
            Assertions.assertEquals(nereids, parser.parseDataType(nereids.toSql()), sql);
        }
        Assertions.assertEquals(16, UuidType.INSTANCE.width());
        Assertions.assertFalse(UuidType.INSTANCE.isNumericType());
        Assertions.assertFalse(UuidType.INSTANCE.isStringLikeType());
    }

    @Test
    void commonTypesPreserveUuidAndNestedNulls() {
        for (DataType type : List.of(UuidType.INSTANCE, ArrayType.of(UuidType.INSTANCE),
                MapType.of(UuidType.INSTANCE, UuidType.INSTANCE))) {
            Assertions.assertEquals(type,
                    TypeCoercionUtils.findWiderTypeForTwo(type, type, false, false).orElseThrow());
            Assertions.assertEquals(type,
                    TypeCoercionUtils.findWiderTypeForTwo(type, NullType.INSTANCE, false, false).orElseThrow());
            Assertions.assertEquals(type,
                    TypeCoercionUtils.findWiderTypeForTwo(NullType.INSTANCE, type, false, false).orElseThrow());
        }
    }

    @Test
    void storageAggregatesPreserveUuidSemantics() {
        for (AggregateType aggregate : List.of(AggregateType.MIN, AggregateType.MAX,
                AggregateType.REPLACE, AggregateType.REPLACE_IF_NOT_NULL, AggregateType.NONE)) {
            Assertions.assertTrue(aggregate.checkCompatibility(PrimitiveType.UUID), aggregate.toSql());
        }
        for (AggregateType aggregate : List.of(AggregateType.SUM, AggregateType.HLL_UNION,
                AggregateType.BITMAP_UNION, AggregateType.QUANTILE_UNION)) {
            Assertions.assertFalse(aggregate.checkCompatibility(PrimitiveType.UUID), aggregate.toSql());
        }
    }

    @Test
    void statisticsUseUnsignedUuidOrdering() throws Exception {
        String[] values = {"00000000-0000-0000-0000-000000000000",
                "00000000-0000-0001-0000-000000000000",
                "7fffffff-ffff-ffff-ffff-ffffffffffff",
                "80000000-0000-0000-0000-000000000000",
                "ffffffff-ffff-ffff-ffff-ffffffffffff"};
        double previous = -1;
        for (String value : values) {
            double estimate = StatisticsUtil.convertToDouble(Type.UUID, value);
            Assertions.assertTrue(Double.isFinite(estimate));
            // Adjacent 128-bit values may share an approximate double statistic.
            Assertions.assertTrue(estimate >= previous);
            Assertions.assertEquals(value, StatisticsUtil.readableValue(Type.UUID, value).getStringValue());
            previous = estimate;
        }
        Assertions.assertEquals(StatisticsUtil.convertToDouble(Type.UUID, values[4]),
                StatisticsUtil.convertToDouble(Type.UUID, "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF"));
    }
}
