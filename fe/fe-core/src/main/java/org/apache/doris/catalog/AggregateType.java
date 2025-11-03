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

package org.apache.doris.catalog;

import org.apache.doris.thrift.TAggregationType;

import com.google.common.collect.Lists;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public enum AggregateType {
    SUM("SUM"),
    MIN("MIN"),
    MAX("MAX"),
    REPLACE("REPLACE"),
    REPLACE_IF_NOT_NULL("REPLACE_IF_NOT_NULL"),
    HLL_UNION("HLL_UNION"),
    NONE("NONE"),
    BITMAP_UNION("BITMAP_UNION"),
    QUANTILE_UNION("QUANTILE_UNION"),
    GENERIC("GENERIC");

    private static EnumMap<AggregateType, EnumSet<PrimitiveType>> compatibilityMap;

    private static final Map<String, AggregateType> aggTypeMap = new HashMap<>();

    static {
        aggTypeMap.put("NONE", AggregateType.NONE);
        aggTypeMap.put("SUM", AggregateType.SUM);
        aggTypeMap.put("MIN", AggregateType.MIN);
        aggTypeMap.put("MAX", AggregateType.MAX);
        aggTypeMap.put("REPLACE", AggregateType.REPLACE);
        aggTypeMap.put("REPLACE_IF_NOT_NULL", AggregateType.REPLACE_IF_NOT_NULL);
        aggTypeMap.put("HLL_UNION", AggregateType.HLL_UNION);
        aggTypeMap.put("BITMAP_UNION", AggregateType.BITMAP_UNION);
        aggTypeMap.put("QUANTILE_UNION", AggregateType.QUANTILE_UNION);
    }

    static {
        compatibilityMap = new EnumMap<>(AggregateType.class);
        List<PrimitiveType> primitiveTypeList = Lists.newArrayList();

        primitiveTypeList.add(PrimitiveType.TINYINT);
        primitiveTypeList.add(PrimitiveType.SMALLINT);
        primitiveTypeList.add(PrimitiveType.INT);
        primitiveTypeList.add(PrimitiveType.BIGINT);
        primitiveTypeList.add(PrimitiveType.LARGEINT);
        primitiveTypeList.add(PrimitiveType.FLOAT);
        primitiveTypeList.add(PrimitiveType.DOUBLE);
        primitiveTypeList.add(PrimitiveType.DECIMALV2);
        primitiveTypeList.add(PrimitiveType.DECIMAL32);
        primitiveTypeList.add(PrimitiveType.DECIMAL64);
        primitiveTypeList.add(PrimitiveType.DECIMAL128);
        compatibilityMap.put(SUM, EnumSet.copyOf(primitiveTypeList));

        primitiveTypeList.clear();
        primitiveTypeList.add(PrimitiveType.TINYINT);
        primitiveTypeList.add(PrimitiveType.SMALLINT);
        primitiveTypeList.add(PrimitiveType.INT);
        primitiveTypeList.add(PrimitiveType.BIGINT);
        primitiveTypeList.add(PrimitiveType.LARGEINT);
        primitiveTypeList.add(PrimitiveType.FLOAT);
        primitiveTypeList.add(PrimitiveType.DOUBLE);
        primitiveTypeList.add(PrimitiveType.DECIMALV2);
        primitiveTypeList.add(PrimitiveType.DECIMAL32);
        primitiveTypeList.add(PrimitiveType.DECIMAL64);
        primitiveTypeList.add(PrimitiveType.DECIMAL128);
        primitiveTypeList.add(PrimitiveType.DATE);
        primitiveTypeList.add(PrimitiveType.DATETIME);
        primitiveTypeList.add(PrimitiveType.DATEV2);
        primitiveTypeList.add(PrimitiveType.DATETIMEV2);
        primitiveTypeList.add(PrimitiveType.CHAR);
        primitiveTypeList.add(PrimitiveType.VARCHAR);
        primitiveTypeList.add(PrimitiveType.STRING);
        compatibilityMap.put(MIN, EnumSet.copyOf(primitiveTypeList));

        primitiveTypeList.clear();
        primitiveTypeList.add(PrimitiveType.TINYINT);
        primitiveTypeList.add(PrimitiveType.SMALLINT);
        primitiveTypeList.add(PrimitiveType.INT);
        primitiveTypeList.add(PrimitiveType.BIGINT);
        primitiveTypeList.add(PrimitiveType.LARGEINT);
        primitiveTypeList.add(PrimitiveType.FLOAT);
        primitiveTypeList.add(PrimitiveType.DOUBLE);
        primitiveTypeList.add(PrimitiveType.DECIMALV2);
        primitiveTypeList.add(PrimitiveType.DECIMAL32);
        primitiveTypeList.add(PrimitiveType.DECIMAL64);
        primitiveTypeList.add(PrimitiveType.DECIMAL128);
        primitiveTypeList.add(PrimitiveType.DATE);
        primitiveTypeList.add(PrimitiveType.DATETIME);
        primitiveTypeList.add(PrimitiveType.DATEV2);
        primitiveTypeList.add(PrimitiveType.DATETIMEV2);
        primitiveTypeList.add(PrimitiveType.CHAR);
        primitiveTypeList.add(PrimitiveType.VARCHAR);
        primitiveTypeList.add(PrimitiveType.STRING);
        compatibilityMap.put(MAX, EnumSet.copyOf(primitiveTypeList));

        primitiveTypeList.clear();
        // all types except agg_state.
        EnumSet<PrimitiveType> excObjectStored = EnumSet.allOf(PrimitiveType.class);
        excObjectStored.remove(PrimitiveType.AGG_STATE);
        compatibilityMap.put(REPLACE, EnumSet.copyOf(excObjectStored));

        compatibilityMap.put(REPLACE_IF_NOT_NULL, EnumSet.copyOf(excObjectStored));

        primitiveTypeList.clear();
        primitiveTypeList.add(PrimitiveType.HLL);
        compatibilityMap.put(HLL_UNION, EnumSet.copyOf(primitiveTypeList));

        primitiveTypeList.clear();
        primitiveTypeList.add(PrimitiveType.BITMAP);
        compatibilityMap.put(BITMAP_UNION, EnumSet.copyOf(primitiveTypeList));

        primitiveTypeList.clear();
        primitiveTypeList.add(PrimitiveType.QUANTILE_STATE);
        compatibilityMap.put(QUANTILE_UNION, EnumSet.copyOf(primitiveTypeList));

        compatibilityMap.put(NONE, EnumSet.copyOf(excObjectStored));
    }

    private final String sqlName;

    private AggregateType(String sqlName) {
        this.sqlName = sqlName;
    }

    public static boolean checkCompatibility(AggregateType aggType, PrimitiveType priType) {
        return compatibilityMap.get(aggType).contains(priType);
    }

    public String toSql() {
        return sqlName;
    }

    @Override
    public String toString() {
        return toSql();
    }

    public boolean checkCompatibility(PrimitiveType priType) {
        return checkCompatibility(this, priType);
    }

    public boolean isReplaceFamily() {
        switch (this) {
            case REPLACE:
            case REPLACE_IF_NOT_NULL:
                return true;
            default:
                return false;
        }
    }

    public TAggregationType toThrift() {
        switch (this) {
            case SUM:
                return TAggregationType.SUM;
            case MAX:
                return TAggregationType.MAX;
            case MIN:
                return TAggregationType.MIN;
            case REPLACE:
                return TAggregationType.REPLACE;
            case REPLACE_IF_NOT_NULL:
                return TAggregationType.REPLACE_IF_NOT_NULL;
            case NONE:
                return TAggregationType.NONE;
            case HLL_UNION:
                return TAggregationType.HLL_UNION;
            case BITMAP_UNION:
                return TAggregationType.BITMAP_UNION;
            case QUANTILE_UNION:
                return TAggregationType.QUANTILE_UNION;
            default:
                return null;
        }
    }

    public static AggregateType getAggTypeFromAggName(String typeName) {
        return aggTypeMap.get(typeName);
    }
}
