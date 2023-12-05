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

package org.apache.doris.common.util;

import org.apache.doris.catalog.AggregateType;

import java.util.HashMap;
import java.util.Map;

public class AggregateTypeConverter {
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

    public static AggregateType getAggTypeFromAggName(String aggName) {
        return aggTypeMap.getOrDefault(aggName, aggName.isEmpty() ? AggregateType.NONE
                                                            : AggregateType.GENERIC_AGGREGATION);
    }
}
