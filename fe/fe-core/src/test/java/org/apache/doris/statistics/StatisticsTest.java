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

package org.apache.doris.statistics;

import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.types.IntegerType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class StatisticsTest {
    @Test
    public void testAvgSizeAbnormal() {
        SlotReference slot = SlotReference.of("a", IntegerType.INSTANCE);
        ColumnStatisticBuilder colBuilder = new ColumnStatisticBuilder();
        colBuilder.setAvgSizeByte(Double.NaN);
        Statistics stats = new Statistics(1, 1, ImmutableMap.of(slot, colBuilder.build()));
        double tupleSize = stats.computeTupleSize(ImmutableList.of(slot));
        Assertions.assertEquals(1, tupleSize);

        colBuilder.setAvgSizeByte(Double.POSITIVE_INFINITY);
        stats = new Statistics(1, 1, ImmutableMap.of(slot, colBuilder.build()));
        tupleSize = stats.computeTupleSize(ImmutableList.of(slot));
        Assertions.assertEquals(1, tupleSize);

        colBuilder.setAvgSizeByte(Double.NEGATIVE_INFINITY);
        stats = new Statistics(1, 1, ImmutableMap.of(slot, colBuilder.build()));
        tupleSize = stats.computeTupleSize(ImmutableList.of(slot));
        Assertions.assertEquals(1, tupleSize);

        colBuilder.setAvgSizeByte(-1.0);
        stats = new Statistics(1, 1, ImmutableMap.of(slot, colBuilder.build()));
        tupleSize = stats.computeTupleSize(ImmutableList.of(slot));
        Assertions.assertEquals(1, tupleSize);
    }
}
