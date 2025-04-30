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

import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.statistics.util.StatisticsUtil;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ColumnStatisticTest {

    @Test
    public void testIsValid() throws AnalysisException {
        // Ndv > 10 * count
        ColumnStatisticBuilder columnStatisticBuilder = new ColumnStatisticBuilder(100);
        columnStatisticBuilder.setNdv(1001);
        columnStatisticBuilder.setNumNulls(300);
        columnStatisticBuilder.setMinValue(StatisticsUtil.convertToDouble(Type.STRING, "min"));
        columnStatisticBuilder.setMinExpr(StatisticsUtil.readableValue(Type.STRING, "min"));
        columnStatisticBuilder.setMaxValue(StatisticsUtil.convertToDouble(Type.STRING, "max"));
        columnStatisticBuilder.setMaxExpr(StatisticsUtil.readableValue(Type.STRING, "max"));
        ColumnStatistic stats = columnStatisticBuilder.build();
        Assertions.assertFalse(stats.isValid());

        // Ndv == 0 and min or max is not null
        columnStatisticBuilder = new ColumnStatisticBuilder(stats);
        columnStatisticBuilder.setNdv(0);
        columnStatisticBuilder.setMaxValue(Double.POSITIVE_INFINITY);
        stats = columnStatisticBuilder.build();
        Assertions.assertFalse(stats.isValid());
        columnStatisticBuilder.setMaxValue(10);
        columnStatisticBuilder.setMinValue(Double.NEGATIVE_INFINITY);
        stats = columnStatisticBuilder.build();
        Assertions.assertFalse(stats.isValid());
        columnStatisticBuilder.setMaxValue(Double.POSITIVE_INFINITY);
        columnStatisticBuilder.setMinValue(Double.NEGATIVE_INFINITY);
        stats = columnStatisticBuilder.build();
        Assertions.assertTrue(stats.isValid());

        // Count > 0, ndv=0, min=null, max=null, count > numNulls * 10
        columnStatisticBuilder.setMaxValue(Double.POSITIVE_INFINITY);
        columnStatisticBuilder.setMinValue(Double.NEGATIVE_INFINITY);
        columnStatisticBuilder.setNdv(0);
        columnStatisticBuilder.setNumNulls(2);
        stats = columnStatisticBuilder.build();
        Assertions.assertFalse(stats.isValid());
        columnStatisticBuilder.setNumNulls(11);
        stats = columnStatisticBuilder.build();
        Assertions.assertTrue(stats.isValid());
    }
}
