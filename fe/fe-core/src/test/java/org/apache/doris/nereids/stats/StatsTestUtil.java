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

package org.apache.doris.nereids.stats;

import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.FloatType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.ColumnStatisticBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class StatsTestUtil {
    public static final float HOT_VALUE_PERCENTAGE = 0.30f;
    public static StatsTestUtil instance = new StatsTestUtil();
    private static final NereidsParser PARSER = new NereidsParser();

    /**
     *
     * create column statistic
     * guess data type by column name prefix
     *  i -> int
     *  f -> float
     *  d -> datetime
     *  str -> string
     *
     */
    public ColumnStatistic createColumnStatistic(
            String colName,
            double ndv, double rowCount,
            String minExprStr, String maxExprStr,
            double numNulls,
            String[] hot) {
        DataType dataType = guessDataType(colName);
        Map<Literal, Float> hotValues = null;
        if (hot.length > 0) {
            hotValues = new HashMap<>();
            for (String oneHot : hot) {
                Literal literal = castStrToLiteral(oneHot, dataType);
                hotValues.put(literal, HOT_VALUE_PERCENTAGE);
            }
        }
        LiteralExpr minExpr = null;
        double minValue = Double.NEGATIVE_INFINITY;
        if (minExprStr != null) {
            Literal literal = castStrToLiteral(minExprStr, dataType);
            minExpr = literal.toLegacyLiteral();
            minValue = literal.getDouble();
        }
        LiteralExpr maxExpr = null;
        double maxValue = Double.POSITIVE_INFINITY;
        if (maxExprStr != null) {
            Literal literal = castStrToLiteral(maxExprStr, dataType);
            maxExpr = literal.toLegacyLiteral();
            maxValue = literal.getDouble();
        }

        return new ColumnStatisticBuilder(rowCount)
                .setNdv(ndv)
                .setMinExpr(minExpr)
                .setMinValue(minValue)
                .setMaxExpr(maxExpr)
                .setMaxValue(maxValue)
                .setAvgSizeByte(4)
                .setNumNulls(numNulls)
                .setHotValues(hotValues)
                .build();
    }

    public ColumnStatistic createColumnStatistic(
            String colName,
            double ndv, double rowCount,
            String minExprStr, String maxExprStr,
            double numNulls,
            Map<String, Float> hot) {
        DataType dataType = guessDataType(colName);
        Map<Literal, Float> hotValues = null;
        if (hot.size() > 0) {
            hotValues = new HashMap<>();
            for (String oneHot : hot.keySet()) {
                Literal literal = castStrToLiteral(oneHot, dataType);
                hotValues.put(literal, hot.get(oneHot));
            }
        }
        LiteralExpr minExpr = null;
        double minValue = Double.NEGATIVE_INFINITY;
        if (minExprStr != null) {
            Literal literal = castStrToLiteral(minExprStr, dataType);
            minExpr = literal.toLegacyLiteral();
            minValue = literal.getDouble();
        }
        LiteralExpr maxExpr = null;
        double maxValue = Double.POSITIVE_INFINITY;
        if (maxExprStr != null) {
            Literal literal = castStrToLiteral(maxExprStr, dataType);
            maxExpr = literal.toLegacyLiteral();
            maxValue = literal.getDouble();
        }

        return new ColumnStatisticBuilder(rowCount)
                .setNdv(ndv)
                .setMinExpr(minExpr)
                .setMinValue(minValue)
                .setMaxExpr(maxExpr)
                .setMaxValue(maxValue)
                .setAvgSizeByte(4)
                .setNumNulls(numNulls)
                .setHotValues(hotValues)
                .build();
    }

    private Literal castStrToLiteral(String str, DataType dataType) {
        Literal literal = new StringLiteral(str);
        try {
            return (Literal) literal.checkedCastTo(dataType);
        } catch (Exception e) {
            // ignore
        }
        return literal;
    }

    private DataType guessDataType(String value) {
        if (value.startsWith("str")) {
            return StringType.INSTANCE;
        }
        if (value.startsWith("i")) {
            return IntegerType.INSTANCE;
        }
        if (value.startsWith("f")) {
            return FloatType.INSTANCE;
        }
        if (value.startsWith("d")) {
            return DateTimeV2Type.SYSTEM_DEFAULT;
        }
        throw new RuntimeException("cannot guess datatype by name: " + value);
    }

    /**
     *
     * create bounded expression.
     * guess data type by column name prefix
     *   i -> int
     *   f -> float
     *   d -> datetime
     *   str -> string
     */
    public Pair<Expression, ArrayList<SlotReference>> createExpr(String expr) {
        ArrayList<SlotReference> slots = new ArrayList<>();
        Expression unbound = PARSER.parseExpression(expr);
        Map<String, SlotReference> slotNameMap = new HashMap<>();
        Expression bound = unbound.rewriteDownShortCircuit(e -> {
            if (e instanceof UnboundSlot) {
                String colName = ((UnboundSlot) e).getName();
                SlotReference slot = slotNameMap.get(colName);
                if (slot == null) {
                    slot = new SlotReference(colName, guessDataType(colName));
                    slots.add(slot);
                }
                return slot;
            } else {
                return e;
            }
        });
        return Pair.of(bound, slots);
    }

    public Plan dummyPlan() {
        return new DummyPlan();
    }

}
