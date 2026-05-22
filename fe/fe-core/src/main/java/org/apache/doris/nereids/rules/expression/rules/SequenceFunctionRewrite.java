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

package org.apache.doris.nereids.rules.expression.rules;

import org.apache.doris.nereids.rules.expression.ExpressionPatternMatcher;
import org.apache.doris.nereids.rules.expression.ExpressionPatternRuleFactory;
import org.apache.doris.nereids.rules.expression.ExpressionRuleType;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayRange;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayRangeDayUnit;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayRangeHourUnit;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayRangeMinuteUnit;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayRangeMonthUnit;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayRangeQuarterUnit;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayRangeSecondUnit;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayRangeWeekUnit;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayRangeYearUnit;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DaysAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.HoursAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MinutesAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MonthsAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.QuartersAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.SecondsAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Sequence;
import org.apache.doris.nereids.trees.expressions.functions.scalar.SequenceDayUnit;
import org.apache.doris.nereids.trees.expressions.functions.scalar.SequenceHourUnit;
import org.apache.doris.nereids.trees.expressions.functions.scalar.SequenceMinuteUnit;
import org.apache.doris.nereids.trees.expressions.functions.scalar.SequenceMonthUnit;
import org.apache.doris.nereids.trees.expressions.functions.scalar.SequenceQuarterUnit;
import org.apache.doris.nereids.trees.expressions.functions.scalar.SequenceSecondUnit;
import org.apache.doris.nereids.trees.expressions.functions.scalar.SequenceWeekUnit;
import org.apache.doris.nereids.trees.expressions.functions.scalar.SequenceYearUnit;
import org.apache.doris.nereids.trees.expressions.functions.scalar.WeeksAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.YearsAdd;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.types.IntegerType;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * rewrite sequence function with sequence function
 */
public class SequenceFunctionRewrite implements ExpressionPatternRuleFactory {

    public static SequenceFunctionRewrite INSTANCE = new SequenceFunctionRewrite();

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return ImmutableList.of(
                matchesType(Sequence.class).then(sequence -> {
                    if (sequence.children().size() == 1) {
                        if (sequence.child(0).getDataType() instanceof IntegerType) {
                            return new ArrayRange(new Cast(new Add(sequence.child(0),
                                    new IntegerLiteral(1)), IntegerType.INSTANCE));
                        }
                    }
                    if (sequence.children().size() == 2) {
                        if (sequence.children().get(0).getDataType() instanceof IntegerType) {
                            return new ArrayRange(sequence.child(0),
                                    new Cast(new Add(sequence.child(1), new IntegerLiteral(1)),
                                            IntegerType.INSTANCE));
                        }
                    }
                    if (sequence.children().size() == 3) {
                        if (sequence.children().get(0).getDataType() instanceof IntegerType) {
                            return new ArrayRange(sequence.child(0),
                                    new Cast(new Add(sequence.child(1), new IntegerLiteral(1)),
                                            IntegerType.INSTANCE), sequence.children().get(2));
                        }
                    }
                    return sequence;
                }).toRule(ExpressionRuleType.SEQUENCE_FUNCTION_REWRITE),
                matchesType(SequenceHourUnit.class).then(sequenceHourUnit -> {
                    return new ArrayRangeHourUnit(
                            sequenceHourUnit.child(0),
                            new HoursAdd(sequenceHourUnit.child(1), new IntegerLiteral(1)),
                            sequenceHourUnit.child(2));
                }).toRule(ExpressionRuleType.SEQUENCE_FUNCTION_REWRITE),
                matchesType(SequenceMinuteUnit.class).then(sequenceMinuteUnit -> {
                    return new ArrayRangeMinuteUnit(
                            sequenceMinuteUnit.child(0),
                            new MinutesAdd(sequenceMinuteUnit.child(1), new BigIntLiteral(1)),
                            sequenceMinuteUnit.child(2));
                }).toRule(ExpressionRuleType.SEQUENCE_FUNCTION_REWRITE),
                matchesType(SequenceSecondUnit.class).then(sequenceSecondUnit -> {
                    return new ArrayRangeSecondUnit(
                            sequenceSecondUnit.child(0),
                            new SecondsAdd(sequenceSecondUnit.child(1), new BigIntLiteral(1)),
                            sequenceSecondUnit.child(2));
                }).toRule(ExpressionRuleType.SEQUENCE_FUNCTION_REWRITE),
                matchesType(SequenceYearUnit.class).then(sequenceYearUnit -> {
                    return new ArrayRangeYearUnit(
                            sequenceYearUnit.child(0),
                            new YearsAdd(sequenceYearUnit.child(1), new IntegerLiteral(1)),
                            sequenceYearUnit.child(2));
                }).toRule(ExpressionRuleType.SEQUENCE_FUNCTION_REWRITE),
                matchesType(SequenceWeekUnit.class).then(sequenceWeekUnit -> {
                    return new ArrayRangeWeekUnit(
                            sequenceWeekUnit.child(0),
                            new WeeksAdd(sequenceWeekUnit.child(1), new IntegerLiteral(1)),
                            sequenceWeekUnit.child(2));
                }).toRule(ExpressionRuleType.SEQUENCE_FUNCTION_REWRITE),
                matchesType(SequenceMonthUnit.class).then(sequenceMonthUnit -> {
                    return new ArrayRangeMonthUnit(
                            sequenceMonthUnit.child(0),
                            new MonthsAdd(sequenceMonthUnit.child(1), new IntegerLiteral(1)),
                            sequenceMonthUnit.child(2));
                }).toRule(ExpressionRuleType.SEQUENCE_FUNCTION_REWRITE),
                matchesType(SequenceQuarterUnit.class).then(sequenceQuarterUnit -> {
                    return new ArrayRangeQuarterUnit(
                            sequenceQuarterUnit.child(0),
                            new QuartersAdd(sequenceQuarterUnit.child(1), new IntegerLiteral(1)),
                            sequenceQuarterUnit.child(2));
                }).toRule(ExpressionRuleType.SEQUENCE_FUNCTION_REWRITE),
                matchesType(SequenceDayUnit.class).then(sequenceDayUnit -> {
                    return new ArrayRangeDayUnit(
                            sequenceDayUnit.child(0),
                            new DaysAdd(sequenceDayUnit.child(1), new IntegerLiteral(1)),
                            sequenceDayUnit.child(2));
                }).toRule(ExpressionRuleType.SEQUENCE_FUNCTION_REWRITE)

        );
    }
}
