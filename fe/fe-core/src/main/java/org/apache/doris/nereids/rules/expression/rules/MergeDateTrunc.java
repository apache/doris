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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DateTrunc;
import org.apache.doris.nereids.trees.expressions.literal.Interval.TimeUnit;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Optional;

/**
 * Rewrite rule to convert
 * For example:
 * date_trunc(date_trunc(data_slot, 'hour'), 'day') -> date_trunc(data_slot, 'day')
 */
public class MergeDateTrunc implements ExpressionPatternRuleFactory {

    public static MergeDateTrunc INSTANCE = new MergeDateTrunc();
    public static ImmutableSet<TimeUnit> UN_SUPPORT_TIME_UNIT = ImmutableSet.of(TimeUnit.WEEK, TimeUnit.QUARTER);

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return ImmutableList.of(
                matchesTopType(DateTrunc.class)
                        .when(dateTrunc -> dateTrunc.getArgument(0) instanceof DateTrunc)
                        .then(MergeDateTrunc::rewrite)
        );
    }

    private static Expression rewrite(DateTrunc dateTrunc) {

        Expression parentTimeUnitArgument = dateTrunc.getArgument(1);
        if (!(parentTimeUnitArgument instanceof Literal)) {
            return dateTrunc;
        }
        Optional<TimeUnit> parentTimeUnit = TimeUnit.of(((Literal) parentTimeUnitArgument).getStringValue());
        DateTrunc childDateTrunc = (DateTrunc) dateTrunc.getArgument(0);
        Expression childTimeUnitArgument = childDateTrunc.getArgument(1);
        if (!(childTimeUnitArgument instanceof Literal)) {
            return dateTrunc;
        }
        Optional<TimeUnit> childTimeUnit = TimeUnit.of(((Literal) childTimeUnitArgument).getStringValue());
        if (!parentTimeUnit.isPresent() || !childTimeUnit.isPresent()) {
            return dateTrunc;
        }
        if (UN_SUPPORT_TIME_UNIT.contains(parentTimeUnit.get())
                || UN_SUPPORT_TIME_UNIT.contains(childTimeUnit.get())) {
            return dateTrunc;
        }
        if (parentTimeUnit.get().getLevel() < childTimeUnit.get().getLevel()) {
            return dateTrunc;
        }
        return new DateTrunc(childDateTrunc.getArgument(0), new VarcharLiteral(parentTimeUnit.get().toString()));
    }
}
