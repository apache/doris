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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.scalar.AddTime;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Timestamp;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Convert two-argument TIMESTAMP function to ADD_TIME function.
 */
public class TimestampToAddTime implements ExpressionPatternRuleFactory {

    public static final TimestampToAddTime INSTANCE = new TimestampToAddTime();

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return ImmutableList.of(
                matchesType(Timestamp.class).then(TimestampToAddTime::rewrite)
                        .toRule(ExpressionRuleType.TIMESTAMP_TO_ADD_TIME)
        );
    }

    public static Expression rewrite(Timestamp timestamp) {
        if (timestamp.arity() == 2) {
            return new AddTime(timestamp.child(0), timestamp.child(1));
        }
        return timestamp;
    }
}
