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

package org.apache.doris.nereids.parser;

import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.pattern.PatternDescriptor;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.GroupMatchingUtils;
import org.apache.doris.nereids.util.PatternMatchSupported;

import org.junit.jupiter.api.Assertions;

import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Base class to check SQL parsing result.
 */
public class ParserTestBase implements PatternMatchSupported {

    private static final NereidsParser PARSER = new NereidsParser();

    protected void planSuccess(String sql, PatternDescriptor<? extends Plan> patternDesc) {
        LogicalPlan parsed = PARSER.parseSingle(sql);
        Assertions.assertTrue(GroupMatchingUtils.topDownFindMatching(new Memo(parsed).getRoot(), patternDesc.pattern),
                () -> "pattern not match, \n input SQL:\n" + sql + "\n, parsed plan :\n" + parsed.treeString() + "\n"
        );
    }

    protected void planFailure(String sql, Predicate<Throwable> exceptionPredicate) {
        assertParseError(sql, PARSER::parseSingle, exceptionPredicate);
    }

    protected void planFailure(String sql, String exceptionMessage) {
        planFailure(sql, t -> t.getMessage().contains(exceptionMessage));
    }

    protected void exprSuccess(String sql, Expression expected) {
        Expression parsed = PARSER.parseExpression(sql);
        Assertions.assertEquals(expected, parsed);
    }

    protected void exprFailure(String sql, String errorMessage) {
        assertParseError(sql, PARSER::parseExpression, t -> t.getMessage().contains(errorMessage));
    }

    protected void assertParseError(String sql, Consumer<String> sqlConsumer, Predicate<Throwable> exceptionPredicate) {
        try {
            sqlConsumer.accept(sql);
        } catch (Throwable t) {
            Assertions.assertTrue(exceptionPredicate.test(t),
                    () -> "Input SQL:\n" + sql + "\nActually exception thrown:\n" + t);
        }
    }
}
