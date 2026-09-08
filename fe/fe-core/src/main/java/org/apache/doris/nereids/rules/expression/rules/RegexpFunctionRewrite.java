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
import org.apache.doris.nereids.trees.expressions.functions.scalar.RegexpExtract;
import org.apache.doris.nereids.trees.expressions.functions.scalar.RegexpReplace;
import org.apache.doris.nereids.trees.expressions.functions.scalar.RegexpReplaceOne;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLikeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Rewrites regexp functions to cheaper equivalent forms when the regexp shape proves the rewrite is safe.
 */
public class RegexpFunctionRewrite implements ExpressionPatternRuleFactory {
    public static final RegexpFunctionRewrite INSTANCE = new RegexpFunctionRewrite();

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return ImmutableList.of(
                matchesType(RegexpReplace.class)
                        .then(RegexpFunctionRewrite::rewriteRegexpReplace)
                        .toRule(ExpressionRuleType.REGEXP_FUNCTION_REWRITE),
                matchesType(RegexpExtract.class)
                        .then(RegexpFunctionRewrite::rewriteRegexpExtract)
                        .toRule(ExpressionRuleType.REGEXP_FUNCTION_REWRITE)
        );
    }

    private static Expression rewriteRegexpReplace(RegexpReplace regexpReplace) {
        String pattern = getStringLiteral(regexpReplace.child(1));
        if (pattern == null || pattern.isEmpty()) {
            return regexpReplace;
        }
        if (!startsWithUnescapedCaret(pattern) && !endsWithUnescapedDollar(pattern)) {
            return regexpReplace;
        }
        if (hasUnescapedAlternation(pattern) || hasInlineRegexpFlag(pattern, 'm')) {
            return regexpReplace;
        }

        if (regexpReplace.arity() == 3) {
            return new RegexpReplaceOne(regexpReplace.child(0), regexpReplace.child(1), regexpReplace.child(2));
        }
        return new RegexpReplaceOne(regexpReplace.child(0), regexpReplace.child(1), regexpReplace.child(2),
                regexpReplace.child(3));
    }

    private static Expression rewriteRegexpExtract(RegexpExtract regexpExtract) {
        String pattern = getStringLiteral(regexpExtract.child(1));
        if (pattern == null || pattern.isEmpty() || !isPositiveGroupIndex(regexpExtract.child(2))
                || !hasCapturingGroup(pattern) || hasUnescapedAlternation(pattern)
                || hasInlineRegexpFlag(pattern, 's')) {
            return regexpExtract;
        }

        String trimmedPattern = trimExtractPattern(pattern);
        if (trimmedPattern.equals(pattern)) {
            return regexpExtract;
        }
        return new RegexpExtract(regexpExtract.child(0), new VarcharLiteral(trimmedPattern), regexpExtract.child(2));
    }

    private static String trimExtractPattern(String pattern) {
        String trimmed = pattern;
        if (endsWithUnescapedDotStarDollar(trimmed)) {
            trimmed = trimmed.substring(0, trimmed.length() - 3);
        }
        return trimmed;
    }

    private static String getStringLiteral(Expression expression) {
        if (!(expression instanceof Literal) || !expression.getDataType().isStringLikeType()) {
            return null;
        }
        return ((Literal) expression).getStringValue();
    }

    private static boolean isPositiveGroupIndex(Expression expression) {
        return expression instanceof IntegerLikeLiteral && ((IntegerLikeLiteral) expression).getLongValue() >= 1;
    }

    private static boolean startsWithUnescapedCaret(String pattern) {
        return !pattern.isEmpty() && pattern.charAt(0) == '^';
    }

    private static boolean endsWithUnescapedDollar(String pattern) {
        int dollarPos = pattern.length() - 1;
        return dollarPos >= 0 && pattern.charAt(dollarPos) == '$'
                && isUnescaped(pattern, dollarPos) && !isInCharClass(pattern, dollarPos);
    }

    private static boolean endsWithUnescapedDotStarDollar(String pattern) {
        if (pattern.length() < 3 || !pattern.endsWith(".*$")) {
            return false;
        }
        int dotPos = pattern.length() - 3;
        return isUnescaped(pattern, dotPos) && !isInCharClass(pattern, dotPos);
    }

    private static boolean isUnescaped(String pattern, int pos) {
        int backslashCount = 0;
        for (int i = pos - 1; i >= 0 && pattern.charAt(i) == '\\'; i--) {
            backslashCount++;
        }
        return backslashCount % 2 == 0;
    }

    private static boolean hasUnescapedAlternation(String pattern) {
        boolean inCharClass = false;
        for (int i = 0; i < pattern.length(); i++) {
            char ch = pattern.charAt(i);
            if (!isUnescaped(pattern, i)) {
                continue;
            }
            if (ch == '[') {
                inCharClass = true;
            } else if (ch == ']' && inCharClass) {
                inCharClass = false;
            } else if (ch == '|' && !inCharClass) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasCapturingGroup(String pattern) {
        boolean inCharClass = false;
        for (int i = 0; i < pattern.length(); i++) {
            char ch = pattern.charAt(i);
            if (!isUnescaped(pattern, i)) {
                continue;
            }
            if (ch == '[') {
                inCharClass = true;
            } else if (ch == ']' && inCharClass) {
                inCharClass = false;
            } else if (ch == '(' && !inCharClass && (i + 1 >= pattern.length() || pattern.charAt(i + 1) != '?')) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasInlineRegexpFlag(String pattern, char targetFlag) {
        boolean inCharClass = false;
        for (int i = 0; i < pattern.length() - 2; i++) {
            char ch = pattern.charAt(i);
            if (!isUnescaped(pattern, i)) {
                continue;
            }
            if (ch == '[') {
                inCharClass = true;
            } else if (ch == ']' && inCharClass) {
                inCharClass = false;
            } else if (ch == '(' && !inCharClass && pattern.charAt(i + 1) == '?'
                    && isInlineFlagChar(pattern.charAt(i + 2))) {
                for (int j = i + 2; j < pattern.length(); j++) {
                    char flag = pattern.charAt(j);
                    if (flag == ':' || flag == ')') {
                        break;
                    }
                    if (flag == targetFlag) {
                        return true;
                    }
                    if (!isInlineFlagChar(flag)) {
                        break;
                    }
                }
            }
        }
        return false;
    }

    private static boolean isInlineFlagChar(char ch) {
        return ch == 'i' || ch == 'm' || ch == 's' || ch == 'U' || ch == '-';
    }

    private static boolean isInCharClass(String pattern, int pos) {
        boolean inCharClass = false;
        for (int i = 0; i < pos; i++) {
            char ch = pattern.charAt(i);
            if (!isUnescaped(pattern, i)) {
                continue;
            }
            if (ch == '[') {
                inCharClass = true;
            } else if (ch == ']' && inCharClass) {
                inCharClass = false;
            }
        }
        return inCharClass;
    }
}
