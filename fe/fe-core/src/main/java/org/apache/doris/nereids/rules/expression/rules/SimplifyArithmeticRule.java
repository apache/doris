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
import org.apache.doris.nereids.trees.expressions.BinaryArithmetic;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Divide;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Multiply;
import org.apache.doris.nereids.trees.expressions.Subtract;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.util.TypeCoercionUtils;
import org.apache.doris.nereids.util.TypeUtils;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

/**
 * Simplify arithmetic rule.
 * This rule run before `FoldConstantRule`.
 * For example:
 * a + 1 + b - 2 - ((c - d) + 1) => a + b - c + d + (1 - 2 - 1)
 * After `FoldConstantRule`:
 * a + b - c + d + (1 - 2 - 1) => a + b - c + d - 2
 *
 * TODO: handle cases like: '1 - IA < 1' to 'IA > 0'
 */
public class SimplifyArithmeticRule implements ExpressionPatternRuleFactory {
    public static final SimplifyArithmeticRule INSTANCE = new SimplifyArithmeticRule();

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return ImmutableList.of(
                matchesTopType(BinaryArithmetic.class).then(SimplifyArithmeticRule::simplify)
                        .toRule(ExpressionRuleType.SIMPLIFY_ARITHMETIC)
        );
    }

    /** simplify */
    public static Expression simplify(BinaryArithmetic binaryArithmetic) {
        if (binaryArithmetic instanceof Add || binaryArithmetic instanceof Subtract) {
            return process(binaryArithmetic, true);
        } else if (binaryArithmetic instanceof Multiply || binaryArithmetic instanceof Divide) {
            return process(binaryArithmetic, false);
        }
        return binaryArithmetic;
    }

    /**
     * The main logic is as follows:
     * 1.flatten the arithmetic expression.
     *   a + 1 + b - 2 - ((c - d) + 1) => a + 1 + b -2 - c + d - 1
     * 2.move variables to left side and move constants to right sid.
     *   a + 1 + b -2 - c + d - 1 => a + b - c + d + 1 - 2 - 1
     * 3.build new arithmetic expression.
     *   (a + b - c + d) + (1 - 2 - 1)
     */
    private static Expression process(BinaryArithmetic arithmetic, boolean isAddOrSub) {
        // 1. flatten the arithmetic expression.
        List<Operand> flattedExpressions = flatten(arithmetic, isAddOrSub);

        List<Operand> variables = Lists.newArrayList();
        List<Operand> constants = Lists.newArrayList();

        // TODO currently we don't process decimal for simplicity.
        for (Operand operand : flattedExpressions) {
            if (operand.expression.getDataType().isDecimalLikeType()) {
                return arithmetic;
            }
        }
        // 2. move variables to left side and move constants to right sid.
        for (Operand operand : flattedExpressions) {
            if (operand.expression instanceof BinaryArithmetic) {
                operand.expression = simplify((BinaryArithmetic) operand.expression);
            }
            if (operand.expression.isConstant()) {
                constants.add(operand);
            } else {
                variables.add(operand);
            }
        }

        // 3. build new arithmetic expression.
        if (!constants.isEmpty()) {
            boolean isOpposite = !constants.get(0).flag;
            Optional<Operand> c = Utils.fastReduce(constants, (x, y) -> {
                Expression expr;
                if (isOpposite && y.flag || !isOpposite && !y.flag) {
                    expr = getSubOrDivide(isAddOrSub, x, y);
                } else {
                    expr = getAddOrMultiply(isAddOrSub, x, y);
                }
                return Operand.of(true, expr);
            });
            boolean firstVariableFlag = variables.isEmpty() || variables.get(0).flag;
            if (isOpposite || firstVariableFlag) {
                variables.add(Operand.of(!isOpposite, c.get().expression));
            } else {
                variables.add(0, Operand.of(!isOpposite, c.get().expression));
            }
        }

        Optional<Operand> result = Utils.fastReduce(variables, (x, y) -> !y.flag
                ? Operand.of(true, getSubOrDivide(isAddOrSub, x, y))
                : Operand.of(true, getAddOrMultiply(isAddOrSub, x, y))
        );
        if (result.isPresent()) {
            return TypeCoercionUtils.castIfNotSameType(result.get().expression, arithmetic.getDataType());
        } else {
            return arithmetic;
        }
    }

    // isAddOrSub: true for extract only "+" or "-" sub expressions, false for extract only "*" or "/" sub expressions
    private static List<Operand> flatten(Expression expr, boolean isAddOrSub) {
        List<Operand> result = Lists.newArrayList();
        doFlatten(true, expr, isAddOrSub, result, Optional.empty());
        return result;
    }

    // flag: true for '+' or '*', false for '-' or '/'
    // isAddOrSub: true for extract only "+" or "-" sub expressions, false for extract only "*" or "/" sub expressions
    private static void doFlatten(boolean flag, Expression expr, boolean isAddOrSub, List<Operand> result,
            Optional<DataType> castType) {
        // cast (a * 10 as double)  *  (cast 20 as double)
        // => cast(a as double) * (cast 10 as double) * (cast 20 as double)
        BinaryArithmetic arithmetic = null;
        Predicate<Expression> isPositiveArithmetic = isAddOrSub
                ? TypeUtils::isAdd : TypeUtils::isMultiply;
        Predicate<Expression> isNegativeArithmetic = isAddOrSub
                ? TypeUtils::isSubtract : TypeUtils::isDivide;
        Predicate<Expression> isPosNegArithmetic = isPositiveArithmetic.or(isNegativeArithmetic);
        if (isPosNegArithmetic.test(expr)) {
            arithmetic = (BinaryArithmetic) expr;
        } else if (expr instanceof Cast && hasConstantOperand(expr, isAddOrSub)) {
            Cast cast = (Cast) expr;
            if (isPosNegArithmetic.test(cast.child())) {
                arithmetic = (BinaryArithmetic) cast.child();
                castType = Optional.of(cast.getDataType());
            }
        }
        if (arithmetic != null) {
            doFlatten(flag, arithmetic.left(), isAddOrSub, result, castType);
            if (isNegativeArithmetic.test(arithmetic) && !flag) {
                doFlatten(true, arithmetic.right(), isAddOrSub, result, castType);
            } else if (isPositiveArithmetic.test(arithmetic) && !flag) {
                doFlatten(false, arithmetic.right(), isAddOrSub, result, castType);
            } else {
                doFlatten(!isNegativeArithmetic.test(arithmetic), arithmetic.right(), isAddOrSub, result, castType);
            }
        } else {
            if (castType.isPresent()) {
                result.add(Operand.of(flag, TypeCoercionUtils.castIfNotSameType(expr, castType.get())));
            } else {
                result.add(Operand.of(flag, expr));
            }
        }
    }

    private static boolean hasConstantOperand(Expression expr, boolean isAddOrSub) {
        if (expr.isConstant()) {
            return true;
        }

        Predicate<Expression> checkArithmetic = isAddOrSub
                ? TypeUtils::isAddOrSubtract : TypeUtils::isMultiplyOrDivide;
        BinaryArithmetic arithmetic = null;
        if (checkArithmetic.test(expr)) {
            arithmetic = (BinaryArithmetic) expr;
        } else if (expr instanceof Cast) {
            Cast cast = (Cast) expr;
            if (checkArithmetic.test(cast.child())) {
                arithmetic = (BinaryArithmetic) cast.child();
            }
        }
        if (arithmetic != null) {
            return hasConstantOperand(arithmetic.left(), isAddOrSub)
                    || hasConstantOperand(arithmetic.right(), isAddOrSub);
        } else {
            return false;
        }
    }

    private static Expression getSubOrDivide(boolean isSubOrDivide, Operand x, Operand y) {
        return isSubOrDivide ? new Subtract(x.expression, y.expression)
                : new Divide(x.expression, y.expression);
    }

    private static Expression getAddOrMultiply(boolean isAddOrMultiply, Operand x, Operand y) {
        return isAddOrMultiply ? new Add(x.expression, y.expression)
                : new Multiply(x.expression, y.expression);
    }

    /**
     * Operational operand.
     * `flag` is true indicates that the operand's operator is `Add`,
     *  otherwise operand's operator is `Subtract in add-subtract.
     *  `flag` is true indicates that the operand's operator is `Multiply`,
     *  otherwise operand's operator is `Divide in multiply-divide.
     */
    public static class Operand {
        boolean flag;
        Expression expression;

        public Operand(boolean flag, Expression expression) {
            this.flag = flag;
            this.expression = expression;
        }

        public static Operand of(boolean flag, Expression expression) {
            return new Operand(flag, expression);
        }

        @Override
        public String toString() {
            return flag + " : " + expression;
        }
    }
}

