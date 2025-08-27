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

package org.apache.doris.nereids.trees.expressions.functions.scalar;

import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;

import java.util.List;

/**
 * unique had a unique id, and two equals only if they have the same unique id.
 *
 * e.g. random(), uuid(), etc.
 *
 * for unique functions in PROJECT/HAVING/QUALIFY/SORT/AGG OUTPUT/REPEAT OUTPUT,
 * if they have a related AGG plan, need bind their unique id to the matched AGG's group by unique functions.
 * case as below:
 *
 * 1. if no aggregate or the aggregate group by expressions don't contain unique function,
 *    then all the unique function will be different.
 *    example i:  for sql 'select random1(), a + random2()
 *                         from t
 *                         order by random3(), a + random4()',
 *                since it doesn't contain aggregate, so random1(), random2(), random3(), random4() will be different
 *                and have different unique id;
 *
 *    example ii: for sql 'select sum(a + random1()), max(a + random2())
 *                         from t',
 *                will rewrite to 'select sum(a + random1()), max(a + random2())
 *                                 from t
 *                                 group by ()',
 *                since the aggregate's group by list is empty, so random1(), random2(), will be different.
 *
 *    example iii: for sql 'select a + random1(), sum(a + random2()), max(a + random3())
 *                          from t
 *                          group by a',
 *                 since the group by list (a) not contain unique function, so random1(), random2(), random3()
 *                 will be different.
 *
 * 2. if some aggregate group by expressions contains unique function, then bind unique id to unique
 *    function  with the longest matched group by.
 *    example: for sql 'select random1(), a + random2(), sum(random3()), sum(a + random4())
 *                      from t
 *                      where random5() > 0 and a + random6() > 0
 *                      group by random7(), random8(), a + random9(), a + random10()
 *                      order by random11(), abs(a + random12())'
 *              firstly, handle with the group by: if two group by can whole match, then their matched unique
 *                       scalar function will be equal and have the same unique id, so random7() equals with
 *                       random8(), random9() will be equals with random10(), but random7() not equals with random9.
 *              then handle with the PROJECT/HAVING/QUALIFY/SORT/AGG OUTPUT/REPEAT OUTPUT expressions, and we will have:
 *                      random1()/random3()/random11() are equal to random7(), and random2()/random4()/random12() are
 *                      equal to random9(), then update their unique id to the same.
 *              notice for FILTER random5()/random6(), they will be different with all other randoms.
 *
 * 3. for logical repeat, will flatten grouping sets like an aggregate's group by lists, then whole match the group by
 *    expressions and bind the unique id.
 *    example: for sql 'select random1(),  random2(),  a + random3(), a + random4()
 *                      from t
 *                      group by grouping sets ((), (random5()), (random6()), (a + random7()), (a + random8()))'
 *             firstly, handle with the grouping sets:  random5() equals to random6(),  random7() equals random8();
 *             then, handle with the repeat output: random1()/random2() equal to random5(),
 *                   random3()/random4() equal to random7()
 *
 * 4. if it's a distinct project and no contains aggregate functions and no contains aggregate plan.
 *    then it will rewrite to an aggregate, but will have some difference with the origin raw aggregate.
 *    example: for sql 'select distinct random1(), random2(), a + random3(), a + random4()
 *                      from t
 *                      order by random5(), a + random6()',
 *             it will rewrite to 'select random1(), random2(), a + random3(), a + random4()
 *                                 from t
 *                                 group by random1(), random2(), a + random3(), a + random4()
 *                                 order by random5(), a + random6()',
 *             for the rewritten aggregate,
 *             firstly, for the group by: the group by expressions will not try to match with each other even if
 *                      they seem look the same, so random1() will not equal to random2(),
 *                      random3() will not equal to random4(),
 *             then handle with the PROJECT/HAVING/QUALIFY/SORT, they will match with the first matched longest
 *                      group by expression, so random5() equals to random1 but not equal to random2(),
 *                      random6() equals to  random3() but not equal to random5(), then update their unique id to
 *                      the same.
 *
 */
public abstract class UniqueFunction extends ScalarFunction {

    protected final ExprId uniqueId;

    // when compare and bind unique id with group by expressions, should ignore the unique id
    protected final boolean ignoreUniqueId;

    /** constructor for withChildren and reuse signature */
    public UniqueFunction(UniqueFunctionParams functionParams) {
        super(functionParams);
        this.uniqueId = functionParams.uniqueId;
        this.ignoreUniqueId = functionParams.ignoreUniqueId;
    }

    public UniqueFunction(String name, ExprId uniqueId, boolean ignoreUniqueId, Expression... arguments) {
        super(name, arguments);
        this.uniqueId = uniqueId;
        this.ignoreUniqueId = ignoreUniqueId;
    }

    public UniqueFunction(String name, ExprId uniqueId, boolean ignoreUniqueId, List<Expression> arguments) {
        super(name, arguments);
        this.uniqueId = uniqueId;
        this.ignoreUniqueId = ignoreUniqueId;
    }

    public abstract UniqueFunction withIgnoreUniqueId(boolean ignoreUniqueId);

    @Override
    protected UniqueFunctionParams getFunctionParams(List<Expression> arguments) {
        return new UniqueFunctionParams(this, getName(), uniqueId, ignoreUniqueId, arguments, isInferred());
    }

    @Override
    public boolean foldable() {
        return false;
    }

    @Override
    public boolean isDeterministic() {
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        UniqueFunction other = (UniqueFunction) o;
        // in BindExpression phase, when compare two expression equals except the unique id,
        // will set ignoreUniqueId = true temporarily, after bind expression, will recover ignoreUniqueId = false
        if (ignoreUniqueId || other.ignoreUniqueId) {
            return super.equals(other);
        }
        return uniqueId.equals(other.uniqueId);
    }

    // The contains method needs to use hashCode, so similar to equals, it only compares exprId
    @Override
    public int computeHashCode() {
        // direct return exprId to speed up
        if (ignoreUniqueId) {
            return super.computeHashCode();
        }
        return uniqueId.asInt();
    }
}
