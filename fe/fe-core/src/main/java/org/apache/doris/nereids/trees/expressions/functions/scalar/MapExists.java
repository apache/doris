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

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNullable;
import org.apache.doris.nereids.trees.expressions.functions.RewriteWhenAnalyze;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.BooleanType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Scalar function map_exists.
 *
 * <p>The Map lambda first produces one Boolean per entry, then ArrayMatchAny checks the result:
 *
 * <pre>
 * map_exists((mapKey, mapValue) -> predicate, inputMap)
 *   ->
 * array_match_any(
 *   array_map(
 *     entry -> predicate(entry[1], entry[2]),
 *     map_entries(inputMap)))
 * </pre>
 */
public class MapExists extends ScalarFunction
        implements HighOrderFunction, AlwaysNullable, RewriteWhenAnalyze {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(BooleanType.INSTANCE).args(ArrayType.of(BooleanType.INSTANCE)));

    /** Constructor with a bound Lambda argument. */
    public MapExists(Expression arg) {
        this(MapLambdaFunctionUtils.requireLambda("map_exists", arg));
    }

    private MapExists(Lambda lambda) {
        this(MapLambdaFunctionUtils.rewrite(lambda, (body, key, value, entry) -> body));
    }

    private MapExists(MapLambdaFunctionUtils.RewrittenMapLambda rewrittenLambda) {
        super("map_exists", rewrittenLambda.toArrayMap());
    }

    private MapExists(ScalarFunctionParams functionParams) {
        super(functionParams);
    }

    @Override
    public MapExists withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new MapExists(getFunctionParams(children));
    }

    @Override
    public List<FunctionSignature> getImplSignature() {
        return SIGNATURES;
    }

    @Override
    public Expression rewriteWhenAnalyze() {
        return new ArrayMatchAny(getArgument(0));
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitMapExists(this, context);
    }
}
