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
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.ArrayItemReference;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.ImplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullable;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.LambdaType;
import org.apache.doris.nereids.types.coercion.FollowToAnyDataType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * array_sorted_merge(merge_fn, arr1, arr2, is_asc): merge two arrays that are already sorted.
 *
 * <pre>
 * array_sorted_merge((a, b) -&gt; ..., arr1, arr2, true)
 * </pre>
 *
 * <p>The inputs must already be sorted by the elements' natural order in the direction given by
 * {@code is_asc}; the function only merges them. The merge lambda receives the two current
 * candidates as {@code (a, b)} and returns the combined element; returning NULL means the two
 * elements must not be combined, so both are kept. Each key occurs at most once per input, so a
 * key appears at most twice across the two arrays and the lambda is called at most once per key.
 *
 * <p>A NULL input array makes the result NULL, like the other higher-order array functions.
 */
public class ArraySortedMerge extends ScalarFunction
        implements ImplicitlyCastableSignature, PropagateNullable {

    // The bound form: the lambda plus the ascending flag; the arrays live in the lambda argument
    // references, like the other lambda functions.
    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(new FollowToAnyDataType(0))
                    .args(LambdaType.INSTANCE, BooleanType.INSTANCE));

    /**
     * The extra arguments are variadic so that the function builder resolved with only the lambda
     * (the higher-order lookup) can also build the bound form that keeps the ascending flag.
     */
    public ArraySortedMerge(Expression lambda, Expression... extra) {
        super("array_sorted_merge",
                ImmutableList.<Expression>builder().add(lambda).add(extra).build());
    }

    /** constructor for withChildren and reuse signature */
    private ArraySortedMerge(ScalarFunctionParams functionParams) {
        super(functionParams);
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    @Override
    public ArraySortedMerge withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 2);
        return new ArraySortedMerge(getFunctionParams(children));
    }

    @Override
    public void checkLegalityBeforeTypeCoercion() {
        if (!(child(0) instanceof Lambda)) {
            throw new AnalysisException(getName() + " requires a merge lambda as the first argument");
        }
        Lambda lambda = (Lambda) child(0);
        if (lambda.getLambdaArgumentNames().size() != 2) {
            throw new AnalysisException(getName() + " requires a two-argument merge lambda, but got "
                    + lambda.getLambdaArgumentNames().size());
        }
        ArrayType left = requireArray(lambda, 0);
        ArrayType right = requireArray(lambda, 1);
        if (!left.getItemType().equals(right.getItemType())) {
            throw new AnalysisException(getName()
                    + " requires both arrays to have the same element type, but got "
                    + left.getItemType().toSql() + " and " + right.getItemType().toSql());
        }
        if (!lambda.getRetType().equals(left.getItemType())) {
            throw new AnalysisException(getName()
                    + " requires the merge lambda to return the element type "
                    + left.getItemType().toSql() + ", but got " + lambda.getRetType().toSql());
        }
        if (!child(1).getDataType().isBooleanType()) {
            throw new AnalysisException(getName()
                    + " requires the ascending flag to be BOOLEAN, but got "
                    + child(1).getDataType().toSql());
        }
    }

    private ArrayType requireArray(Lambda lambda, int argumentIndex) {
        DataType arrayType = lambda.getLambdaArgument(argumentIndex).getArrayExpression().getDataType();
        if (!(arrayType instanceof ArrayType)) {
            throw new AnalysisException(getName() + " requires array arguments, but argument "
                    + (argumentIndex + 1) + " is " + arrayType.toSql());
        }
        return (ArrayType) arrayType;
    }

    @Override
    public DataType getDataType() {
        return ArrayType.of(((Lambda) child(0)).getRetType());
    }

    @Override
    public boolean nullable() {
        Lambda lambda = (Lambda) child(0);
        return lambda.getLambdaArguments().stream()
                .map(ArrayItemReference::getArrayExpression)
                .anyMatch(Expression::nullable);
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitArraySortedMerge(this, context);
    }
}
