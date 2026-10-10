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

package org.apache.doris.nereids.trees.expressions.functions.generator;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullable;
import org.apache.doris.nereids.trees.expressions.functions.RewriteWhenAnalyze;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.functions.scalar.SplitByString;
import org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.trees.expressions.shape.BinaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * explode_split("a,b,c", ","), generate 3 lines include 'a', 'b' and 'c'.
 */
public class ExplodeSplit extends TableGeneratingFunction
        implements BinaryExpression, PropagateNullable, RewriteWhenAnalyze {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(VarcharType.SYSTEM_DEFAULT)
                    .args(VarcharType.SYSTEM_DEFAULT, VarcharType.SYSTEM_DEFAULT)
    );

    /**
     * constructor with 2 arguments.
     */
    public ExplodeSplit(Expression arg0, Expression arg1) {
        super("explode_split", arg0, arg1);
    }

    /** constructor for withChildren and reuse signature */
    private ExplodeSplit(GeneratorFunctionParams functionParams) {
        super(functionParams);
    }

    /**
     * withChildren.
     */
    @Override
    public ExplodeSplit withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 2);
        return new ExplodeSplit(getFunctionParams(children));
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitExplodeSplit(this, context);
    }

    @Override
    public Expression rewriteWhenAnalyze() {
        return new Explode(splitToArray(children.get(0), children.get(1)));
    }

    /**
     * Build the array expanded by explode_split and explode_split_outer.
     *
     * split_by_string('', delimiter) returns an empty array, but splitting an empty string must still
     * produce one empty-string row, so an empty string with a non-null delimiter becomes [''].
     * A NULL string or NULL delimiter still produces NULL, like split_by_string.
     */
    static Expression splitToArray(Expression str, Expression delimiter) {
        Expression isEmptyString = new And(new EqualTo(str, new VarcharLiteral("")), new Not(new IsNull(delimiter)));
        ArrayLiteral emptyStringArray = new ArrayLiteral(ImmutableList.of(new VarcharLiteral("")));
        return TypeCoercionUtils.processBoundFunction(
                new If(isEmptyString, emptyStringArray, new SplitByString(str, delimiter)));
    }
}
