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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullable;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.coercion.AnyDataType;
import org.apache.doris.nereids.types.coercion.CharacterType;
import org.apache.doris.nereids.types.coercion.FollowToAnyDataType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * ScalarFunction 'array_match_any'.
 * expression: array_match_any(col1, "a,b") with result true.
 * expression: array_match_any(["a", "b", "c", 'd'], "a,e") with result true.
 * expression: array_match_any(["a", "b", "c", 'd'], "e,f") with result false.
 */
public class ArrayMatchAny extends ScalarFunction
        implements ExplicitlyCastableSignature, PropagateNullable {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(BooleanType.INSTANCE)
                    .args(ArrayType.of(new AnyDataType(0)), new FollowToAnyDataType(0)),
            FunctionSignature.ret(BooleanType.INSTANCE)
                    .args(ArrayType.of(new AnyDataType(0)), new FollowToAnyDataType(0),
                            MapType.of(StringType.INSTANCE, StringType.INSTANCE))
    );

    /**
     * constructor with 2 arguments.
     */
    public ArrayMatchAny(Expression arg0, Expression arg1) {
        super("array_match_any", arg0, arg1);
    }

    /**
     * constructor with 3 arguments.
     * Note the last argument is invertedIndexParserMap which is produced by ArrayMatchAnyExpander rule.
     */
    private ArrayMatchAny(Expression arg0, Expression arg1, Expression invertedIndexParserMap) {
        super("array_match_any", arg0, arg1, invertedIndexParserMap);
    }

    /**
     * withChildren.
     */
    @Override
    public ArrayMatchAny withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 2 || children.size() == 3);
        if (children.size() == 2) {
            return new ArrayMatchAny(children.get(0), children.get(1));
        }
        return new ArrayMatchAny(children.get(0), children.get(1), children.get(2));
    }

    @Override
    public void checkLegalityBeforeTypeCoercion() {
        if (!(child(0).getDataType() instanceof ArrayType
                && ((ArrayType) child(0).getDataType()).getItemType() instanceof CharacterType)) {
            throw new AnalysisException("The first argument of array_match_any should be an array of characters.");
        }
        if (!(child(1).getDataType() instanceof CharacterType)) {
            throw new AnalysisException("The second argument of array_match_any should be a character.");
        }
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitArrayMatchAny(this, context);
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

}
