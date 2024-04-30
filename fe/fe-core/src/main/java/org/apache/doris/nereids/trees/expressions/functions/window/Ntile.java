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

package org.apache.doris.nereids.trees.expressions.functions.window;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNotNullable;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.shape.LeafExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.LargeIntType;
import org.apache.doris.nereids.types.SmallIntType;
import org.apache.doris.nereids.types.TinyIntType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * window function: Ntile()
 */
public class Ntile extends WindowFunction implements LeafExpression, AlwaysNotNullable, ExplicitlyCastableSignature {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(BigIntType.INSTANCE).args(TinyIntType.INSTANCE),
            FunctionSignature.ret(BigIntType.INSTANCE).args(SmallIntType.INSTANCE),
            FunctionSignature.ret(BigIntType.INSTANCE).args(IntegerType.INSTANCE),
            FunctionSignature.ret(BigIntType.INSTANCE).args(BigIntType.INSTANCE),
            FunctionSignature.ret(LargeIntType.INSTANCE).args(LargeIntType.INSTANCE)
    );

    private Expression buckets;

    public Ntile(Expression buckets) {
        super("ntile", buckets);
        this.buckets = buckets;
    }

    public Expression getBuckets() {
        return buckets;
    }

    @Override
    public Ntile withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new Ntile(children.get(0));
    }

    @Override
    public void checkLegalityBeforeTypeCoercion() {
        DataType type = getBuckets().getDataType();
        if (!type.isIntegralType()) {
            throw new AnalysisException("The bucket of NTILE must be a integer: " + this.toSql());
        }
        if (!getBuckets().isConstant()) {
            throw new AnalysisException(
                "The bucket of NTILE must be a constant value: " + this.toSql());
        }
        if (getBuckets() instanceof Literal) {
            if (((Literal) getBuckets()).getDouble() <= 0) {
                throw new AnalysisException(
                    "The bucket parameter of NTILE must be a constant positive integer: " + this.toSql());
            }
        } else {
            throw new AnalysisException(
                "The bucket parameter of NTILE must be a constant positive integer: " + this.toSql());
        }
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitNtile(this, context);
    }
}
