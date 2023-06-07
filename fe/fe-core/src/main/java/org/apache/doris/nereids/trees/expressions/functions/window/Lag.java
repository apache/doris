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
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.shape.TernaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DataType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;

/** Window function: lag */
public class Lag extends WindowFunction implements TernaryExpression, ExplicitlyCastableSignature,
        RequireTrivialTypes {

    static {
        List<FunctionSignature> signatures = Lists.newArrayList();
        trivialTypes.forEach(t ->
                signatures.add(FunctionSignature.ret(t).args(t, BigIntType.INSTANCE, t))
        );
        SIGNATURES = ImmutableList.copyOf(signatures);
    }

    private static final List<FunctionSignature> SIGNATURES;

    public Lag(Expression child, Expression offset, Expression defaultValue) {
        super("lag", child, offset, defaultValue);
    }

    private Lag(List<Expression> children) {
        super("lag", children);
    }

    public Expression getOffset() {
        if (children().size() <= 1) {
            throw new AnalysisException("Not set offset of Lead(): " + this.toSql());
        }
        return child(1);
    }

    public Expression getDefaultValue() {
        if (children.size() <= 2) {
            throw new AnalysisException("Not set default value of Lead(): " + this.toSql());
        }
        return child(2);
    }

    @Override
    public boolean nullable() {
        if (children.size() == 3 && child(2) instanceof NullLiteral) {
            return true;
        }
        return child(0).nullable();
    }

    @Override
    public Lag withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() >= 1 && children.size() <= 3);
        return new Lag(children);
    }

    @Override
    public void checkLegalityBeforeTypeCoercion() {
        if (children().size() == 1) {
            return;
        }
        if (children().size() >= 2) {
            DataType offsetType = getOffset().getDataType();
            if (!offsetType.isNumericType()) {
                throw new AnalysisException("The offset of LEAD must be a number:" + this.toSql());
            }
        }
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitLag(this, context);
    }

    @Override
    public DataType getDataType() {
        return child(0).getDataType();
    }

}
