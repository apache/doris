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
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullable;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.VarcharType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

/**
 * ScalarFunction 'substr_spark'.
 * <p>
 * Spark-compatible substring semantics ({@code UTF8String#substringSQL} in Spark 3.5+). Doris
 * built-in {@code substr}/{@link Substring}/{@code substring} are unchanged; use this function only
 * when Spark-compatible results are required (e.g. negative {@code pos} with {@code len + pos &lt; 0}
 * can still yield a non-empty slice).
 */
public class SubstrSpark extends ScalarFunction
        implements ExplicitlyCastableSignature, PropagateNullable {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(VarcharType.SYSTEM_DEFAULT).args(VarcharType.SYSTEM_DEFAULT, IntegerType.INSTANCE),
            FunctionSignature.ret(StringType.INSTANCE).args(StringType.INSTANCE, IntegerType.INSTANCE),
            FunctionSignature.ret(VarcharType.SYSTEM_DEFAULT)
                    .args(VarcharType.SYSTEM_DEFAULT, IntegerType.INSTANCE, IntegerType.INSTANCE),
            FunctionSignature.ret(StringType.INSTANCE)
                    .args(StringType.INSTANCE, IntegerType.INSTANCE, IntegerType.INSTANCE)
    );

    /**
     * constructor with 2 arguments.
     */
    public SubstrSpark(Expression arg0, Expression arg1) {
        super("substr_spark", arg0, arg1, Literal.of(Integer.MAX_VALUE));
    }

    /**
     * constructor with 3 arguments.
     */
    public SubstrSpark(Expression arg0, Expression arg1, Expression arg2) {
        super("substr_spark", arg0, arg1, arg2);
    }

    /** constructor for withChildren and reuse signature */
    private SubstrSpark(ScalarFunctionParams functionParams) {
        super(functionParams);
    }

    @Override
    public FunctionSignature computeSignature(FunctionSignature signature) {
        Optional<Expression> length = arity() == 3
                ? Optional.of(getArgument(2))
                : Optional.empty();
        DataType returnType = VarcharType.SYSTEM_DEFAULT;
        if (length.isPresent() && length.get() instanceof IntegerLiteral) {
            returnType = VarcharType.createVarcharType(((IntegerLiteral) length.get()).getValue());
        }
        return signature.withReturnType(returnType);
    }

    public Expression getSource() {
        return child(0);
    }

    public Expression getPosition() {
        return child(1);
    }

    public Optional<Expression> getLength() {
        return arity() == 3 ? Optional.of(child(2)) : Optional.empty();
    }

    /**
     * withChildren.
     */
    @Override
    public SubstrSpark withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 2 || children.size() == 3);
        return new SubstrSpark(getFunctionParams(children));
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitSubstrSpark(this, context);
    }
}
