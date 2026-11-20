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
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.VarcharType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/** Scalar expression for regexp_position. */
public class RegexpPosition extends ScalarFunction
        implements ExplicitlyCastableSignature, PropagateNullable {
    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(BigIntType.INSTANCE)
                    .args(VarcharType.SYSTEM_DEFAULT, VarcharType.SYSTEM_DEFAULT, BigIntType.INSTANCE,
                            BigIntType.INSTANCE, BigIntType.INSTANCE, VarcharType.SYSTEM_DEFAULT, BigIntType.INSTANCE),
            FunctionSignature.ret(BigIntType.INSTANCE)
                    .args(StringType.INSTANCE, StringType.INSTANCE, BigIntType.INSTANCE,
                            BigIntType.INSTANCE, BigIntType.INSTANCE, StringType.INSTANCE, BigIntType.INSTANCE));

    private static final List<String> ARGUMENT_NAMES = ImmutableList.of(
            "haystack", "pattern", "position", "occurrence", "return_option", "flags", "subexpression");

    public RegexpPosition(Expression haystack, Expression pattern) {
        this(haystack, pattern, new IntegerLiteral(1));
    }

    public RegexpPosition(Expression haystack, Expression pattern, Expression position) {
        this(haystack, pattern, position, new IntegerLiteral(1));
    }

    public RegexpPosition(Expression haystack, Expression pattern, Expression position, Expression occurrence) {
        this(haystack, pattern, position, occurrence, new IntegerLiteral(0));
    }

    public RegexpPosition(Expression haystack, Expression pattern, Expression position, Expression occurrence,
            Expression returnOption) {
        this(haystack, pattern, position, occurrence, returnOption, new StringLiteral(""));
    }

    public RegexpPosition(Expression haystack, Expression pattern, Expression position, Expression occurrence,
            Expression returnOption, Expression flags) {
        this(haystack, pattern, position, occurrence, returnOption, flags, new IntegerLiteral(0));
    }

    public RegexpPosition(Expression haystack, Expression pattern, Expression position, Expression occurrence,
            Expression returnOption, Expression flags, Expression subexpression) {
        super("regexp_position", haystack, pattern, position, occurrence, returnOption, flags, subexpression);
    }

    /** Constructor for withChildren and signature reuse. */
    private RegexpPosition(ScalarFunctionParams functionParams) {
        super(functionParams);
    }

    @Override
    public RegexpPosition withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() >= 2 && children.size() <= 7,
                "RegexpPosition should have 2 to 7 children, but got: " + children.size());
        List<Expression> defaults = ImmutableList.of(new IntegerLiteral(1), new IntegerLiteral(1),
                new IntegerLiteral(0), new StringLiteral(""), new IntegerLiteral(0));
        List<Expression> fullArguments = ImmutableList.<Expression>builder()
                .addAll(children)
                .addAll(defaults.subList(children.size() - 2, defaults.size()))
                .build();
        return new RegexpPosition(getFunctionParams(fullArguments));
    }

    @Override
    public void checkLegalityBeforeTypeCoercion() {
        List<Expression> arguments = getArguments();
        for (int i = 0; i < arguments.size(); i++) {
            DataType type = arguments.get(i).getDataType();
            boolean stringArgument = i == 0 || i == 1 || i == 5;
            if (!type.isNullType()
                    && !(stringArgument ? type.isStringType() || type.isVarcharType() : type.isIntegerLikeType())) {
                throw new AnalysisException(String.format(
                        "The %s argument of regexp_position must be %s or NULL, but was %s",
                        ARGUMENT_NAMES.get(i),
                        stringArgument ? "STRING or VARCHAR" : "TINYINT, SMALLINT, INT or BIGINT",
                        type.toSql()));
            }
        }
        if (!getArgument(1).isConstant()) {
            throw new AnalysisException("The pattern argument of regexp_position must be constant");
        }
        if (!getArgument(5).isConstant()) {
            throw new AnalysisException("The flags argument of regexp_position must be constant");
        }
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitRegexpPosition(this, context);
    }
}
