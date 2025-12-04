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
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.VarcharType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * ScalarFunction 'str_to_map'.
 * str_to_map(expr [, pairDelim [, keyValueDelim] ] )
 * - expr: An STRING expression.
 * - pairDelim: An optional STRING literal defaulting to ',' that specifies how
 * to split entries.
 * - keyValueDelim: An optional STRING literal defaulting to ':' that specifies
 * how to split each key-value pair.
 * Returns:
 * A MAP of STRING for both keys and values.
 * Both pairDelim and keyValueDelim are treated as regular expressions.
 */
public class StrToMap extends ScalarFunction
        implements ExplicitlyCastableSignature, PropagateNullable {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(MapType.of(StringType.INSTANCE, StringType.INSTANCE))
                    .args(VarcharType.SYSTEM_DEFAULT),
            FunctionSignature.ret(MapType.of(StringType.INSTANCE, StringType.INSTANCE)).args(VarcharType.SYSTEM_DEFAULT,
                    VarcharType.SYSTEM_DEFAULT),
            FunctionSignature.ret(MapType.of(StringType.INSTANCE, StringType.INSTANCE)).args(VarcharType.SYSTEM_DEFAULT,
                    VarcharType.SYSTEM_DEFAULT,
                    VarcharType.SYSTEM_DEFAULT),
            FunctionSignature.ret(MapType.of(StringType.INSTANCE, StringType.INSTANCE)).args(StringType.INSTANCE),
            FunctionSignature.ret(MapType.of(StringType.INSTANCE, StringType.INSTANCE)).args(StringType.INSTANCE,
                    StringType.INSTANCE),
            FunctionSignature.ret(MapType.of(StringType.INSTANCE, StringType.INSTANCE)).args(StringType.INSTANCE,
                    StringType.INSTANCE,
                    StringType.INSTANCE));

    /**
     * constructor with 1 arguments.
     */
    public StrToMap(Expression arg0) {
        super("str_to_map", arg0, Literal.of(","), Literal.of(":"));
    }

    /**
     * constructor with 2 arguments.
     */
    public StrToMap(Expression arg0, Expression arg1) {
        super("str_to_map", arg0, arg1, Literal.of(":"));
    }

    /**
     * constructor with 3 arguments.
     */
    public StrToMap(Expression arg0, Expression arg1, Expression arg2) {
        super("str_to_map", arg0, arg1, arg2);
    }

    /** constructor for withChildren and reuse signature */
    private StrToMap(ScalarFunctionParams functionParams) {
        super(functionParams);
    }

    /**
     * withChildren.
     */
    @Override
    public StrToMap withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1 || children.size() == 2 || children.size() == 3);
        return new StrToMap(getFunctionParams(children));
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitStrToMap(this, context);
    }
}
