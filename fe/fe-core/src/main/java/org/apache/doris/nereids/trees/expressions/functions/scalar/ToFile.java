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
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.FileType;
import org.apache.doris.nereids.types.StringType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * ScalarFunction 'to_file'.
 * to_file(url, region, endpoint, ak, sk) — construct FILE with AK/SK auth.
 * to_file(url, region, endpoint, ak, sk, role_arn, external_id) — construct FILE with IAM Role auth.
 */
public class ToFile extends ScalarFunction
        implements ExplicitlyCastableSignature, PropagateNullable {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(FileType.INSTANCE).args(
                    StringType.INSTANCE, StringType.INSTANCE,
                    StringType.INSTANCE, StringType.INSTANCE,
                    StringType.INSTANCE),
            FunctionSignature.ret(FileType.INSTANCE).args(
                    StringType.INSTANCE, StringType.INSTANCE,
                    StringType.INSTANCE, StringType.INSTANCE,
                    StringType.INSTANCE, StringType.INSTANCE,
                    StringType.INSTANCE)
    );

    /**
     * constructor with 5 arguments: object URL, region, endpoint, ak, sk.
     */
    public ToFile(Expression arg0, Expression arg1, Expression arg2,
            Expression arg3, Expression arg4) {
        super("to_file", arg0, arg1, arg2, arg3, arg4);
    }

    /**
     * constructor with 7 arguments: object URL, region, endpoint, ak, sk, role_arn, external_id.
     */
    public ToFile(Expression arg0, Expression arg1, Expression arg2,
            Expression arg3, Expression arg4, Expression arg5, Expression arg6) {
        super("to_file", arg0, arg1, arg2, arg3, arg4, arg5, arg6);
    }

    /** constructor for withChildren and reuse signature */
    private ToFile(ScalarFunctionParams functionParams) {
        super(functionParams);
    }

    @Override
    public ToFile withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 5 || children.size() == 7);
        return new ToFile(getFunctionParams(children));
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitToFile(this, context);
    }
}
