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
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.JsonType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.VarcharType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * JsonKeys returns the keys from the top-level value of a JSON object as a JSON array,
 * or, if a path argument is given, the top-level keys from the selected path.
 * Returns NULL if any argument is NULL, the json_doc argument is not an object,
 * or path, if given, does not locate an object.
 * An error occurs if the json_doc argument is not a valid JSON document
 * or the path argument is not a valid path expression.
 */
public class JsonKeys extends ScalarFunction
        implements ExplicitlyCastableSignature, AlwaysNullable {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(ArrayType.of(StringType.INSTANCE))
                    .args(JsonType.INSTANCE, VarcharType.SYSTEM_DEFAULT),
            FunctionSignature.ret(ArrayType.of(StringType.INSTANCE)).args(JsonType.INSTANCE));

    /**
     * constructor with one or two arguments.
     */
    public JsonKeys(Expression arg0) {
        super("json_keys", arg0);
    }

    public JsonKeys(Expression arg0, Expression arg1) {
        super("json_keys", arg0, arg1);
    }

    /**
     * withChildren.
     */
    @Override
    public JsonKeys withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1 || children.size() == 2);
        if (children.size() == 1) {
            return new JsonKeys(children.get(0));
        } else {
            return new JsonKeys(children.get(0), children.get(1));
        }
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitJsonKeys(this, context);
    }
}
