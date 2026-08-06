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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNullable;
import org.apache.doris.nereids.trees.expressions.literal.StructLiteral;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.JsonType;
import org.apache.doris.nereids.types.StringType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * json_each_text(json) expands the top-level JSON object into a set of
 * key/value pairs.
 * Returns: Struct(key VARCHAR, value VARCHAR) — the JSON value is returned as
 * plain text.
 *
 * Example:
 * SELECT key, value FROM LATERAL VIEW json_each_text('{"a":"foo","b":"bar"}') t
 * AS key, value
 * → key="a", value=foo (plain string, not JSON-quoted)
 * → key="b", value=bar
 */
public class JsonEachText extends TableGeneratingFunction implements UnaryExpression, AlwaysNullable {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(StructLiteral.constructStructType(
                    ImmutableList.of(StringType.INSTANCE, StringType.INSTANCE)))
                    .args(JsonType.INSTANCE));

    /**
     * Constructor with 1 argument.
     */
    public JsonEachText(Expression arg) {
        super("json_each_text", arg);
    }

    /** Constructor for withChildren and reuse signature. */
    private JsonEachText(GeneratorFunctionParams functionParams) {
        super(functionParams);
    }

    @Override
    public JsonEachText withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new JsonEachText(getFunctionParams(children));
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitJsonEachText(this, context);
    }
}
