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
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNotNullable;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.shape.TernaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.StringType;
import com.google.common.base.Preconditions;
import java.util.List;

public class DictGetString extends ScalarFunction
        implements TernaryExpression, ExplicitlyCastableSignature, AlwaysNotNullable {

    public static final List<FunctionSignature> SIGNATURES = DictGet.creatSignatures(StringType.INSTANCE);

    /**
     * constructor with 3 argument.
     */
    public DictGetString(Expression arg0, Expression arg1, Expression arg2) {
        super("dict_get_string", arg0, arg1, arg2);
        if (!arg0.isConstant()) {
            throw new AnalysisException(
                    "dict_get_string(name, att, key): name support const value only.");
        }
        if (!arg1.isConstant()) {
            throw new AnalysisException(
                    "dict_get_string(name, att, key): att support const value only.");
        }
    }

    /**
     * withChildren.
     */
    @Override
    public DictGetString withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 3);
        return new DictGetString(children.get(0), children.get(1), children.get(2));
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitScalarFunction(this, context);
    }
}
