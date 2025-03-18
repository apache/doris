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
import org.apache.doris.nereids.trees.expressions.functions.ExpressionTrait;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullable;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Printf function
 * printf(strfmt[, obj1, ...])
 * - strfmt: A STRING expression.
 * - objN: A STRING or numeric expression.
 * Returns a formatted string from printf-style format strings.
 */
public class Printf extends ScalarFunction
        implements ExplicitlyCastableSignature, PropagateNullable {

    /**
     * constructor with 1 or more arguments.
     */
    public Printf(Expression arg0, Expression... varArgs) {
        super("Printf", ExpressionUtils.mergeArguments(arg0, varArgs));
    }

    /**
     * withChildren.
     */
    @Override
    public Printf withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() >= 1);
        return new Printf(children.get(0), children.subList(1, children.size()).toArray(new Expression[0]));
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitPrintf(this, context);
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        List<DataType> argTypes = children.stream().map(ExpressionTrait::getDataType).collect(Collectors.toList());
        return ImmutableList.of(
                FunctionSignature.ret(StringType.INSTANCE).args(argTypes.toArray(new DataType[0])));
    }
}
