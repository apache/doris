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
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNullable;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * explode({"amory":1, "doris": 2}) generate two column and two lines with:
 *      key column: amory, doris
 *      value column: 1, 2
 */
public class ExplodeMap extends TableGeneratingFunction implements UnaryExpression, AlwaysNullable {

    /**
     * constructor with 1 argument.
     */
    public ExplodeMap(Expression arg) {
        super("explode_map", arg);
    }

    /**
     * withChildren.
     */
    @Override
    public ExplodeMap withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new ExplodeMap(children.get(0));
    }

    @Override
    public void checkLegalityBeforeTypeCoercion() {
        if (!(child().getDataType() instanceof MapType)) {
            throw new AnalysisException("only support map type for explode_map function but got "
                    + child().getDataType());
        }
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return ImmutableList.of(
                FunctionSignature.ret(new StructType(ImmutableList.of(
                                new StructField("col1", ((MapType) child().getDataType()).getKeyType(), true, ""),
                                new StructField("col2", ((MapType) child().getDataType()).getValueType(), true, ""))))
                        .args(child().getDataType())
        );
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitExplodeMap(this, context);
    }
}
