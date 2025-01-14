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
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullable;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * PosExplode(array('a','b','c')) generate two columns and three rows with:
 * pose column: 0, 1, 2
 * value column: 'a', 'b', 'c'
 */
public class PosExplode extends TableGeneratingFunction implements UnaryExpression, PropagateNullable {

    /**
     * constructor with 1 argument.
     */
    public PosExplode(Expression arg) {
        super("posexplode", arg);
    }

    /**
     * withChildren.
     */
    @Override
    public PosExplode withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new PosExplode(children.get(0));
    }

    @Override
    public void checkLegalityBeforeTypeCoercion() {
        if (!(child().getDataType() instanceof ArrayType)) {
            throw new AnalysisException("only support array type for posexplode function but got "
                    + child().getDataType());
        }
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return ImmutableList.of(
                FunctionSignature.ret(new StructType(ImmutableList.of(
                        new StructField("pos", IntegerType.INSTANCE, true, ""),
                        new StructField("col", ((ArrayType) child().getDataType()).getItemType(), true, ""))))
                        .args(child().getDataType()));
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitPosExplode(this, context);
    }
}
