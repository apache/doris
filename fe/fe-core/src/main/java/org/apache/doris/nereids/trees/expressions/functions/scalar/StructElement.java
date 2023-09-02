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
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNullable;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.functions.SearchSignature;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLikeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.StructType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * ScalarFunction 'struct_element'.
 */
public class StructElement extends ScalarFunction
        implements ExplicitlyCastableSignature, AlwaysNullable {

    /**
     * constructor with 0 or more arguments.
     */
    public StructElement(Expression arg0, Expression arg1) {
        super("struct_element", arg0, arg1);
    }

    @Override
    public void checkLegalityBeforeTypeCoercion() {
        if (!(child(0).getDataType() instanceof StructType)) {
            SearchSignature.throwCanNotFoundFunctionException(this.getName(), this.getArguments());
        }
        if (!(child(1) instanceof StringLikeLiteral || child(1) instanceof IntegerLikeLiteral)) {
            throw new AnalysisException("struct_element only allows"
                    + " constant int or string second parameter: " + this.toSql());
        }
    }

    /**
     * withChildren.
     */
    @Override
    public StructElement withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 2, "children size should be 2");
        return new StructElement(children.get(0), children.get(1));
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitStructElement(this, context);
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        StructType structArgType = (StructType) child(0).getDataType();
        DataType retType;
        if (child(1) instanceof IntegerLikeLiteral) {
            int offset = ((IntegerLikeLiteral) child(1)).getIntValue();
            if (offset <= 0 || offset > structArgType.getFields().size()) {
                throw new AnalysisException("the specified field index out of bound: " + this.toSql());
            } else {
                retType = structArgType.getFields().get(offset - 1).getDataType();
            }
        } else if (child(1) instanceof StringLikeLiteral) {
            String name = ((StringLikeLiteral) child(1)).getStringValue();
            if (!structArgType.getNameToFields().containsKey(name)) {
                throw new AnalysisException("the specified field name " + name + " was not found: " + this.toSql());
            } else {
                retType = structArgType.getNameToFields().get(name).getDataType();
            }
        } else {
            throw new AnalysisException("struct_element only allows"
                    + " constant int or string second parameter: " + this.toSql());
        }
        return ImmutableList.of(FunctionSignature.ret(retType).args(structArgType, child(1).getDataType()));
    }
}
