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
import org.apache.doris.nereids.trees.expressions.functions.ExpressionTrait;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Set;

/**
 * ScalarFunction 'named_struct'.
 */
public class CreateNamedStruct extends ScalarFunction
        implements ExplicitlyCastableSignature, AlwaysNotNullable {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(StructType.SYSTEM_DEFAULT).args()
    );

    /**
     * constructor with 0 or more arguments.
     */
    public CreateNamedStruct(Expression... varArgs) {
        super("named_struct", varArgs);
    }

    @Override
    public void checkLegalityBeforeTypeCoercion() {
        if (arity() % 2 != 0) {
            throw new AnalysisException("named_struct can't be odd parameters, need even parameters " + this.toSql());
        }
        Set<String> names = Sets.newHashSet();
        for (int i = 0; i < arity(); i = i + 2) {
            if (!(child(i) instanceof StringLikeLiteral)) {
                throw new AnalysisException("named_struct only allows"
                        + " constant string parameter in odd position: " + this);
            } else {
                String name = ((StringLikeLiteral) child(i)).getStringValue();
                if (names.contains(name)) {
                    throw new AnalysisException("The name of the struct field cannot be repeated."
                            + " same name fields are " + name);
                } else {
                    names.add(name);
                }
            }
        }
    }

    /**
     * withChildren.
     */
    @Override
    public CreateNamedStruct withChildren(List<Expression> children) {
        return new CreateNamedStruct(children.toArray(new Expression[0]));
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitCreateNamedStruct(this, context);
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        if (arity() == 0) {
            return SIGNATURES;
        } else {
            ImmutableList.Builder<StructField> structFields = ImmutableList.builder();
            for (int i = 0; i < arity(); i = i + 2) {
                StringLikeLiteral nameLiteral = (StringLikeLiteral) child(i);
                structFields.add(new StructField(nameLiteral.getStringValue(),
                        children.get(i + 1).getDataType(), true, ""));
            }
            return ImmutableList.of(FunctionSignature.ret(new StructType(structFields.build()))
                    .args(children.stream().map(ExpressionTrait::getDataType).toArray(DataType[]::new)));
        }
    }
}
