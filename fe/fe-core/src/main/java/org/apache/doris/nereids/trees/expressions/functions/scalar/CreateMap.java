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
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.MapType;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * ScalarFunction 'map'.
 */
public class CreateMap extends ScalarFunction
        implements ExplicitlyCastableSignature, AlwaysNotNullable {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(MapType.SYSTEM_DEFAULT).args()
    );

    /**
     * constructor with 0 or more arguments.
     */
    public CreateMap(Expression... varArgs) {
        super("map", varArgs);
    }

    @Override
    public DataType getDataType() {
        if (arity() >= 2) {
            return MapType.of(child(0).getDataType(), child(1).getDataType());
        }
        return MapType.SYSTEM_DEFAULT;
    }

    @Override
    public void checkLegalityBeforeTypeCoercion() {
        if (arity() % 2 != 0) {
            throw new AnalysisException("map can't be odd parameters, need even parameters " + this.toSql());
        }
    }

    /**
     * withChildren.
     */
    @Override
    public CreateMap withChildren(List<Expression> children) {
        return new CreateMap(children.toArray(new Expression[0]));
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitCreateMap(this, context);
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        if (arity() == 0) {
            return SIGNATURES;
        } else {
            return ImmutableList.of(FunctionSignature.of(
                    getDataType(),
                    children.stream()
                            .map(ExpressionTrait::getDataType)
                            .collect(ImmutableList.toImmutableList())
            ));
        }
    }
}
