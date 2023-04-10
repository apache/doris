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

package org.apache.doris.nereids.trees.expressions.functions.agg;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.functions.Udf;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Java UDAF for Nereids
 */
public class JavaUdaf extends AggregateFunction implements ExplicitlyCastableSignature, Udf {
    private final String functionName;
    private final FunctionSignature signature;
    private final org.apache.doris.catalog.AggregateFunction catalogFunction;

    /**
     * Constructor of UDAF
     */
    public JavaUdaf(org.apache.doris.catalog.AggregateFunction catalogFunction, FunctionSignature signature,
            String functionName, boolean isDistinct, Expression... args) {
        super(functionName, isDistinct, args);
        this.catalogFunction = catalogFunction;
        this.signature = signature;
        this.functionName = functionName;
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return ImmutableList.of(signature);
    }

    @Override
    public boolean nullable() {
        return false;
    }

    public org.apache.doris.catalog.AggregateFunction getCatalogFunction() {
        return catalogFunction;
    }

    /**
     * withChildren.
     */
    @Override
    public JavaUdaf withDistinctAndChildren(boolean isDistinct, List<Expression> children) {
        Preconditions.checkArgument(children.size() == this.children.size());
        return new JavaUdaf(catalogFunction, signature, functionName, isDistinct, children.toArray(new Expression[0]));
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitJavaUdaf(this, context);
    }
}
