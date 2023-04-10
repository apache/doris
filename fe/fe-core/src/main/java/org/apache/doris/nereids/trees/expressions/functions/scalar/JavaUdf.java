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
import org.apache.doris.common.util.URI;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Java UDF for Nereids
 */
public class JavaUdf extends ScalarFunction implements ExplicitlyCastableSignature {
    private final String symbolName;
    private final String functionName;
    private final List<String> functionNameParts;
    private final FunctionSignature signature;
    private final URI functionUri;
    private final String checkSum;

    /**
     * Constructor of UDF
     */
    public JavaUdf(String symbolName, String functionName, List<String> functionNameParts,
            FunctionSignature signature, URI functionUri, String checkSum, Expression... args) {
        super(symbolName, args);
        this.symbolName = symbolName;
        this.functionName = functionName;
        this.functionNameParts = functionNameParts;
        this.signature = signature;
        this.functionUri = functionUri;
        this.checkSum = checkSum;
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return ImmutableList.of(signature);
    }

    @Override
    public boolean nullable() {
        return false;
    }

    /**
     * withChildren.
     */
    @Override
    public JavaUdf withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == this.children.size());
        return new JavaUdf(symbolName, functionName, functionNameParts, signature,
                functionUri, checkSum, children.toArray(new Expression[0]));
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitJavaUdf(this, context);
    }
}
