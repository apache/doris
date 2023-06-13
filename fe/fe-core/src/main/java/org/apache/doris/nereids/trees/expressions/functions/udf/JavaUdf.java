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

package org.apache.doris.nereids.trees.expressions.functions.udf;

import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.functions.Udf;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ScalarFunction;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.coercion.AbstractDataType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Java UDF for Nereids
 */
public class JavaUdf extends ScalarFunction implements ExplicitlyCastableSignature, Udf {
    private final FunctionSignature signature;
    private final String objectFile;
    private final String symbol;
    private final String prepareFn;
    private final String closeFn;

    /**
     * Constructor of UDF
     */
    public JavaUdf(String name, FunctionSignature signature,
            String objectFile, String symbol, String prepareFn, String closeFn,
            Expression... args) {
        super(name, args);
        this.signature = signature;
        this.objectFile = objectFile;
        this.symbol = symbol;
        this.prepareFn = prepareFn;
        this.closeFn = closeFn;
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return ImmutableList.of(signature);
    }

    /**
     * withChildren.
     */
    @Override
    public JavaUdf withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == this.children.size());
        return new JavaUdf(getName(), signature, objectFile, symbol, prepareFn, closeFn,
                children.toArray(new Expression[0]));
    }

    /**
     * translate catalog java udf to nereids java udf
     */
    public static void translateToNereids(String dbName, org.apache.doris.catalog.ScalarFunction scalar) {
        String fnName = scalar.functionName();
        DataType retType = DataType.fromCatalogType(scalar.getReturnType());
        List<DataType> argTypes = Arrays.stream(scalar.getArgs())
                .map(DataType::fromCatalogType)
                .collect(Collectors.toList());

        FunctionSignature.FuncSigBuilder builder = FunctionSignature.ret(retType);
        FunctionSignature sig = scalar.hasVarArgs()
                ? builder.varArgs(argTypes.toArray(new DataType[0]))
                : builder.args(argTypes.toArray(new DataType[0]));

        JavaUdf udf = new JavaUdf(fnName, sig,
                scalar.getLocation().getLocation(),
                scalar.getSymbolName(),
                scalar.getPrepareFnSymbol(),
                scalar.getCloseFnSymbol());
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitJavaUdf(this, context);
    }

    @Override
    public Function getCatalogFunction() {
        return null;
    }
}
