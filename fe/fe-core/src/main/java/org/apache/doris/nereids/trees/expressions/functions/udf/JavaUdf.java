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

import org.apache.doris.analysis.FunctionName;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.Function.NullableMode;
import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.util.URI;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.VirtualSlotReference;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.functions.Udf;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ScalarFunction;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.thrift.TFunctionBinaryType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Java UDF for Nereids
 */
public class JavaUdf extends ScalarFunction implements ExplicitlyCastableSignature, Udf {
    private final String dbName;
    private final long functionId;
    private final TFunctionBinaryType binaryType;
    private final FunctionSignature signature;
    private final NullableMode nullableMode;
    private final String objectFile;
    private final String symbol;
    private final String prepareFn;
    private final String closeFn;
    private final String checkSum;

    /**
     * Constructor of UDF
     */
    public JavaUdf(String name, long functionId, String dbName, TFunctionBinaryType binaryType,
            FunctionSignature signature,
            NullableMode nullableMode, String objectFile, String symbol, String prepareFn, String closeFn,
            String checkSum, Expression... args) {
        super(name, args);
        this.dbName = dbName;
        this.functionId = functionId;
        this.binaryType = binaryType;
        this.signature = signature;
        this.nullableMode = nullableMode;
        this.objectFile = objectFile;
        this.symbol = symbol;
        this.prepareFn = prepareFn;
        this.closeFn = closeFn;
        this.checkSum = checkSum;
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return ImmutableList.of(signature);
    }

    @Override
    public boolean hasVarArguments() {
        return signature.hasVarArgs;
    }

    @Override
    public int arity() {
        return signature.argumentsTypes.size();
    }

    @Override
    public NullableMode getNullableMode() {
        return nullableMode;
    }

    /**
     * withChildren.
     */
    @Override
    public JavaUdf withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == this.children.size());
        return new JavaUdf(getName(), functionId, dbName, binaryType, signature, nullableMode,
                objectFile, symbol, prepareFn, closeFn, checkSum, children.toArray(new Expression[0]));
    }

    /**
     * translate catalog java udf to nereids java udf
     */
    public static void translateToNereidsFunction(String dbName, org.apache.doris.catalog.ScalarFunction scalar) {
        String fnName = scalar.functionName();
        DataType retType = DataType.fromCatalogType(scalar.getReturnType());
        List<DataType> argTypes = Arrays.stream(scalar.getArgs())
                .map(DataType::fromCatalogType)
                .collect(Collectors.toList());

        FunctionSignature.FuncSigBuilder sigBuilder = FunctionSignature.ret(retType);
        FunctionSignature sig = scalar.hasVarArgs()
                ? sigBuilder.varArgs(argTypes.toArray(new DataType[0]))
                : sigBuilder.args(argTypes.toArray(new DataType[0]));

        VirtualSlotReference[] virtualSlots = argTypes.stream()
                .map(type -> new VirtualSlotReference(type.toString(), type, Optional.empty(),
                        (shape) -> ImmutableList.of()))
                .toArray(VirtualSlotReference[]::new);

        JavaUdf udf = new JavaUdf(fnName, scalar.getId(), dbName, scalar.getBinaryType(), sig,
                scalar.getNullableMode(),
                scalar.getLocation().getLocation(),
                scalar.getSymbolName(),
                scalar.getPrepareFnSymbol(),
                scalar.getCloseFnSymbol(),
                scalar.getChecksum(),
                virtualSlots);

        JavaUdfBuilder builder = new JavaUdfBuilder(udf);
        Env.getCurrentEnv().getFunctionRegistry().addUdf(dbName, fnName, builder);
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitJavaUdf(this, context);
    }

    @Override
    public Function getCatalogFunction() {
        try {
            org.apache.doris.catalog.ScalarFunction expr = org.apache.doris.catalog.ScalarFunction.createUdf(
                    binaryType,
                    new FunctionName(dbName, getName()),
                    signature.argumentsTypes.stream().map(DataType::toCatalogDataType).toArray(Type[]::new),
                    signature.returnType.toCatalogDataType(),
                    signature.hasVarArgs,
                    URI.create(objectFile),
                    symbol,
                    prepareFn,
                    closeFn
            );
            expr.setNullableMode(nullableMode);
            expr.setChecksum(checkSum);
            expr.setId(functionId);
            return expr;
        } catch (Exception e) {
            throw new AnalysisException(e.getMessage(), e.getCause());
        }
    }
}
