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
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.functions.Udf;
import org.apache.doris.nereids.trees.expressions.functions.generator.TableGeneratingFunction;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.thrift.TFunctionBinaryType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Python UDTF for Nereids
 */
public class PythonUdtf extends TableGeneratingFunction implements ExplicitlyCastableSignature, Udf {
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
    private final boolean isStaticLoad;
    private final long expirationTime;
    private final String runtimeVersion;
    private final String functionCode;

    /**
     * Constructor of Python UDTF
     */
    public PythonUdtf(String name, long functionId, String dbName, TFunctionBinaryType binaryType,
            FunctionSignature signature,
            NullableMode nullableMode, String objectFile, String symbol, String prepareFn, String closeFn,
            String checkSum, boolean isStaticLoad, long expirationTime,
            String runtimeVersion, String functionCode, Expression... args) {
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
        this.isStaticLoad = isStaticLoad;
        this.expirationTime = expirationTime;
        this.runtimeVersion = runtimeVersion;
        this.functionCode = functionCode;
    }

    /**
     * withChildren.
     */
    @Override
    public PythonUdtf withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == this.children.size());
        return new PythonUdtf(getName(), functionId, dbName, binaryType, signature, nullableMode,
                objectFile, symbol, prepareFn, closeFn, checkSum, isStaticLoad, expirationTime,
                runtimeVersion, functionCode, children.toArray(new Expression[0]));
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
    public Function getCatalogFunction() {
        try {
            org.apache.doris.catalog.ScalarFunction expr = org.apache.doris.catalog.ScalarFunction.createUdf(
                    binaryType,
                    new FunctionName(dbName, getName()),
                    signature.argumentsTypes.stream().map(DataType::toCatalogDataType).toArray(Type[]::new),
                    signature.returnType.toCatalogDataType(),
                    signature.hasVarArgs,
                    objectFile == null ? null : URI.create(objectFile),
                    symbol,
                    prepareFn,
                    closeFn
            );
            expr.setNullableMode(nullableMode);
            expr.setChecksum(checkSum);
            expr.setId(functionId);
            expr.setStaticLoad(isStaticLoad);
            expr.setExpirationTime(expirationTime);
            expr.setUDTFunction(true);
            expr.setRuntimeVersion(runtimeVersion);
            expr.setFunctionCode(functionCode);
            return expr;
        } catch (Exception e) {
            throw new AnalysisException(e.getMessage(), e.getCause());
        }
    }

    /**
     * translate catalog python udtf to nereids python udtf
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

        SlotReference[] arguments = argTypes.stream()
                .map(type -> new SlotReference(type.toString(), type))
                .toArray(SlotReference[]::new);

        PythonUdtf udtf = new PythonUdtf(fnName, scalar.getId(), dbName, scalar.getBinaryType(), sig,
                scalar.getNullableMode(),
                scalar.getLocation() == null ? null : scalar.getLocation().getLocation(),
                scalar.getSymbolName(),
                scalar.getPrepareFnSymbol(),
                scalar.getCloseFnSymbol(),
                scalar.getChecksum(),
                scalar.isStaticLoad(),
                scalar.getExpirationTime(),
                scalar.getRuntimeVersion(),
                scalar.getFunctionCode(),
                arguments);

        PythonUdtfBuilder builder = new PythonUdtfBuilder(udtf);
        Env.getCurrentEnv().getFunctionRegistry().addUdf(dbName, fnName, builder);
    }

    @Override
    public NullableMode getNullableMode() {
        return nullableMode;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitPythonUdtf(this, context);
    }
}
