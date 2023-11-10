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
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
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
 * Java UDAF for Nereids
 */
public class JavaUdaf extends AggregateFunction implements ExplicitlyCastableSignature, Udf {
    private final String dbName;
    private final long functionId;
    private final TFunctionBinaryType binaryType;
    private final FunctionSignature signature;
    private final DataType intermediateType;
    private final NullableMode nullableMode;
    private final String objectFile;
    private final String symbol;
    private final String initFn;
    private final String updateFn;
    private final String mergeFn;
    private final String serializeFn;
    private final String finalizeFn;
    private final String getValueFn;
    private final String removeFn;
    private final String checkSum;

    /**
     * Constructor of UDAF
     */
    public JavaUdaf(String name, long functionId, String dbName, TFunctionBinaryType binaryType,
            FunctionSignature signature,
            DataType intermediateType, NullableMode nullableMode,
            String objectFile, String symbol,
            String initFn, String updateFn, String mergeFn,
            String serializeFn, String finalizeFn, String getValueFn, String removeFn,
            boolean isDistinct, String checkSum, Expression... args) {
        super(name, isDistinct, args);
        this.dbName = dbName;
        this.functionId = functionId;
        this.binaryType = binaryType;
        this.signature = signature;
        this.intermediateType = intermediateType == null ? signature.returnType : intermediateType;
        this.nullableMode = nullableMode;
        this.objectFile = objectFile;
        this.symbol = symbol;
        this.initFn = initFn;
        this.updateFn = updateFn;
        this.mergeFn = mergeFn;
        this.serializeFn = serializeFn;
        this.finalizeFn = finalizeFn;
        this.getValueFn = getValueFn;
        this.removeFn = removeFn;
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
    public JavaUdaf withDistinctAndChildren(boolean isDistinct, List<Expression> children) {
        Preconditions.checkArgument(children.size() == this.children.size());
        return new JavaUdaf(getName(), functionId, dbName, binaryType, signature, intermediateType, nullableMode,
                objectFile, symbol, initFn, updateFn, mergeFn, serializeFn, finalizeFn, getValueFn, removeFn,
                isDistinct, checkSum, children.toArray(new Expression[0]));
    }

    /**
     * translate catalog java udf to nereids java udf
     */
    public static void translateToNereidsFunction(String dbName, org.apache.doris.catalog.AggregateFunction aggregate) {
        String fnName = aggregate.functionName();
        DataType retType = DataType.fromCatalogType(aggregate.getReturnType());
        List<DataType> argTypes = Arrays.stream(aggregate.getArgs())
                .map(DataType::fromCatalogType)
                .collect(Collectors.toList());

        FunctionSignature.FuncSigBuilder sigBuilder = FunctionSignature.ret(retType);
        FunctionSignature sig = aggregate.hasVarArgs()
                ? sigBuilder.varArgs(argTypes.toArray(new DataType[0]))
                : sigBuilder.args(argTypes.toArray(new DataType[0]));

        VirtualSlotReference[] virtualSlots = argTypes.stream()
                .map(type -> new VirtualSlotReference(type.toString(), type, Optional.empty(),
                        (shape) -> ImmutableList.of()))
                .toArray(VirtualSlotReference[]::new);

        DataType intermediateType = null;
        if (aggregate.getIntermediateType() != null) {
            intermediateType = DataType.fromCatalogType(aggregate.getIntermediateType());
        }

        JavaUdaf udaf = new JavaUdaf(fnName, aggregate.getId(), dbName, aggregate.getBinaryType(), sig,
                intermediateType,
                aggregate.getNullableMode(),
                aggregate.getLocation().getLocation(),
                aggregate.getSymbolName(),
                aggregate.getInitFnSymbol(),
                aggregate.getUpdateFnSymbol(),
                aggregate.getMergeFnSymbol(),
                aggregate.getSerializeFnSymbol(),
                aggregate.getFinalizeFnSymbol(),
                aggregate.getGetValueFnSymbol(),
                aggregate.getRemoveFnSymbol(),
                false,
                aggregate.getChecksum(),
                virtualSlots);

        JavaUdafBuilder builder = new JavaUdafBuilder(udaf);
        Env.getCurrentEnv().getFunctionRegistry().addUdf(dbName, fnName, builder);
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitJavaUdaf(this, context);
    }

    @Override
    public Function getCatalogFunction() {
        try {
            org.apache.doris.catalog.AggregateFunction expr = new org.apache.doris.catalog.AggregateFunction(
                    new FunctionName(dbName, getName()),
                    signature.argumentsTypes.stream().map(DataType::toCatalogDataType).toArray(Type[]::new),
                    signature.returnType.toCatalogDataType(),
                    signature.hasVarArgs,
                    intermediateType.toCatalogDataType(),
                    URI.create(objectFile),
                    initFn,
                    updateFn,
                    mergeFn,
                    serializeFn,
                    finalizeFn,
                    getValueFn,
                    removeFn
            );
            expr.setSymbolName(symbol);
            expr.setBinaryType(binaryType);
            expr.setNullableMode(nullableMode);
            expr.setChecksum(checkSum);
            expr.setId(functionId);
            return expr;
        } catch (Exception e) {
            throw new AnalysisException(e.getMessage(), e.getCause());
        }
    }
}
