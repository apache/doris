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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.analysis.FunctionName;
import org.apache.doris.catalog.AggregateFunction;
import org.apache.doris.catalog.AliasFunction;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.Function.CompareMode;
import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.catalog.FunctionSignature.FuncSigBuilder;
import org.apache.doris.catalog.ScalarFunction;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.agg.JavaUdaf;
import org.apache.doris.nereids.trees.expressions.functions.scalar.JavaUdf;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.coercion.AbstractDataType;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * handle bind UDF
 */
public class UdfBinder {

    /**
     * rewrite catalog-style function to nereids-style bound function.
     */
    public Expression rewriteFunction(UnboundFunction function, CascadesContext context) {
        Database db = getDb(context);
        if (db == null) {
            return null;
        }
        Function catalogFunction = getFunction(function, db);
        if (catalogFunction == null) {
            return null;
        }
        if (catalogFunction instanceof ScalarFunction) {
            return handleJavaUdf(function, ((ScalarFunction) catalogFunction));
        } else if (catalogFunction instanceof AggregateFunction) {
            return handleJavaUDAF(function, ((AggregateFunction) catalogFunction));
        } else if (catalogFunction instanceof AliasFunction) {
            return handleAliasFunction(function, ((AliasFunction) catalogFunction), context);
        }
        throw new AnalysisException(String.format("unsupported alias function type %s for Nereids",
                catalogFunction.getClass()));
    }

    private Database getDb(CascadesContext context) {
        Env env = context.getConnectContext().getEnv();
        String dbName = context.getConnectContext().getDatabase();
        return env.getInternalCatalog().getDbNullable(dbName);
    }

    private Function getFunction(UnboundFunction function, Database database) {
        List<Type> types = function.getArgumentsTypes().stream()
                .map(AbstractDataType::toCatalogDataType).collect(Collectors.toList());
        Function desc = new Function(new FunctionName(database.getFullName(), function.getName()),
                types, Type.INVALID, false);
        Function catalogFn;
        try {
            catalogFn = database.getFunction(desc, CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        } catch (Exception e) {
            throw new AnalysisException(e.getMessage());
        }
        return catalogFn;
    }

    private Expression handleAliasFunction(UnboundFunction nereidsFunction, AliasFunction catalogFunction,
            CascadesContext context) {
        AliasFunctionRewriter rewriter = new AliasFunctionRewriter();
        Expression aliasFunction = rewriter.rewriteFunction(nereidsFunction, catalogFunction);
        if (aliasFunction == null) {
            return null;
        }
        return FunctionBinder.INSTANCE.bind(aliasFunction, context);
    }

    private Expression handleJavaUdf(UnboundFunction nereidsFunction, ScalarFunction catalogFunction) {
        Type retType = catalogFunction.getReturnType();
        Type[] argTypes = catalogFunction.getArgs();
        Type varArgTypes = catalogFunction.getVarArgsType();
        boolean hasVarArgs = catalogFunction.hasVarArgs();
        FunctionSignature signature;
        FuncSigBuilder sigBuilder = FunctionSignature
                .ret(DataType.fromCatalogType(retType));
        if (hasVarArgs) {
            List<DataType> dataTypes = Arrays.stream(argTypes).map(DataType::fromCatalogType)
                    .collect(Collectors.toList());
            dataTypes.add(DataType.fromCatalogType(varArgTypes));
            signature = sigBuilder.varArgs(dataTypes.toArray(new AbstractDataType[0]));
        } else {
            signature = sigBuilder.args(Arrays.stream(argTypes).map(DataType::fromCatalogType)
                    .toArray(AbstractDataType[]::new));
        }
        return TypeCoercionUtils.processBoundFunction(new JavaUdf(catalogFunction,
                signature,
                nereidsFunction.getName(),
                nereidsFunction.children().toArray(new Expression[0])));
    }

    private Expression handleJavaUDAF(UnboundFunction nereidsFunction, AggregateFunction catalogFunction) {
        Type retType = catalogFunction.getReturnType();
        Type[] argTypes = catalogFunction.getArgs();
        Type varArgTypes = catalogFunction.getVarArgsType();
        boolean hasVarArgs = catalogFunction.hasVarArgs();
        FunctionSignature signature;
        FuncSigBuilder sigBuilder = FunctionSignature
                .ret(DataType.fromCatalogType(retType));
        if (hasVarArgs) {
            List<DataType> dataTypes = Arrays.stream(argTypes).map(DataType::fromCatalogType)
                    .collect(Collectors.toList());
            dataTypes.add(DataType.fromCatalogType(varArgTypes));
            signature = sigBuilder.varArgs(dataTypes.toArray(new AbstractDataType[0]));
        } else {
            signature = sigBuilder.args(Arrays.stream(argTypes).map(DataType::fromCatalogType)
                    .toArray(AbstractDataType[]::new));
        }
        return TypeCoercionUtils.processBoundFunction(new JavaUdaf(catalogFunction,
                signature,
                nereidsFunction.getName(),
                false,
                nereidsFunction.children().toArray(new Expression[0])));
    }
}
