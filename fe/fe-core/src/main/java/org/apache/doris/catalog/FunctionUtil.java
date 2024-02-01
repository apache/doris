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

package org.apache.doris.catalog;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.SetType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Text;
import org.apache.doris.nereids.trees.expressions.functions.udf.AliasUdf;
import org.apache.doris.nereids.trees.expressions.functions.udf.JavaUdaf;
import org.apache.doris.nereids.trees.expressions.functions.udf.JavaUdf;
import org.apache.doris.nereids.types.DataType;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * function util.
 */
public class FunctionUtil {
    private static final Logger LOG = LogManager.getLogger(FunctionUtil.class);


    /**
     * @param function
     * @param ifExists
     * @param name2Function
     * @return return true if we do drop the function, otherwise, return false.
     * @throws UserException
     */
    public static boolean dropFunctionImpl(FunctionSearchDesc function, boolean ifExists,
            ConcurrentMap<String, ImmutableList<Function>> name2Function) throws UserException {
        String functionName = function.getName().getFunction();
        List<Function> existFuncs = name2Function.get(functionName);
        if (existFuncs == null) {
            if (ifExists) {
                LOG.debug("function name does not exist: " + functionName);
                return false;
            }
            throw new UserException("function name does not exist: " + functionName);
        }
        boolean isFound = false;
        ImmutableList.Builder<Function> builder = ImmutableList.builder();
        for (Function existFunc : existFuncs) {
            if (function.isIdentical(existFunc)) {
                isFound = true;
            } else {
                builder.add(existFunc);
            }
        }
        if (!isFound) {
            if (ifExists) {
                LOG.debug("function does not exist: " + function);
                return false;
            }
            throw new UserException("function does not exist: " + function);
        }
        ImmutableList<Function> newFunctions = builder.build();
        if (newFunctions.isEmpty()) {
            name2Function.remove(functionName);
        } else {
            name2Function.put(functionName, newFunctions);
        }
        return true;
    }

    /**
     * @param function
     * @param ifNotExists
     * @param isReplay
     * @param name2Function
     * @return return true if we do add the function, otherwise, return false.
     * @throws UserException
     */
    public static boolean addFunctionImpl(Function function, boolean ifNotExists, boolean isReplay,
            ConcurrentMap<String, ImmutableList<Function>> name2Function) throws UserException {
        String functionName = function.getFunctionName().getFunction();
        List<Function> existFuncs = name2Function.get(functionName);
        if (!isReplay) {
            if (existFuncs != null) {
                for (Function existFunc : existFuncs) {
                    if (function.compare(existFunc, Function.CompareMode.IS_IDENTICAL)) {
                        if (ifNotExists) {
                            LOG.debug("function already exists");
                            return false;
                        }
                        throw new UserException("function already exists");
                    }
                }
            }
            // Get function id for this UDF, use CatalogIdGenerator. Only get function id
            // when isReplay is false
            long functionId = Env.getCurrentEnv().getNextId();
            function.setId(functionId);
        }

        ImmutableList.Builder<Function> builder = ImmutableList.builder();
        if (existFuncs != null) {
            builder.addAll(existFuncs);
        }
        builder.add(function);
        name2Function.put(functionName, builder.build());
        return true;
    }

    public static Function getFunction(FunctionSearchDesc function,
            ConcurrentMap<String, ImmutableList<Function>> name2Function) throws AnalysisException {
        String functionName = function.getName().getFunction();
        List<Function> existFuncs = name2Function.get(functionName);
        if (existFuncs == null) {
            throw new AnalysisException("Unknown function, function=" + function);
        }

        for (Function existFunc : existFuncs) {
            if (function.isIdentical(existFunc)) {
                return existFunc;
            }
        }
        throw new AnalysisException("Unknown function, function=" + function);
    }

    public static List<Function> getFunctions(ConcurrentMap<String, ImmutableList<Function>> name2Function) {
        List<Function> functions = Lists.newArrayList();
        for (Map.Entry<String, ImmutableList<Function>> entry : name2Function.entrySet()) {
            functions.addAll(entry.getValue());
        }
        return functions;
    }

    public static Function getFunction(Function desc, Function.CompareMode mode,
            ConcurrentMap<String, ImmutableList<Function>> name2Function) {
        List<Function> fns = name2Function.get(desc.getFunctionName().getFunction());
        if (fns == null) {
            return null;
        }
        return Function.getFunction(fns, desc, mode);
    }

    public static void write(DataOutput out, ConcurrentMap<String, ImmutableList<Function>> name2Function)
            throws IOException {
        // write functions
        out.writeInt(name2Function.size());
        for (Entry<String, ImmutableList<Function>> entry : name2Function.entrySet()) {
            Text.writeString(out, entry.getKey());
            out.writeInt(entry.getValue().size());
            for (Function function : entry.getValue()) {
                function.write(out);
            }
        }
    }

    public static void readFields(DataInput in, String dbName,
            ConcurrentMap<String, ImmutableList<Function>> name2Function)
            throws IOException {
        int numEntries = in.readInt();
        for (int i = 0; i < numEntries; ++i) {
            String name = Text.readString(in);
            ImmutableList.Builder<Function> builder = ImmutableList.builder();
            int numFunctions = in.readInt();
            for (int j = 0; j < numFunctions; ++j) {
                builder.add(Function.read(in));
            }
            ImmutableList<Function> functions = builder.build();
            name2Function.put(name, functions);
            for (Function f : functions) {
                translateToNereids(dbName, f);
            }
        }
    }

    /***
     * is global function
     * @return
     */
    public static boolean isGlobalFunction(SetType type) {
        return SetType.GLOBAL == type;
    }

    /***
     * reAcquire dbName and check "No database selected"
     * @param analyzer
     * @param dbName
     * @param clusterName
     * @return
     * @throws AnalysisException
     */
    public static String reAcquireDbName(Analyzer analyzer, String dbName)
            throws AnalysisException {
        if (Strings.isNullOrEmpty(dbName)) {
            dbName = analyzer.getDefaultDb();
            if (Strings.isNullOrEmpty(dbName)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }
        }
        return dbName;
    }

    public static boolean translateToNereids(String dbName, Function function) {
        try {
            if (function instanceof AliasFunction) {
                AliasUdf.translateToNereidsFunction(dbName, ((AliasFunction) function));
            } else if (function instanceof ScalarFunction) {
                JavaUdf.translateToNereidsFunction(dbName, ((ScalarFunction) function));
            } else if (function instanceof AggregateFunction) {
                JavaUdaf.translateToNereidsFunction(dbName, ((AggregateFunction) function));
            }
        } catch (Exception e) {
            LOG.warn("Nereids create function {}:{} failed, caused by: {}", dbName == null ? "_global_" : dbName,
                    function.getFunctionName().getFunction(), e);
        }
        return true;
    }

    public static boolean dropFromNereids(String dbName, FunctionSearchDesc function) {
        try {
            String fnName = function.getName().getFunction();
            List<DataType> argTypes = Arrays.stream(function.getArgTypes()).map(DataType::fromCatalogType)
                    .collect(Collectors.toList());
            Env.getCurrentEnv().getFunctionRegistry().dropUdf(dbName, fnName, argTypes);
        } catch (Exception e) {
            LOG.warn("Nereids drop function {}:{} failed, caused by: {}", dbName == null ? "_global_" : dbName,
                    function.getName(), e);
        }
        return false;
    }

    public static void checkEnableJavaUdf() throws AnalysisException {
        if (!Config.enable_java_udf) {
            throw new AnalysisException("java_udf has been disabled.");
        }
    }

    public static void checkEnableJavaUdfForNereids() {
        if (!Config.enable_java_udf) {
            throw new org.apache.doris.nereids.exceptions.AnalysisException("java_udf has been disabled.");
        }
    }
}
