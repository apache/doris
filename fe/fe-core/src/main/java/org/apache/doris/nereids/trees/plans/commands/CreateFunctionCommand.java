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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.FunctionName;
import org.apache.doris.analysis.FunctionParams;
import org.apache.doris.analysis.SetType;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StmtType;
import org.apache.doris.catalog.AggregateFunction;
import org.apache.doris.catalog.AliasFunction;
import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.Function.NullableMode;
import org.apache.doris.catalog.FunctionUtil;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarFunction;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.util.URI;
import org.apache.doris.common.util.Util;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.analyzer.Scope;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.glue.translator.ExpressionTranslator;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.analysis.ExpressionAnalyzer;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.BitAnd;
import org.apache.doris.nereids.trees.expressions.BitNot;
import org.apache.doris.nereids.trees.expressions.BitOr;
import org.apache.doris.nereids.trees.expressions.BitXor;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Divide;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IntegralDivide;
import org.apache.doris.nereids.trees.expressions.Mod;
import org.apache.doris.nereids.trees.expressions.Multiply;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.Subtract;
import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.FunctionArgTypesInfo;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.proto.FunctionService;
import org.apache.doris.proto.PFunctionServiceGrpc;
import org.apache.doris.proto.Types;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TFunctionBinaryType;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.collections.map.CaseInsensitiveMap;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * create a alias or user defined function
 */
public class CreateFunctionCommand extends Command implements ForwardWithSync {
    @Deprecated
    public static final String OBJECT_FILE_KEY = "object_file";
    public static final String FILE_KEY = "file";
    public static final String SYMBOL_KEY = "symbol";
    public static final String PREPARE_SYMBOL_KEY = "prepare_fn";
    public static final String CLOSE_SYMBOL_KEY = "close_fn";
    public static final String MD5_CHECKSUM = "md5";
    public static final String INIT_KEY = "init_fn";
    public static final String UPDATE_KEY = "update_fn";
    public static final String MERGE_KEY = "merge_fn";
    public static final String SERIALIZE_KEY = "serialize_fn";
    public static final String FINALIZE_KEY = "finalize_fn";
    public static final String GET_VALUE_KEY = "get_value_fn";
    public static final String REMOVE_KEY = "remove_fn";
    public static final String BINARY_TYPE = "type";
    public static final String EVAL_METHOD_KEY = "evaluate";
    public static final String CREATE_METHOD_NAME = "create";
    public static final String DESTROY_METHOD_NAME = "destroy";
    public static final String ADD_METHOD_NAME = "add";
    public static final String SERIALIZE_METHOD_NAME = "serialize";
    public static final String MERGE_METHOD_NAME = "merge";
    public static final String GETVALUE_METHOD_NAME = "getValue";
    public static final String STATE_CLASS_NAME = "State";
    // add for java udf check return type nullable mode, always_nullable or always_not_nullable
    public static final String IS_RETURN_NULL = "always_nullable";
    // iff is static load, BE will be cache the udf class load, so only need load once
    public static final String IS_STATIC_LOAD = "static_load";
    public static final String EXPIRATION_TIME = "expiration_time";

    // timeout for both connection and read. 10 seconds is long enough.
    private static final int HTTP_TIMEOUT_MS = 10000;
    private final SetType setType;
    private final boolean ifNotExists;
    private final FunctionName functionName;
    private final boolean isAggregate;
    private final boolean isAlias;
    private final boolean isTableFunction;
    private final FunctionArgTypesInfo argsDef;
    private final DataType returnType;
    private DataType intermediateType;
    private final Map<String, String> properties;
    private final List<String> parameters;
    private final Expression originFunction;
    private TFunctionBinaryType binaryType = TFunctionBinaryType.JAVA_UDF;
    // needed item set after analyzed
    private String userFile;
    private Function function;
    private String checksum = "";
    private boolean isStaticLoad = false;
    private long expirationTime = 360; // default 6 hours = 360 minutes
    // now set udf default NullableMode is ALWAYS_NULLABLE
    // if not, will core dump when input is not null column, but need return null
    // like https://github.com/apache/doris/pull/14002/files
    private NullableMode returnNullMode = NullableMode.ALWAYS_NULLABLE;

    /**
     * CreateFunctionCommand
     */
    public CreateFunctionCommand(SetType setType, boolean ifNotExists, boolean isAggregate, boolean isAlias,
                                 boolean isTableFunction, FunctionName functionName, FunctionArgTypesInfo argsDef,
                                 DataType returnType, DataType intermediateType, List<String> parameters,
                                 Expression originFunction, Map<String, String> properties) {
        super(PlanType.CREATE_FUNCTION_COMMAND);
        this.setType = setType;
        this.ifNotExists = ifNotExists;
        this.isAggregate = isAggregate;
        this.isAlias = isAlias;
        this.isTableFunction = isTableFunction;
        this.functionName = functionName;
        this.argsDef = argsDef;
        this.returnType = returnType;
        this.intermediateType = intermediateType;
        if (parameters == null) {
            this.parameters = ImmutableList.of();
        } else {
            this.parameters = ImmutableList.copyOf(parameters);
        }
        this.originFunction = originFunction;
        if (properties == null) {
            this.properties = ImmutableSortedMap.of();
        } else {
            this.properties = ImmutableSortedMap.copyOf(properties, String.CASE_INSENSITIVE_ORDER);
        }
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        analyze(ctx);
        if (SetType.GLOBAL.equals(setType)) {
            Env.getCurrentEnv().getGlobalFunctionMgr().addFunction(function, ifNotExists);
        } else {
            String dbName = functionName.getDb();
            if (dbName == null) {
                dbName = ctx.getDatabase();
                functionName.setDb(dbName);
            }
            Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(dbName);
            db.addFunction(function, ifNotExists);
            if (function.isUDTFunction()) {
                // all of the table function in doris will have two function
                // one is the noraml, and another is outer, the different of them is deal with
                // empty: whether need to insert NULL result value
                Function outerFunction = function.clone();
                FunctionName name = outerFunction.getFunctionName();
                name.setFn(name.getFunction() + "_outer");
                db.addFunction(outerFunction, ifNotExists);
            }
        }
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCreateFunctionCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.CREATE;
    }

    private void analyze(ConnectContext ctx) throws Exception {
        // https://github.com/apache/doris/issues/17810
        // this error report in P0 test, so we suspect that it is related to concurrency
        // add this change to test it.
        if (Config.use_fuzzy_session_variable) {
            synchronized (CreateFunctionCommand.class) {
                analyzeCommon(ctx);
                // check
                if (isAggregate) {
                    analyzeUdaf();
                } else if (isAlias) {
                    analyzeAliasFunction(ctx);
                } else if (isTableFunction) {
                    analyzeUdtf();
                } else {
                    analyzeUdf();
                }
            }
        } else {
            analyzeCommon(ctx);
            // check
            if (isAggregate) {
                analyzeUdaf();
            } else if (isAlias) {
                analyzeAliasFunction(ctx);
            } else if (isTableFunction) {
                analyzeUdtf();
            } else {
                analyzeUdf();
            }
        }
    }

    private void analyzeCommon(ConnectContext ctx) throws AnalysisException {
        // check function name
        if (functionName.getDb() == null) {
            String db = ctx.getDatabase();
            if (Strings.isNullOrEmpty(db) && setType != SetType.GLOBAL) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }
        }

        // check operation privilege
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }
        // check argument
        argsDef.analyze();

        // alias function does not need analyze following params
        if (isAlias) {
            return;
        }
        returnType.validateDataType();
        if (intermediateType != null) {
            intermediateType.validateDataType();
        } else {
            intermediateType = returnType;
        }

        String type = properties.getOrDefault(BINARY_TYPE, "JAVA_UDF");
        binaryType = getFunctionBinaryType(type);
        if (binaryType == null) {
            throw new AnalysisException("unknown function type");
        }
        if (type.equals("NATIVE")) {
            throw new AnalysisException("do not support 'NATIVE' udf type after doris version 1.2.0,"
                    + "please use JAVA_UDF or RPC instead");
        }

        userFile = properties.getOrDefault(FILE_KEY, properties.get(OBJECT_FILE_KEY));
        if (!Strings.isNullOrEmpty(userFile) && binaryType != TFunctionBinaryType.RPC) {
            try {
                computeObjectChecksum();
            } catch (IOException | NoSuchAlgorithmException e) {
                throw new AnalysisException("cannot to compute object's checksum. err: " + e.getMessage());
            }
            String md5sum = properties.get(MD5_CHECKSUM);
            if (md5sum != null && !md5sum.equalsIgnoreCase(checksum)) {
                throw new AnalysisException("library's checksum is not equal with input, checksum=" + checksum);
            }
        }
        if (binaryType == TFunctionBinaryType.JAVA_UDF) {
            FunctionUtil.checkEnableJavaUdf();

            // always_nullable the default value is true, equal null means true
            Boolean isReturnNull = parseBooleanFromProperties(IS_RETURN_NULL);
            if (isReturnNull != null && !isReturnNull) {
                returnNullMode = NullableMode.ALWAYS_NOT_NULLABLE;
            }
            // static_load the default value is false, equal null means false
            Boolean staticLoad = parseBooleanFromProperties(IS_STATIC_LOAD);
            if (staticLoad != null && staticLoad) {
                isStaticLoad = true;
            }
            String expirationTimeString = properties.get(EXPIRATION_TIME);
            if (expirationTimeString != null) {
                long timeMinutes = 0;
                try {
                    timeMinutes = Long.parseLong(expirationTimeString);
                } catch (NumberFormatException e) {
                    throw new AnalysisException(e.getMessage());
                }
                if (timeMinutes <= 0) {
                    throw new AnalysisException("expirationTime should greater than zero: ");
                }
                this.expirationTime = timeMinutes;
            }
        }
    }

    private Boolean parseBooleanFromProperties(String propertyString) throws AnalysisException {
        String valueOfString = properties.get(propertyString);
        if (valueOfString == null) {
            return null;
        }
        if (!valueOfString.equalsIgnoreCase("false") && !valueOfString.equalsIgnoreCase("true")) {
            throw new AnalysisException(propertyString + " in properties, you should set it false or true");
        }
        return Boolean.parseBoolean(valueOfString);
    }

    private void computeObjectChecksum() throws IOException, NoSuchAlgorithmException {
        if (FeConstants.runningUnitTest) {
            // skip checking checksum when running ut
            return;
        }

        try (InputStream inputStream = Util.getInputStreamFromUrl(userFile, null, HTTP_TIMEOUT_MS, HTTP_TIMEOUT_MS)) {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            byte[] buf = new byte[4096];
            int bytesRead = 0;
            do {
                bytesRead = inputStream.read(buf);
                if (bytesRead < 0) {
                    break;
                }
                digest.update(buf, 0, bytesRead);
            } while (true);

            checksum = Hex.encodeHexString(digest.digest());
        }
    }

    private void analyzeUdtf() throws AnalysisException {
        String symbol = properties.get(SYMBOL_KEY);
        if (Strings.isNullOrEmpty(symbol)) {
            throw new AnalysisException("No 'symbol' in properties");
        }
        if (!returnType.isArrayType()) {
            throw new AnalysisException("JAVA_UDF OF UDTF return type must be array type");
        }
        analyzeJavaUdf(symbol);
        URI location;
        if (!Strings.isNullOrEmpty(userFile)) {
            location = URI.create(userFile);
        } else {
            location = null;
        }
        function = ScalarFunction.createUdf(binaryType,
                functionName, argsDef.getArgTypes(),
                ((ArrayType) (returnType.toCatalogDataType())).getItemType(), argsDef.isVariadic(),
                location, symbol, null, null);
        function.setChecksum(checksum);
        function.setNullableMode(returnNullMode);
        function.setUDTFunction(true);
        // Todo: maybe in create tables function, need register two function, one is
        // normal and one is outer as those have different result when result is NULL.
    }

    private void analyzeUdaf() throws AnalysisException {
        AggregateFunction.AggregateFunctionBuilder builder = AggregateFunction.AggregateFunctionBuilder
                .createUdfBuilder();
        URI location;
        if (!Strings.isNullOrEmpty(userFile)) {
            location = URI.create(userFile);
        } else {
            location = null;
        }
        builder.name(functionName).argsType(argsDef.getArgTypes()).retType(returnType.toCatalogDataType())
                .hasVarArgs(argsDef.isVariadic()).intermediateType(intermediateType.toCatalogDataType())
                .location(location);
        String initFnSymbol = properties.get(INIT_KEY);
        if (initFnSymbol == null && !(binaryType == TFunctionBinaryType.JAVA_UDF
                || binaryType == TFunctionBinaryType.RPC)) {
            throw new AnalysisException("No 'init_fn' in properties");
        }
        String updateFnSymbol = properties.get(UPDATE_KEY);
        if (updateFnSymbol == null && !(binaryType == TFunctionBinaryType.JAVA_UDF)) {
            throw new AnalysisException("No 'update_fn' in properties");
        }
        String mergeFnSymbol = properties.get(MERGE_KEY);
        if (mergeFnSymbol == null && !(binaryType == TFunctionBinaryType.JAVA_UDF)) {
            throw new AnalysisException("No 'merge_fn' in properties");
        }
        String serializeFnSymbol = properties.get(SERIALIZE_KEY);
        String finalizeFnSymbol = properties.get(FINALIZE_KEY);
        String getValueFnSymbol = properties.get(GET_VALUE_KEY);
        String removeFnSymbol = properties.get(REMOVE_KEY);
        String symbol = properties.get(SYMBOL_KEY);
        if (binaryType == TFunctionBinaryType.RPC && !userFile.contains("://")) {
            if (initFnSymbol != null) {
                checkRPCUdf(initFnSymbol);
            }
            checkRPCUdf(updateFnSymbol);
            checkRPCUdf(mergeFnSymbol);
            if (serializeFnSymbol != null) {
                checkRPCUdf(serializeFnSymbol);
            }
            if (finalizeFnSymbol != null) {
                checkRPCUdf(finalizeFnSymbol);
            }
            if (getValueFnSymbol != null) {
                checkRPCUdf(getValueFnSymbol);
            }
            if (removeFnSymbol != null) {
                checkRPCUdf(removeFnSymbol);
            }
        } else if (binaryType == TFunctionBinaryType.JAVA_UDF) {
            if (Strings.isNullOrEmpty(symbol)) {
                throw new AnalysisException("No 'symbol' in properties of java-udaf");
            }
            analyzeJavaUdaf(symbol);
        }
        function = builder.initFnSymbol(initFnSymbol).updateFnSymbol(updateFnSymbol).mergeFnSymbol(mergeFnSymbol)
                .serializeFnSymbol(serializeFnSymbol).finalizeFnSymbol(finalizeFnSymbol)
                .getValueFnSymbol(getValueFnSymbol).removeFnSymbol(removeFnSymbol).symbolName(symbol).build();
        function.setLocation(location);
        function.setBinaryType(binaryType);
        function.setChecksum(checksum);
        function.setNullableMode(returnNullMode);
        function.setStaticLoad(isStaticLoad);
        function.setExpirationTime(expirationTime);
    }

    private void analyzeUdf() throws AnalysisException {
        String symbol = properties.get(SYMBOL_KEY);
        if (Strings.isNullOrEmpty(symbol)) {
            throw new AnalysisException("No 'symbol' in properties");
        }
        String prepareFnSymbol = properties.get(PREPARE_SYMBOL_KEY);
        String closeFnSymbol = properties.get(CLOSE_SYMBOL_KEY);
        // TODO(yangzhg) support check function in FE when function service behind load balancer
        // the format for load balance can ref https://github.com/apache/incubator-brpc/blob/master/docs/en/client.md#connect-to-a-cluster
        if (binaryType == TFunctionBinaryType.RPC && !userFile.contains("://")) {
            if (StringUtils.isNotBlank(prepareFnSymbol) || StringUtils.isNotBlank(closeFnSymbol)) {
                throw new AnalysisException("prepare and close in RPC UDF are not supported.");
            }
            checkRPCUdf(symbol);
        } else if (binaryType == TFunctionBinaryType.JAVA_UDF) {
            analyzeJavaUdf(symbol);
        }
        URI location;
        if (!Strings.isNullOrEmpty(userFile)) {
            location = URI.create(userFile);
        } else {
            location = null;
        }
        function = ScalarFunction.createUdf(binaryType,
                functionName, argsDef.getArgTypes(),
                returnType.toCatalogDataType(), argsDef.isVariadic(),
                location, symbol, prepareFnSymbol, closeFnSymbol);
        function.setChecksum(checksum);
        function.setNullableMode(returnNullMode);
        function.setStaticLoad(isStaticLoad);
        function.setExpirationTime(expirationTime);
    }

    private void analyzeJavaUdaf(String clazz) throws AnalysisException {
        HashMap<String, Method> allMethods = new HashMap<>();

        try {
            if (Strings.isNullOrEmpty(userFile)) {
                try {
                    ClassLoader cl = this.getClass().getClassLoader();
                    checkUdafClass(clazz, cl, allMethods);
                    return;
                } catch (ClassNotFoundException e) {
                    throw new AnalysisException("Class [" + clazz + "] not found in classpath");
                }
            }
            URL[] urls = { new URL("jar:" + userFile + "!/") };
            try (URLClassLoader cl = URLClassLoader.newInstance(urls)) {
                checkUdafClass(clazz, cl, allMethods);
            } catch (ClassNotFoundException e) {
                throw new AnalysisException(
                        "Class [" + clazz + "] or inner class [State] not found in file :" + userFile);
            } catch (IOException e) {
                throw new AnalysisException("Failed to load file: " + userFile);
            }
        } catch (MalformedURLException e) {
            throw new AnalysisException("Failed to load file: " + userFile);
        }
    }

    private void checkUdafClass(String clazz, ClassLoader cl, HashMap<String, Method> allMethods)
            throws ClassNotFoundException, AnalysisException {
        Class udfClass = cl.loadClass(clazz);
        String udfClassName = udfClass.getCanonicalName();
        String stateClassName = udfClassName + "$" + STATE_CLASS_NAME;
        Class stateClass = cl.loadClass(stateClassName);

        for (Method m : udfClass.getMethods()) {
            if (!m.getDeclaringClass().equals(udfClass)) {
                continue;
            }
            String name = m.getName();
            if (allMethods.containsKey(name)) {
                throw new AnalysisException(
                        String.format("UDF class '%s' has multiple methods with name '%s' ", udfClassName,
                                name));
            }
            allMethods.put(name, m);
        }

        if (allMethods.get(CREATE_METHOD_NAME) == null) {
            throw new AnalysisException(
                    String.format("No method '%s' in class '%s'!", CREATE_METHOD_NAME, udfClassName));
        } else {
            checkMethodNonStaticAndPublic(CREATE_METHOD_NAME, allMethods.get(CREATE_METHOD_NAME), udfClassName);
            checkArgumentCount(allMethods.get(CREATE_METHOD_NAME), 0, udfClassName);
            checkReturnJavaType(udfClassName, allMethods.get(CREATE_METHOD_NAME), stateClass);
        }

        if (allMethods.get(DESTROY_METHOD_NAME) == null) {
            throw new AnalysisException(
                    String.format("No method '%s' in class '%s'!", DESTROY_METHOD_NAME, udfClassName));
        } else {
            checkMethodNonStaticAndPublic(DESTROY_METHOD_NAME, allMethods.get(DESTROY_METHOD_NAME),
                    udfClassName);
            checkArgumentCount(allMethods.get(DESTROY_METHOD_NAME), 1, udfClassName);
            checkReturnJavaType(udfClassName, allMethods.get(DESTROY_METHOD_NAME), void.class);
        }

        if (allMethods.get(ADD_METHOD_NAME) == null) {
            throw new AnalysisException(
                    String.format("No method '%s' in class '%s'!", ADD_METHOD_NAME, udfClassName));
        } else {
            checkMethodNonStaticAndPublic(ADD_METHOD_NAME, allMethods.get(ADD_METHOD_NAME), udfClassName);
            checkArgumentCount(allMethods.get(ADD_METHOD_NAME), argsDef.getArgTypes().length + 1, udfClassName);
            checkReturnJavaType(udfClassName, allMethods.get(ADD_METHOD_NAME), void.class);
            for (int i = 0; i < argsDef.getArgTypes().length; i++) {
                Parameter p = allMethods.get(ADD_METHOD_NAME).getParameters()[i + 1];
                checkUdfType(udfClass, allMethods.get(ADD_METHOD_NAME), argsDef.getArgTypes()[i], p.getType(),
                        p.getName());
            }
        }

        if (allMethods.get(SERIALIZE_METHOD_NAME) == null) {
            throw new AnalysisException(
                    String.format("No method '%s' in class '%s'!", SERIALIZE_METHOD_NAME, udfClassName));
        } else {
            checkMethodNonStaticAndPublic(SERIALIZE_METHOD_NAME, allMethods.get(SERIALIZE_METHOD_NAME),
                    udfClassName);
            checkArgumentCount(allMethods.get(SERIALIZE_METHOD_NAME), 2, udfClassName);
            checkReturnJavaType(udfClassName, allMethods.get(SERIALIZE_METHOD_NAME), void.class);
        }

        if (allMethods.get(MERGE_METHOD_NAME) == null) {
            throw new AnalysisException(
                    String.format("No method '%s' in class '%s'!", MERGE_METHOD_NAME, udfClassName));
        } else {
            checkMethodNonStaticAndPublic(MERGE_METHOD_NAME, allMethods.get(MERGE_METHOD_NAME), udfClassName);
            checkArgumentCount(allMethods.get(MERGE_METHOD_NAME), 2, udfClassName);
            checkReturnJavaType(udfClassName, allMethods.get(MERGE_METHOD_NAME), void.class);
        }

        if (allMethods.get(GETVALUE_METHOD_NAME) == null) {
            throw new AnalysisException(
                    String.format("No method '%s' in class '%s'!", GETVALUE_METHOD_NAME, udfClassName));
        } else {
            checkMethodNonStaticAndPublic(GETVALUE_METHOD_NAME, allMethods.get(GETVALUE_METHOD_NAME),
                    udfClassName);
            checkArgumentCount(allMethods.get(GETVALUE_METHOD_NAME), 1, udfClassName);
            checkReturnUdfType(udfClass, allMethods.get(GETVALUE_METHOD_NAME), returnType.toCatalogDataType());
        }

        if (!Modifier.isPublic(stateClass.getModifiers()) || !Modifier.isStatic(stateClass.getModifiers())) {
            throw new AnalysisException(
                    String.format(
                            "UDAF '%s' should have one public & static 'State' class to Construction data ",
                            udfClassName));
        }
    }

    private void checkMethodNonStaticAndPublic(String methoName, Method method, String udfClassName)
            throws AnalysisException {
        if (Modifier.isStatic(method.getModifiers())) {
            throw new AnalysisException(
                    String.format("Method '%s' in class '%s' should be non-static", methoName, udfClassName));
        }
        if (!Modifier.isPublic(method.getModifiers())) {
            throw new AnalysisException(
                    String.format("Method '%s' in class '%s' should be public", methoName, udfClassName));
        }
    }

    private void checkArgumentCount(Method method, int argumentCount, String udfClassName) throws AnalysisException {
        if (method.getParameters().length != argumentCount) {
            throw new AnalysisException(
                    String.format("The number of parameters for method '%s' in class '%s' should be %d",
                            method.getName(), udfClassName, argumentCount));
        }
    }

    private void checkReturnJavaType(String udfClassName, Method method, Class expType) throws AnalysisException {
        checkJavaType(udfClassName, method, expType, method.getReturnType(), "return");
    }

    private void checkJavaType(String udfClassName, Method method, Class expType, Class ptype, String pname)
            throws AnalysisException {
        if (!expType.equals(ptype)) {
            throw new AnalysisException(
                    String.format("UDF class '%s' method '%s' parameter %s[%s] expect type %s", udfClassName,
                            method.getName(), pname, ptype.getCanonicalName(), expType.getCanonicalName()));
        }
    }

    private void checkReturnUdfType(Class clazz, Method method, Type expType) throws AnalysisException {
        checkUdfType(clazz, method, expType, method.getReturnType(), "return");
    }

    private void analyzeJavaUdf(String clazz) throws AnalysisException {
        try {
            if (Strings.isNullOrEmpty(userFile)) {
                try {
                    ClassLoader cl = this.getClass().getClassLoader();
                    checkUdfClass(clazz, cl);
                    return;
                } catch (ClassNotFoundException e) {
                    throw new AnalysisException("Class [" + clazz + "] not found in classpath");
                }
            }
            URL[] urls = { new URL("jar:" + userFile + "!/") };
            try (URLClassLoader cl = URLClassLoader.newInstance(urls)) {
                checkUdfClass(clazz, cl);
            } catch (ClassNotFoundException e) {
                throw new AnalysisException("Class [" + clazz + "] not found in file :" + userFile);
            } catch (IOException e) {
                throw new AnalysisException("Failed to load file: " + userFile);
            }
        } catch (MalformedURLException e) {
            throw new AnalysisException("Failed to load file: " + userFile);
        }
    }

    private void checkUdfClass(String clazz, ClassLoader cl) throws ClassNotFoundException, AnalysisException {
        Class udfClass = cl.loadClass(clazz);
        List<Method> evalList = Arrays.stream(udfClass.getMethods())
                .filter(m -> m.getDeclaringClass().equals(udfClass) && EVAL_METHOD_KEY.equals(m.getName()))
                .collect(Collectors.toList());
        if (evalList.size() == 0) {
            throw new AnalysisException(String.format(
                    "No method '%s' in class '%s'!", EVAL_METHOD_KEY, udfClass.getCanonicalName()));
        }
        List<Method> evalNonStaticAndPublicList = evalList.stream()
                .filter(m -> !Modifier.isStatic(m.getModifiers()) && Modifier.isPublic(m.getModifiers()))
                .collect(Collectors.toList());
        if (evalNonStaticAndPublicList.size() == 0) {
            throw new AnalysisException(
                    String.format("Method '%s' in class '%s' should be non-static and public", EVAL_METHOD_KEY,
                            udfClass.getCanonicalName()));
        }
        List<Method> evalArgLengthMatchList = evalNonStaticAndPublicList.stream().filter(
                m -> m.getParameters().length == argsDef.getArgTypes().length).collect(Collectors.toList());
        if (evalArgLengthMatchList.size() == 0) {
            throw new AnalysisException(
                    String.format(
                            "The arguments number udf provided and create function command is not equal,"
                                    + " the parameters of '%s' method in class '%s' maybe should %d.",
                            EVAL_METHOD_KEY, udfClass.getCanonicalName(), argsDef.getArgTypes().length));
        } else if (evalArgLengthMatchList.size() == 1) {
            Method method = evalArgLengthMatchList.get(0);
            checkUdfType(udfClass, method, returnType.toCatalogDataType(), method.getReturnType(), "return");
            for (int i = 0; i < method.getParameters().length; i++) {
                Parameter p = method.getParameters()[i];
                checkUdfType(udfClass, method, argsDef.getArgTypes()[i], p.getType(), p.getName());
            }
        } else {
            // If multiple methods have the same parameters,
            // the error message returned cannot be as specific as a single method
            boolean hasError = false;
            for (Method method : evalArgLengthMatchList) {
                try {
                    checkUdfType(udfClass, method, returnType.toCatalogDataType(), method.getReturnType(), "return");
                    for (int i = 0; i < method.getParameters().length; i++) {
                        Parameter p = method.getParameters()[i];
                        checkUdfType(udfClass, method, argsDef.getArgTypes()[i], p.getType(), p.getName());
                    }
                    hasError = false;
                    break;
                } catch (AnalysisException e) {
                    hasError = true;
                }
            }
            if (hasError) {
                throw new AnalysisException(String.format(
                        "Multi methods '%s' in class '%s' and no one passed parameter matching verification",
                        EVAL_METHOD_KEY, udfClass.getCanonicalName()));
            }
        }
    }

    private void checkUdfType(Class clazz, Method method, Type expType, Class pType, String pname)
            throws AnalysisException {
        Set<Class> javaTypes;
        if (expType instanceof ScalarType) {
            ScalarType scalarType = (ScalarType) expType;
            javaTypes = Type.PrimitiveTypeToJavaClassType.get(scalarType.getPrimitiveType());
        } else if (expType instanceof ArrayType) {
            ArrayType arrayType = (ArrayType) expType;
            javaTypes = Type.PrimitiveTypeToJavaClassType.get(arrayType.getPrimitiveType());
        } else if (expType instanceof MapType) {
            MapType mapType = (MapType) expType;
            javaTypes = Type.PrimitiveTypeToJavaClassType.get(mapType.getPrimitiveType());
        } else if (expType instanceof StructType) {
            StructType structType = (StructType) expType;
            javaTypes = Type.PrimitiveTypeToJavaClassType.get(structType.getPrimitiveType());
        } else {
            throw new AnalysisException(
                    String.format("Method '%s' in class '%s' does not support type '%s'.",
                            method.getName(), clazz.getCanonicalName(), expType));
        }

        if (javaTypes == null) {
            throw new AnalysisException(
                    String.format("Method '%s' in class '%s' does not support type '%s'.",
                            method.getName(), clazz.getCanonicalName(), expType.getPrimitiveType().toString()));
        }
        if (!javaTypes.contains(pType)) {
            throw new AnalysisException(
                    String.format(
                            "UDF class '%s' of method '%s' %s is [%s] type, but create function command type is %s.",
                            clazz.getCanonicalName(), method.getName(), pname, pType.getCanonicalName(),
                            expType.getPrimitiveType().toString()));
        }
    }

    private void checkRPCUdf(String symbol) throws AnalysisException {
        // TODO(yangzhg) support check function in FE when function service behind load balancer
        // the format for load balance can ref https://github.com/apache/incubator-brpc/blob/master/docs/en/client.md#connect-to-a-cluster
        String[] url = userFile.split(":");
        if (url.length != 2) {
            throw new AnalysisException("function server address invalid.");
        }
        String host = url[0];
        int port = Integer.valueOf(url[1]);
        ManagedChannel channel = NettyChannelBuilder.forAddress(host, port)
                .flowControlWindow(Config.grpc_max_message_size_bytes)
                .maxInboundMessageSize(Config.grpc_max_message_size_bytes)
                .enableRetry().maxRetryAttempts(3)
                .usePlaintext().build();
        PFunctionServiceGrpc.PFunctionServiceBlockingStub stub = PFunctionServiceGrpc.newBlockingStub(channel);
        FunctionService.PCheckFunctionRequest.Builder builder = FunctionService.PCheckFunctionRequest.newBuilder();
        builder.getFunctionBuilder().setFunctionName(symbol);
        for (Type arg : argsDef.getArgTypes()) {
            builder.getFunctionBuilder().addInputs(convertToPParameterType(arg));
        }
        builder.getFunctionBuilder().setOutput(convertToPParameterType(returnType.toCatalogDataType()));
        FunctionService.PCheckFunctionResponse response = stub.checkFn(builder.build());
        if (response == null || !response.hasStatus()) {
            throw new AnalysisException("cannot access function server");
        }
        if (response.getStatus().getStatusCode() != 0) {
            throw new AnalysisException("check function [" + symbol + "] failed: " + response.getStatus());
        }
    }

    private Types.PGenericType convertToPParameterType(Type arg) throws AnalysisException {
        Types.PGenericType.Builder typeBuilder = Types.PGenericType.newBuilder();
        switch (arg.getPrimitiveType()) {
            case INVALID_TYPE:
                typeBuilder.setId(Types.PGenericType.TypeId.UNKNOWN);
                break;
            case BOOLEAN:
                typeBuilder.setId(Types.PGenericType.TypeId.BOOLEAN);
                break;
            case SMALLINT:
                typeBuilder.setId(Types.PGenericType.TypeId.INT16);
                break;
            case TINYINT:
                typeBuilder.setId(Types.PGenericType.TypeId.INT8);
                break;
            case INT:
                typeBuilder.setId(Types.PGenericType.TypeId.INT32);
                break;
            case BIGINT:
                typeBuilder.setId(Types.PGenericType.TypeId.INT64);
                break;
            case FLOAT:
                typeBuilder.setId(Types.PGenericType.TypeId.FLOAT);
                break;
            case DOUBLE:
                typeBuilder.setId(Types.PGenericType.TypeId.DOUBLE);
                break;
            case CHAR:
            case VARCHAR:
                typeBuilder.setId(Types.PGenericType.TypeId.STRING);
                break;
            case HLL:
                typeBuilder.setId(Types.PGenericType.TypeId.HLL);
                break;
            case BITMAP:
                typeBuilder.setId(Types.PGenericType.TypeId.BITMAP);
                break;
            case QUANTILE_STATE:
                typeBuilder.setId(Types.PGenericType.TypeId.QUANTILE_STATE);
                break;
            case AGG_STATE:
                typeBuilder.setId(Types.PGenericType.TypeId.AGG_STATE);
                break;
            case DATE:
                typeBuilder.setId(Types.PGenericType.TypeId.DATE);
                break;
            case DATEV2:
                typeBuilder.setId(Types.PGenericType.TypeId.DATEV2);
                break;
            case DATETIME:
                typeBuilder.setId(Types.PGenericType.TypeId.DATETIME);
                break;
            case DATETIMEV2:
            case TIMEV2:
                typeBuilder.setId(Types.PGenericType.TypeId.DATETIMEV2);
                break;
            case DECIMALV2:
            case DECIMAL128:
                typeBuilder.setId(Types.PGenericType.TypeId.DECIMAL128)
                        .getDecimalTypeBuilder()
                        .setPrecision(((ScalarType) arg).getScalarPrecision())
                        .setScale(((ScalarType) arg).getScalarScale());
                break;
            case DECIMAL32:
                typeBuilder.setId(Types.PGenericType.TypeId.DECIMAL32)
                        .getDecimalTypeBuilder()
                        .setPrecision(((ScalarType) arg).getScalarPrecision())
                        .setScale(((ScalarType) arg).getScalarScale());
                break;
            case DECIMAL64:
                typeBuilder.setId(Types.PGenericType.TypeId.DECIMAL64)
                        .getDecimalTypeBuilder()
                        .setPrecision(((ScalarType) arg).getScalarPrecision())
                        .setScale(((ScalarType) arg).getScalarScale());
                break;
            case LARGEINT:
                typeBuilder.setId(Types.PGenericType.TypeId.INT128);
                break;
            default:
                throw new AnalysisException("type " + arg.getPrimitiveType().toString() + " is not supported");
        }
        return typeBuilder.build();
    }

    private TFunctionBinaryType getFunctionBinaryType(String type) {
        TFunctionBinaryType binaryType = null;
        try {
            binaryType = TFunctionBinaryType.valueOf(type);
        } catch (IllegalArgumentException e) {
            // ignore enum Exception
        }
        return binaryType;
    }

    private void analyzeAliasFunction(ConnectContext ctx) throws AnalysisException {
        if (parameters.size() != argsDef.getArgTypes().length) {
            throw new AnalysisException(
                    "Alias function [" + functionName + "] args number is not equal to parameters number");
        }
        List<Expression> exprs;
        List<String> typeDefParams = new ArrayList<>();
        if (originFunction instanceof org.apache.doris.nereids.trees.expressions.functions.Function) {
            exprs = originFunction.getArguments();
        } else if (originFunction instanceof Cast) {
            exprs = originFunction.children();
            DataType targetType = originFunction.getDataType();
            Type type = targetType.toCatalogDataType();
            if (type.isScalarType()) {
                ScalarType scalarType = (ScalarType) type;
                PrimitiveType primitiveType = scalarType.getPrimitiveType();
                switch (primitiveType) {
                    case DECIMAL32:
                    case DECIMAL64:
                    case DECIMAL128:
                    case DECIMAL256:
                    case DECIMALV2:
                        if (!Strings.isNullOrEmpty(scalarType.getScalarPrecisionStr())) {
                            typeDefParams.add(scalarType.getScalarPrecisionStr());
                        }
                        if (!Strings.isNullOrEmpty(scalarType.getScalarScaleStr())) {
                            typeDefParams.add(scalarType.getScalarScaleStr());
                        }
                        break;
                    case CHAR:
                    case VARCHAR:
                        if (!Strings.isNullOrEmpty(scalarType.getLenStr())) {
                            typeDefParams.add(scalarType.getLenStr());
                        }
                        break;
                    default:
                        throw new AnalysisException("Alias type is invalid: " + primitiveType);
                }
            }
        } else {
            throw new AnalysisException("Not supported expr type: " + originFunction);
        }
        Set<String> set = new HashSet<>();
        for (String str : parameters) {
            if (!set.add(str)) {
                throw new AnalysisException(
                        "Alias function [" + functionName + "] has duplicate parameter [" + str + "].");
            }
            boolean existFlag = false;
            // check exprs
            for (Expression expr : exprs) {
                existFlag |= checkParams(expr, str);
            }
            // check targetTypeDef
            for (String typeDefParam : typeDefParams) {
                existFlag |= typeDefParam.equals(str);
            }
            if (!existFlag) {
                throw new AnalysisException("Alias function [" + functionName + "]  do not contain parameter [" + str
                        + "]. typeDefParams="
                        + typeDefParams.stream().map(String::toString).collect(Collectors.joining(", ")));
            }
        }
        function = AliasFunction.createFunction(functionName, argsDef.getArgTypes(),
                Type.VARCHAR, argsDef.isVariadic(), parameters, translateToLegacyExpr(originFunction, ctx));
    }

    private boolean checkParams(Expression expr, String param) {
        for (Expression e : expr.children()) {
            if (checkParams(e, param)) {
                return true;
            }
        }
        if (expr instanceof Slot) {
            if (param.equals(((Slot) expr).getName())) {
                return true;
            }
        }
        return false;
    }

    /**
     * translate to legacy expr, which do not need complex expression and table columns
     */
    private Expr translateToLegacyExpr(Expression expression, ConnectContext ctx) throws AnalysisException {
        LogicalEmptyRelation plan = new LogicalEmptyRelation(
                ConnectContext.get().getStatementContext().getNextRelationId(), new ArrayList<>());
        CascadesContext cascadesContext = CascadesContext.initContext(ctx.getStatementContext(), plan,
                PhysicalProperties.ANY);
        Map<String, DataType> argTypeMap = new CaseInsensitiveMap();
        List<DataType> argTypes = argsDef.getArgTypeDefs();
        if (!parameters.isEmpty()) {
            if (parameters.size() != argTypes.size()) {
                throw new AnalysisException(String.format("arguments' size must be same as parameters' size,"
                        + "arguments : %s, parameters : %s", argTypes.size(), parameters.size()));
            }
            for (int i = 0; i < parameters.size(); ++i) {
                argTypeMap.put(parameters.get(i), argTypes.get(i));
            }
        }
        ExpressionAnalyzer analyzer = new CustomExpressionAnalyzer(cascadesContext, argTypeMap);
        expression = analyzer.analyze(expression);

        PlanTranslatorContext translatorContext = new PlanTranslatorContext(cascadesContext);
        ExpressionToExpr translator = new ExpressionToExpr();
        return expression.accept(translator, translatorContext);
    }

    private static class CustomExpressionAnalyzer extends ExpressionAnalyzer {
        private Map<String, DataType> argTypeMap;

        public CustomExpressionAnalyzer(CascadesContext cascadesContext, Map<String, DataType> argTypeMap) {
            super(null, new Scope(ImmutableList.of()), cascadesContext, false, false);
            this.argTypeMap = argTypeMap;
        }

        @Override
        public Expression visitUnboundSlot(UnboundSlot unboundSlot, ExpressionRewriteContext context) {
            DataType dataType = argTypeMap.get(unboundSlot.getName());
            if (dataType == null) {
                throw new org.apache.doris.nereids.exceptions.AnalysisException(
                        String.format("param %s's datatype is missed", unboundSlot.getName()));
            }
            return new SlotReference(unboundSlot.getName(), dataType);
        }
    }

    private static class ExpressionToExpr extends ExpressionTranslator {
        @Override
        public Expr visitSlotReference(SlotReference slotReference, PlanTranslatorContext context) {
            SlotRef slotRef = new SlotRef(slotReference.getDataType().toCatalogDataType(), slotReference.nullable());
            slotRef.setLabel(slotReference.getName());
            slotRef.setCol(slotReference.getName());
            slotRef.disableTableName();
            return slotRef;
        }

        @Override
        public Expr visitBoundFunction(BoundFunction function, PlanTranslatorContext context) {
            return makeFunctionCallExpr(function, function.getName(), function.hasVarArguments(), context);
        }

        @Override
        public Expr visitAdd(Add add, PlanTranslatorContext context) {
            return makeFunctionCallExpr(add, "add", false, context);
        }

        @Override
        public Expr visitSubtract(Subtract subtract, PlanTranslatorContext context) {
            return makeFunctionCallExpr(subtract, "subtract", false, context);
        }

        @Override
        public Expr visitMultiply(Multiply multiply, PlanTranslatorContext context) {
            return makeFunctionCallExpr(multiply, "multiply", false, context);
        }

        @Override
        public Expr visitDivide(Divide divide, PlanTranslatorContext context) {
            return makeFunctionCallExpr(divide, "divide", false, context);
        }

        @Override
        public Expr visitIntegralDivide(IntegralDivide integralDivide, PlanTranslatorContext context) {
            return makeFunctionCallExpr(integralDivide, "integralDivide", false, context);
        }

        @Override
        public Expr visitMod(Mod mod, PlanTranslatorContext context) {
            return makeFunctionCallExpr(mod, "mod", false, context);
        }

        @Override
        public Expr visitBitAnd(BitAnd bitAnd, PlanTranslatorContext context) {
            return makeFunctionCallExpr(bitAnd, "bitAnd", false, context);
        }

        @Override
        public Expr visitBitOr(BitOr bitOr, PlanTranslatorContext context) {
            return makeFunctionCallExpr(bitOr, "bitOr", false, context);
        }

        @Override
        public Expr visitBitXor(BitXor bitXor, PlanTranslatorContext context) {
            return makeFunctionCallExpr(bitXor, "bitXor", false, context);
        }

        @Override
        public Expr visitBitNot(BitNot bitNot, PlanTranslatorContext context) {
            return makeFunctionCallExpr(bitNot, "bitNot", false, context);
        }

        private Expr makeFunctionCallExpr(Expression expression, String name, boolean hasVarArguments,
                PlanTranslatorContext context) {
            List<Expr> arguments = expression.getArguments().stream()
                    .map(arg -> arg.accept(this, context))
                    .collect(Collectors.toList());

            List<Type> argTypes = expression.getArguments().stream()
                    .map(Expression::getDataType)
                    .map(DataType::toCatalogDataType)
                    .collect(Collectors.toList());

            NullableMode nullableMode = expression.nullable()
                    ? NullableMode.ALWAYS_NULLABLE
                    : NullableMode.ALWAYS_NOT_NULLABLE;

            org.apache.doris.catalog.ScalarFunction catalogFunction = new org.apache.doris.catalog.ScalarFunction(
                    new FunctionName(name), argTypes,
                    expression.getDataType().toCatalogDataType(), hasVarArguments,
                    "", TFunctionBinaryType.BUILTIN, true, true, nullableMode);

            FunctionCallExpr functionCallExpr;
            // create catalog FunctionCallExpr without analyze again
            functionCallExpr = new FunctionCallExpr(catalogFunction, new FunctionParams(false, arguments));
            functionCallExpr.setNullableFromNereids(expression.nullable());
            return functionCallExpr;
        }
    }
}
