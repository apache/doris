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

package org.apache.doris.analysis;

import org.apache.doris.catalog.AggregateFunction;
import org.apache.doris.catalog.AliasFunction;
import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.Function.NullableMode;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.ScalarFunction;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.URI;
import org.apache.doris.common.util.Util;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.proto.FunctionService;
import org.apache.doris.proto.PFunctionServiceGrpc;
import org.apache.doris.proto.Types;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TFunctionBinaryType;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

// create a user define function
public class CreateFunctionStmt extends DdlStmt {
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
    private static final Logger LOG = LogManager.getLogger(CreateFunctionStmt.class);

    private SetType type = SetType.DEFAULT;
    private final boolean ifNotExists;
    private final FunctionName functionName;
    private final boolean isAggregate;
    private final boolean isAlias;
    private final FunctionArgsDef argsDef;
    private final TypeDef returnType;
    private TypeDef intermediateType;
    private final Map<String, String> properties;
    private final List<String> parameters;
    private final Expr originFunction;
    TFunctionBinaryType binaryType = TFunctionBinaryType.JAVA_UDF;

    // needed item set after analyzed
    private String userFile;
    private Function function;
    private String checksum = "";
    // now set udf default NullableMode is ALWAYS_NULLABLE
    // if not, will core dump when input is not null column, but need return null
    // like https://github.com/apache/doris/pull/14002/files
    private NullableMode returnNullMode = NullableMode.ALWAYS_NULLABLE;

    // timeout for both connection and read. 10 seconds is long enough.
    private static final int HTTP_TIMEOUT_MS = 10000;

    public CreateFunctionStmt(SetType type, boolean ifNotExists, boolean isAggregate, FunctionName functionName,
                              FunctionArgsDef argsDef,
                              TypeDef returnType, TypeDef intermediateType, Map<String, String> properties) {
        this.type = type;
        this.ifNotExists = ifNotExists;
        this.functionName = functionName;
        this.isAggregate = isAggregate;
        this.argsDef = argsDef;
        this.returnType = returnType;
        this.intermediateType = intermediateType;
        if (properties == null) {
            this.properties = ImmutableSortedMap.of();
        } else {
            this.properties = ImmutableSortedMap.copyOf(properties, String.CASE_INSENSITIVE_ORDER);
        }
        this.isAlias = false;
        this.parameters = ImmutableList.of();
        this.originFunction = null;
    }

    public CreateFunctionStmt(SetType type, boolean ifNotExists, FunctionName functionName, FunctionArgsDef argsDef,
            List<String> parameters, Expr originFunction) {
        this.type = type;
        this.ifNotExists = ifNotExists;
        this.functionName = functionName;
        this.isAlias = true;
        this.argsDef = argsDef;
        if (parameters == null) {
            this.parameters = ImmutableList.of();
        } else {
            this.parameters = ImmutableList.copyOf(parameters);
        }
        this.originFunction = originFunction;
        this.isAggregate = false;
        this.returnType = new TypeDef(Type.VARCHAR);
        this.properties = ImmutableSortedMap.of();
    }

    public SetType getType() {
        return type;
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    public FunctionName getFunctionName() {
        return functionName;
    }

    public Function getFunction() {
        return function;
    }

    public Expr getOriginFunction() {
        return originFunction;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);

        // https://github.com/apache/doris/issues/17810
        // this error report in P0 test, so we suspect that it is related to concurrency
        // add this change to test it.
        if (Config.use_fuzzy_session_variable) {
            synchronized (CreateFunctionStmt.class) {
                analyzeCommon(analyzer);
                // check
                if (isAggregate) {
                    analyzeUda();
                } else if (isAlias) {
                    analyzeAliasFunction();
                } else {
                    analyzeUdf();
                }
            }
        } else {
            analyzeCommon(analyzer);
            // check
            if (isAggregate) {
                analyzeUda();
            } else if (isAlias) {
                analyzeAliasFunction();
            } else {
                analyzeUdf();
            }
        }
    }

    private void analyzeCommon(Analyzer analyzer) throws AnalysisException {
        // check function name
        functionName.analyze(analyzer, this.type);

        // check operation privilege
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }
        // check argument
        argsDef.analyze(analyzer);

        // alias function does not need analyze following params
        if (isAlias) {
            return;
        }

        returnType.analyze(analyzer);
        if (intermediateType != null) {
            intermediateType.analyze(analyzer);
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
        if (Strings.isNullOrEmpty(userFile)) {
            throw new AnalysisException("No 'file' or 'object_file' in properties");
        }
        if (binaryType != TFunctionBinaryType.RPC) {
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
            String returnNullModeStr = properties.get(IS_RETURN_NULL);
            if (returnNullModeStr == null) {
                return;
            }
            if (!returnNullModeStr.equalsIgnoreCase("false") && !returnNullModeStr.equalsIgnoreCase("true")) {
                throw new AnalysisException("'always_nullable' in properties, you should set it false or true");
            }

            if (!Boolean.parseBoolean(returnNullModeStr)) {
                returnNullMode = NullableMode.ALWAYS_NOT_NULLABLE;
            }
        }
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

    private void analyzeUda() throws AnalysisException {
        AggregateFunction.AggregateFunctionBuilder builder
                = AggregateFunction.AggregateFunctionBuilder.createUdfBuilder();

        builder.name(functionName).argsType(argsDef.getArgTypes()).retType(returnType.getType())
                .hasVarArgs(argsDef.isVariadic()).intermediateType(intermediateType.getType())
                .location(URI.create(userFile));
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

        URI location = URI.create(userFile);
        function.setLocation(location);
        function.setBinaryType(binaryType);
        function.setChecksum(checksum);
        function.setNullableMode(returnNullMode);
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
        URI location = URI.create(userFile);
        function = ScalarFunction.createUdf(binaryType,
                functionName, argsDef.getArgTypes(),
                returnType.getType(), argsDef.isVariadic(),
                location, symbol, prepareFnSymbol, closeFnSymbol);
        function.setChecksum(checksum);
        function.setNullableMode(returnNullMode);
    }

    private void analyzeJavaUdaf(String clazz) throws AnalysisException {
        HashMap<String, Method> allMethods = new HashMap<>();

        try {
            URL[] urls = {new URL("jar:" + userFile + "!/")};
            try (URLClassLoader cl = URLClassLoader.newInstance(urls)) {
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
                    checkReturnUdfType(udfClass, allMethods.get(GETVALUE_METHOD_NAME), returnType.getType());
                }

                if (!Modifier.isPublic(stateClass.getModifiers()) || !Modifier.isStatic(stateClass.getModifiers())) {
                    throw new AnalysisException(
                            String.format(
                                    "UDAF '%s' should have one public & static 'State' class to Construction data ",
                                    udfClassName));
                }
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
            URL[] urls = {new URL("jar:" + userFile + "!/")};
            try (URLClassLoader cl = URLClassLoader.newInstance(urls)) {
                Class udfClass = cl.loadClass(clazz);

                Method eval = null;
                for (Method m : udfClass.getMethods()) {
                    if (!m.getDeclaringClass().equals(udfClass)) {
                        continue;
                    }
                    String name = m.getName();
                    if (EVAL_METHOD_KEY.equals(name) && eval == null) {
                        eval = m;
                    } else if (EVAL_METHOD_KEY.equals(name)) {
                        throw new AnalysisException(String.format(
                                "UDF class '%s' has multiple methods with name '%s' ", udfClass.getCanonicalName(),
                                EVAL_METHOD_KEY));
                    }
                }
                if (eval == null) {
                    throw new AnalysisException(String.format(
                            "No method '%s' in class '%s'!", EVAL_METHOD_KEY, udfClass.getCanonicalName()));
                }
                if (Modifier.isStatic(eval.getModifiers())) {
                    throw new AnalysisException(
                            String.format("Method '%s' in class '%s' should be non-static", eval.getName(),
                                    udfClass.getCanonicalName()));
                }
                if (!Modifier.isPublic(eval.getModifiers())) {
                    throw new AnalysisException(
                            String.format("Method '%s' in class '%s' should be public", eval.getName(),
                                    udfClass.getCanonicalName()));
                }
                if (eval.getParameters().length != argsDef.getArgTypes().length) {
                    throw new AnalysisException(
                            String.format("The number of parameters for method '%s' in class '%s' should be %d",
                                    eval.getName(), udfClass.getCanonicalName(), argsDef.getArgTypes().length));
                }

                checkUdfType(udfClass, eval, returnType.getType(), eval.getReturnType(), "return");
                for (int i = 0; i < eval.getParameters().length; i++) {
                    Parameter p = eval.getParameters()[i];
                    checkUdfType(udfClass, eval, argsDef.getArgTypes()[i], p.getType(), p.getName());
                }
            } catch (ClassNotFoundException e) {
                throw new AnalysisException("Class [" + clazz + "] not found in file :" + userFile);
            } catch (IOException e) {
                throw new AnalysisException("Failed to load file: " + userFile);
            }
        } catch (MalformedURLException e) {
            throw new AnalysisException("Failed to load file: " + userFile);
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
        } else {
            throw new AnalysisException(
                    String.format("Method '%s' in class '%s' does not support type '%s'",
                            method.getName(), clazz.getCanonicalName(), expType));
        }

        if (javaTypes == null) {
            throw new AnalysisException(
                    String.format("Method '%s' in class '%s' does not support type '%s'",
                            method.getName(), clazz.getCanonicalName(), expType.toString()));
        }
        if (!javaTypes.contains(pType)) {
            throw new AnalysisException(
                    String.format("UDF class '%s' method '%s' %s[%s] type is not supported!",
                            clazz.getCanonicalName(), method.getName(), pname, pType.getCanonicalName()));
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
        builder.getFunctionBuilder().setOutput(convertToPParameterType(returnType.getType()));
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
            case TIME:
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

    private void analyzeAliasFunction() throws AnalysisException {
        function = AliasFunction.createFunction(functionName, argsDef.getArgTypes(),
                Type.VARCHAR, argsDef.isVariadic(), parameters, originFunction);
        ((AliasFunction) function).analyze();
    }

    @Override
    public String toSql() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("CREATE ");
        if (isAggregate) {
            stringBuilder.append("AGGREGATE ");
        } else if (isAlias) {
            stringBuilder.append("ALIAS ");
        }

        stringBuilder.append("FUNCTION ");
        stringBuilder.append(functionName.toString());
        stringBuilder.append(argsDef.toSql());
        if (isAlias) {
            stringBuilder.append(" WITH PARAMETER (")
                    .append(parameters.toString())
                    .append(") AS ")
                    .append(originFunction.toSql());
        } else {
            stringBuilder.append(" RETURNS ");
            stringBuilder.append(returnType.toString());
        }
        if (properties.size() > 0) {
            stringBuilder.append(" PROPERTIES (");
            int i = 0;
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                if (i != 0) {
                    stringBuilder.append(", ");
                }
                stringBuilder.append('"').append(entry.getKey()).append('"');
                stringBuilder.append("=");
                stringBuilder.append('"').append(entry.getValue()).append('"');
                i++;
            }
            stringBuilder.append(")");

        }
        return stringBuilder.toString();
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_WITH_SYNC;
    }
}
