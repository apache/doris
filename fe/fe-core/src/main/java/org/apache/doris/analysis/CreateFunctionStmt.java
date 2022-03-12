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
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Function;
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

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;

import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;

// create a user define function
public class CreateFunctionStmt extends DdlStmt {
    private final static Logger LOG = LogManager.getLogger(CreateFunctionStmt.class);
    public static final String OBJECT_FILE_KEY = "object_file";
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

    private final FunctionName functionName;
    private final boolean isAggregate;
    private final boolean isAlias;
    private final FunctionArgsDef argsDef;
    private final TypeDef returnType;
    private TypeDef intermediateType;
    private final Map<String, String> properties;
    private final List<String> parameters;
    private final Expr originFunction;
    TFunctionBinaryType binaryType = TFunctionBinaryType.NATIVE;

    // needed item set after analyzed
    private String objectFile;
    private Function function;
    private String checksum = "";

    // timeout for both connection and read. 10 seconds is long enough.
    private static final int HTTP_TIMEOUT_MS = 10000;

    public CreateFunctionStmt(boolean isAggregate, FunctionName functionName, FunctionArgsDef argsDef,
                              TypeDef returnType, TypeDef intermediateType, Map<String, String> properties) {
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

    public CreateFunctionStmt(FunctionName functionName, FunctionArgsDef argsDef,
                              List<String> parameters, Expr originFunction) {
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

    private void analyzeCommon(Analyzer analyzer) throws AnalysisException {
        // check function name
        functionName.analyze(analyzer);

        // check operation privilege
        if (!Catalog.getCurrentCatalog().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
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

        String type = properties.getOrDefault(BINARY_TYPE, "NATIVE");
        binaryType = getFunctionBinaryType(type);
        if (binaryType == null) {
            throw new AnalysisException("unknown function type");
        }

        objectFile = properties.get(OBJECT_FILE_KEY);
        if (Strings.isNullOrEmpty(objectFile)) {
            throw new AnalysisException("No 'object_file' in properties");
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
    }

    private void computeObjectChecksum() throws IOException, NoSuchAlgorithmException {
        if (FeConstants.runningUnitTest) {
            // skip checking checksum when running ut
            return;
        }

        try (InputStream inputStream = Util.getInputStreamFromUrl(objectFile, null, HTTP_TIMEOUT_MS, HTTP_TIMEOUT_MS)) {
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
        if (binaryType == TFunctionBinaryType.RPC) {
            throw new AnalysisException("RPC UDAF is not supported.");
        }
        AggregateFunction.AggregateFunctionBuilder builder = AggregateFunction.AggregateFunctionBuilder.createUdfBuilder();

        builder.name(functionName).argsType(argsDef.getArgTypes()).retType(returnType.getType()).
                hasVarArgs(argsDef.isVariadic()).intermediateType(intermediateType.getType()).location(URI.create(objectFile));
        String initFnSymbol = properties.get(INIT_KEY);
        if (initFnSymbol == null) {
            throw new AnalysisException("No 'init_fn' in properties");
        }
        String updateFnSymbol = properties.get(UPDATE_KEY);
        if (updateFnSymbol == null) {
            throw new AnalysisException("No 'update_fn' in properties");
        }
        String mergeFnSymbol = properties.get(MERGE_KEY);
        if (mergeFnSymbol == null) {
            throw new AnalysisException("No 'merge_fn' in properties");
        }
        function = builder.initFnSymbol(initFnSymbol)
                .updateFnSymbol(updateFnSymbol).mergeFnSymbol(mergeFnSymbol)
                .serializeFnSymbol(properties.get(SERIALIZE_KEY)).finalizeFnSymbol(properties.get(FINALIZE_KEY))
                .getValueFnSymbol(properties.get(GET_VALUE_KEY)).removeFnSymbol(properties.get(REMOVE_KEY))
                .build();
        function.setChecksum(checksum);
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
        if (binaryType == TFunctionBinaryType.RPC && !objectFile.contains("://")) {
            if (StringUtils.isNotBlank(prepareFnSymbol) || StringUtils.isNotBlank(closeFnSymbol)) {
                throw new AnalysisException(" prepare and close in RPC UDF are not supported.");
            }
            String[] url = objectFile.split(":");
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
        URI location = URI.create(objectFile);
        function = ScalarFunction.createUdf(binaryType,
                functionName, argsDef.getArgTypes(),
                returnType.getType(), argsDef.isVariadic(),
                location, symbol, prepareFnSymbol, closeFnSymbol);
        function.setChecksum(checksum);
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
            case DATE:
                typeBuilder.setId(Types.PGenericType.TypeId.DATE);
                break;
            case DATETIME:
            case TIME:
                typeBuilder.setId(Types.PGenericType.TypeId.DATETIME);
                break;
            case DECIMALV2:
                typeBuilder.setId(Types.PGenericType.TypeId.DECIMAL128)
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
