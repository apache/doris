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
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.ScalarFunction;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.Util;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSortedMap;

import org.apache.commons.codec.binary.Hex;

import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

// create a user define function
public class CreateFunctionStmt extends DdlStmt {
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

    private final FunctionName functionName;
    private final boolean isAggregate;
    private final FunctionArgsDef argsDef;
    private final TypeDef returnType;
    private TypeDef intermediateType;
    private final Map<String, String> properties;

    // needed item set after analyzed
    private String objectFile;
    private Function function;
    private String checksum;

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
    }

    public FunctionName getFunctionName() { return functionName; }
    public Function getFunction() { return function; }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);

        analyzeCommon(analyzer);
        // check
        if (isAggregate) {
            analyzeUda();
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

        returnType.analyze(analyzer);
        if (intermediateType != null) {
            intermediateType.analyze(analyzer);
        } else {
            intermediateType = returnType;
        }

        objectFile = properties.get(OBJECT_FILE_KEY);
        if (Strings.isNullOrEmpty(objectFile)) {
            throw new AnalysisException("No 'object_file' in properties");
        }
        try {
            computeObjectChecksum();
        } catch (IOException | NoSuchAlgorithmException e) {
            throw new AnalysisException("cannot to compute object's checksum");
        }

        String md5sum = properties.get(MD5_CHECKSUM);
        if (md5sum != null && !md5sum.equalsIgnoreCase(checksum)) {
            throw new AnalysisException("library's checksum is not equal with input, checksum=" + checksum);
        }
    }

    private void computeObjectChecksum() throws IOException, NoSuchAlgorithmException {
        if (FeConstants.runningUnitTest) {
            // skip checking checksum when running ut
            checksum = "";
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
        AggregateFunction.AggregateFunctionBuilder builder = AggregateFunction.AggregateFunctionBuilder.createUdfBuilder();

        builder.name(functionName).argsType(argsDef.getArgTypes()).retType(returnType.getType()).
                hasVarArgs(argsDef.isVariadic()).intermediateType(intermediateType.getType()).objectFile(objectFile);
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
        function = ScalarFunction.createUdf(
                functionName, argsDef.getArgTypes(),
                returnType.getType(), argsDef.isVariadic(),
                objectFile, symbol, prepareFnSymbol, closeFnSymbol);
        function.setChecksum(checksum);
    }

    @Override
    public String toSql() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("CREATE ");
        if (isAggregate) {
            stringBuilder.append("AGGREGATE ");
        }
        stringBuilder.append("FUNCTION ");
        stringBuilder.append(functionName.toString());
        stringBuilder.append(argsDef.toSql());
        stringBuilder.append(" RETURNS ");
        stringBuilder.append(returnType.toString());
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
