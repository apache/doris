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

import com.google.common.collect.ImmutableSortedMap;
import org.apache.commons.codec.binary.Hex;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.ScalarFunction;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

// create a user define function
public class CreateFunctionStmt extends DdlStmt {

    private final FunctionName functionName;
    private final boolean isAggregate;
    private final FunctionArgsDef argsDef;
    private final TypeDef returnType;
    private final Map<String, String> properties;

    // needed item set after analyzed
    private String objectFile;
    private Function function;
    private String checksum;

    public CreateFunctionStmt(boolean isAggregate, FunctionName functionName, FunctionArgsDef argsDef,
                              TypeDef returnType, Map<String, String> properties) {
        this.functionName = functionName;
        this.isAggregate = isAggregate;
        this.argsDef = argsDef;
        this.returnType = returnType;
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
        if (!Catalog.getCurrentCatalog().getAuth().checkDbPriv(
                ConnectContext.get(), functionName.getDb(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }
        // check argument
        argsDef.analyze(analyzer);

        returnType.analyze(analyzer);

        String OBJECT_FILE_KEY = "object_file";
        objectFile = properties.get(OBJECT_FILE_KEY);
        if (objectFile == null) {
            throw new AnalysisException("No 'object_file' in properties");
        }
        try {
            computeObjectChecksum();
        } catch (IOException | NoSuchAlgorithmException e) {
            throw new AnalysisException("cannot to compute object's checksum");
        }
    }

    private void computeObjectChecksum() throws IOException, NoSuchAlgorithmException {
        URL url = new URL(objectFile);
        URLConnection urlConnection = url.openConnection();
        InputStream inputStream = urlConnection.getInputStream();

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

    private void analyzeUda() throws AnalysisException {
        throw new AnalysisException("Not support aggregate function now.");
    }

    private void analyzeUdf() throws AnalysisException {
        final String SYMBOL_KEY = "symbol";
        String symbol = properties.get(SYMBOL_KEY);
        if (symbol == null) {
            throw new AnalysisException("No 'symbol' in properties");
        }
        function = ScalarFunction.createUdf(
                functionName, argsDef.getArgTypes(),
                returnType.getType(), argsDef.isVariadic(),
                objectFile, symbol);
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
