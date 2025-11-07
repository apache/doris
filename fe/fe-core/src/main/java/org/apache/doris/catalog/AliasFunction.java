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

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionName;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.thrift.TFunctionBinaryType;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Internal representation of an alias function.
 */
public class AliasFunction extends Function {
    private static final Logger LOG = LogManager.getLogger(AliasFunction.class);

    private static final String DIGITAL_MASKING = "digital_masking";

    @SerializedName("of")
    private Expr originFunction;
    @SerializedName("pm")
    private List<String> parameters = new ArrayList<>();
    private List<String> typeDefParams = new ArrayList<>();

    // Only used for serialization
    protected AliasFunction() {
    }

    public AliasFunction(FunctionName fnName, List<Type> argTypes, Type retType, boolean hasVarArgs) {
        super(fnName, argTypes, retType, hasVarArgs);
    }

    public static AliasFunction createFunction(FunctionName functionName, Type[] argTypes, Type retType,
            boolean hasVarArgs, List<String> parameters, Expr originFunction) {
        AliasFunction aliasFunction = new AliasFunction(functionName, Arrays.asList(argTypes), retType, hasVarArgs);
        aliasFunction.setBinaryType(TFunctionBinaryType.JAVA_UDF);
        aliasFunction.setUserVisible(true);
        aliasFunction.originFunction = originFunction;
        aliasFunction.parameters = parameters;
        return aliasFunction;
    }

    public Expr getOriginFunction() {
        return originFunction;
    }

    public List<String> getParameters() {
        return parameters;
    }

    @Override
    public String toSql(boolean ifNotExists) {
        setSlotRefLabel(originFunction);
        StringBuilder sb = new StringBuilder("CREATE ");

        if (this.isGlobal) {
            sb.append("GLOBAL ");
        }
        sb.append("ALIAS FUNCTION ");

        if (ifNotExists) {
            sb.append("IF NOT EXISTS ");
        }
        sb.append(signatureString())
                .append(" WITH PARAMETER(")
                .append(getParamsSting(parameters))
                .append(") AS ")
                .append(originFunction.toSqlWithoutTbl())
                .append(";");
        return sb.toString();
    }

    @Override
    public String getProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put("parameter", getParamsSting(parameters));
        setSlotRefLabel(originFunction);
        String functionStr = originFunction.toSqlWithoutTbl();
        functionStr = functionStr.replaceAll("'", "`");
        properties.put("origin_function", functionStr);
        return new Gson().toJson(properties);
    }

    /**
     * set slotRef label to column name
     *
     * @param expr
     */
    private void setSlotRefLabel(Expr expr) {
        for (Expr e : expr.getChildren()) {
            setSlotRefLabel(e);
        }
        if (expr instanceof SlotRef) {
            ((SlotRef) expr).setLabel("`" + ((SlotRef) expr).getColumnName() + "`");
        }
    }

    private String getParamsSting(List<String> parameters) {
        return parameters.stream()
                .map(String::toString)
                .collect(Collectors.joining(", "));
    }
}
