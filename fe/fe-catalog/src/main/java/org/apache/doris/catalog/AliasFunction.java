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
import org.apache.doris.persist.gson.GsonPostProcessable;

import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Internal representation of an alias function.
 */
public class AliasFunction extends Function implements GsonPostProcessable {

    @SerializedName("of")
    private Expr originFunction;
    @SerializedName("pm")
    private List<String> parameters = new ArrayList<>();
    @SerializedName("sv")
    private Map<String, String> sessionVariables;

    // Only used for serialization
    protected AliasFunction() {
    }

    public AliasFunction(FunctionName fnName, List<Type> argTypes, Type retType, boolean hasVarArgs) {
        super(fnName, argTypes, retType, hasVarArgs);
    }

    public static AliasFunction createFunction(FunctionName functionName, Type[] argTypes, Type retType,
            boolean hasVarArgs, List<String> parameters, Expr originFunction) {
        return createFunction(functionName, argTypes, retType, hasVarArgs, parameters, originFunction, null);
    }

    public static AliasFunction createFunction(FunctionName functionName, Type[] argTypes, Type retType,
            boolean hasVarArgs, List<String> parameters, Expr originFunction, Map<String, String> sessionVariables) {
        AliasFunction aliasFunction = new AliasFunction(functionName, Arrays.asList(argTypes), retType, hasVarArgs);
        aliasFunction.setBinaryType(BinaryType.JAVA_UDF);
        aliasFunction.setUserVisible(true);
        aliasFunction.originFunction = originFunction;
        aliasFunction.parameters = parameters;
        aliasFunction.sessionVariables = sessionVariables;
        return aliasFunction;
    }

    public Expr getOriginFunction() {
        return originFunction;
    }

    public List<String> getParameters() {
        return parameters;
    }

    public Map<String, String> getSessionVariables() {
        return sessionVariables;
    }

    public void setSessionVariables(Map<String, String> sessionVariables) {
        this.sessionVariables = sessionVariables;
    }

    @Override
    public boolean equals(Object o) {
        boolean equalBasic = super.equals(o);
        if (!equalBasic) {
            return false;
        }
        AliasFunction other = (AliasFunction) o;
        return Objects.equals(sessionVariables, other.sessionVariables);
    }

    @Override
    public void gsonPostProcess() throws IOException {
        if (sessionVariables == null) {
            sessionVariables = Maps.newHashMap();
        }
    }
}
