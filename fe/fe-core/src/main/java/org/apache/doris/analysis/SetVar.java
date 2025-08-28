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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;

// change one variable.
public class SetVar {

    public enum SetVarType {
        DEFAULT,
        SET_SESSION_VAR,
        SET_PASS_VAR,
        SET_LDAP_PASS_VAR,
        SET_NAMES_VAR,
        SET_TRANSACTION,
        SET_USER_PROPERTY_VAR,
        SET_USER_DEFINED_VAR,
    }

    private String variable;
    private Expr value;
    private SetType type;
    public SetVarType varType;
    private LiteralExpr result;

    public SetVar() {
    }

    public SetVar(SetType type, String variable, Expr value) {
        this.type = type;
        this.varType = SetVarType.SET_SESSION_VAR;
        this.variable = variable;
        this.value = value;
        if (value instanceof LiteralExpr) {
            this.result = (LiteralExpr) value;
        }
    }

    public SetVar(String variable, Expr value) {
        this.type = SetType.DEFAULT;
        this.varType = SetVarType.SET_SESSION_VAR;
        this.variable = variable;
        this.value = value;
        if (value instanceof LiteralExpr) {
            this.result = (LiteralExpr) value;
        }
    }

    public SetVar(SetType setType, String variable, Expr value, SetVarType varType) {
        this.type = setType;
        this.varType = varType;
        this.variable = variable;
        this.value = value;
        if (value instanceof LiteralExpr) {
            this.result = (LiteralExpr) value;
        }
    }

    public String getVariable() {
        return variable;
    }

    public Expr getValue() {
        return value;
    }

    public void setValue(Expr value) {
        this.value = value;
    }

    public LiteralExpr getResult() {
        return result;
    }

    public void setResult(LiteralExpr result) {
        this.result = result;
    }

    public SetType getType() {
        return type;
    }

    public void setType(SetType type) {
        this.type = type;
    }

    public SetVarType getVarType() {
        return varType;
    }

    public void setVarType(SetVarType varType) {
        this.varType = varType;
    }

    // Value can be null. When value is null, means to set variable to DEFAULT.
    public void analyze() throws AnalysisException, UserException {

    }

    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append(type.toSql());
        sb.append(" ").append(variable).append(" = ").append(value.toSql());
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }
}
