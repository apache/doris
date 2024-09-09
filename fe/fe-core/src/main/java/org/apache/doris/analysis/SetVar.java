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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.ParseUtil;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.GlobalVariable;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.system.HeartbeatFlags;

import com.google.common.base.Strings;

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
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        if (type == null) {
            type = SetType.DEFAULT;
        }

        if (Strings.isNullOrEmpty(variable)) {
            throw new AnalysisException("No variable name in set statement.");
        }

        if (type == SetType.GLOBAL) {
            if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                        "ADMIN");
            }
        }

        if (value == null) {
            return;
        }

        // For the case like "set character_set_client = utf8", we change SlotRef to StringLiteral.
        if (value instanceof SlotRef) {
            value = new StringLiteral(((SlotRef) value).getColumnName());
        }

        value.analyze(analyzer);
        if (!value.isConstant()) {
            throw new AnalysisException("Set statement does't support non-constant expr.");
        }

        final Expr literalExpr = value.getResultValue(false);
        if (!(literalExpr instanceof LiteralExpr)) {
            throw new AnalysisException("Set statement does't support computing expr:" + literalExpr.toSql());
        }

        result = (LiteralExpr) literalExpr;

        if (variable.equalsIgnoreCase(GlobalVariable.DEFAULT_ROWSET_TYPE)) {
            if (result != null && !HeartbeatFlags.isValidRowsetType(result.getStringValue())) {
                throw new AnalysisException("Invalid rowset type, now we support {alpha, beta}.");
            }
        }

        if (getVariable().equalsIgnoreCase(SessionVariable.PREFER_JOIN_METHOD)) {
            String value = getResult().getStringValue();
            if (!value.equalsIgnoreCase("broadcast") && !value.equalsIgnoreCase("shuffle")) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_WRONG_VALUE_FOR_VAR,
                        SessionVariable.PREFER_JOIN_METHOD, value);
            }
        }

        // Check variable time_zone value is valid
        if (getVariable().equalsIgnoreCase(SessionVariable.TIME_ZONE)) {
            this.value = new StringLiteral(TimeUtils.checkTimeZoneValidAndStandardize(getResult().getStringValue()));
            this.result = (LiteralExpr) this.value;
        }

        if (getVariable().equalsIgnoreCase(SessionVariable.EXEC_MEM_LIMIT)
                || getVariable().equalsIgnoreCase(SessionVariable.SCAN_QUEUE_MEM_LIMIT)) {
            this.value = new StringLiteral(Long.toString(ParseUtil.analyzeDataVolume(getResult().getStringValue())));
            this.result = (LiteralExpr) this.value;
        }
        if (getVariable().equalsIgnoreCase(SessionVariable.FILE_SPLIT_SIZE)) {
            try {
                this.value = new StringLiteral(
                        Long.toString(ParseUtil.analyzeDataVolume(getResult().getStringValue())));
            } catch (Throwable t) {
                // The way of handling file_split_size should be same as exec_mem_limit or scan_queue_mem_limit.
                // But ParseUtil.analyzeDataVolume() does not accept 0 as a valid value.
                // So for compatibility, we set origin value to file_split_size
                // when the value is 0 or other invalid value.
                this.value = new StringLiteral(getResult().getStringValue());
            }
            this.result = (LiteralExpr) this.value;
        }
        if (getVariable().equalsIgnoreCase("is_report_success")) {
            variable = SessionVariable.ENABLE_PROFILE;
        }
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
