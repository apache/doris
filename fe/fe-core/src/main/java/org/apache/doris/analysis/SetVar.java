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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.ParseUtil;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.mysql.privilege.UserResource;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.GlobalVariable;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.system.HeartbeatFlags;

import com.google.common.base.Strings;

// change one variable.
public class SetVar {

    private String variable;
    private Expr value;
    private SetType type;
    private LiteralExpr result;

    public SetVar() {
    }

    public SetVar(SetType type, String variable, Expr value) {
        this.type = type;
        this.variable = variable;
        this.value = value;
        if (value instanceof LiteralExpr) {
            this.result = (LiteralExpr)value;
        }
    }

    public SetVar(String variable, Expr value) {
        this.type = SetType.DEFAULT;
        this.variable = variable;
        this.value = value;
        if (value instanceof LiteralExpr) {
            this.result = (LiteralExpr)value;
        }
    }

    public String getVariable() {
        return variable;
    }

    public LiteralExpr getValue() {
        return result;
    }

    public SetType getType() {
        return type;
    }

    public void setType(SetType type) {
        this.type = type;
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
            if (!Catalog.getCurrentCatalog().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
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

        final Expr literalExpr = value.getResultValue();
        if (!(literalExpr instanceof LiteralExpr)) {
            throw new AnalysisException("Set statement does't support computing expr:" + literalExpr.toSql());
        }

        result = (LiteralExpr)literalExpr;

        // Need to check if group is valid
        if (variable.equalsIgnoreCase(SessionVariable.RESOURCE_VARIABLE)) {
            if (result != null && !UserResource.isValidGroup(result.getStringValue())) {
                throw new AnalysisException("Invalid resource group, now we support {low, normal, high}.");
            }
        }

        if (variable.equalsIgnoreCase(GlobalVariable.DEFAULT_ROWSET_TYPE)) {
            if (result != null && !HeartbeatFlags.isValidRowsetType(result.getStringValue())) {
                throw new AnalysisException("Invalid rowset type, now we support {alpha, beta}.");
            }
        }

        if (getVariable().equalsIgnoreCase(SessionVariable.PREFER_JOIN_METHOD)) {
            String value = getValue().getStringValue();
            if (!value.equalsIgnoreCase("broadcast") && !value.equalsIgnoreCase("shuffle")) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_WRONG_VALUE_FOR_VAR, SessionVariable.PREFER_JOIN_METHOD, value);
            }
        }

        // Check variable time_zone value is valid
        if (getVariable().equalsIgnoreCase(SessionVariable.TIME_ZONE)) {
            this.value = new StringLiteral(TimeUtils.checkTimeZoneValidAndStandardize(getValue().getStringValue()));
            this.result = (LiteralExpr) this.value;
        }

        if (getVariable().equalsIgnoreCase(SessionVariable.EXEC_MEM_LIMIT)) {
            this.value = new StringLiteral(Long.toString(ParseUtil.analyzeDataVolumn(getValue().getStringValue())));
            this.result = (LiteralExpr) this.value;
        }
        if (getVariable().equalsIgnoreCase("is_report_success")) {
            variable = SessionVariable.ENABLE_PROFILE;
        }

        if (getVariable().equalsIgnoreCase(SessionVariable.PARTITION_PRUNE_ALGORITHM_VERSION)) {
            String value = getValue().getStringValue();
            if (!"1".equals(value) && !"2".equals(value)) {
                throw new AnalysisException("Value of " +
                    SessionVariable.PARTITION_PRUNE_ALGORITHM_VERSION + " should be " +
                    "either 1 or 2, but meet " + value);
            }
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
