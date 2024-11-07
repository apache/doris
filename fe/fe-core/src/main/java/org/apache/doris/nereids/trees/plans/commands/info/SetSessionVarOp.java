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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.SetType;
import org.apache.doris.analysis.SetVar;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.ParseUtil;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.glue.translator.ExpressionTranslator;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.GlobalVariable;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.VariableMgr;
import org.apache.doris.system.HeartbeatFlags;

import java.util.ArrayList;

/**
 * SetSessionVarOp
 */
public class SetSessionVarOp extends SetVarOp {
    private String name;
    private final Expression expression;
    private Literal value;
    private final boolean isDefault;

    /** constructor*/
    public SetSessionVarOp(SetType type, String name, Expression expression) {
        super(type);
        this.name = name;
        this.expression = expression;
        this.isDefault = expression == null;
    }

    @Override
    public void validate(ConnectContext ctx) throws UserException {
        if (isDefault) {
            value = new StringLiteral("default");
            return;
        }
        value = ExpressionUtils.analyzeAndFoldToLiteral(ctx, expression);

        if (getType() == SetType.GLOBAL) {
            if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
            }
        }

        if (name.equalsIgnoreCase(GlobalVariable.DEFAULT_ROWSET_TYPE)) {
            if (value != null && !HeartbeatFlags.isValidRowsetType(value.getStringValue())) {
                throw new AnalysisException("Invalid rowset type, now we support {alpha, beta}.");
            }
        }

        if (name.equalsIgnoreCase(SessionVariable.PREFER_JOIN_METHOD)) {
            String val = value.getStringValue();
            if (!val.equalsIgnoreCase("broadcast") && !val.equalsIgnoreCase("shuffle")) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_WRONG_VALUE_FOR_VAR,
                        SessionVariable.PREFER_JOIN_METHOD, val);
            }
        }

        // Check variable time_zone value is valid
        if (name.equalsIgnoreCase(SessionVariable.TIME_ZONE)) {
            this.value = new StringLiteral(TimeUtils.checkTimeZoneValidAndStandardize(value.getStringValue()));
        }

        if (name.equalsIgnoreCase(SessionVariable.EXEC_MEM_LIMIT)
                || name.equalsIgnoreCase(SessionVariable.SCAN_QUEUE_MEM_LIMIT)) {
            this.value = new StringLiteral(Long.toString(ParseUtil.analyzeDataVolume(value.getStringValue())));
        }

        if (name.equalsIgnoreCase(SessionVariable.FILE_SPLIT_SIZE)) {
            try {
                this.value = new StringLiteral(
                        Long.toString(ParseUtil.analyzeDataVolume(value.getStringValue())));
            } catch (Throwable t) {
                // The way of handling file_split_size should be same as exec_mem_limit or scan_queue_mem_limit.
                // But ParseUtil.analyzeDataVolume() does not accept 0 as a valid value.
                // So for compatibility, we set origin value to file_split_size
                // when the value is 0 or other invalid value.
                this.value = new StringLiteral(value.getStringValue());
            }
        }

        if (name.equalsIgnoreCase("is_report_success")) {
            name = SessionVariable.ENABLE_PROFILE;
        }
    }

    public void run(ConnectContext ctx) throws Exception {
        VariableMgr.setVar(ctx.getSessionVariable(), translateToLegacyVar(ctx));
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append(getType().toSql());
        sb.append(" ").append(name).append(" = ").append(value.toSql());
        return sb.toString();
    }

    public void afterForwardToMaster(ConnectContext ctx) throws Exception {
        setType(SetType.SESSION);
        VariableMgr.setVarForNonMasterFE(ctx.getSessionVariable(), translateToLegacyVar(ctx));
    }

    // TODO delete this method after removing dependence of SetVar in VariableMgr
    private SetVar translateToLegacyVar(ConnectContext ctx) {
        if (isDefault) {
            return new SetVar(getType(), name, null);
        } else {
            LogicalEmptyRelation plan = new LogicalEmptyRelation(
                    ConnectContext.get().getStatementContext().getNextRelationId(), new ArrayList<>());
            CascadesContext cascadesContext = CascadesContext.initContext(ctx.getStatementContext(), plan,
                    PhysicalProperties.ANY);
            Expr expr = ExpressionTranslator.translate(value, new PlanTranslatorContext(cascadesContext));
            return new SetVar(getType(), name, expr);
        }
    }
}
