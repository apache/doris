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

package org.apache.doris.nereids.util;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;

import org.apache.commons.lang3.StringUtils;

import java.io.IOException;

/**
 * MemoUtils.
 */
public class MemoTestUtils {
    public static ConnectContext createConnectContext() {
        return createCtx(UserIdentity.ROOT, "127.0.0.1");
    }

    public static LogicalPlan parse(String sql) {
        return new NereidsParser().parseSingle(sql);
    }

    public static StatementContext createStatementContext(String sql) {
        return createStatementContext(createConnectContext(), sql);
    }

    public static StatementContext createStatementContext(ConnectContext connectContext, String sql) {
        StatementContext statementContext = new StatementContext(connectContext, new OriginStatement(sql, 0));
        connectContext.setStatementContext(statementContext);
        return statementContext;
    }

    public static CascadesContext createCascadesContext(String sql) {
        return createCascadesContext(createConnectContext(), sql);
    }

    public static CascadesContext createCascadesContext(Plan initPlan) {
        return createCascadesContext(createConnectContext(), initPlan);
    }

    public static CascadesContext createCascadesContext(ConnectContext connectContext, String sql) {
        StatementContext statementCtx = createStatementContext(connectContext, sql);
        return createCascadesContext(statementCtx, sql);
    }

    public static CascadesContext createCascadesContext(StatementContext statementContext, String sql) {
        LogicalPlan initPlan = new NereidsParser().parseSingle(sql);
        return createCascadesContext(statementContext, initPlan);
    }

    public static CascadesContext createCascadesContext(ConnectContext connectContext, Plan initPlan) {
        StatementContext statementCtx = createStatementContext(connectContext, "");
        return createCascadesContext(statementCtx, initPlan);
    }

    public static CascadesContext createCascadesContext(StatementContext statementContext, Plan initPlan) {
        PhysicalProperties requestProperties = NereidsPlanner.buildInitRequireProperties();
        CascadesContext cascadesContext = CascadesContext.initContext(
                statementContext, initPlan, requestProperties);
        cascadesContext.toMemo();
        MemoValidator.validateInitState(cascadesContext.getMemo(), initPlan);
        return cascadesContext;
    }

    public static LogicalPlan analyze(String sql) {
        CascadesContext cascadesContext = createCascadesContext(sql);
        cascadesContext.newAnalyzer().analyze();
        return (LogicalPlan) cascadesContext.getRewritePlan();
    }

    /**
     * create test connection context.
     * @param user connect user
     * @param host connect host
     * @return ConnectContext
     * @throws IOException exception
     */
    public static ConnectContext createCtx(UserIdentity user, String host) {
        try {
            ConnectContext ctx = new ConnectContext();
            ctx.setCurrentUserIdentity(user);
            ctx.setQualifiedUser(user.getQualifiedUser());
            ctx.setRemoteIP(host);
            ctx.setEnv(Env.getCurrentEnv());
            ctx.setThreadLocalInfo();
            return ctx;
        } catch (Throwable t) {
            throw new IllegalStateException("can not create test connect context", t);
        }
    }

    private static String getIdentStr(int indent) {
        return StringUtils.repeat("   ", indent);
    }
}
