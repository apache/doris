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
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.system.SystemInfoService;

import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.nio.channels.SocketChannel;

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
        return new StatementContext(connectContext, new OriginStatement(sql, 0));
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
        CascadesContext cascadesContext = CascadesContext.newContext(statementContext, initPlan);
        MemoValidator.validateInitState(cascadesContext.getMemo(), initPlan);
        return cascadesContext;
    }

    public static LogicalPlan analyze(String sql) {
        CascadesContext cascadesContext = createCascadesContext(sql);
        cascadesContext.newAnalyzer().analyze();
        return (LogicalPlan) cascadesContext.getMemo().copyOut();
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
            SocketChannel channel = SocketChannel.open();
            ConnectContext ctx = new ConnectContext(channel);
            ctx.setCluster(SystemInfoService.DEFAULT_CLUSTER);
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

    public static String printGroupTree(Memo memo) {
        Group root = memo.getRoot();
        StringBuilder builder = new StringBuilder();
        printGroup(root, 0, true, builder);
        return builder.toString();
    }

    private static void printGroup(Group group, int depth, boolean isLastGroup, StringBuilder builder) {
        if (!group.getLogicalExpressions().isEmpty()) {
            builder.append(getIdentStr(depth + 1))
                    .append(group.getPhysicalExpressions().isEmpty() ? "+--" : "|--")
                    .append("logicalExpressions:\n");
            for (int i = 0; i < group.getLogicalExpressions().size(); i++) {
                GroupExpression logicalExpression = group.getLogicalExpressions().get(i);
                boolean isLastExpression = i + 1 == group.getLogicalExpressions().size();
                printGroupExpression(logicalExpression, depth + 2, isLastExpression, builder);
            }
        }

        if (!group.getPhysicalExpressions().isEmpty()) {
            builder.append(getIdentStr(depth + 1)).append("+-- physicalExpressions:\n");
            for (int i = 0; i < group.getPhysicalExpressions().size(); i++) {
                GroupExpression logicalExpression = group.getPhysicalExpressions().get(i);
                boolean isLastExpression = i + 1 == group.getPhysicalExpressions().size();
                printGroupExpression(logicalExpression, depth + 2, isLastExpression, builder);
            }
        }
    }

    private static void printGroupExpression(GroupExpression groupExpression, int indent,
            boolean isLastExpression, StringBuilder builder) {
        builder.append(getIdentStr(indent))
                .append(isLastExpression ? "+--" : "|--")
                .append(groupExpression.getPlan().toString()).append("\n");
        for (int i = 0; i < groupExpression.children().size(); i++) {
            Group childGroup = groupExpression.children().get(i);
            boolean isLastGroup = i + 1 == groupExpression.children().size();
            printGroup(childGroup, indent + 1, isLastGroup, builder);
        }
    }

    private static String getIdentStr(int indent) {
        return StringUtils.repeat("   ", indent);
    }
}
