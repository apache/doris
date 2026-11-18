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

package org.apache.doris.nereids.spm;

import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.nereids.spm.placeholder.SPMPlaceholderBuilder;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SqlModeHelper;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/** Raw-fallback fixtures built from the production parser, parameterizer and baseline assembly. */
public final class SpmPlannerTestAccess {
    private SpmPlannerTestAccess() {
    }

    public static BaselinePlan buildBaseline(SPMPlanner planner, String bindSql, String planSql)
            throws UserException {
        long mode = SqlModeHelper.currentMode();
        LogicalPlan bind = parse(mode, bindSql, "SPM bindSql must be a SELECT statement: ");
        LogicalPlan plan = bindSql.equals(planSql) ? bind
                : parse(mode, planSql, "SPM planSql must be a SELECT statement: ");
        return buildBaseline(planner, bind, plan, bindSql, planSql, 0.0);
    }

    public static BaselinePlan buildBaseline(SPMPlanner planner, LogicalPlan bind, LogicalPlan plan,
            String bindSql, String planSql, double cost) {
        try {
            invoke(null, "rejectReplayContextExpressions",
                    new Class<?>[] {LogicalPlan.class, LogicalPlan.class, String.class}, bind, plan, bindSql);
            SPMPlanTreeSupport.rejectManualPlanMismatch(bind, plan, bindSql);
            SPMPlanTreeSupport.rejectVolatileFunctionDependencies(bind, plan);
            Pair<LogicalPlan, LogicalPlan> trees = invoke(null, "parameterizeWholeTrees",
                    new Class<?>[] {LogicalPlan.class, LogicalPlan.class, SPMPlaceholderBuilder.class},
                    bind, plan, new SPMPlaceholderBuilder());
            ConnectContext ctx = ConnectContext.get();
            String catalog = invoke(null, "captureCatalogName", new Class<?>[] {ConnectContext.class}, ctx);
            String database = invoke(null, "captureDatabaseName", new Class<?>[] {ConnectContext.class}, ctx);
            BaselinePlan baseline = invoke(null, "assembleBaseline", new Class<?>[] {
                    LogicalPlan.class, LogicalPlan.class, LogicalPlan.class, String.class, String.class,
                    double.class, String.class, String.class, long.class},
                    bind, trees.first, trees.second, bindSql, planSql, cost, catalog, database,
                    SqlModeHelper.currentMode());
            baseline.setPlanFrozen(false);
            baseline.setPlanSqlMode(baseline.getCreatorSqlMode());
            baseline.setSchemaFingerprint(SPMPlanTreeSupport.schemaFingerprint(ctx, bind));
            return baseline;
        } catch (UserException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    private static LogicalPlan parse(long mode, String sql, String message) throws UserException {
        return invoke(null, "parseSelectIsolated", new Class<?>[] {long.class, String.class, String.class},
                mode, sql, message + sql);
    }

    private static <T> T invoke(SPMPlanner receiver, String name, Class<?>[] params, Object... args)
            throws UserException {
        try {
            Method method = SPMPlanner.class.getDeclaredMethod(name, params);
            method.setAccessible(true);
            return (T) method.invoke(receiver, args);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof UserException) {
                throw (UserException) cause;
            }
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            throw new IllegalStateException(cause);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }
}
