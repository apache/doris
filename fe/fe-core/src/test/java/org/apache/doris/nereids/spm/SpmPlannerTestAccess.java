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

import org.apache.doris.common.UserException;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Planner-test entries for {@link SPMPlanner}: the two UT-only construction paths build a
 * baseline from texts / pre-built plans and are private in production, so the tests reach
 * them through reflection from this helper.
 */
public final class SpmPlannerTestAccess {

    private SpmPlannerTestAccess() {
    }

    private static Method method(String name, Class<?>... params) {
        try {
            Method m = SPMPlanner.class.getDeclaredMethod(name, params);
            m.setAccessible(true);
            return m;
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }

    /** UT entry: build a baseline from the two SQL texts. */
    public static BaselinePlan buildBaseline(SPMPlanner planner, String bindSql, String planSql)
            throws UserException {
        try {
            return (BaselinePlan) method("buildBaselineFromSqlTexts", String.class, String.class)
                    .invoke(planner, bindSql, planSql);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof UserException) {
                throw (UserException) cause;
            }
            throw rethrow(cause);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }

    /** UT entry: build a baseline from the two pre-built plans. */
    public static BaselinePlan buildBaseline(SPMPlanner planner, LogicalPlan bindPlan, LogicalPlan planPlan,
            String bindSql, String planSql, double cost) {
        try {
            return (BaselinePlan) method("assembleBaselineFromPlans", LogicalPlan.class, LogicalPlan.class,
                    String.class, String.class, double.class).invoke(planner, bindPlan, planPlan, bindSql, planSql,
                    cost);
        } catch (InvocationTargetException e) {
            throw rethrow(e.getCause());
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }

    private static RuntimeException rethrow(Throwable cause) {
        if (cause instanceof RuntimeException) {
            return (RuntimeException) cause;
        }
        if (cause instanceof Error) {
            throw (Error) cause;
        }
        return new IllegalStateException(cause);
    }
}
