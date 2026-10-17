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

import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.trees.TableSample;
import org.apache.doris.nereids.trees.plans.Plan;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * White-box access to {@link SPMPlanTreeSupport} internals for unit tests: the cap /
 * ordering / scan-identity comparisons are reached through reflection instead of
 * production-side test hooks.
 */
public final class SpmPlanTreeSupportTestAccess {

    private SpmPlanTreeSupportTestAccess() {
    }

    private static Method method(String name, Class<?>... params) {
        try {
            Method m = SPMPlanTreeSupport.class.getDeclaredMethod(name, params);
            m.setAccessible(true);
            return m;
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }

    private static Object call(String name, Class<?>[] params, Object... args) {
        try {
            return method(name, params).invoke(null, args);
        } catch (java.lang.reflect.InvocationTargetException e) {
            Throwable cause = e.getCause();
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

    /** The collected row-limit cap keys of one tree (sorted), like the historic accessor. */
    public static String rowLimitKeys(Plan plan) {
        Map<String, Integer> caps = new HashMap<>();
        call("collectRowLimits", new Class<?>[] {Plan.class, Map.class}, plan, caps);
        List<String> keys = new ArrayList<>(caps.keySet());
        Collections.sort(keys);
        return keys.toString();
    }

    /** The partition / tablet multiset comparison of the L3 scan identity. */
    public static boolean sameSelectionIgnoreOrder(List<?> bind, List<?> user) {
        return (Boolean) call("sameSelectionIgnoreOrder", new Class<?>[] {List.class, List.class}, bind, user);
    }

    /** The L3 scan-identity comparison (incl. the temp-partition namespace). */
    public static boolean sameScanIdentity(UnboundRelation bind, UnboundRelation user) {
        return (Boolean) call("sameScanIdentity",
                new Class<?>[] {UnboundRelation.class, UnboundRelation.class}, bind, user);
    }

    /** The stable table-sample rendering of the scan selector comparison. */
    public static String describeTableSample(TableSample sample) {
        return (String) call("describeTableSample", new Class<?>[] {TableSample.class}, sample);
    }
}
