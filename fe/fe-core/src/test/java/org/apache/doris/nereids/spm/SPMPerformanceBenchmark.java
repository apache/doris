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

import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.spm.placeholder.SPMPlaceholderBuilder;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import java.lang.management.ManagementFactory;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/** Standalone stage benchmark; run outside Maven after compiling the FE tests. */
public final class SPMPerformanceBenchmark {
    private static volatile Object sink;
    private static final com.sun.management.ThreadMXBean MX =
            (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();

    @FunctionalInterface
    private interface Work {
        Object run() throws Exception;
    }

    private SPMPerformanceBenchmark() {
    }

    private static void measure(String name, int size, int repetitions, Work work) throws Exception {
        for (int i = 0; i < 12; i++) {
            sink = work.run();
        }
        long[] nanos = new long[repetitions];
        long[] bytes = new long[repetitions];
        long thread = Thread.currentThread().getId();
        for (int i = 0; i < repetitions; i++) {
            long allocated = MX.getThreadAllocatedBytes(thread);
            long start = System.nanoTime();
            sink = work.run();
            nanos[i] = System.nanoTime() - start;
            bytes[i] = MX.getThreadAllocatedBytes(thread) - allocated;
        }
        if (Boolean.FALSE.equals(sink)) {
            throw new IllegalStateException("benchmark contract or match failed: " + name);
        }
        Arrays.sort(nanos);
        Arrays.sort(bytes);
        System.out.println(name + "," + size + "," + nanos[repetitions / 2]
                + "," + bytes[repetitions / 2]);
    }

    private static String query(int predicates, boolean reverse) {
        StringBuilder sql = new StringBuilder("SELECT a0 FROM t WHERE ");
        for (int i = 0; i < predicates; i++) {
            if (i > 0) {
                sql.append(" AND ");
            }
            int key = reverse ? predicates - i - 1 : i;
            sql.append("a").append(key).append(" = ").append(key + (reverse ? 10000 : 0));
        }
        return sql.toString();
    }

    /** Prints per-case median time and thread allocation in CSV form. */
    public static void main(String[] args) throws Exception {
        MX.setThreadAllocatedMemoryEnabled(true);
        NereidsParser parser = new NereidsParser();
        System.out.println("case,size,median_ns,median_allocated_bytes");
        for (int size : new int[] {128, 512, 2048}) {
            List<Expression> expressions = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                expressions.add(parser.parseExpression("a = " + i));
            }
            measure("parameterize", size, 15,
                    () -> new SPMPlaceholderBuilder().parameterizeExpressions(expressions));
        }
        Method contract = SPMPlanner.class.getDeclaredMethod("limitContractPreserved",
                LogicalPlan.class, LogicalPlan.class, LogicalPlan.class);
        contract.setAccessible(true);
        for (int depth : new int[] {8, 32, 64}) {
            String sql = "SELECT k FROM t LIMIT 1";
            for (int i = 0; i < depth; i++) {
                sql = "SELECT k FROM (" + sql + ") s" + i + " LIMIT " + (i + 2);
            }
            LogicalPlan user = (LogicalPlan) parser.parseSingle(sql);
            LogicalPlan replay = (LogicalPlan) parser.parseSingle(sql);
            measure("limit_contract", depth, 15, () -> contract.invoke(null, replay, user, user));
        }
        for (int size : new int[] {32, 128}) {
            LogicalPlan bind = SPMPlanTreeSupport.transform((LogicalPlan) parser.parseSingle(query(size, false)),
                    new SPMPlaceholderBuilder());
            LogicalPlan user = (LogicalPlan) parser.parseSingle(query(size, true));
            measure("unordered_match", size, 9,
                    () -> SPMPlanTreeSupport.check(bind, user, new HashMap<>()));
        }
        String sql = query(128, false);
        measure("stored_sql_parse", 128, 21,
                () -> SPMPlanTreeSupport.parseStoredSqlIsolated(sql, 0L));
    }
}
