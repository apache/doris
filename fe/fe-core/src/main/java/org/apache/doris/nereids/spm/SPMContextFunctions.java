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

import org.apache.doris.catalog.Env;
import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.analyzer.UnboundStar;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;
import org.apache.doris.nereids.trees.expressions.functions.FunctionBuilder;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ConnectionId;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CurrentCatalog;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CurrentDate;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CurrentTime;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CurrentUser;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Database;
import org.apache.doris.nereids.trees.expressions.functions.scalar.LastQueryId;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Now;
import org.apache.doris.nereids.trees.expressions.functions.scalar.SessionUser;
import org.apache.doris.nereids.trees.expressions.functions.scalar.UnixTimestamp;
import org.apache.doris.nereids.trees.expressions.functions.scalar.User;
import org.apache.doris.nereids.trees.expressions.functions.scalar.UtcDate;
import org.apache.doris.nereids.trees.expressions.functions.scalar.UtcTime;
import org.apache.doris.nereids.trees.expressions.functions.scalar.UtcTimestamp;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Version;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalUsingJoin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/** Builtin functions whose statement/session value must be computed by the replay, not the capture. */
public final class SPMContextFunctions {
    private static final Map<Class<?>, String> BUILTINS = Map.ofEntries(
            Map.entry(Now.class, "now"), Map.entry(CurrentDate.class, "current_date"),
            Map.entry(CurrentTime.class, "current_time"), Map.entry(UtcTimestamp.class, "utc_timestamp"),
            Map.entry(UtcDate.class, "utc_date"), Map.entry(UtcTime.class, "utc_time"),
            Map.entry(UnixTimestamp.class, "unix_timestamp"), Map.entry(ConnectionId.class, "connection_id"),
            Map.entry(LastQueryId.class, "last_query_id"), Map.entry(Database.class, "database"),
            Map.entry(CurrentCatalog.class, "current_catalog"), Map.entry(CurrentUser.class, "current_user"),
            Map.entry(SessionUser.class, "session_user"), Map.entry(User.class, "user"),
            Map.entry(Version.class, "version"));

    private SPMContextFunctions() {
    }

    private static String kind(Expression expression) {
        String name;
        if (expression instanceof UnboundFunction) {
            name = ((UnboundFunction) expression).getName().toLowerCase(Locale.ROOT);
            name = switch (name) {
                case "current_timestamp", "localtimestamp", "localtime" -> "now";
                case "curdate" -> "current_date";
                case "curtime" -> "current_time";
                case "schema", "current_database" -> "database";
                default -> name;
            };
            if (!BUILTINS.containsValue(name)) {
                return null;
            }
        } else {
            name = BUILTINS.get(expression.getClass());
            if (name == null) {
                return null;
            }
        }
        return expression.arity() == 0 || hasPrecision(name) && expression.arity() == 1 ? name : null;
    }

    private static boolean hasPrecision(String name) {
        return "now".equals(name) || "current_time".equals(name)
                || "utc_timestamp".equals(name) || "utc_time".equals(name);
    }

    /** Parsed candidate or actual bound builtin. An argument-taking UNIX_TIMESTAMP is not a clock. */
    public static boolean isCandidate(Expression expression) {
        return expression instanceof BoundFunction || expression instanceof UnboundFunction
                ? kind(expression) != null : false;
    }

    /** Precision supplies the function signature and is not a substitutable business value. */
    public static boolean isPrecisionCall(Expression expression) {
        return expression.arity() == 1 && hasPrecision(kind(expression));
    }

    /** Only a bound builtin may be protected from evaluation, and only in the freeze capture scope. */
    public static boolean shouldPreserve(Expression expression) {
        return SPMOptimizer.isPreservingContextFunctions()
                && expression instanceof BoundFunction && kind(expression) != null;
    }

    private static void collect(Expression expression, List<Expression> calls) {
        if (isCandidate(expression)) {
            calls.add(expression);
        }
        if (expression instanceof UnboundStar) {
            for (Expression replacement : ((UnboundStar) expression).getReplacedAlias()) {
                collect(replacement, calls);
            }
        }
        for (Expression child : expression.children()) {
            collect(child, calls);
        }
    }

    private static List<Expression> calls(Plan plan) {
        List<Expression> calls = new ArrayList<>();
        SPMPlanTreeSupport.<RuntimeException>walkPlans(plan, node -> {
            for (Expression expression : node.getExpressions()) {
                collect(expression, calls);
            }
            if (node instanceof LogicalUsingJoin) {
                ((LogicalUsingJoin<?, ?>) node).getMatchCondition().ifPresent(expr -> collect(expr, calls));
            }
        });
        return calls;
    }

    /** Compares concrete function inputs in a manual bind/plan pair, including precision expressions. */
    static Map<String, Integer> structure(Plan plan) {
        Map<String, Integer> structures = new HashMap<>();
        for (Expression call : calls(plan)) {
            String key = kind(call) + "(" + call.children().stream()
                    .map(Expression::toSql).collect(java.util.stream.Collectors.joining(",")) + ")";
            structures.merge(key, 1, Integer::sum);
        }
        return structures;
    }

    static Map<String, Integer> counts(Plan plan) {
        Map<String, Integer> counts = new HashMap<>();
        for (Expression call : calls(plan)) {
            counts.merge(kind(call) + "/" + call.arity(), 1, Integer::sum);
        }
        return counts;
    }

    /** Resolves parsed candidates using the ordinary registry, not a name-based assumption. */
    private static BoundFunction resolve(Expression expression) {
        if (expression instanceof BoundFunction) {
            return (BoundFunction) expression;
        }
        UnboundFunction function = (UnboundFunction) expression;
        FunctionBuilder builder = Env.getCurrentEnv().getFunctionRegistry()
                .findFunctionBuilder(function.getDbName(), function.getName(), function.children());
        if (!kind(expression).equals(BUILTINS.get(builder.functionClass()))) {
            throw new IllegalStateException("SPM context call does not resolve to its builtin: " + function);
        }
        return builder.build(function.getName(), function.children()).second;
    }

    /** A same-name UDF must not be certified as an equivalent built-in. */
    static boolean resolvesToBuiltins(Plan plan) {
        try {
            for (Expression call : calls(plan)) {
                resolve(call);
            }
            return true;
        } catch (RuntimeException e) {
            return false;
        }
    }

    /** Bound function identity, precision and result type used for the round-trip proof. */
    static Set<String> signatures(Plan plan) {
        Set<String> signatures = new HashSet<>();
        for (Expression expression : calls(plan)) {
            BoundFunction function = resolve(expression);
            signatures.add(kind(function) + "/" + function.arity()
                    + "/" + function.children().stream().map(Expression::toSql)
                            .collect(java.util.stream.Collectors.joining(","))
                    + "/" + function.getDataType());
        }
        return signatures;
    }

    /** No input context call may disappear from the rendered SQL. Extra copies are harmless. */
    static boolean preservesCalls(Map<String, Integer> expected, Plan rendered) {
        Map<String, Integer> actual = counts(rendered);
        return expected.keySet().equals(actual.keySet())
                && expected.entrySet().stream().allMatch(e -> actual.get(e.getKey()) >= e.getValue());
    }
}
