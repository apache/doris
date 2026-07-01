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

package org.apache.doris.nereids.rules.rewrite.eageraggregation;

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Branch-scoped eager aggregation push-down hints parsed from the session variable
 * {@code force_eager_agg_hint}.
 *
 * <p>Format: semicolon-separated list of {@code <key>=<action>} entries, where
 * {@code key = <funcName>:<argSig>} and {@code action ∈ {push, nopush}}.
 *
 * <p>The key is matched per aggregate-function occurrence, but the effect is applied at the
 * current candidate push-down branch/subtree instead of independently per function:
 * if any matched aggregate in the branch is marked {@code nopush}, push-down is disabled for
 * that branch; otherwise, if any matched aggregate in the branch is marked {@code push},
 * push-down may be forced for that branch. Other aggregates in the same branch follow that
 * branch-level decision.
 *
 * <p>{@code argSig} rules:
 * <ul>
 *   <li>{@code count(*)} → {@code "*"}</li>
 *   <li>single-arg agg over a SlotReference → {@code "<last-qualifier-segment>.<column>"}
 *       or {@code "<column>"} when the slot has no qualifier</li>
 *   <li>otherwise → {@code Expression#toSql()} lower-cased</li>
 * </ul>
 *
 * <p>Examples:
 * <pre>
 *   set force_eager_agg_hint = 'sum:t1.a=push; sum:t2.a=nopush; count:*=push';
 * </pre>
 *
 * <p>This feature is intended for tests/debugging of the eager-aggregation rewrite only;
 * when unset, all decisions fall back to {@code eager_aggregation_mode} + statistics.
 */
public final class EagerAggHints {

    /** Matched hint action for a specific aggregate-function occurrence. */
    public enum Action {
        PUSH,
        NOPUSH
    }

    private EagerAggHints() {
    }

    /**
     * Returns the matched hint action for the given aggregate function based on the current
     * session's {@code force_eager_agg_hint}, or empty if no matching entry is configured.
     */
    public static Optional<Action> decide(AggregateFunction aggFunction) {
        Map<String, Action> hints = currentHints();
        if (hints.isEmpty()) {
            return Optional.empty();
        }
        Action action = hints.get(keyOf(aggFunction));
        return Optional.ofNullable(action);
    }

    /** Builds the canonical hint key for the given aggregate function. */
    public static String keyOf(AggregateFunction aggFunction) {
        String fn = aggFunction.getName().toLowerCase();
        if (aggFunction instanceof Count && ((Count) aggFunction).isStar()) {
            return fn + ":*";
        }
        if (aggFunction.arity() == 1) {
            Expression arg = aggFunction.child(0);
            if (arg instanceof SlotReference) {
                SlotReference slot = (SlotReference) arg;
                List<String> qualifier = slot.getQualifier();
                String prefix = qualifier.isEmpty()
                        ? ""
                        : qualifier.get(qualifier.size() - 1).toLowerCase() + ".";
                return fn + ":" + prefix + slot.getName().toLowerCase();
            }
        }
        return fn + ":" + aggFunction.child(0).toSql().toLowerCase();
    }

    private static Map<String, Action> currentHints() {
        ConnectContext ctx = ConnectContext.get();
        if (ctx == null) {
            return ImmutableMap.of();
        }
        return ctx.getSessionVariable().getForceEagerAggHintMap();
    }

    /** Parse a raw hint string into a map. */
    public static Map<String, Action> parse(String raw) {
        if (Strings.isNullOrEmpty(raw)) {
            return ImmutableMap.of();
        }
        ImmutableMap.Builder<String, Action> map = ImmutableMap.builder();
        for (String entry : raw.split(";")) {
            String trimmed = entry.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            int eq = trimmed.lastIndexOf('=');
            if (eq <= 0 || eq == trimmed.length() - 1) {
                throw invalidHint(raw);
            }
            String key = trimmed.substring(0, eq).trim().toLowerCase();
            String val = trimmed.substring(eq + 1).trim().toLowerCase();
            Action action;
            switch (val) {
                case "push":
                    action = Action.PUSH;
                    break;
                case "nopush":
                case "no_push":
                case "no-push":
                    action = Action.NOPUSH;
                    break;
                default:
                    throw invalidHint(raw);
            }
            int keySeparator = key.indexOf(':');
            if (keySeparator <= 0 || keySeparator == key.length() - 1) {
                throw invalidHint(raw);
            }
            map.put(key, action);
        }
        return map.buildKeepingLast();
    }

    private static IllegalArgumentException invalidHint(String raw) {
        return new IllegalArgumentException("Invalid force_eager_agg_hint: " + raw
                + ". Expected format: '<func>:<qualifier.column | *>=<push|nopush>'"
                + " separated by ';'.");
    }
}
