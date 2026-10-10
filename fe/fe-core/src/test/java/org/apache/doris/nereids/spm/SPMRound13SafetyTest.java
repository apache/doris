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
import org.apache.doris.catalog.FunctionRegistry;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SessionVarGuardExpr;
import org.apache.doris.nereids.trees.expressions.functions.FunctionBuilder;
import org.apache.doris.nereids.trees.expressions.functions.udf.AliasUdf;
import org.apache.doris.nereids.trees.expressions.functions.udf.AliasUdfBuilder;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The schema fingerprint must follow the function the call
 * ACTUALLY resolves to.
 *
 * The old lookup always searched ctx.getDatabase() (ignoring the WRITTEN qualifier),
 * hashed only the FIRST same-name alias overload, and ignored prefer_udf_over_builtin:
 * for a baseline using other_db.f(varchar_col) a changed overload left the fingerprint
 * unchanged, and flipping prefer_udf_over_builtin after freezing a colliding alias such
 * as abs(INT) could replay the alias body although the same SQL now resolves to builtin
 * abs. The entry now covers the written scope, EVERY same-name overload, and the
 * effective builtin/UDF choice.
 */
public class SPMRound13SafetyTest {

    private static LogicalPlan parse(String sql) {
        return (LogicalPlan) new NereidsParser().parseSingle(sql);
    }

    /** A session with a statement context and spm_db as the current database. */
    private static ConnectContext context() {
        ConnectContext ctx = new ConnectContext();
        ctx.setStatementContext(new StatementContext());
        ctx.setSessionVariable(new SessionVariable());
        ctx.setDatabase("spm_db");
        ctx.setThreadLocalInfo();
        return ctx;
    }

    /** A registry with ONE builtin name (empty builder list) and no UDFs. */
    private static FunctionRegistry builtinRegistry(String name) {
        FunctionRegistry registry = Mockito.mock(FunctionRegistry.class);
        Map<String, List<FunctionBuilder>> builtins = new HashMap<>();
        builtins.put(name, List.of());
        Mockito.when(registry.getName2BuiltinBuilders()).thenReturn(builtins);
        Mockito.when(registry.findUdfBuilder(Mockito.any(), Mockito.any()))
                .thenReturn(List.of());
        return registry;
    }

    private static String fingerprint(ConnectContext ctx, String sql, FunctionRegistry registry) {
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            Env env = Mockito.mock(Env.class);
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getFunctionRegistry()).thenReturn(registry);
            return SPMPlanTreeSupport.schemaFingerprint(ctx, parse(sql));
        }
    }

    @Test
    public void testFunctionNameCaseIsNormalized() {
        ConnectContext ctx = context();
        try {
            FunctionRegistry registry = builtinRegistry("abs");
            String upper = fingerprint(ctx, "SELECT ABS(1)", registry);
            String lower = fingerprint(ctx, "SELECT abs(1)", registry);
            Assertions.assertEquals(lower, upper,
                    "the parser preserves the written case: SUM and sum are the same"
                            + " function and must produce the same entry");
            Assertions.assertTrue(lower.contains("fn:abs|builtin|u=-"), lower);
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testQualifiedCallUsesWrittenDatabaseScope() {
        ConnectContext ctx = context();
        try {
            FunctionRegistry registry = builtinRegistry("abs");
            String qualified = fingerprint(ctx, "SELECT other_db.abs(1)", registry);
            String unqualified = fingerprint(ctx, "SELECT abs(1)", registry);
            // a qualified call resolves ONLY through other_db's UDF scope - it never
            // falls back to a builtin
            Assertions.assertTrue(qualified.contains("fn:other_db.abs|none|u=-"), qualified);
            Assertions.assertTrue(unqualified.contains("fn:abs|builtin|u=-"), unqualified);
            Assertions.assertNotEquals(unqualified, qualified,
                    "ignoring the WRITTEN qualifier made other_db.abs(...) hash the wrong"
                            + " scope: changing the real overload left the entry unchanged");
            Mockito.verify(registry).findUdfBuilder("other_db", "abs");
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testPreferUdfOverBuiltinFlipsTheEffectiveChoice() {
        ConnectContext ctx = context();
        try {
            FunctionRegistry registry = builtinRegistry("abs");
            Mockito.when(registry.findUdfBuilder("spm_db", "abs"))
                    .thenReturn(List.of(aliasAbs("x + 1")));
            String builtinWins = fingerprint(ctx, "SELECT abs(1)", registry);
            Assertions.assertTrue(builtinWins.contains("fn:abs|builtin|u="), builtinWins);

            ctx.getSessionVariable().preferUdfOverBuiltin = true;
            String udfWins = fingerprint(ctx, "SELECT abs(1)", registry);
            Assertions.assertTrue(udfWins.contains("fn:abs|udf|u="), udfWins);
            Assertions.assertNotEquals(builtinWins, udfWins,
                    "flipping prefer_udf_over_builtin changes which implementation the SQL"
                            + " resolves to: a baseline frozen with the builtin must be"
                            + " invalidated instead of replaying the alias body");

            // every same-name overload takes part: a SECOND overload (or a changed body)
            // must change the entry even though the exact overload of an unbound call
            // cannot be reproduced
            ctx.getSessionVariable().preferUdfOverBuiltin = false;
            Mockito.when(registry.findUdfBuilder("spm_db", "abs"))
                    .thenReturn(List.of(aliasAbs("x + 1"), aliasAbs("x + 2")));
            String twoOverloads = fingerprint(ctx, "SELECT abs(1)", registry);
            Assertions.assertNotEquals(builtinWins, twoOverloads,
                    "adding / changing an overload must change the entry (the old lookup"
                            + " hashed only the first same-name alias)");
        } finally {
            ConnectContext.remove();
        }
    }

    /**
     * The guard on an alias-UDF expansion is the DEPENDENCY MARKER of the definition's
     * saved settings, so it must be retained even when the creator's result-affecting
     * variables currently EQUAL them: without it SPM saw no guard and froze the plain
     * arithmetic as ordinary SQL, and a later caller with different enable_decimal256
     * / decimal_overflow_scale re-planned the frozen arithmetic under the CALLER's
     * settings instead of the definition's saved ones.
     */
    @Test
    public void testUdfDependencyMarkerIsRetainedWhenCreatorVariablesMatch() {
        ConnectContext ctx = context();
        try {
            Map<String, String> savedVariables =
                    ctx.getSessionVariable().getAffectQueryResultInPlanVariables();
            AliasUdf alias = new AliasUdf("abs", List.of(IntegerType.INSTANCE),
                    new NereidsParser().parseExpression("x * 1"), List.of("x"), savedVariables);
            Expression expansion = new AliasUdfBuilder(alias)
                    .build("abs", List.of(new IntegerLiteral(1))).first;
            Assertions.assertTrue(containsSessionVarGuard(expansion),
                    "the expansion must keep the guard marker although the creator's variables"
                            + " match the saved ones: " + expansion);
        } finally {
            ConnectContext.remove();
        }
    }

    private static boolean containsSessionVarGuard(Expression expression) {
        if (expression instanceof SessionVarGuardExpr) {
            return true;
        }
        for (Expression child : expression.children()) {
            if (containsSessionVarGuard(child)) {
                return true;
            }
        }
        return false;
    }

    private static AliasUdfBuilder aliasAbs(String bodySql) {
        AliasUdf alias = new AliasUdf("abs", List.of(IntegerType.INSTANCE),
                new NereidsParser().parseExpression(bodySql), List.of("x"),
                Map.of("enable_decimal256", "1"));
        return new AliasUdfBuilder(alias);
    }
}
