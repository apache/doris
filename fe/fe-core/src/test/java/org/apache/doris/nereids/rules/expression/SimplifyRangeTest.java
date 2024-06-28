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

package org.apache.doris.nereids.rules.expression;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.rules.analysis.ExpressionAnalyzer;
import org.apache.doris.nereids.rules.expression.rules.SimplifyRange;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.util.MemoTestUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class SimplifyRangeTest extends ExpressionRewrite {

    private static final NereidsParser PARSER = new NereidsParser();
    private ExpressionRuleExecutor executor;
    private ExpressionRewriteContext context;

    public SimplifyRangeTest() {
        CascadesContext cascadesContext = MemoTestUtils.createCascadesContext(
                new UnboundRelation(new RelationId(1), ImmutableList.of("tbl")));
        context = new ExpressionRewriteContext(cascadesContext);
    }

    @Test
    public void testSimplify() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
            bottomUp(SimplifyRange.INSTANCE)
        ));
        assertRewrite("TA", "TA");
        assertRewrite("TA > 3 or TA > null", "TA > 3");
        assertRewrite("TA > 3 or TA < null", "TA > 3");
        assertRewrite("TA > 3 or TA = null", "TA > 3");
        assertRewrite("TA > 3 or TA <> null", "TA > 3 or TA <> null");
        assertRewrite("TA > 3 or TA <=> null", "TA > 3 or TA <=> null");
        assertRewrite("TA > 3 and TA > null", "false");
        assertRewrite("TA > 3 and TA < null", "false");
        assertRewrite("TA > 3 and TA = null", "false");
        assertRewrite("TA > 3 and TA <> null", "TA > 3 and TA <> null");
        assertRewrite("TA > 3 and TA <=> null", "TA > 3 and TA <=> null");
        assertRewrite("(TA >= 1 and TA <=3 ) or (TA > 5 and TA < 7)", "(TA >= 1 and TA <=3 ) or (TA > 5 and TA < 7)");
        assertRewrite("(TA > 3 and TA < 1) or (TA > 7 and TA < 5)", "FALSE");
        assertRewrite("TA > 3 and TA < 1", "FALSE");
        assertRewrite("TA >= 3 and TA < 3", "TA >= 3 and TA < 3");
        assertRewrite("TA = 1 and TA > 10", "FALSE");
        assertRewrite("TA > 5 or TA < 1", "TA > 5 or TA < 1");
        assertRewrite("TA > 5 or TA > 1 or TA > 10", "TA > 1");
        assertRewrite("TA > 5 or TA > 1 or TA < 10", "TA IS NOT NULL");
        assertRewriteNotNull("TA > 5 or TA > 1 or TA < 10", "TRUE");
        assertRewrite("TA > 5 and TA > 1 and TA > 10", "TA > 10");
        assertRewrite("TA > 5 and TA > 1 and TA < 10", "TA > 5 and TA < 10");
        assertRewrite("TA > 1 or TA < 1", "TA > 1 or TA < 1");
        assertRewrite("TA > 1 or TA < 10", "TA IS NOT NULL");
        assertRewriteNotNull("TA > 1 or TA < 10", "TRUE");
        assertRewrite("TA > 5 and TA < 10", "TA > 5 and TA < 10");
        assertRewrite("TA > 5 and TA > 10", "TA > 10");
        assertRewrite("TA > 5 + 1 and TA > 10", "TA > 5 + 1 and TA > 10");
        assertRewrite("TA > 5 + 1 and TA > 10", "TA > 5 + 1 and TA > 10");
        assertRewrite("(TA > 1 and TA > 10) or TA > 20", "TA > 10");
        assertRewrite("(TA > 1 or TA > 10) and TA > 20", "TA > 20");
        assertRewrite("(TA + TB > 1 or TA + TB > 10) and TA + TB > 20", "TA + TB > 20");
        assertRewrite("TA > 10 or TA > 10", "TA > 10");
        assertRewrite("(TA > 10 or TA > 20) and (TB > 10 and TB < 20)", "TA > 10 and (TB > 10 and TB < 20) ");
        assertRewrite("(TA > 10 or TA > 20) and (TB > 10 and TB > 20)", "TA > 10 and TB > 20");
        assertRewrite("((TB > 30 and TA > 40) and TA > 20) and (TB > 10 and TB > 20)", "TB > 30 and TA > 40");
        assertRewrite("(TA > 10 and TB > 10) or (TB > 10 and TB > 20)", "TA > 10 and TB > 10 or TB > 20");
        assertRewrite("((TA > 10 or TA > 5) and TB > 10) or (TB > 10 and (TB > 20 or TB < 10))", "(TA > 5 and TB > 10) or (TB > 10 and (TB > 20 or TB < 10))");
        assertRewrite("TA in (1,2,3) and TA > 10", "FALSE");
        assertRewrite("TA in (1,2,3) and TA >= 1", "TA in (1,2,3)");
        assertRewrite("TA in (1,2,3) and TA > 1", "TA IN (2, 3)");
        assertRewrite("TA in (1,2,3) or TA >= 1", "TA >= 1");
        assertRewrite("TA in (1)", "TA in (1)");
        assertRewrite("TA in (1,2,3) and TA < 10", "TA in (1,2,3)");
        assertRewrite("TA in (1,2,3) and TA < 1", "FALSE");
        assertRewrite("TA in (1,2,3) or TA < 1", "TA in (1,2,3) or TA < 1");
        assertRewrite("TA in (1,2,3) or TA in (2,3,4)", "TA in (1,2,3,4)");
        assertRewrite("TA in (1,2,3) or TA in (4,5,6)", "TA in (1,2,3,4,5,6)");
        assertRewrite("TA in (1,2,3) and TA in (4,5,6)", "FALSE");
        assertRewrite("TA in (1,2,3) and TA in (3,4,5)", "TA = 3");
        assertRewrite("TA + TB in (1,2,3) and TA + TB in (3,4,5)", "TA + TB = 3");
        assertRewrite("TA in (1,2,3) and DA > 1.5", "TA in (1,2,3) and DA > 1.5");
        assertRewrite("TA = 1 and TA = 3", "FALSE");
        assertRewrite("TA in (1) and TA in (3)", "FALSE");
        assertRewrite("TA in (1) and TA in (1)", "TA = 1");
        assertRewrite("(TA > 3 and TA < 1) and TB < 5", "FALSE");
        assertRewrite("(TA > 3 and TA < 1) or TB < 5", "TB < 5");
        assertRewrite("((IA = 1 AND SC ='1') OR SC = '1212') AND IA =1", "((IA = 1 AND SC ='1') OR SC = '1212') AND IA =1");

        assertRewrite("TA + TC", "TA + TC");
        assertRewrite("(TA + TC >= 1 and TA + TC <=3 ) or (TA + TC > 5 and TA + TC < 7)", "(TA + TC >= 1 and TA + TC <=3 ) or (TA + TC > 5 and TA + TC < 7)");
        assertRewrite("(TA + TC > 3 and TA + TC < 1) or (TA + TC > 7 and TA + TC < 5)", "FALSE");
        assertRewrite("TA + TC > 3 and TA + TC < 1", "FALSE");
        assertRewrite("TA + TC >= 3 and TA + TC < 3", "TA + TC >= 3 and TA + TC < 3");
        assertRewrite("TA + TC = 1 and TA + TC > 10", "FALSE");
        assertRewrite("TA + TC > 5 or TA + TC < 1", "TA + TC > 5 or TA + TC < 1");
        assertRewrite("TA + TC > 5 or TA + TC > 1 or TA + TC > 10", "TA + TC > 1");
        assertRewrite("TA + TC > 5 or TA + TC > 1 or TA + TC < 10", "(not (TA + TC) IS NULL)");
        assertRewrite("TA + TC > 5 and TA + TC > 1 and TA + TC > 10", "TA + TC > 10");
        assertRewrite("TA + TC > 5 and TA + TC > 1 and TA + TC < 10", "TA + TC > 5 and TA + TC < 10");
        assertRewrite("TA + TC > 1 or TA + TC < 1", "TA + TC > 1 or TA + TC < 1");
        assertRewrite("TA + TC > 1 or TA + TC < 10", "(not (TA + TC) IS NULL)");
        assertRewrite("TA + TC > 5 and TA + TC < 10", "TA + TC > 5 and TA + TC < 10");
        assertRewrite("TA + TC > 5 and TA + TC > 10", "TA + TC > 10");
        assertRewrite("TA + TC > 5 + 1 and TA + TC > 10", "TA + TC > 5 + 1 and TA + TC > 10");
        assertRewrite("TA + TC > 5 + 1 and TA + TC > 10", "TA + TC > 5 + 1 and TA + TC > 10");
        assertRewrite("(TA + TC > 1 and TA + TC > 10) or TA + TC > 20", "TA + TC > 10");
        assertRewrite("(TA + TC > 1 or TA + TC > 10) and TA + TC > 20", "TA + TC > 20");
        assertRewrite("(TA + TC + TB > 1 or TA + TC + TB > 10) and TA + TC + TB > 20", "TA + TC + TB > 20");
        assertRewrite("TA + TC > 10 or TA + TC > 10", "TA + TC > 10");
        assertRewrite("(TA + TC > 10 or TA + TC > 20) and (TB > 10 and TB < 20)", "TA + TC > 10 and (TB > 10 and TB < 20) ");
        assertRewrite("(TA + TC > 10 or TA + TC > 20) and (TB > 10 and TB > 20)", "TA + TC > 10 and TB > 20");
        assertRewrite("((TB > 30 and TA + TC > 40) and TA + TC > 20) and (TB > 10 and TB > 20)", "TB > 30 and TA + TC > 40");
        assertRewrite("(TA + TC > 10 and TB > 10) or (TB > 10 and TB > 20)", "TA + TC > 10 and TB > 10 or TB > 20");
        assertRewrite("((TA + TC > 10 or TA + TC > 5) and TB > 10) or (TB > 10 and (TB > 20 or TB < 10))", "(TA + TC > 5 and TB > 10) or (TB > 10 and (TB > 20 or TB < 10))");
        assertRewrite("TA + TC in (1,2,3) and TA + TC > 10", "FALSE");
        assertRewrite("TA + TC in (1,2,3) and TA + TC >= 1", "TA + TC in (1,2,3)");
        assertRewrite("TA + TC in (1,2,3) and TA + TC > 1", "(TA + TC) IN (2, 3)");
        assertRewrite("TA + TC in (1,2,3) or TA + TC >= 1", "TA + TC >= 1");
        assertRewrite("TA + TC in (1)", "TA + TC in (1)");
        assertRewrite("TA + TC in (1,2,3) and TA + TC < 10", "TA + TC in (1,2,3)");
        assertRewrite("TA + TC in (1,2,3) and TA + TC < 1", "FALSE");
        assertRewrite("TA + TC in (1,2,3) or TA + TC < 1", "TA + TC in (1,2,3) or TA + TC < 1");
        assertRewrite("TA + TC in (1,2,3) or TA + TC in (2,3,4)", "TA + TC in (1,2,3,4)");
        assertRewrite("TA + TC in (1,2,3) or TA + TC in (4,5,6)", "TA + TC in (1,2,3,4,5,6)");
        assertRewrite("TA + TC in (1,2,3) and TA + TC in (4,5,6)", "FALSE");
        assertRewrite("TA + TC in (1,2,3) and TA + TC in (3,4,5)", "TA + TC = 3");
        assertRewrite("TA + TC + TB in (1,2,3) and TA + TC + TB in (3,4,5)", "TA + TC + TB = 3");
        assertRewrite("TA + TC in (1,2,3) and DA > 1.5", "TA + TC in (1,2,3) and DA > 1.5");
        assertRewrite("TA + TC = 1 and TA + TC = 3", "FALSE");
        assertRewrite("TA + TC in (1) and TA + TC in (3)", "FALSE");
        assertRewrite("TA + TC in (1) and TA + TC in (1)", "TA + TC = 1");
        assertRewrite("(TA + TC > 3 and TA + TC < 1) and TB < 5", "FALSE");
        assertRewrite("(TA + TC > 3 and TA + TC < 1) or TB < 5", "TB < 5");

        assertRewrite("(TA + TC > 3 OR TA < 1) AND TB = 2) AND IA =1", "(TA + TC > 3 OR TA < 1) AND TB = 2) AND IA =1");

    }

    @Test
    public void testSimplifyDate() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
            bottomUp(SimplifyRange.INSTANCE)
        ));
        assertRewrite("TA", "TA");
        assertRewrite(
                "(TA >= date '2024-01-01' and TA <= date '2024-01-03') or (TA > date '2024-01-05' and TA < date '2024-01-07')",
                "(TA >= date '2024-01-01' and TA <= date '2024-01-03') or (TA > date '2024-01-05' and TA < date '2024-01-07')");
        assertRewrite(
                "(TA > date '2024-01-03' and TA < date '2024-01-01') or (TA > date '2024-01-07'and TA < date '2024-01-05')",
                "FALSE");
        assertRewrite("TA > date '2024-01-03' and TA < date '2024-01-01'", "FALSE");
        assertRewrite("TA >= date '2024-01-01' and TA < date '2024-01-01'",
                "TA >= date '2024-01-01' and TA < date '2024-01-01'");
        assertRewrite("TA = date '2024-01-01' and TA > date '2024-01-10'", "FALSE");
        assertRewrite("TA > date '2024-01-05' or TA < date '2024-01-01'",
                "TA > date '2024-01-05' or TA < date '2024-01-01'");
        assertRewrite("TA > date '2024-01-05' or TA > date '2024-01-01' or TA > date '2024-01-10'",
                "TA > date '2024-01-01'");
        assertRewrite("TA > date '2024-01-05' or TA > date '2024-01-01' or TA < date '2024-01-10'", "cast(TA as date) IS NOT NULL");
        assertRewriteNotNull("TA > date '2024-01-05' or TA > date '2024-01-01' or TA < date '2024-01-10'", "TRUE");
        assertRewrite("TA > date '2024-01-05' and TA > date '2024-01-01' and TA > date '2024-01-10'",
                "TA > date '2024-01-10'");
        assertRewrite("TA > date '2024-01-05' and TA > date '2024-01-01' and TA < date '2024-01-10'",
                "TA > date '2024-01-05' and TA < date '2024-01-10'");
        assertRewrite("TA > date '2024-01-05' or TA < date '2024-01-05'",
                "TA > date '2024-01-05' or TA < date '2024-01-05'");
        assertRewrite("TA > date '2024-01-01' or TA < date '2024-01-10'", "cast(TA as date) IS NOT NULL");
        assertRewriteNotNull("TA > date '2024-01-01' or TA < date '2024-01-10'", "TRUE");
        assertRewrite("TA > date '2024-01-05' and TA < date '2024-01-10'",
                "TA > date '2024-01-05' and TA < date '2024-01-10'");
        assertRewrite("TA > date '2024-01-05' and TA > date '2024-01-10'", "TA > date '2024-01-10'");
        assertRewrite("(TA > date '2024-01-01' and TA > date '2024-01-10') or TA > date '2024-01-20'",
                "TA > date '2024-01-10'");
        assertRewrite("(TA > date '2024-01-01' or TA > date '2024-01-10') and TA > date '2024-01-20'",
                "TA > date '2024-01-20'");
        assertRewrite("TA > date '2024-01-05' or TA > date '2024-01-05'", "TA > date '2024-01-05'");
        assertRewrite(
                "(TA > date '2024-01-10' or TA > date '2024-01-20') and (TB > date '2024-01-10' and TB < date '2024-01-20')",
                "TA > date '2024-01-10' and (TB > date '2024-01-10' and TB < date '2024-01-20') ");
        assertRewrite("TA in (date '2024-01-01',date '2024-01-02',date '2024-01-03') and TA > date '2024-01-10'",
                "FALSE");
        assertRewrite("TA in (date '2024-01-01',date '2024-01-02',date '2024-01-03') and TA >= date '2024-01-01'",
                "TA in (date '2024-01-01',date '2024-01-02',date '2024-01-03')");
        assertRewrite("TA in (date '2024-01-01',date '2024-01-02',date '2024-01-03') and TA > date '2024-01-01'",
                "TA IN (date '2024-01-02', date '2024-01-03')");
        assertRewrite("TA in (date '2024-01-01',date '2024-01-02',date '2024-01-03') or TA >= date '2024-01-01'",
                "TA >= date '2024-01-01'");
        assertRewrite("TA in (date '2024-01-01')", "TA in (date '2024-01-01')");
        assertRewrite("TA in (date '2024-01-01',date '2024-01-02',date '2024-01-03') and TA < date '2024-01-10'",
                "TA in (date '2024-01-01',date '2024-01-02',date '2024-01-03')");
        assertRewrite("TA in (date '2024-01-01',date '2024-01-02',date '2024-01-03') and TA < date '2024-01-01'",
                "FALSE");
        assertRewrite("TA in (date '2024-01-01',date '2024-01-02',date '2024-01-03') or TA < date '2024-01-01'",
                "TA in (date '2024-01-01',date '2024-01-02',date '2024-01-03') or TA < date '2024-01-01'");
        assertRewrite("TA in (date '2024-01-01',date '2024-01-02') or TA in (date '2024-01-02', date '2024-01-03')",
                "TA in (date '2024-01-01',date '2024-01-02',date '2024-01-03')");
        assertRewrite("TA in (date '2024-01-01',date '2024-01-02') and TA in (date '2024-01-03', date '2024-01-04')",
                "FALSE");
        assertRewrite("TA = date '2024-01-03' and TA = date '2024-01-01'", "FALSE");
        assertRewrite("TA in (date '2024-01-01') and TA in (date '2024-01-03')", "FALSE");
        assertRewrite("TA in (date '2024-01-03') and TA in (date '2024-01-03')", "TA = date '2024-01-03'");
        assertRewrite("(TA > date '2024-01-03' and TA < date '2024-01-01') and TB < date '2024-01-05'", "FALSE");
        assertRewrite("(TA > date '2024-01-03' and TA < date '2024-01-01') or TB < date '2024-01-05'",
                "TB < date '2024-01-05'");
    }

    @Test
    public void testSimplifyDateTime() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
            bottomUp(SimplifyRange.INSTANCE)
        ));
        assertRewrite("TA", "TA");
        assertRewrite(
                "(TA >= timestamp '2024-01-01 00:00:00' and TA <= timestamp '2024-01-03 00:00:00') or (TA > timestamp '2024-01-05 00:00:00' and TA < timestamp '2024-01-07 00:00:00')",
                "(TA >= timestamp '2024-01-01 00:00:00' and TA <= timestamp '2024-01-03 00:00:00') or (TA > timestamp '2024-01-05 00:00:00' and TA < timestamp '2024-01-07 00:00:00')");
        assertRewrite(
                "(TA > timestamp '2024-01-03 00:00:10' and TA < timestamp '2024-01-01 00:00:10') or (TA > timestamp '2024-01-07 00:00:10'and TA < timestamp '2024-01-05 00:00:10')",
                "FALSE");
        assertRewrite("TA > timestamp '2024-01-03 00:00:10' and TA < timestamp '2024-01-01 01:00:00'", "FALSE");
        assertRewrite("TA >= timestamp '2024-01-01 00:00:10' and TA < timestamp '2024-01-01 00:00:10'",
                "TA >= timestamp '2024-01-01 00:00:10' and TA < timestamp '2024-01-01 00:00:10'");
        assertRewrite("TA = timestamp '2024-01-01 10:00:10' and TA > timestamp '2024-01-10 00:00:10'", "FALSE");
        assertRewrite("TA > timestamp '2024-01-05 00:00:10' or TA < timestamp '2024-01-01 00:00:10'",
                "TA > timestamp '2024-01-05 00:00:10' or TA < timestamp '2024-01-01 00:00:10'");
        assertRewrite("TA > timestamp '2024-01-05 00:00:10' or TA > timestamp '2024-01-01 00:00:10' or TA > timestamp '2024-01-10 00:00:10'",
                "TA > timestamp '2024-01-01 00:00:10'");
        assertRewrite("TA > timestamp '2024-01-05 00:00:10' or TA > timestamp '2024-01-01 00:00:10' or TA < timestamp '2024-01-10 00:00:10'", "cast(TA as datetime) IS NOT NULL");
        assertRewriteNotNull("TA > timestamp '2024-01-05 00:00:10' or TA > timestamp '2024-01-01 00:00:10' or TA < timestamp '2024-01-10 00:00:10'", "TRUE");
        assertRewrite("TA > timestamp '2024-01-05 00:00:10' and TA > timestamp '2024-01-01 00:00:10' and TA > timestamp '2024-01-10 00:00:15'",
                "TA > timestamp '2024-01-10 00:00:15'");
        assertRewrite("TA > timestamp '2024-01-05 00:00:10' and TA > timestamp '2024-01-01 00:00:10' and TA < timestamp '2024-01-10 00:00:10'",
                "TA > timestamp '2024-01-05 00:00:10' and TA < timestamp '2024-01-10 00:00:10'");
        assertRewrite("TA > timestamp '2024-01-05 00:00:10' or TA < timestamp '2024-01-05 00:00:10'",
                "TA > timestamp '2024-01-05 00:00:10' or TA < timestamp '2024-01-05 00:00:10'");
        assertRewrite("TA > timestamp '2024-01-01 00:02:10' or TA < timestamp '2024-01-10 00:02:10'", "cast(TA as datetime) IS NOT NULL");
        assertRewriteNotNull("TA > timestamp '2024-01-01 00:00:00' or TA < timestamp '2024-01-10 00:00:00'", "TRUE");
        assertRewrite("TA > timestamp '2024-01-05 01:00:00' and TA < timestamp '2024-01-10 01:00:00'",
                "TA > timestamp '2024-01-05 01:00:00' and TA < timestamp '2024-01-10 01:00:00'");
        assertRewrite("TA > timestamp '2024-01-05 01:00:00' and TA > timestamp '2024-01-10 01:00:00'", "TA > timestamp '2024-01-10 01:00:00'");
        assertRewrite("(TA > timestamp '2024-01-01 01:00:00' and TA > timestamp '2024-01-10 01:00:00') or TA > timestamp '2024-01-20 01:00:00'",
                "TA > timestamp '2024-01-10 01:00:00'");
        assertRewrite("(TA > timestamp '2024-01-01 01:00:00' or TA > timestamp '2024-01-10 01:00:00') and TA > timestamp '2024-01-20 01:00:00'",
                "TA > timestamp '2024-01-20 01:00:00'");
        assertRewrite("TA > timestamp '2024-01-05 01:00:00' or TA > timestamp '2024-01-05 01:00:00'", "TA > timestamp '2024-01-05 01:00:00'");
        assertRewrite(
                "(TA > timestamp '2024-01-10 01:00:00' or TA > timestamp '2024-01-20 01:00:00') and (TB > timestamp '2024-01-10 01:00:00' and TB < timestamp '2024-01-20 01:00:00')",
                "TA > timestamp '2024-01-10 01:00:00' and (TB > timestamp '2024-01-10 01:00:00' and TB < timestamp '2024-01-20 01:00:00') ");
        assertRewrite("TA in (timestamp '2024-01-01 01:00:00',timestamp '2024-01-02 02:00:00',timestamp '2024-01-03 03:00:00') and TA > timestamp '2024-01-10 01:00:00'",
                "FALSE");
        assertRewrite("TA in (timestamp '2024-01-01 01:00:00',timestamp '2024-01-02 01:50:00',timestamp '2024-01-03 02:00:00') and TA >= timestamp '2024-01-01'",
                "TA in (timestamp '2024-01-01 01:00:00',timestamp '2024-01-02 01:50:00',timestamp '2024-01-03 02:00:00')");
        assertRewrite("TA in (timestamp '2024-01-01 02:00:00',timestamp '2024-01-02 02:00:00',timestamp '2024-01-03 02:00:00') and TA > timestamp '2024-01-01 02:10:00'",
                "TA IN (timestamp '2024-01-02 02:00:00', timestamp '2024-01-03 02:00:00')");
        assertRewrite("TA in (timestamp '2024-01-01 02:00:00',timestamp '2024-01-02 02:00:00',timestamp '2024-01-03 02:00:00') or TA >= timestamp '2024-01-01 01:00:00'",
                "TA >= timestamp '2024-01-01 01:00:00'");
        assertRewrite("TA in (timestamp '2024-01-01 02:00:00')", "TA in (timestamp '2024-01-01 02:00:00')");
        assertRewrite("TA in (timestamp '2024-01-01 02:00:00',timestamp '2024-01-02 02:00:00',timestamp '2024-01-03 02:00:00') and TA < timestamp '2024-01-10 02:00:00'",
                "TA in (timestamp '2024-01-01 02:00:00',timestamp '2024-01-02 02:00:00',timestamp '2024-01-03 02:00:00')");
        assertRewrite("TA in (timestamp '2024-01-01 02:00:00',timestamp '2024-01-02 02:00:00',timestamp '2024-01-03 02:00:00') and TA < timestamp '2024-01-01 02:00:00'",
                "FALSE");
        assertRewrite("TA in (timestamp '2024-01-01 02:00:00',timestamp '2024-01-02 02:00:00',timestamp '2024-01-03 02:00:00') and TA < timestamp '2024-01-01 02:00:01'",
                "TA = timestamp '2024-01-01 02:00:00'");
        assertRewrite("TA in (timestamp '2024-01-01 02:00:00',timestamp '2024-01-02 02:00:00',timestamp '2024-01-03 02:00:00') or TA < timestamp '2024-01-01 01:00:00'",
                "TA in (timestamp '2024-01-01 02:00:00',timestamp '2024-01-02 02:00:00',timestamp '2024-01-03 02:00:00') or TA < timestamp '2024-01-01 01:00:00'");
        assertRewrite("TA in (timestamp '2024-01-01 00:00:00',timestamp '2024-01-02 00:00:00') or TA in (timestamp '2024-01-02 00:00:00', timestamp '2024-01-03 00:00:00')",
                "TA in (timestamp '2024-01-01 00:00:00',timestamp '2024-01-02 00:00:00',timestamp '2024-01-03 00:00:00')");
        assertRewrite("TA in (timestamp '2024-01-01 00:50:00',timestamp '2024-01-02 00:50:00') and TA in (timestamp '2024-01-03 00:50:00', timestamp '2024-01-04 00:50:00')",
                "FALSE");
        assertRewrite("TA = timestamp '2024-01-03 00:50:00' and TA = timestamp '2024-01-01 00:50:00'", "FALSE");
        assertRewrite("TA in (timestamp '2024-01-01 00:50:00') and TA in (timestamp '2024-01-03 00:50:00')", "FALSE");
        assertRewrite("TA in (timestamp '2024-01-03 00:50:00') and TA in (timestamp '2024-01-03 00:50:00')", "TA = timestamp '2024-01-03 00:50:00'");
        assertRewrite("(TA > timestamp '2024-01-03 00:50:00' and TA < timestamp '2024-01-01 00:50:00') and TB < timestamp '2024-01-05 00:50:00'", "FALSE");
        assertRewrite("(TA > timestamp '2024-01-03 00:50:00' and TA < timestamp '2024-01-01 00:50:00') or TB < timestamp '2024-01-05 00:50:00'",
                "TB < timestamp '2024-01-05 00:50:00'");
    }

    private void assertRewrite(String expression, String expected) {
        Map<String, Slot> mem = Maps.newHashMap();
        Expression needRewriteExpression = replaceUnboundSlot(PARSER.parseExpression(expression), mem);
        needRewriteExpression = typeCoercion(needRewriteExpression);
        Expression expectedExpression = replaceUnboundSlot(PARSER.parseExpression(expected), mem);
        expectedExpression = typeCoercion(expectedExpression);
        Expression rewrittenExpression = executor.rewrite(needRewriteExpression, context);
        Assertions.assertEquals(expectedExpression, rewrittenExpression);
    }

    private void assertRewriteNotNull(String expression, String expected) {
        Map<String, Slot> mem = Maps.newHashMap();
        Expression needRewriteExpression = replaceNotNullUnboundSlot(PARSER.parseExpression(expression), mem);
        Expression expectedExpression = replaceNotNullUnboundSlot(PARSER.parseExpression(expected), mem);
        Expression rewrittenExpression = executor.rewrite(needRewriteExpression, context);
        Assertions.assertEquals(expectedExpression, rewrittenExpression);
    }

    private Expression replaceUnboundSlot(Expression expression, Map<String, Slot> mem) {
        List<Expression> children = Lists.newArrayList();
        boolean hasNewChildren = false;
        for (Expression child : expression.children()) {
            Expression newChild = replaceUnboundSlot(child, mem);
            if (newChild != child) {
                hasNewChildren = true;
            }
            children.add(newChild);
        }
        if (expression instanceof UnboundSlot) {
            String name = ((UnboundSlot) expression).getName();
            mem.putIfAbsent(name, new SlotReference(name, getType(name.charAt(0))));
            return mem.get(name);
        }
        return hasNewChildren ? expression.withChildren(children) : expression;
    }

    private Expression replaceNotNullUnboundSlot(Expression expression, Map<String, Slot> mem) {
        List<Expression> children = Lists.newArrayList();
        boolean hasNewChildren = false;
        for (Expression child : expression.children()) {
            Expression newChild = replaceNotNullUnboundSlot(child, mem);
            if (newChild != child) {
                hasNewChildren = true;
            }
            children.add(newChild);
        }
        if (expression instanceof UnboundSlot) {
            String name = ((UnboundSlot) expression).getName();
            mem.putIfAbsent(name, new SlotReference(name, getType(name.charAt(0)), false));
            return mem.get(name);
        }
        return hasNewChildren ? expression.withChildren(children) : expression;
    }

    protected Expression typeCoercion(Expression expression) {
        return ExpressionAnalyzer.FUNCTION_ANALYZER_RULE.rewrite(expression, null);
    }

    private DataType getType(char t) {
        switch (t) {
            case 'T':
                return TinyIntType.INSTANCE;
            case 'I':
                return IntegerType.INSTANCE;
            case 'D':
                return DoubleType.INSTANCE;
            case 'S':
                return StringType.INSTANCE;
            case 'B':
                return BooleanType.INSTANCE;
            default:
                return BigIntType.INSTANCE;
        }
    }
}
