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
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DateV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
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
        assertRewrite("TA > 3 or TA > null", "TA > 3 OR NULL");
        assertRewrite("TA > 3 or TA < null", "TA > 3 OR NULL");
        assertRewrite("TA > 3 or TA = null", "TA > 3 OR NULL");
        assertRewrite("TA > 3 or TA <> null", "TA > 3 or null");
        assertRewrite("TA > 3 or TA <=> null", "TA > 3 or TA <=> null");
        assertRewriteNotNull("TA > 3 and TA > null", "TA > 3 and NULL");
        assertRewriteNotNull("TA > 3 and TA < null", "TA > 3 and NULL");
        assertRewriteNotNull("TA > 3 and TA = null", "TA > 3 and NULL");
        assertRewrite("(TA > 3 and TA > null) is null", "(TA > 3 and null) is null");
        assertRewrite("TA > 3 and TA > null", "TA > 3 and null");
        assertRewrite("TA > 3 and TA < null", "TA > 3 and null");
        assertRewrite("TA > 3 and TA = null", "TA > 3 and null");
        assertRewrite("TA > 3 and TA <> null", "TA > 3 and null");
        assertRewrite("TA > 3 and TA <=> null", "TA > 3 and TA <=> null");
        assertRewrite("(TA >= 1 and TA <=3 ) or (TA > 5 and TA < 7)", "(TA >= 1 and TA <=3 ) or (TA > 5 and TA < 7)");
        assertRewriteNotNull("(TA > 3 and TA < 1) or (TA > 7 and TA < 5)", "FALSE");
        assertRewrite("(TA > 3 and TA < 1) or (TA > 7 and TA < 5)", "TA is null and null");
        assertRewriteNotNull("TA > 3 and TA < 1", "FALSE");
        assertRewrite("TA > 3 and TA < 1", "TA is null and null");
        assertRewrite("TA >= 3 and TA < 3", "TA >= 3 and TA < 3");
        assertRewriteNotNull("TA = 1 and TA > 10", "FALSE");
        assertRewrite("TA = 1 and TA > 10", "TA is null and null");
        assertRewrite("TA > 5 or TA < 1", "TA > 5 or TA < 1");
        assertRewrite("TA > 5 or TA > 1 or TA > 10", "TA > 1");
        assertRewrite("TA > 5 or TA > 1 or TA < 10", "TA is not null or null");
        assertRewriteNotNull("TA > 5 or TA > 1 or TA < 10", "TRUE");
        assertRewrite("TA > 5 and TA > 1 and TA > 10", "TA > 10");
        assertRewrite("TA > 5 and TA > 1 and TA < 10", "TA > 5 and TA < 10");
        assertRewrite("TA > 1 or TA < 1", "TA > 1 or TA < 1");
        assertRewrite("TA > 1 or TA < 10", "TA is not null or null");
        assertRewriteNotNull("TA > 1 or TA < 10", "TRUE");
        assertRewrite("TA > 5 and TA < 10", "TA > 5 and TA < 10");
        assertRewrite("TA > 5 and TA > 10", "TA > 10");
        assertRewrite("TA > 5 + 1 and TA > 10", "cast(TA as smallint) > 6 and TA > 10");
        assertRewrite("(TA > 1 and TA > 10) or TA > 20", "TA > 10");
        assertRewrite("(TA > 1 or TA > 10) and TA > 20", "TA > 20");
        assertRewrite("(TA < 1 and TA > 10) or TA = 20 and TB > 10", "(TA is null and null) or TA = 20 and TB > 10");
        assertRewrite("(TA + TB > 1 or TA + TB > 10) and TA + TB > 20", "TA + TB > 20");
        assertRewrite("TA > 10 or TA > 10", "TA > 10");
        assertRewrite("(TA > 10 or TA > 20) and (TB > 10 and TB < 20)", "TA > 10 and (TB > 10 and TB < 20) ");
        assertRewrite("(TA > 10 or TA > 20) and (TB > 10 and TB > 20)", "TA > 10 and TB > 20");
        assertRewrite("((TB > 30 and TA > 40) and TA > 20) and (TB > 10 and TB > 20)", "TB > 30 and TA > 40");
        assertRewrite("(TA > 10 and TB > 10) or (TB > 10 and TB > 20)", "TA > 10 and TB > 10 or TB > 20");
        assertRewrite("((TA > 10 or TA > 5) and TB > 10) or (TB > 10 and (TB > 20 or TB < 10))", "(TA > 5 and TB > 10) or (TB > 10 and (TB > 20 or TB < 10))");
        assertRewriteNotNull("TA in (1,2,3) and TA > 10", "FALSE");
        assertRewrite("TA in (1,2,3) and TA > 10", "TA is null and null");
        assertRewrite("TA in (1,2,3) and TA >= 1", "TA in (1,2,3)");
        assertRewrite("TA in (1,2,3) and TA > 1", "TA IN (2, 3)");
        assertRewrite("TA in (1,2,3) or TA >= 1", "TA >= 1");
        assertRewrite("TA in (1)", "TA in (1)");
        assertRewrite("TA in (1,2,3) and TA < 10", "TA in (1,2,3)");
        assertRewriteNotNull("TA in (1,2,3) and TA < 1", "FALSE");
        assertRewrite("TA in (1,2,3) and TA < 1", "TA is null and null");
        assertRewrite("TA in (1,2,3) or TA < 1", "TA in (1,2,3) or TA < 1");
        assertRewrite("TA in (1,2,3) or TA in (2,3,4)", "TA in (1,2,3,4)");
        assertRewrite("TA in (1,2,3) or TA in (4,5,6)", "TA in (1,2,3,4,5,6)");
        assertRewrite("TA in (1,2,3) and TA in (4,5,6)", "TA is null and null");
        assertRewriteNotNull("TA in (1,2,3) and TA in (4,5,6)", "FALSE");
        assertRewrite("TA in (1,2,3) and TA in (3,4,5)", "TA = 3");
        assertRewrite("TA + TB in (1,2,3) and TA + TB in (3,4,5)", "TA + TB = 3");
        assertRewrite("TA in (1,2,3) and DA > 1.5", "TA in (1,2,3) and DA > 1.5");
        assertRewriteNotNull("TA = 1 and TA = 3", "FALSE");
        assertRewrite("TA = 1 and TA = 3", "TA is null and null");
        assertRewriteNotNull("TA in (1) and TA in (3)", "FALSE");
        assertRewrite("TA in (1) and TA in (3)", "TA is null and null");
        assertRewrite("TA in (1) and TA in (1)", "TA = 1");
        assertRewriteNotNull("(TA > 3 and TA < 1) and TB < 5", "FALSE");
        assertRewrite("(TA > 3 and TA < 1) and (TA > 5 and TA = 4)", "TA is null and null");
        assertRewrite("(TA > 3 and TA < 1) or (TA > 5 and TA = 4)", "TA is null and null");
        assertRewrite("(TA > 3 and TA < 1) and TB < 5", "TA is null and null and TB < 5");
        assertRewrite("(TA > 3 and TA < 1) and (TB < 5 and TB = 6)", "TA is null and null and TB is null");
        assertRewrite("TA > 3 and TB < 5 and TA < 1", "TA is null and null and TB < 5");
        assertRewrite("(TA > 3 and TA < 1) or TB < 5", "(TA is null and null) or TB < 5");
        assertRewrite("((IA = 1 AND SC ='1') OR SC = '1212') AND IA =1", "((IA = 1 AND SC ='1') OR SC = '1212') AND IA =1");

        assertRewrite("TA + TC", "TA + TC");
        assertRewrite("(TA + TC >= 1 and TA + TC <=3 ) or (TA + TC > 5 and TA + TC < 7)", "(TA + TC >= 1 and TA + TC <=3 ) or (TA + TC > 5 and TA + TC < 7)");
        assertRewriteNotNull("(TA + TC > 3 and TA + TC < 1) or (TA + TC > 7 and TA + TC < 5)", "FALSE");
        assertRewrite("(TA + TC > 3 and TA + TC < 1) or (TA + TC > 7 and TA + TC < 5)", "(TA + TC) is null and null");
        assertRewriteNotNull("TA + TC > 3 and TA + TC < 1", "FALSE");
        assertRewrite("TA + TC > 3 and TA + TC < 1", "(TA + TC) is null and null");
        assertRewrite("TA + TC >= 3 and TA + TC < 3", "TA + TC >= 3 and TA + TC < 3");
        assertRewriteNotNull("TA + TC = 1 and TA + TC > 10", "FALSE");
        assertRewrite("TA + TC = 1 and TA + TC > 10", "(TA + TC) is null and null");
        assertRewrite("TA + TC > 5 or TA + TC < 1", "TA + TC > 5 or TA + TC < 1");
        assertRewrite("TA + TC > 5 or TA + TC > 1 or TA + TC > 10", "TA + TC > 1");
        assertRewrite("TA + TC > 5 or TA + TC > 1 or TA + TC < 10", "(TA + TC) is not null or null");
        assertRewrite("TA + TC > 5 and TA + TC > 1 and TA + TC > 10", "TA + TC > 10");
        assertRewrite("TA + TC > 5 and TA + TC > 1 and TA + TC < 10", "TA + TC > 5 and TA + TC < 10");
        assertRewrite("TA + TC > 1 or TA + TC < 1", "TA + TC > 1 or TA + TC < 1");
        assertRewrite("TA + TC > 1 or TA + TC < 10", "(TA + TC) is not null or null");
        assertRewrite("TA + TC > 5 and TA + TC < 10", "TA + TC > 5 and TA + TC < 10");
        assertRewrite("TA + TC > 5 and TA + TC > 10", "TA + TC > 10");
        assertRewrite("TA + TC > 5 + 1 and TA + TC > 10", "TA + TC > 10");
        assertRewrite("(TA + TC > 1 and TA + TC > 10) or TA + TC > 20", "TA + TC > 10");
        assertRewrite("(TA + TC > 1 or TA + TC > 10) and TA + TC > 20", "TA + TC > 20");
        assertRewrite("(TA + TC + TB > 1 or TA + TC + TB > 10) and TA + TC + TB > 20", "TA + TC + TB > 20");
        assertRewrite("TA + TC > 10 or TA + TC > 10", "TA + TC > 10");
        assertRewrite("(TA + TC > 10 or TA + TC > 20) and (TB > 10 and TB < 20)", "TA + TC > 10 and (TB > 10 and TB < 20) ");
        assertRewrite("(TA + TC > 10 or TA + TC > 20) and (TB > 10 and TB > 20)", "TA + TC > 10 and TB > 20");
        assertRewrite("((TB > 30 and TA + TC > 40) and TA + TC > 20) and (TB > 10 and TB > 20)", "TB > 30 and TA + TC > 40");
        assertRewrite("(TA + TC > 10 and TB > 10) or (TB > 10 and TB > 20)", "TA + TC > 10 and TB > 10 or TB > 20");
        assertRewrite("((TA + TC > 10 or TA + TC > 5) and TB > 10) or (TB > 10 and (TB > 20 or TB < 10))", "(TA + TC > 5 and TB > 10) or (TB > 10 and (TB > 20 or TB < 10))");
        assertRewriteNotNull("TA + TC in (1,2,3) and TA + TC > 10", "FALSE");
        assertRewrite("TA + TC in (1,2,3) and TA + TC > 10", "(TA + TC) is null and null");
        assertRewrite("TA + TC in (1,2,3) and TA + TC >= 1", "TA + TC in (1,2,3)");
        assertRewrite("TA + TC in (1,2,3) and TA + TC > 1", "(TA + TC) IN (2, 3)");
        assertRewrite("TA + TC in (1,2,3) or TA + TC >= 1", "TA + TC >= 1");
        assertRewrite("TA + TC in (1)", "TA + TC in (1)");
        assertRewrite("TA + TC in (1,2,3) and TA + TC < 10", "TA + TC in (1,2,3)");
        assertRewriteNotNull("TA + TC in (1,2,3) and TA + TC < 1", "FALSE");
        assertRewrite("TA + TC in (1,2,3) and TA + TC < 1", "(TA + TC) is null and null");
        assertRewrite("TA + TC in (1,2,3) or TA + TC < 1", "TA + TC in (1,2,3) or TA + TC < 1");
        assertRewrite("TA + TC in (1,2,3) or TA + TC in (2,3,4)", "TA + TC in (1,2,3,4)");
        assertRewrite("TA + TC in (1,2,3) or TA + TC in (4,5,6)", "TA + TC in (1,2,3,4,5,6)");
        assertRewriteNotNull("TA + TC in (1,2,3) and TA + TC in (4,5,6)", "FALSE");
        assertRewrite("TA + TC in (1,2,3) and TA + TC in (4,5,6)", "(TA + TC) is null and null");
        assertRewrite("TA + TC in (1,2,3) and TA + TC in (3,4,5)", "TA + TC = 3");
        assertRewrite("TA + TC + TB in (1,2,3) and TA + TC + TB in (3,4,5)", "TA + TC + TB = 3");
        assertRewrite("TA + TC in (1,2,3) and DA > 1.5", "TA + TC in (1,2,3) and DA > 1.5");
        assertRewriteNotNull("TA + TC = 1 and TA + TC = 3", "FALSE");
        assertRewrite("TA + TC = 1 and TA + TC = 3", "(TA + TC) is null and null");
        assertRewriteNotNull("TA + TC in (1) and TA + TC in (3)", "FALSE");
        assertRewrite("TA + TC in (1) and TA + TC in (3)", "(TA + TC) is null and null");
        assertRewrite("TA + TC in (1) and TA + TC in (1)", "TA + TC = 1");
        assertRewriteNotNull("(TA + TC > 3 and TA + TC < 1) and TB < 5", "FALSE");
        assertRewrite("(TA + TC > 3 and TA + TC < 1) and TB < 5", "(TA + TC) is null and null and TB < 5");
        assertRewrite("(TA + TC > 3 and TA + TC < 1) or TB < 5", "((TA + TC) is null and null) OR TB < 5");

        assertRewrite("(TA + TC > 3 OR TA < 1) AND TB = 2) AND IA =1", "(TA + TC > 3 OR TA < 1) AND TB = 2) AND IA =1");

    }

    @Test
    public void testSimplifyDate() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
            bottomUp(SimplifyRange.INSTANCE)
        ));
        assertRewrite("AA", "AA");
        assertRewrite(
                "(AA >= date '2024-01-01' and AA <= date '2024-01-03') or (AA > date '2024-01-05' and AA < date '2024-01-07')",
                "(AA >= date '2024-01-01' and AA <= date '2024-01-03') or (AA > date '2024-01-05' and AA < date '2024-01-07')");
        assertRewriteNotNull(
                "(AA > date '2024-01-03' and AA < date '2024-01-01') or (AA > date '2024-01-07'and AA < date '2024-01-05')",
                "false");
        assertRewrite(
                "(AA > date '2024-01-03' and AA < date '2024-01-01') or (AA > date '2024-01-07'and AA < date '2024-01-05')",
                "AA is null and null");
        assertRewriteNotNull("AA > date '2024-01-03' and AA < date '2024-01-01'", "FALSE");
        assertRewrite("AA > date '2024-01-03' and AA < date '2024-01-01'", "AA is null and null");
        assertRewrite("AA >= date '2024-01-01' and AA < date '2024-01-01'",
                "AA >= date '2024-01-01' and AA < date '2024-01-01'");
        assertRewriteNotNull("AA = date '2024-01-01' and AA > date '2024-01-10'", "FALSE");
        assertRewrite("AA = date '2024-01-01' and AA > date '2024-01-10'", "AA is null and null");
        assertRewrite("AA > date '2024-01-05' or AA < date '2024-01-01'",
                "AA > date '2024-01-05' or AA < date '2024-01-01'");
        assertRewrite("AA > date '2024-01-05' or AA > date '2024-01-01' or AA > date '2024-01-10'",
                "AA > date '2024-01-01'");
        assertRewrite("AA > date '2024-01-05' or AA > date '2024-01-01' or AA < date '2024-01-10'", "AA is not null or null");
        assertRewriteNotNull("AA > date '2024-01-05' or AA > date '2024-01-01' or AA < date '2024-01-10'", "TRUE");
        assertRewrite("AA > date '2024-01-05' and AA > date '2024-01-01' and AA > date '2024-01-10'",
                "AA > date '2024-01-10'");
        assertRewrite("AA > date '2024-01-05' and AA > date '2024-01-01' and AA < date '2024-01-10'",
                "AA > date '2024-01-05' and AA < date '2024-01-10'");
        assertRewrite("AA > date '2024-01-05' or AA < date '2024-01-05'",
                "AA > date '2024-01-05' or AA < date '2024-01-05'");
        assertRewrite("AA > date '2024-01-01' or AA < date '2024-01-10'", "AA is not null or null");
        assertRewriteNotNull("AA > date '2024-01-01' or AA < date '2024-01-10'", "TRUE");
        assertRewrite("AA > date '2024-01-05' and AA < date '2024-01-10'",
                "AA > date '2024-01-05' and AA < date '2024-01-10'");
        assertRewrite("AA > date '2024-01-05' and AA > date '2024-01-10'", "AA > date '2024-01-10'");
        assertRewrite("(AA > date '2024-01-01' and AA > date '2024-01-10') or AA > date '2024-01-20'",
                "AA > date '2024-01-10'");
        assertRewrite("(AA > date '2024-01-01' or AA > date '2024-01-10') and AA > date '2024-01-20'",
                "AA > date '2024-01-20'");
        assertRewrite("AA > date '2024-01-05' or AA > date '2024-01-05'", "AA > date '2024-01-05'");
        assertRewrite(
                "(AA > date '2024-01-10' or AA > date '2024-01-20') and (AB > date '2024-01-10' and AB < date '2024-01-20')",
                "AA > date '2024-01-10' and (AB > date '2024-01-10' and AB < date '2024-01-20') ");
        assertRewriteNotNull("AA in (date '2024-01-01',date '2024-01-02',date '2024-01-03') and AA > date '2024-01-10'", "FALSE");
        assertRewrite("AA in (date '2024-01-01',date '2024-01-02',date '2024-01-03') and AA > date '2024-01-10'", "AA is null and null");
        assertRewrite("AA in (date '2024-01-01',date '2024-01-02',date '2024-01-03') and AA >= date '2024-01-01'",
                "AA in (date '2024-01-01',date '2024-01-02',date '2024-01-03')");
        assertRewrite("AA in (date '2024-01-01',date '2024-01-02',date '2024-01-03') and AA > date '2024-01-01'",
                "AA IN (date '2024-01-02', date '2024-01-03')");
        assertRewrite("AA in (date '2024-01-01',date '2024-01-02',date '2024-01-03') or AA >= date '2024-01-01'",
                "AA >= date '2024-01-01'");
        assertRewrite("AA in (date '2024-01-01')", "AA in (date '2024-01-01')");
        assertRewrite("AA in (date '2024-01-01',date '2024-01-02',date '2024-01-03') and AA < date '2024-01-10'",
                "AA in (date '2024-01-01',date '2024-01-02',date '2024-01-03')");
        assertRewriteNotNull("AA in (date '2024-01-01',date '2024-01-02',date '2024-01-03') and AA < date '2024-01-01'",
                "FALSE");
        assertRewrite("AA in (date '2024-01-01',date '2024-01-02',date '2024-01-03') and AA < date '2024-01-01'",
                "AA is null and null");
        assertRewrite("AA in (date '2024-01-01',date '2024-01-02',date '2024-01-03') or AA < date '2024-01-01'",
                "AA in (date '2024-01-01',date '2024-01-02',date '2024-01-03') or AA < date '2024-01-01'");
        assertRewrite("AA in (date '2024-01-01',date '2024-01-02') or AA in (date '2024-01-02', date '2024-01-03')",
                "AA in (date '2024-01-01',date '2024-01-02',date '2024-01-03')");
        assertRewriteNotNull("AA in (date '2024-01-01',date '2024-01-02') and AA in (date '2024-01-03', date '2024-01-04')",
                "FALSE");
        assertRewrite("AA in (date '2024-01-01',date '2024-01-02') and AA in (date '2024-01-03', date '2024-01-04')",
                "AA is null and null");
        assertRewriteNotNull("AA = date '2024-01-03' and AA = date '2024-01-01'", "FALSE");
        assertRewrite("AA = date '2024-01-03' and AA = date '2024-01-01'", "AA is null and null");
        assertRewriteNotNull("AA in (date '2024-01-01') and AA in (date '2024-01-03')", "FALSE");
        assertRewrite("AA in (date '2024-01-01') and AA in (date '2024-01-03')", "AA is null and null");
        assertRewrite("AA in (date '2024-01-03') and AA in (date '2024-01-03')", "AA = date '2024-01-03'");
        assertRewriteNotNull("(AA > date '2024-01-03' and AA < date '2024-01-01') and AB < date '2024-01-05'", "FALSE");
        assertRewrite("(AA > date '2024-01-03' and AA < date '2024-01-01') and AB < date '2024-01-05'", "AA is null and null and AB < date '2024-01-05'");
        assertRewrite("AA > date '2024-01-03' and AB < date '2024-01-05' and AA < date '2024-01-01'", "AA is null and null and AB < date '2024-01-05'");
        assertRewrite("(AA > date '2024-01-03' and AA < date '2024-01-01') or AB < date '2024-01-05'", "(AA is null and null) or AB < date '2024-01-05'");
    }

    @Test
    public void testSimplifyDateTime() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
            bottomUp(SimplifyRange.INSTANCE)
        ));
        assertRewrite("CA", "CA");
        assertRewrite(
                "(CA >= timestamp '2024-01-01 00:00:00' and CA <= timestamp '2024-01-03 00:00:00') or (CA > timestamp '2024-01-05 00:00:00' and CA < timestamp '2024-01-07 00:00:00')",
                "(CA >= timestamp '2024-01-01 00:00:00' and CA <= timestamp '2024-01-03 00:00:00') or (CA > timestamp '2024-01-05 00:00:00' and CA < timestamp '2024-01-07 00:00:00')");
        assertRewriteNotNull(
                "(CA > timestamp '2024-01-03 00:00:10' and CA < timestamp '2024-01-01 00:00:10') or (CA > timestamp '2024-01-07 00:00:10'and CA < timestamp '2024-01-05 00:00:10')",
                "FALSE");
        assertRewrite(
                "(CA > timestamp '2024-01-03 00:00:10' and CA < timestamp '2024-01-01 00:00:10') or (CA > timestamp '2024-01-07 00:00:10'and CA < timestamp '2024-01-05 00:00:10')",
                "CA is null and null");
        assertRewriteNotNull("CA > timestamp '2024-01-03 00:00:10' and CA < timestamp '2024-01-01 01:00:00'", "FALSE");
        assertRewrite("CA > timestamp '2024-01-03 00:00:10' and CA < timestamp '2024-01-01 01:00:00'", "CA is null and null");
        assertRewrite("CA >= timestamp '2024-01-01 00:00:10' and CA < timestamp '2024-01-01 00:00:10'",
                "CA >= timestamp '2024-01-01 00:00:10' and CA < timestamp '2024-01-01 00:00:10'");
        assertRewriteNotNull("CA = timestamp '2024-01-01 10:00:10' and CA > timestamp '2024-01-10 00:00:10'", "FALSE");
        assertRewrite("CA = timestamp '2024-01-01 10:00:10' and CA > timestamp '2024-01-10 00:00:10'", "CA is null and null");
        assertRewrite("CA > timestamp '2024-01-05 00:00:10' or CA < timestamp '2024-01-01 00:00:10'",
                "CA > timestamp '2024-01-05 00:00:10' or CA < timestamp '2024-01-01 00:00:10'");
        assertRewrite("CA > timestamp '2024-01-05 00:00:10' or CA > timestamp '2024-01-01 00:00:10' or CA > timestamp '2024-01-10 00:00:10'",
                "CA > timestamp '2024-01-01 00:00:10'");
        assertRewrite("CA > timestamp '2024-01-05 00:00:10' or CA > timestamp '2024-01-01 00:00:10' or CA < timestamp '2024-01-10 00:00:10'", "CA is not null or null");
        assertRewriteNotNull("CA > timestamp '2024-01-05 00:00:10' or CA > timestamp '2024-01-01 00:00:10' or CA < timestamp '2024-01-10 00:00:10'", "TRUE");
        assertRewrite("CA > timestamp '2024-01-05 00:00:10' and CA > timestamp '2024-01-01 00:00:10' and CA > timestamp '2024-01-10 00:00:15'",
                "CA > timestamp '2024-01-10 00:00:15'");
        assertRewrite("CA > timestamp '2024-01-05 00:00:10' and CA > timestamp '2024-01-01 00:00:10' and CA < timestamp '2024-01-10 00:00:10'",
                "CA > timestamp '2024-01-05 00:00:10' and CA < timestamp '2024-01-10 00:00:10'");
        assertRewrite("CA > timestamp '2024-01-05 00:00:10' or CA < timestamp '2024-01-05 00:00:10'",
                "CA > timestamp '2024-01-05 00:00:10' or CA < timestamp '2024-01-05 00:00:10'");
        assertRewrite("CA > timestamp '2024-01-01 00:02:10' or CA < timestamp '2024-01-10 00:02:10'", "CA is not null or null");
        assertRewriteNotNull("CA > timestamp '2024-01-01 00:00:00' or CA < timestamp '2024-01-10 00:00:00'", "TRUE");
        assertRewrite("CA > timestamp '2024-01-05 01:00:00' and CA < timestamp '2024-01-10 01:00:00'",
                "CA > timestamp '2024-01-05 01:00:00' and CA < timestamp '2024-01-10 01:00:00'");
        assertRewrite("CA > timestamp '2024-01-05 01:00:00' and CA > timestamp '2024-01-10 01:00:00'", "CA > timestamp '2024-01-10 01:00:00'");
        assertRewrite("(CA > timestamp '2024-01-01 01:00:00' and CA > timestamp '2024-01-10 01:00:00') or CA > timestamp '2024-01-20 01:00:00'",
                "CA > timestamp '2024-01-10 01:00:00'");
        assertRewrite("(CA > timestamp '2024-01-01 01:00:00' or CA > timestamp '2024-01-10 01:00:00') and CA > timestamp '2024-01-20 01:00:00'",
                "CA > timestamp '2024-01-20 01:00:00'");
        assertRewrite("CA > timestamp '2024-01-05 01:00:00' or CA > timestamp '2024-01-05 01:00:00'", "CA > timestamp '2024-01-05 01:00:00'");
        assertRewrite(
                "(CA > timestamp '2024-01-10 01:00:00' or CA > timestamp '2024-01-20 01:00:00') and (CB > timestamp '2024-01-10 01:00:00' and CB < timestamp '2024-01-20 01:00:00')",
                "CA > timestamp '2024-01-10 01:00:00' and (CB > timestamp '2024-01-10 01:00:00' and CB < timestamp '2024-01-20 01:00:00') ");
        assertRewriteNotNull("CA in (timestamp '2024-01-01 01:00:00',timestamp '2024-01-02 02:00:00',timestamp '2024-01-03 03:00:00') and CA > timestamp '2024-01-10 01:00:00'",
                "FALSE");
        assertRewrite("CA in (timestamp '2024-01-01 01:00:00',timestamp '2024-01-02 02:00:00',timestamp '2024-01-03 03:00:00') and CA > timestamp '2024-01-10 01:00:00'",
                "CA is null and null");
        assertRewrite("CA in (timestamp '2024-01-01 01:00:00',timestamp '2024-01-02 01:50:00',timestamp '2024-01-03 02:00:00') and CA >= timestamp '2024-01-01'",
                "CA in (timestamp '2024-01-01 01:00:00',timestamp '2024-01-02 01:50:00',timestamp '2024-01-03 02:00:00')");
        assertRewrite("CA in (timestamp '2024-01-01 02:00:00',timestamp '2024-01-02 02:00:00',timestamp '2024-01-03 02:00:00') and CA > timestamp '2024-01-01 02:10:00'",
                "CA IN (timestamp '2024-01-02 02:00:00', timestamp '2024-01-03 02:00:00')");
        assertRewrite("CA in (timestamp '2024-01-01 02:00:00',timestamp '2024-01-02 02:00:00',timestamp '2024-01-03 02:00:00') or CA >= timestamp '2024-01-01 01:00:00'",
                "CA >= timestamp '2024-01-01 01:00:00'");
        assertRewrite("CA in (timestamp '2024-01-01 02:00:00')", "CA in (timestamp '2024-01-01 02:00:00')");
        assertRewrite("CA in (timestamp '2024-01-01 02:00:00',timestamp '2024-01-02 02:00:00',timestamp '2024-01-03 02:00:00') and CA < timestamp '2024-01-10 02:00:00'",
                "CA in (timestamp '2024-01-01 02:00:00',timestamp '2024-01-02 02:00:00',timestamp '2024-01-03 02:00:00')");
        assertRewriteNotNull("CA in (timestamp '2024-01-01 02:00:00',timestamp '2024-01-02 02:00:00',timestamp '2024-01-03 02:00:00') and CA < timestamp '2024-01-01 02:00:00'",
                "FALSE");
        assertRewrite("CA in (timestamp '2024-01-01 02:00:00',timestamp '2024-01-02 02:00:00',timestamp '2024-01-03 02:00:00') and CA < timestamp '2024-01-01 02:00:00'",
                "CA is null and null");
        assertRewrite("CA in (timestamp '2024-01-01 02:00:00',timestamp '2024-01-02 02:00:00',timestamp '2024-01-03 02:00:00') and CA < timestamp '2024-01-01 02:00:01'",
                "CA = timestamp '2024-01-01 02:00:00'");
        assertRewrite("CA in (timestamp '2024-01-01 02:00:00',timestamp '2024-01-02 02:00:00',timestamp '2024-01-03 02:00:00') or CA < timestamp '2024-01-01 01:00:00'",
                "CA in (timestamp '2024-01-01 02:00:00',timestamp '2024-01-02 02:00:00',timestamp '2024-01-03 02:00:00') or CA < timestamp '2024-01-01 01:00:00'");
        assertRewrite("CA in (timestamp '2024-01-01 00:00:00',timestamp '2024-01-02 00:00:00') or CA in (timestamp '2024-01-02 00:00:00', timestamp '2024-01-03 00:00:00')",
                "CA in (timestamp '2024-01-01 00:00:00',timestamp '2024-01-02 00:00:00',timestamp '2024-01-03 00:00:00')");
        assertRewriteNotNull("CA in (timestamp '2024-01-01 00:50:00',timestamp '2024-01-02 00:50:00') and CA in (timestamp '2024-01-03 00:50:00', timestamp '2024-01-04 00:50:00')",
                "FALSE");
        assertRewrite("CA in (timestamp '2024-01-01 00:50:00',timestamp '2024-01-02 00:50:00') and CA in (timestamp '2024-01-03 00:50:00', timestamp '2024-01-04 00:50:00')",
                "CA is null and null");
        assertRewriteNotNull("CA = timestamp '2024-01-03 00:50:00' and CA = timestamp '2024-01-01 00:50:00'", "FALSE");
        assertRewrite("CA = timestamp '2024-01-03 00:50:00' and CA = timestamp '2024-01-01 00:50:00'", "CA is null and null");
        assertRewriteNotNull("CA in (timestamp '2024-01-01 00:50:00') and CA in (timestamp '2024-01-03 00:50:00')", "FALSE");
        assertRewrite("CA in (timestamp '2024-01-01 00:50:00') and CA in (timestamp '2024-01-03 00:50:00')", "CA is null and null");
        assertRewrite("CA in (timestamp '2024-01-03 00:50:00') and CA in (timestamp '2024-01-03 00:50:00')", "CA = timestamp '2024-01-03 00:50:00'");
        assertRewriteNotNull("(CA > timestamp '2024-01-03 00:50:00' and CA < timestamp '2024-01-01 00:50:00') and CB < timestamp '2024-01-05 00:50:00'", "FALSE");
        assertRewrite("(CA > timestamp '2024-01-03 00:50:00' and CA < timestamp '2024-01-01 00:50:00') and CB < timestamp '2024-01-05 00:50:00'", "CA is null and null and CB < timestamp '2024-01-05 00:50:00'");
        assertRewrite("CA > timestamp '2024-01-03 00:50:00' and CB < timestamp '2024-01-05 00:50:00' and CA < timestamp '2024-01-01 00:50:00'", "CA is null and null and CB < timestamp '2024-01-05 00:50:00'");
        assertRewrite("(CA > timestamp '2024-01-03 00:50:00' and CA < timestamp '2024-01-01 00:50:00') or CB < timestamp '2024-01-05 00:50:00'",
                "(CA is null and null) OR CB < timestamp '2024-01-05 00:50:00'");
    }

    private void assertRewrite(String expression, String expected) {
        Map<String, Slot> mem = Maps.newHashMap();
        Expression needRewriteExpression = replaceUnboundSlot(PARSER.parseExpression(expression), mem);
        needRewriteExpression = typeCoercion(needRewriteExpression);
        Expression expectedExpression = replaceUnboundSlot(PARSER.parseExpression(expected), mem);
        expectedExpression = typeCoercion(expectedExpression);
        Expression rewrittenExpression = sortChildren(executor.rewrite(needRewriteExpression, context));
        Assertions.assertEquals(expectedExpression, rewrittenExpression);
    }

    private void assertRewriteNotNull(String expression, String expected) {
        Map<String, Slot> mem = Maps.newHashMap();
        Expression needRewriteExpression = replaceNotNullUnboundSlot(PARSER.parseExpression(expression), mem);
        needRewriteExpression = typeCoercion(needRewriteExpression);
        Expression expectedExpression = replaceNotNullUnboundSlot(PARSER.parseExpression(expected), mem);
        expectedExpression = typeCoercion(expectedExpression);
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
                return DecimalV3Type.createDecimalV3Type(2, 1);
            case 'S':
                return StringType.INSTANCE;
            case 'B':
                return BooleanType.INSTANCE;
            case 'C':
                return DateTimeV2Type.SYSTEM_DEFAULT;
            case 'A':
                return DateV2Type.INSTANCE;
            default:
                return BigIntType.INSTANCE;
        }
    }

    private Expression sortChildren(Expression expression) {
        if (expression instanceof InPredicate) {
            return ((InPredicate) expression).sortOptions();
        }
        List<Expression> children = Lists.newArrayList();
        boolean hasNewChildren = false;
        for (Expression child : expression.children()) {
            Expression newChild = sortChildren(child);
            if (newChild != child) {
                hasNewChildren = true;
            }
            children.add(newChild);
        }
        return hasNewChildren ? expression.withChildren(children) : expression;
    }
}
