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
import org.apache.doris.nereids.rules.expression.rules.RangeInference;
import org.apache.doris.nereids.rules.expression.rules.RangeInference.CompoundValue;
import org.apache.doris.nereids.rules.expression.rules.RangeInference.EmptyValue;
import org.apache.doris.nereids.rules.expression.rules.RangeInference.IsNotNullValue;
import org.apache.doris.nereids.rules.expression.rules.RangeInference.IsNullValue;
import org.apache.doris.nereids.rules.expression.rules.RangeInference.NotDiscreteValue;
import org.apache.doris.nereids.rules.expression.rules.RangeInference.RangeValue;
import org.apache.doris.nereids.rules.expression.rules.RangeInference.UnknownValue;
import org.apache.doris.nereids.rules.expression.rules.RangeInference.ValueDesc;
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
    private final Map<String, Slot> commonMem;

    public SimplifyRangeTest() {
        CascadesContext cascadesContext = MemoTestUtils.createCascadesContext(
                new UnboundRelation(new RelationId(1), ImmutableList.of("tbl")));
        context = new ExpressionRewriteContext(cascadesContext);
        commonMem = Maps.newHashMap();
    }

    @Test
    public void testRangeInference() {
        ValueDesc valueDesc = getValueDesc("TA IS NULL");
        Assertions.assertInstanceOf(IsNullValue.class, valueDesc);
        Assertions.assertEquals("TA", valueDesc.getReference().toSql());

        valueDesc = getValueDesc("NULL");
        Assertions.assertInstanceOf(UnknownValue.class, valueDesc);
        Assertions.assertEquals("NULL", valueDesc.getReference().toSql());

        valueDesc = getValueDesc("TA IS NOT NULL");
        Assertions.assertInstanceOf(IsNotNullValue.class, valueDesc);
        Assertions.assertEquals("TA", valueDesc.getReference().toSql());

        valueDesc = getValueDesc("TA != 10");
        Assertions.assertInstanceOf(NotDiscreteValue.class, valueDesc);
        Assertions.assertEquals("TA", valueDesc.getReference().toSql());

        valueDesc = getValueDesc("TA IS NULL AND NULL");
        Assertions.assertInstanceOf(EmptyValue.class, valueDesc);
        Assertions.assertEquals("TA", valueDesc.getReference().toSql());

        valueDesc = getValueDesc("TA IS NOT NULL OR NULL");
        Assertions.assertInstanceOf(RangeValue.class, valueDesc);
        Assertions.assertEquals("TA", valueDesc.getReference().toSql());
        Assertions.assertTrue(((RangeValue) valueDesc).isRangeAll());

        valueDesc = getValueDesc("TA IS NULL AND TB IS NULL AND NULL");
        Assertions.assertInstanceOf(CompoundValue.class, valueDesc);
        List<ValueDesc> sourceValues = ((CompoundValue) valueDesc).getSourceValues();
        Assertions.assertEquals(2, sourceValues.size());
        Assertions.assertInstanceOf(EmptyValue.class, sourceValues.get(0));
        Assertions.assertInstanceOf(EmptyValue.class, sourceValues.get(1));
        Assertions.assertEquals("TA", sourceValues.get(0).getReference().toSql());
        Assertions.assertEquals("TB", sourceValues.get(1).getReference().toSql());

        valueDesc = getValueDesc("L + RANDOM(1, 10) > 8 AND L + RANDOM(1, 10) <  1");
        Assertions.assertInstanceOf(CompoundValue.class, valueDesc);
        sourceValues = ((CompoundValue) valueDesc).getSourceValues();
        Assertions.assertEquals(2, sourceValues.size());
        for (ValueDesc value : sourceValues) {
            Assertions.assertInstanceOf(RangeValue.class, value);
            Assertions.assertEquals("(L + random(1, 10))", value.getReference().toSql());
        }
    }

    @Test
    public void testValueDescContainsAll() {
        SlotReference xa = new SlotReference("xa", IntegerType.INSTANCE, false);

        checkContainsAll(true, "TA is null and null", "TA is null and null");
        checkContainsAll(false, "TA is null and null", "TA > 1");
        checkContainsAll(false, "TA is null and null", "TA = 1");
        checkContainsAll(false, "TA is null and null", "TA != 1");
        checkContainsAll(false, "TA is null and null", "TA is null");
        // XA is null and null will rewrite to 'FALSE'
        // checkContainsAll(true, "XA is null and null", "XA is null");
        Assertions.assertTrue(new EmptyValue(context, xa).containsAll(new IsNullValue(context, xa)));
        checkContainsAll(false, "TA is null and null", "TA = 1 or TA > 10");

        checkContainsAll(true, "TA > 1", "TA is null and null");
        checkContainsAll(true, "TA > 1", "TA > 10");
        checkContainsAll(false, "TA > 1", "TA > 0");
        checkContainsAll(true, "TA >= 1", "TA > 1");
        checkContainsAll(false, "TA > 1", "TA >= 1");
        checkContainsAll(true, "TA > 1", "TA > 1");
        checkContainsAll(true, "TA > 1", "TA > 1 and TA < 10");
        checkContainsAll(false, "TA > 1", "TA >= 1 and TA < 10");
        checkContainsAll(true, "TA > 0", "TA in (1, 2, 3)");
        checkContainsAll(false, "TA > 0", "TA in (-1, 1, 2, 3)");
        checkContainsAll(false, "TA > 1", "TA != 0");
        checkContainsAll(false, "TA > 1", "TA != 1");
        checkContainsAll(false, "TA > 1", "TA != 2");
        checkContainsAll(true, "TA is not null or null", "TA != 2");
        checkContainsAll(false, "TA is not null or null", "TA is null");
        checkContainsAll(true, "TA is not null or null", "TA is not null");
        checkContainsAll(true, "TA is not null or null", "TA is null and null");
        checkContainsAll(false, "TA > 1", "TA is null");
        checkContainsAll(false, "TA > 1", "TA is not null");
        checkContainsAll(true, "TA > 1", "(TA > 2 and  TA < 5) or (TA > 7 and TA < 9)");
        checkContainsAll(false, "TA > 1", "(TA >= 1 and  TA < 5) or (TA > 7 and TA < 9)");
        checkContainsAll(true, "TA > 1", "TA > 5 and TA is not null");
        checkContainsAll(true, "TA > 1", "(TA > 5 and TA < 8) and TA is not null");
        checkContainsAll(true, "TA > 1", "TA > 5 and TA != 0");
        checkContainsAll(false, "TA > 1", "TA > 5 or TA is not null");
        checkContainsAll(false, "TA > 1", "TA > 5 or TA != 0");

        checkContainsAll(true, "TA in (1, 2, 3)", "TA is null and null");
        checkContainsAll(false, "TA in (1, 2, 3, 4)", "TA between 2 and 3");
        checkContainsAll(true, "TA in (1, 2, 3)", "TA in (1, 2)");
        checkContainsAll(false, "TA in (1, 2, 3)", "TA in (1, 2, 4)");
        checkContainsAll(false, "TA in (1, 2, 3)", "TA not in (1, 2)");
        checkContainsAll(false, "TA in (1, 2, 3)", "TA not in (5, 6)");
        checkContainsAll(false, "TA in (1, 2, 3)", "TA is null");
        checkContainsAll(false, "TA in (1, 2, 3)", "TA is not null");
        checkContainsAll(true, "TA in (1, 2, 3)", "TA in (1, 2) and TA is not null");
        checkContainsAll(true, "TA in (1, 2, 3)", "TA in (1, 2) and TA is null");
        checkContainsAll(false, "TA in (1, 2, 3)", "TA != 1 and TA is not null");
        checkContainsAll(false, "TA in (0, 1, 2, 3)", "TA between 1 and 2 and TA is not null");

        checkContainsAll(true, "TA not in (1, 2)", "TA is null and null");
        checkContainsAll(false, "TA not in (1, 2, 3, 4, 5)", "TA between 2 and 4");
        checkContainsAll(false, "TA not in (1, 2, 3, 4, 5)", "TA is not null or null");
        checkContainsAll(false, "TA not in (1, 2)", "TA in (1)");
        checkContainsAll(false, "TA not in (1, 2)", "TA in (1, 2)");
        checkContainsAll(true, "TA not in (1, 2)", "TA in (3, 4)");
        checkContainsAll(false, "TA not in (1, 2, 3)", "TA in (1, 4)");
        checkContainsAll(false, "TA not in (1, 2, 3)", "TA is null");
        checkContainsAll(false, "TA not in (1, 2, 3)", "TA is not null");
        checkContainsAll(false, "TA not in (1, 2, 3)", "TA is not null or null");
        checkContainsAll(true, "TA not in (1, 2)", "(TA is not null or null) and (TA is null or TA > 10)");

        checkContainsAll(false, "TA is null", "TA is null and null");
        checkContainsAll(false, "TA is null", "TA > 10");
        checkContainsAll(false, "TA is null", "TA = 10");
        checkContainsAll(false, "TA is null", "TA != 10");
        checkContainsAll(true, "TA is null", "TA is null");
        checkContainsAll(false, "TA is null", "TA is not null");
        checkContainsAll(false, "TA is null", "TA is null and (TA > 10)");
        checkContainsAll(false, "TA is null", "TA is null or (TA > 10)");

        checkContainsAll(false, "TA is not null", "TA is null and null");
        checkContainsAll(false, "TA is not null", "TA > 10");
        checkContainsAll(false, "TA is not null", "TA = 10");
        checkContainsAll(false, "TA is not null", "TA != 10");
        checkContainsAll(false, "TA is not null", "TA is null");
        checkContainsAll(true, "TA is not null", "TA is not null");
        checkContainsAll(false, "TA is not null", "TA is not null or null");
        checkContainsAll(true, "TA is not null", "TA is not null and (TA > 10)");
        checkContainsAll(false, "TA is not null", "TA is not null or (TA > 10)");

        checkContainsAll(true, "TA < 1 or TA > 10", "TA is null and null");
        checkContainsAll(true, "TA < 1 or TA > 10", "TA < 0");
        checkContainsAll(false, "TA < 1 or TA > 10", "TA <= 1");
        checkContainsAll(true, "TA < 1 or TA > 10", "TA = 0");
        checkContainsAll(false, "TA < 1 or TA > 10", "TA in (0, 1)");
        checkContainsAll(true, "TA not in (1, 2, 13) or TA > 10", "TA not in (1, 2, 13, 15)");
    }

    private void checkContainsAll(boolean isContains, String expr1, String expr2) {
        Assertions.assertEquals(isContains, getValueDesc(expr1).containsAll(getValueDesc(expr2)));
    }

    @Test
    public void testSimplifyNumeric() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
            bottomUp(SimplifyRange.INSTANCE)
        ));
        assertRewrite("TA", "TA");
        assertRewrite("TA > 10 and (TA > 20 or TA < 10)", "TA > 20");
        assertRewrite("TA > 3 or TA > null", "TA > 3 OR NULL");
        assertRewrite("TA > 3 or TA < null", "TA > 3 OR NULL");
        assertRewrite("TA > 3 or TA = null", "TA > 3 OR NULL");
        assertRewrite("TA > 3 or TA = 3 or TA < null", "TA >= 3 OR NULL");
        assertRewrite("TA < 10 or TA in (1, 2, 3, 11, 12, 13)", "TA in (11, 12, 13) OR TA < 10");
        assertRewrite("TA < 10 or TA in (1, 2, 3, 10, 11, 12, 13) or TA > 13 or TA < 10 or TA in (1, 2, 3, 10, 11, 12, 13) or TA > 13",
                "TA in (11, 12) OR TA <= 10 OR TA >= 13");
        assertRewrite("TA > 3 or TA <> null", "TA > 3 or null");
        assertRewrite("TA > 3 or TA <=> null", "TA > 3 or TA <=> null");
        assertRewrite("TA >= 0 and TA <= 3", "TA >= 0 and TA <= 3");
        assertRewrite("(TA < 1 or TA > 2) or (TA >= 0 and TA <= 3)", "TA IS NOT NULL OR NULL");
        assertRewrite("TA between 10 and 20 or TA between 100 and 120 or TA between 15 and 25 or TA between 115 and 125",
                "TA >= 10 and TA <= 25 or TA >= 100 and TA <= 125");
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
        assertRewrite("TA >= 3 and TA < 3", "TA is null and null");
        assertRewriteNotNull("TA = 1 and TA > 10", "FALSE");
        assertRewrite("TA = 1 and TA > 10", "TA is null and null");
        assertRewrite("TA >= 1 and TA <= 1", "TA = 1");
        assertRewrite("TA = 1 and TA = 2", "TA IS NULL AND NULL");
        assertRewriteNotNull("TA = 1 and TA = 2", "FALSE");
        assertRewrite("TA not in (1) and TA not in (1)", "TA != 1");
        assertRewrite("TA not in (1, 2, 3) and TA not in (1, 4, 5)", "TA not in (1, 2, 3, 4, 5)");
        assertRewrite("TA = 1 and TA not in (2)", "TA = 1");
        assertRewrite("TA = 1 and TA not in (1, 2)", "TA is null and null");
        assertRewriteNotNull("TA = 1 and TA not in (1, 2)", "FALSE");
        assertRewrite("TA > 10 and TA not in (1, 2, 3)", "TA > 10");
        assertRewrite("TA > 10 and TA not in (1, 2, 3, 11)", "TA > 10 and TA != 11");
        assertRewrite("TA > 10 and TA not in (1, 2, 3, 11, 12)", "TA > 10 and TA NOT IN (11, 12)");
        assertRewrite("TA is null", "TA is null");
        assertRewriteNotNull("TA is null", "TA is null");
        assertRewrite("TA is not null", "TA is not null");
        assertRewrite("TA is null and TA is not null", "FALSE");
        assertRewriteNotNull("TA is null and TA is not null", "FALSE");
        assertRewrite("TA = 1 and TA != 1 and TA is null", "TA is null and null");
        assertRewriteNotNull("TA = 1 and TA != 1 and TA is null", "FALSE");
        assertRewrite("TA = 1 and TA != 1 and TA is not null", "FALSE");
        assertRewriteNotNull("TA = 1 and TA != 1 and TA is not null", "FALSE");
        assertRewrite("TA = 1 and TA != 1 and (TA > 10 or TA < 5)", "TA is null and null");
        assertRewriteNotNull("TA = 1 and TA != 1 and (TA > 10 or TA < 5)", "FALSE");
        assertRewrite("TA = 1 and TA != 1 and (TA > 10 or TA is not null)", "TA is null and null");
        assertRewrite("TA = 1 and TA != 1 and (TA > 10 or (TA < 5 and TA is not null))", "TA is null and null");
        assertRewrite("TA = 1 and TA != 1 and (TA > 10 or (TA < 5 and TA is not null) or (TA > 7 and TA is not null))",
                "TA is null and null");
        assertRewrite("TA > 5 or TA < 1", "TA < 1 or TA > 5");
        assertRewrite("TA > 5 or TA > 1 or TA > 10", "TA > 1");
        assertRewrite("TA > 5 or TA > 1 or TA < 10", "TA is not null or null");
        assertRewriteNotNull("TA > 5 or TA > 1 or TA < 10", "TRUE");
        assertRewrite("TA != 1 or TA != 1", "TA != 1");
        assertRewrite("TA != 1 or TA != 2", "TA is not null or null");
        assertRewriteNotNull("TA != 1 or TA != 2", "TRUE");
        assertRewrite("TA not in (1, 2, 3) or TA not in (1, 2, 4)", "TA not in (1, 2)");
        assertRewrite("TA not in (1, 2) or TA in (2, 1)", "TA is not null or null");
        assertRewrite("TA not in (1, 2) or TA in (1)", "TA != 2");
        assertRewrite("TA not in (1, 2) or TA in (1, 2, 3)", "TA is not null or null");
        assertRewrite("TA not in (1, 3) or TA < 2", "TA != 3");
        assertRewrite("TA is null and null", "TA is null and null");
        assertRewrite("TA is null", "TA is null");
        assertRewrite("TA is null and null or TA = 1", "TA = 1");
        assertRewrite("TA is null and null or TA is null", "TA is null");
        assertRewrite("TA is null and null or (TA is null and TA > 10) ", "TA is null and null");
        assertRewrite("TA is null and null or TA is not null", "TA is not null or null");
        assertRewriteNotNull("TA != 1 or TA != 2", "TRUE");
        assertRewrite("TA is null or TA is not null", "TRUE");
        assertRewrite("TA > 5 and TA > 1 and TA > 10", "TA > 10");
        assertRewrite("TA > 5 and TA > 1 and TA < 10", "TA > 5 and TA < 10");
        assertRewrite("TA > 1 or TA < 1", "TA < 1 or TA > 1");
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
        assertRewrite("((TA > 10 or TA > 5) and TB > 10) or (TB > 10 and (TB > 20 or TB < 10))", "(TA > 5 and TB > 10) or TB > 20");
        assertRewriteNotNull("TA in (1,2,3) and TA > 10", "FALSE");
        assertRewrite("TA in (1,2,3) and TA > 10", "TA is null and null");
        assertRewrite("TA in (1,2,3) and TA >= 1", "TA in (1,2,3)");
        assertRewrite("TA in (1,2,3) and TA > 1", "TA IN (2, 3)");
        assertRewrite("TA in (1,2,3) or TA >= 1", "TA >= 1");
        assertRewrite("TA is null and (TA = 4 or TA = 5)", "TA is null and null");
        assertRewrite("(TA != 3 or TA is null) and (TA = 4 or TA = 5)", "TA in (4, 5)");
        assertRewrite("TA in (1)", "TA in (1)");
        assertRewrite("TA in (1,2,3) and TA < 10", "TA in (1,2,3)");
        assertRewriteNotNull("TA in (1,2,3) and TA < 1", "FALSE");
        assertRewrite("TA in (1,2,3) and TA < 1", "TA is null and null");
        assertRewrite("TA in (1,2,3) or TA < 1", "TA in (2,3) or TA <= 1");
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

        assertRewrite("TA is null and TA > 10", "TA is null and null");
        assertRewrite("TA is null and TA = 10", "TA is null and null");
        assertRewrite("TA is null and TA != 10", "TA is null and null");
        assertRewriteNotNull("TA is null and TA > 10", "FALSE");
        assertRewriteNotNull("TA is null and TA = 10", "FALSE");
        assertRewriteNotNull("TA is null and TA != 10", "FALSE");
        assertRewrite("TA is not null or TA > 10", "TA is not null or null");
        assertRewrite("TA is not null or TA = 10", "TA is not null or null");
        assertRewrite("TA is not null or TA != 10", "TA is not null or null");
        assertRewriteNotNull("TA is not null or TA > 10", "TRUE");
        assertRewriteNotNull("TA is not null or TA = 10", "TRUE");
        assertRewriteNotNull("TA is not null or TA != 10", "TRUE");

        // A and (B or C) = A
        assertRewrite("TA < 10 and (TA is not null or TA is null and null)", "TA < 10");
        assertRewrite("TA > 10 and (TA > 5 or (TA is not null and TA > 1))", "TA > 10");
        assertRewrite("TA > 10 and (TA != 4 or (TA is not null and TA > 1))", "TA > 10");
        assertRewrite("TA = 5 and (TA != 4 or (TA is not null and TA > 1))", "TA = 5");
        assertRewrite("TA = 5 and (TA in (1, 2, 5) or (TA is not null and TA > 1))", "TA = 5");
        assertRewrite("TA = 5 and (TA > 3 or (TA is not null and TA > 1))", "TA = 5");
        assertRewrite("TA not in (1, 2) and (TA not in (1) or (TA is not null and TA > 1))", "TA not in (1, 2)");
        assertRewrite("TA not in (1, 2) and (TA not in (1, 2) or (TA is not null and TA > 1))", "TA not in (1, 2)");
        assertRewrite("TA not in (2, 3) or (TA is not null and TA > 1)", "TA is not null or null");
        assertRewrite("TA not in (1, 2) and (TA not in (2, 3) or (TA is not null and TA > 1))", "TA not in (1, 2)");
        assertRewrite("TA is null and null and (TA = 10 or (TA is not null and TA > 1))", "TA is null and null");
        assertRewrite("TA is null and null and (TA != 10 or (TA is not null and TA > 1))", "TA is null and null");
        assertRewrite("TA is null and null and (TA > 20 or (TA is not null and TA > 1))", "TA is null and null");
        assertRewrite("TA is null and null and (TA is null and null or (TA is not null and TA > 1))", "TA is null and null");
        assertRewrite("TA is null and null and (TA is null or (TA is not null and TA > 1))", "TA is null and null");
        assertRewrite("TA is null and (TA is null or (TA is not null and TA > 1))", "TA is null");
        assertRewrite("TA is not null and (TA is not null or (TA is not null and TA > 1))", "TA is not null");

        assertRewrite("TA is null and null", "TA is null and null");
        assertRewriteNotNull("TA is null and null", "FALSE");
        assertRewrite("TA is null", "TA is null");
        assertRewriteNotNull("TA is null", "TA is null");
        assertRewrite("TA is not null", "TA is not null");
        assertRewriteNotNull("TA is not null", "TA is not null");
        assertRewrite("TA is null and null or TA is null", "TA is null");
        assertRewriteNotNull("TA is null and null or TA is null", "TA is null");
        assertRewrite("TA is null and null or TA is not null", "TA is not null or null");
        assertRewriteNotNull("TA is null and null or TA is not null", "not TA is null");
        assertRewrite("TA is null or TA is not null", "TRUE");
        assertRewriteNotNull("TA is null or TA is not null", "TRUE");
        assertRewrite("(TA is null and null) and TA is null", "TA is null and null");
        assertRewriteNotNull("(TA is null and null) and TA is null", "FALSE");
        assertRewrite("TA is null and null and TA is not null", "FALSE");
        assertRewriteNotNull("TA is null and null and TA is not null", "FALSE");
        assertRewrite("TA is null and TA is not null", "FALSE");
        assertRewriteNotNull("TA is null and TA is not null", "FALSE");

        assertRewrite("(TA is not null or null) and TA > 10", "TA > 10");
        assertRewrite("(TA is not null or null) or TA > 10", "TA is not null or null");

        assertRewrite("(TA is null and null) and TA is null", "TA is null and null");
        assertRewrite("(TA is null and null) or TA is null", "TA is null");
        // can simplify to 'TA is null', but not supported yet
        assertRewrite("(TA is null or null) and TA is null", "(TA is null or null) and TA is null");
        assertRewrite("(TA is null or null) or TA is null", "TA is null or null");
        assertRewrite("(TA is not null and null) and TA is null", "FALSE");
        assertRewrite("(TA is not null and null) or TA is null", "TA is not null and null or TA is null");
        assertRewrite("(TA is not null or null) and TA is null", "TA is null and null");
        assertRewrite("(TA is not null or null) or TA is null", "TRUE");
        assertRewrite("(TA is null and null) and TA is not null", "FALSE");
        assertRewrite("(TA is null and null) or TA is not null", "TA is not null or null");
        assertRewrite("(TA is null or null) and TA is not null", "(TA is null or null) and TA is not null");
        assertRewrite("(TA is null or null) or TA is not null", "TRUE");
        assertRewrite("(TA is not null and null) and TA is not null", "TA is not null and null");
        // can simplify to 'TA is not null', but not supported yet
        assertRewrite("(TA is not null and null) or TA is not null", "TA is not null and null or TA is not null");
        // can simplify to 'TA is not null', but not supported yet
        assertRewrite("(TA is not null or null) and TA is not null", "TA is not null");
        assertRewrite("(TA is not null or null) or TA is not null", "TA is not null or null");

        assertRewrite("(XA is null and null) and XA is null", "FALSE");
        // can simplify to 'FALSE', but not supported yet
        assertRewrite("(XA is null and null) or XA is null", "XA is null");
        // can simplify to 'FALSE', but not supported yet
        assertRewrite("(XA is null or null) and XA is null", "(XA is null or null) and XA is null");
        // can simplify to 'null', but not supported yet
        assertRewrite("(XA is null or null) or XA is null", "XA is null or null");
        assertRewrite("(XA is not null and null) and XA is null", "FALSE");
        // can simplify to 'null', but not supported yet
        assertRewrite("(XA is not null and null) or XA is null", "(XA is not null and null) or XA is null");
        assertRewrite("(XA is not null or null) and XA is null", "FALSE");
        assertRewrite("(XA is not null or null) or XA is null", "TRUE");
        assertRewrite("(XA is null and null) and XA is not null", "FALSE");
        // can simplify to 'TRUE', but not supported yet
        assertRewrite("(XA is null and null) or XA is not null", "XA is not null");
        // can simplify to 'NULL', but not supported yet
        assertRewrite("(XA is null or null) and XA is not null", "(XA is null or null) and XA is not null");
        assertRewrite("(XA is null or null) or XA is not null", "TRUE");
        // can simplify to 'NULL', but not supported yet
        assertRewrite("(XA is not null and null) and XA is not null", "XA is not null and null");
        // can simplify to 'NULL', but not supported yet
        assertRewrite("(XA is not null and null) or XA is not null", "XA is not null and null or XA is not null");
        // can simplify to 'TRUE', but not supported yet
        assertRewrite("(XA is not null or null) and XA is not null", "XA is not null");
        assertRewrite("(XA is not null or null) or XA is not null", "TRUE");

        assertRewrite("TA < 10 or (TA is null or (TA != 1 and TA != 2))", "TRUE");
        assertRewrite("TA < 10 or ((TA != 1 or TA is null) and (TA != 2 or TA is null))", "TRUE");

        assertRewrite("(TA between 10 and 20 or TA between 30 and 40) and (TA between 5 and 15 or TA between 35 and 45)",
                "(TA between 10 and 20 or TA between 30 and 40) and (TA between 5 and 15 or TA between 35 and 45)");
        assertRewrite("(TA between 10 and 20 or TA > 30) and (TA between 5 and 15 or TA > 40)",
                "(TA between 10 and 20 or TA > 30) and (TA between 5 and 15 or TA > 40)");

        assertRewrite("TA < 10 and TA is not null or TA > 20 and TA is not null",
                "TA < 10 and TA is not null or TA > 20 and TA is not null");
        assertRewrite("TA < 10 and TA != 0 or TA > 20 and TA != 25", "TA < 10 and TA != 0 or TA > 20 and TA != 25");

        // A and ((B1 and B2) or (C1 and C2))
        assertRewrite("TA = 15 and (TA < 10 and TA is not null or TA > 20 and TA is not null)", "FALSE");
        assertRewrite("TA = 15 and (TA < 10 and TA is not null or TA > 20 and TA is null)", "TA is null and null");
        assertRewrite("TA = 15 and (TA < 10 and TA != 0 or TA > 20 and TA != 25)", "TA is null and null");
        assertRewriteNotNull("TA = 15 and (TA < 10 and TA is not null or TA > 20 and TA is not null)", "FALSE");
        assertRewriteNotNull("TA = 15 and (TA < 10 and TA is not null or TA > 20 and TA is null)", "FALSE");
        assertRewriteNotNull("TA = 15 and (TA < 10 and TA != 0 or TA > 20 and TA != 25)", "FALSE");

        // A or ((B1 or B2) and (C1 or C2))
        assertRewrite("TA < 10 or ((TA != 1 or TA is null) and (TA != 2 or TA is null))", "TRUE");
        assertRewrite("TA < 10 or ((TA != 1 or TA is not null) and (TA != 2 or TA is not null))", "TA is not null or null");
        assertRewrite("TA < 10 or ((TA != 1 or TA is null) and (TA != 2 or TA is not null))", "TA is not null or null");
        assertRewrite("TA < 10 or ((TA != 1 or TA is null) and (TA is null or TA is not null))", "TRUE");
        assertRewrite("TA < 100 or (TA between 10 and 20 or TA > 30) and (TA between 5 and 15 or TA > 40)", "TA is not null or null");
        assertRewriteNotNull("(TA between 10 and 20 or TA between 30 and 40) and (TA between 5 and 15 or TA between 35 and 45)",
                "(TA between 10 and 20 or TA between 30 and 40) and (TA between 5 and 15 or TA between 35 and 45)");
        assertRewriteNotNull("(TA between 10 and 20 or TA > 30) and (TA between 5 and 15 or TA > 40)",
                "(TA between 10 and 20 or TA > 30) and (TA between 5 and 15 or TA > 40)");
        assertRewriteNotNull("TA < 10 or ((TA != 1 or TA is null) and (TA != 2 or TA is null))", "TRUE");
        assertRewriteNotNull("TA < 10 or ((TA != 1 or TA is not null) and (TA != 2 or TA is not null))", "TRUE");
        assertRewriteNotNull("TA < 10 or ((TA != 1 or TA is null) and (TA != 2 or TA is not null))", "TRUE");
        assertRewriteNotNull("TA < 10 or ((TA != 1 or TA is null) and (TA is null or TA is not null))", "TRUE");
        assertRewriteNotNull("TA < 100 or (TA between 10 and 20 or TA > 30) and (TA between 5 and 15 or TA > 40)", "TRUE");

        assertRewrite("TA is not null or TA is null and null", "TA is not null or null");
        assertRewrite("TA > 100 and (TA < 10 or TA between 15 and 20)", "TA is null and null");
        assertRewrite("TA > 100 and (TA < 10 or TA between 15 and 20 or TA between 110 and 115)", "TA between 110 and 115");
        assertRewrite("TA > 100 and (TA < 10 or TA between 15 and 20 or TA is null)", "TA is null and null");
        assertRewrite("TA > 100 and (TA < 10 or TA between 15 and 20 or TA is not null)", "TA > 100");
        assertRewriteNotNull("TA is not null or TA is null and null", "TA is not null");
        assertRewriteNotNull("TA > 100 and (TA < 10 or TA between 15 and 20)", "FALSE");
        assertRewriteNotNull("TA > 100 and (TA < 10 or TA between 15 and 20 or TA is null)", "FALSE");
        assertRewriteNotNull("TA > 100 and (TA < 10 or TA between 15 and 20 or TA is not null)", "TA > 100");
        assertRewrite("TA > 100 or (TA < 120 and TA is null)", "TA > 100");
        assertRewrite("TA > 100 or (TA < 120 and TA is not null)", "TA is not null or null");
        assertRewrite("TA > 100 or (TA < 120 and TA != 80)",
                "TA > 100 or ((TA is not null or null) and TA != 80)");
        assertRewrite("TA > 100 or (TA < 120 and TA != 110)", "TA is not null or null");
        assertRewriteNotNull("TA > 100 or (TA < 120 and TA is null)", "TA > 100");
        assertRewriteNotNull("TA > 100 or (TA < 120 and TA is not null)", "TRUE");
        assertRewriteNotNull("TA > 100 or (TA < 120 and TA != 80)",
                "TA != 80");
        assertRewriteNotNull("TA > 100 or (TA < 120 and TA != 110)", "TRUE");

        assertRewrite("TA + TC", "TA + TC");
        assertRewrite("(TA + TC >= 1 and TA + TC <=3 ) or (TA + TC > 5 and TA + TC < 7)", "(TA + TC >= 1 and TA + TC <=3 ) or (TA + TC > 5 and TA + TC < 7)");
        assertRewriteNotNull("(TA + TC > 3 and TA + TC < 1) or (TA + TC > 7 and TA + TC < 5)", "FALSE");
        assertRewrite("(TA + TC > 3 and TA + TC < 1) or (TA + TC > 7 and TA + TC < 5)", "(TA + TC) is null and null");
        assertRewriteNotNull("TA + TC > 3 and TA + TC < 1", "FALSE");
        assertRewrite("TA + TC > 3 and TA + TC < 1", "(TA + TC) is null and null");
        assertRewrite("TA + TC >= 3 and TA + TC < 3", "TA + TC is null and null");
        assertRewriteNotNull("TA + TC = 1 and TA + TC > 10", "FALSE");
        assertRewrite("TA + TC = 1 and TA + TC > 10", "(TA + TC) is null and null");
        assertRewrite("TA + TC > 5 or TA + TC < 1", "TA + TC < 1 or TA + TC > 5");
        assertRewrite("TA + TC > 5 or TA + TC > 1 or TA + TC > 10", "TA + TC > 1");
        assertRewrite("TA + TC > 5 or TA + TC > 1 or TA + TC < 10", "(TA + TC) is not null or null");
        assertRewrite("TA + TC > 5 and TA + TC > 1 and TA + TC > 10", "TA + TC > 10");
        assertRewrite("TA + TC > 5 and TA + TC > 1 and TA + TC < 10", "TA + TC > 5 and TA + TC < 10");
        assertRewrite("TA + TC > 1 or TA + TC < 1", "TA + TC < 1 or TA + TC > 1");
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
        assertRewrite("((TA + TC > 10 or TA + TC > 5) and TB > 10) or (TB > 10 and (TB > 20 or TB < 10))",
                "(TA + TC > 5 and TB > 10) or TB > 20");
        assertRewriteNotNull("TA + TC in (1,2,3) and TA + TC > 10", "FALSE");
        assertRewrite("TA + TC in (1,2,3) and TA + TC > 10", "(TA + TC) is null and null");
        assertRewrite("TA + TC in (1,2,3) and TA + TC >= 1", "TA + TC in (1,2,3)");
        assertRewrite("TA + TC in (1,2,3) and TA + TC > 1", "(TA + TC) IN (2, 3)");
        assertRewrite("TA + TC in (1,2,3) or TA + TC >= 1", "TA + TC >= 1");
        assertRewrite("TA + TC in (1)", "TA + TC in (1)");
        assertRewrite("TA + TC in (1,2,3) and TA + TC < 10", "TA + TC in (1,2,3)");
        assertRewriteNotNull("TA + TC in (1,2,3) and TA + TC < 1", "FALSE");
        assertRewrite("TA + TC in (1,2,3) and TA + TC < 1", "(TA + TC) is null and null");
        assertRewrite("TA + TC in (1,2,3) or TA + TC < 1", "TA + TC in (2,3) or TA + TC <= 1");
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
        assertRewrite("(TA + TC > 3 OR TA < 1) AND TB = 2 AND IA =1", "(TA + TC > 3 OR TA < 1) AND TB = 2 AND IA =1");

        // random is non-foldable, so the two random(1, 10) are distinct, cann't merge range for them.
        Expression expr = rewriteExpression("X + random(1, 10) > 10 AND  X + random(1, 10) < 1", true);
        Assertions.assertEquals("AND[((X + random(1, 10)) > 10),((X + random(1, 10)) < 1)]", expr.toSql());
        expr = rewrite("TA + random(1, 10) between 10 and 20", Maps.newHashMap());
        Assertions.assertEquals("AND[((cast(TA as BIGINT) + random(1, 10)) >= 10),((cast(TA as BIGINT) + random(1, 10)) <= 20)]", expr.toSql());
        expr = rewrite("TA + random(1, 10) between 20 and 10", Maps.newHashMap());
        Assertions.assertEquals("AND[(cast(TA as BIGINT) + random(1, 10)) IS NULL,NULL]", expr.toSql());
    }

    @Test
    public void testSimplifyString() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(SimplifyRange.INSTANCE)
        ));
        assertRewrite("SA = '20250101' and SA < '20200101'", "SA is null and null");
        assertRewrite("SA > '20250101' and SA > '20260110'", "SA > '20260110'");
        assertRewrite("((IA = 1 AND SC ='1') OR SC = '1212') AND IA =1", "((IA = 1 AND SC ='1') OR SC = '1212') AND IA =1");
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
                "AA IS NULL AND NULL");
        assertRewriteNotNull("AA = date '2024-01-01' and AA > date '2024-01-10'", "FALSE");
        assertRewrite("AA = date '2024-01-01' and AA > date '2024-01-10'", "AA is null and null");
        assertRewrite("AA > date '2024-01-05' or AA < date '2024-01-01'",
                "AA < date '2024-01-01' or AA > date '2024-01-05'");
        assertRewrite("AA > date '2024-01-05' or AA > date '2024-01-01' or AA > date '2024-01-10'",
                "AA > date '2024-01-01'");
        assertRewrite("AA > date '2024-01-05' or AA > date '2024-01-01' or AA < date '2024-01-10'", "AA is not null or null");
        assertRewriteNotNull("AA > date '2024-01-05' or AA > date '2024-01-01' or AA < date '2024-01-10'", "TRUE");
        assertRewrite("AA > date '2024-01-05' and AA > date '2024-01-01' and AA > date '2024-01-10'",
                "AA > date '2024-01-10'");
        assertRewrite("AA > date '2024-01-05' and AA > date '2024-01-01' and AA < date '2024-01-10'",
                "AA > date '2024-01-05' and AA < date '2024-01-10'");
        assertRewrite("AA > date '2024-01-05' or AA < date '2024-01-05'",
                "AA < date '2024-01-05' or AA > date '2024-01-05'");
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
                "AA in (date '2024-01-02',date '2024-01-03') or AA <= date '2024-01-01'");
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
                "CA is null and null");
        assertRewriteNotNull("CA = timestamp '2024-01-01 10:00:10' and CA > timestamp '2024-01-10 00:00:10'", "FALSE");
        assertRewrite("CA = timestamp '2024-01-01 10:00:10' and CA > timestamp '2024-01-10 00:00:10'", "CA is null and null");
        assertRewrite("CA > timestamp '2024-01-05 00:00:10' or CA < timestamp '2024-01-01 00:00:10'",
                "CA < timestamp '2024-01-01 00:00:10' or CA > timestamp '2024-01-05 00:00:10'");
        assertRewrite("CA > timestamp '2024-01-05 00:00:10' or CA > timestamp '2024-01-01 00:00:10' or CA > timestamp '2024-01-10 00:00:10'",
                "CA > timestamp '2024-01-01 00:00:10'");
        assertRewrite("CA > timestamp '2024-01-05 00:00:10' or CA > timestamp '2024-01-01 00:00:10' or CA < timestamp '2024-01-10 00:00:10'", "CA is not null or null");
        assertRewriteNotNull("CA > timestamp '2024-01-05 00:00:10' or CA > timestamp '2024-01-01 00:00:10' or CA < timestamp '2024-01-10 00:00:10'", "TRUE");
        assertRewrite("CA > timestamp '2024-01-05 00:00:10' and CA > timestamp '2024-01-01 00:00:10' and CA > timestamp '2024-01-10 00:00:15'",
                "CA > timestamp '2024-01-10 00:00:15'");
        assertRewrite("CA > timestamp '2024-01-05 00:00:10' and CA > timestamp '2024-01-01 00:00:10' and CA < timestamp '2024-01-10 00:00:10'",
                "CA > timestamp '2024-01-05 00:00:10' and CA < timestamp '2024-01-10 00:00:10'");
        assertRewrite("CA > timestamp '2024-01-05 00:00:10' or CA < timestamp '2024-01-05 00:00:10'",
                "CA < timestamp '2024-01-05 00:00:10' or CA > timestamp '2024-01-05 00:00:10'");
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

    @Test
    public void testMixTypes() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(SimplifyRange.INSTANCE)
        ));
        assertRewrite("(TA > 1 and FALSE or FALSE and SA > 'aaaa') and TB is null", "FALSE");
        assertRewrite("(TA > 1 and FALSE or FALSE and SA > 'aaaa') and (TA > 1 and FALSE or FALSE and SA > 'aaaa') and TB is null",
                "FALSE");
    }

    private ValueDesc getValueDesc(String expression) {
        Expression parseExpression = replaceUnboundSlot(PARSER.parseExpression(expression), commonMem);
        parseExpression = typeCoercion(parseExpression);
        return (new RangeInference()).getValue(parseExpression, context);
    }

    private void assertRewrite(String expression, String expected) {
        Map<String, Slot> mem = Maps.newHashMap();
        Expression rewrittenExpression = rewrite(expression, mem);
        Expression expectedExpression = replaceUnboundSlot(PARSER.parseExpression(expected), mem);
        expectedExpression = typeCoercion(expectedExpression);
        Assertions.assertEquals(expectedExpression, rewrittenExpression);
    }

    private Expression rewrite(String expression, Map<String, Slot> mem) {
        Expression rewriteExpression = replaceUnboundSlot(PARSER.parseExpression(expression), mem);
        rewriteExpression = typeCoercion(rewriteExpression);
        rewriteExpression = executor.rewrite(rewriteExpression, context);
        return sortChildren(rewriteExpression);
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

    private Expression rewriteExpression(String expression, boolean nullable) {
        Map<String, Slot> mem = Maps.newHashMap();
        Expression needRewriteExpression = PARSER.parseExpression(expression);
        needRewriteExpression = nullable ? replaceUnboundSlot(needRewriteExpression, mem) : replaceNotNullUnboundSlot(needRewriteExpression, mem);
        needRewriteExpression = typeCoercion(needRewriteExpression);
        return executor.rewrite(needRewriteExpression, context);
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
            boolean notNullable = name.charAt(0) == 'X' || name.length() >= 2 && name.charAt(1) == 'X';
            mem.putIfAbsent(name, new SlotReference(name, getType(name.charAt(0)), !notNullable));
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
