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

package org.apache.doris.nereids.glue.translator;

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.datasource.lance.source.LanceScanNode;
import org.apache.doris.nereids.properties.DataTrait;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.physical.PhysicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.ColumnStatisticBuilder;
import org.apache.doris.statistics.Statistics;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class LanceScalarIndexSelectivityTest {
    @Test
    public void testTranslationUsesIndependentColumnEstimatesAndHonorsSwitch() throws Exception {
        for (boolean enabled : Arrays.asList(true, false)) {
            SlotReference broad = new SlotReference("broad", BigIntType.INSTANCE);
            SlotReference selective = new SlotReference("selective", BigIntType.INSTANCE);
            Map<Expression, ColumnStatistic> columns = new HashMap<>();
            columns.put(broad, columnStats(10));
            columns.put(selective, columnStats(1000));
            Statistics input = new Statistics(1000, columns);
            LogicalProperties properties = new LogicalProperties(
                    () -> Arrays.asList(broad, selective), () -> DataTrait.EMPTY_TRAIT);
            PhysicalEmptyRelation child = new PhysicalEmptyRelation(new RelationId(0),
                    Arrays.asList(broad, selective), properties);
            child.setStatistics(input);
            PhysicalFilter<PhysicalEmptyRelation> filter = new PhysicalFilter<>(new HashSet<>(Arrays.asList(
                    new EqualTo(broad, new BigIntLiteral(1)),
                    new EqualTo(selective, new BigIntLiteral(1)))), properties, child);
            PlanTranslatorContext context = new PlanTranslatorContext();
            TupleDescriptor tuple = context.generateTupleDesc();
            context.createSlotDesc(tuple, broad);
            context.createSlotDesc(tuple, selective);
            SessionVariable variables = new SessionVariable();
            variables.enableLanceScalarIndexAutoSelection = enabled;
            LanceScanNode node = new LanceScanNode(new PlanNodeId(0), tuple, false, variables, ScanContext.EMPTY);
            Method method = PhysicalPlanTranslator.class.getDeclaredMethod("addConjunctsToPlanNode",
                    PhysicalFilter.class, PlanNode.class, PlanTranslatorContext.class);
            method.setAccessible(true);
            method.invoke(new PhysicalPlanTranslator(context), filter, node, context);
            Assertions.assertEquals(2, node.getConjuncts().size());
            for (Expr predicate : node.getConjuncts()) {
                if (enabled) {
                    String column = ((SlotRef) predicate.getChild(0)).getDesc().getLabel();
                    Assertions.assertEquals("broad".equals(column) ? 0.1 : 0.001,
                            predicate.getSelectivity(), 1e-9, column + ": " + predicate.toSql());
                } else {
                    Assertions.assertFalse(predicate.hasSelectivity());
                }
            }
            Assertions.assertEquals(1000, input.getRowCount());
            Assertions.assertEquals(10, input.findColumnStatistics(broad).ndv);
            Assertions.assertEquals(1000, input.findColumnStatistics(selective).ndv);
        }
    }

    private static ColumnStatistic columnStats(double ndv) {
        return new ColumnStatisticBuilder(1000).setNdv(ndv).setNumNulls(0)
                .setMinValue(0).setMaxValue(1000).setAvgSizeByte(8).setDataSize(8000).build();
    }
}
