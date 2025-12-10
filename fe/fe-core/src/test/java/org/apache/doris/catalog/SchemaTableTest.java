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

package org.apache.doris.catalog;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.SchemaTable.SchemaTableAggregateType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AnyValue;
import org.apache.doris.nereids.trees.expressions.functions.agg.Avg;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.JsonType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.io.IOException;

public class SchemaTableTest {
    @Test
    public void testGenerateAggBySchemaAggType() {
        Slot a = new SlotReference("a", IntegerType.INSTANCE);
        Expression expression = SchemaTable.generateAggBySchemaAggType(a, SchemaTableAggregateType.AVG);
        Assertions.assertEquals(IntegerType.INSTANCE, expression.getDataType());
        Assertions.assertTrue(expression.child(0) instanceof Avg);
        expression = SchemaTable.generateAggBySchemaAggType(a, SchemaTableAggregateType.MAX);
        Assertions.assertTrue(expression instanceof Max);
        expression = SchemaTable.generateAggBySchemaAggType(a, SchemaTableAggregateType.MIN);
        Assertions.assertTrue(expression instanceof Min);
        expression = SchemaTable.generateAggBySchemaAggType(a, SchemaTableAggregateType.SUM);
        Assertions.assertTrue(expression.child(0) instanceof Sum);
        expression = SchemaTable.generateAggBySchemaAggType(a, null);
        Assertions.assertTrue(expression instanceof AnyValue);

        a = new SlotReference("a", DoubleType.INSTANCE);
        expression = SchemaTable.generateAggBySchemaAggType(a, SchemaTableAggregateType.AVG);
        Assertions.assertEquals(DoubleType.INSTANCE, expression.getDataType());

        a = new SlotReference("a", BigIntType.INSTANCE);
        expression = SchemaTable.generateAggBySchemaAggType(a, SchemaTableAggregateType.AVG);
        Assertions.assertEquals(BigIntType.INSTANCE, expression.getDataType());

        a = new SlotReference("a", JsonType.INSTANCE);
        try {
            SchemaTable.generateAggBySchemaAggType(a, SchemaTableAggregateType.AVG);
        } catch (Exception e) {
            Assertions.assertTrue(e.getMessage().contains("can not cast"));
        }
        a = new SlotReference("a", StringType.INSTANCE);
        expression = SchemaTable.generateAggBySchemaAggType(a, SchemaTableAggregateType.AVG);
        Assertions.assertEquals(StringType.INSTANCE, expression.getDataType());
    }

    @Test
    public void testShouldFetchAllFe() throws AnalysisException, IOException {
        UserIdentity user1 = new UserIdentity("user1", "%");
        user1.analyze();
        TestWithFeService.createCtx(user1, "127.0.0.1");

        SchemaTable sqlBlockRuleStatus = (SchemaTable) SchemaTable.TABLE_MAP.get("sql_block_rule_status");
        Assertions.assertTrue(sqlBlockRuleStatus.shouldFetchAllFe());
        Assertions.assertTrue(sqlBlockRuleStatus.shouldAddAgg());

        SchemaTable processlist = (SchemaTable) SchemaTable.TABLE_MAP.get("processlist");
        Assertions.assertTrue(processlist.shouldFetchAllFe());
        Assertions.assertFalse(processlist.shouldAddAgg());

        SchemaTable viewDependency = (SchemaTable) SchemaTable.TABLE_MAP.get("view_dependency");
        Assertions.assertFalse(viewDependency.shouldFetchAllFe());
        Assertions.assertFalse(viewDependency.shouldAddAgg());
    }
}
