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

package org.apache.doris.nereids.parser;

import org.apache.doris.nereids.DorisParser;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Maps;
import mockit.Mock;
import mockit.MockUp;
import org.antlr.v4.runtime.ParserRuleContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Map;

/**
 * Unit tests for LogicalPlanBuilder.parseInsertPartitionSpec() method.
 */
public class ParseInsertPartitionSpecTest {

    private static LogicalPlanBuilder logicalPlanBuilder;

    @BeforeAll
    public static void init() {
        ConnectContext ctx = new ConnectContext();
        new MockUp<ConnectContext>() {
            @Mock
            public ConnectContext get() {
                return ctx;
            }
        };
    }

    /**
     * Helper method to invoke the private parseInsertPartitionSpec method via
     * reflection.
     * Uses reflection to find the method with PartitionSpecContext parameter.
     */
    private InsertPartitionSpec invokeParseInsertPartitionSpec(Object ctx) throws Exception {
        logicalPlanBuilder = new LogicalPlanBuilder(Maps.newHashMap());
        // Find the parseInsertPartitionSpec method - it takes PartitionSpecContext as
        // parameter
        Method targetMethod = null;
        for (Method method : LogicalPlanBuilder.class.getDeclaredMethods()) {
            if (method.getName().equals("parseInsertPartitionSpec")) {
                targetMethod = method;
                break;
            }
        }
        if (targetMethod == null) {
            throw new NoSuchMethodException("parseInsertPartitionSpec method not found");
        }
        targetMethod.setAccessible(true);
        return (InsertPartitionSpec) targetMethod.invoke(logicalPlanBuilder, ctx);
    }

    /**
     * Helper method to parse SQL and extract PartitionSpecContext using reflection.
     */
    private Object parsePartitionSpec(String insertSql) throws Exception {
        // Use NereidsParser.toAst() to parse the SQL and get the AST
        ParserRuleContext tree = NereidsParser.toAst(
                insertSql, DorisParser::singleStatement);

        // The tree is a SingleStatementContext, which contains a StatementContext
        // which contains a StatementBaseContext which contains an InsertTableContext
        // Use reflection to navigate the AST structure
        Method getChildMethod = ParserRuleContext.class.getMethod("getChild", int.class);

        // Get statement from singleStatement (index 0)
        Object statement = getChildMethod.invoke(tree, 0);

        // Get statementBase from statement (index 0)
        Object statementBase = getChildMethod.invoke(statement, 0);

        // Get insertTable from statementBase (index 0)
        Object insertTableCtx = getChildMethod.invoke(statementBase, 0);

        // Get partitionSpec() from insertTableCtx using the method
        Method partitionSpecMethod = insertTableCtx.getClass().getMethod("partitionSpec");
        return partitionSpecMethod.invoke(insertTableCtx);
    }

    @Test
    public void testParseNullContext() throws Exception {
        InsertPartitionSpec spec = invokeParseInsertPartitionSpec(null);

        Assertions.assertFalse(spec.isStaticPartition());
        Assertions.assertFalse(spec.hasDynamicPartitionNames());
        Assertions.assertFalse(spec.isTemporary());
        Assertions.assertFalse(spec.isAutoDetect());
    }

    @Test
    public void testParseAutoDetect() throws Exception {
        // Parse: INSERT INTO tbl PARTITION (*) SELECT ...
        String sql = "INSERT INTO tbl PARTITION (*) SELECT * FROM src";
        Object ctx = parsePartitionSpec(sql);

        InsertPartitionSpec spec = invokeParseInsertPartitionSpec(ctx);

        Assertions.assertFalse(spec.isStaticPartition());
        Assertions.assertFalse(spec.hasDynamicPartitionNames());
        Assertions.assertFalse(spec.isTemporary());
        Assertions.assertTrue(spec.isAutoDetect());
    }

    @Test
    public void testParseDynamicPartitionSingle() throws Exception {
        // Parse: INSERT INTO tbl PARTITION p1 SELECT ...
        String sql = "INSERT INTO tbl PARTITION p1 SELECT * FROM src";
        Object ctx = parsePartitionSpec(sql);

        InsertPartitionSpec spec = invokeParseInsertPartitionSpec(ctx);

        Assertions.assertFalse(spec.isStaticPartition());
        Assertions.assertTrue(spec.hasDynamicPartitionNames());
        Assertions.assertFalse(spec.isTemporary());
        Assertions.assertFalse(spec.isAutoDetect());
        Assertions.assertEquals(1, spec.getPartitionNames().size());
        Assertions.assertEquals("p1", spec.getPartitionNames().get(0));
    }

    @Test
    public void testParseDynamicPartitionList() throws Exception {
        // Parse: INSERT INTO tbl PARTITION (p1, p2, p3) SELECT ...
        String sql = "INSERT INTO tbl PARTITION (p1, p2, p3) SELECT * FROM src";
        Object ctx = parsePartitionSpec(sql);

        InsertPartitionSpec spec = invokeParseInsertPartitionSpec(ctx);

        Assertions.assertFalse(spec.isStaticPartition());
        Assertions.assertTrue(spec.hasDynamicPartitionNames());
        Assertions.assertFalse(spec.isTemporary());
        Assertions.assertFalse(spec.isAutoDetect());
        Assertions.assertEquals(3, spec.getPartitionNames().size());
        Assertions.assertTrue(spec.getPartitionNames().contains("p1"));
        Assertions.assertTrue(spec.getPartitionNames().contains("p2"));
        Assertions.assertTrue(spec.getPartitionNames().contains("p3"));
    }

    @Test
    public void testParseTemporaryPartition() throws Exception {
        // Parse: INSERT INTO tbl TEMPORARY PARTITION (p1) SELECT ...
        String sql = "INSERT INTO tbl TEMPORARY PARTITION (p1) SELECT * FROM src";
        Object ctx = parsePartitionSpec(sql);

        InsertPartitionSpec spec = invokeParseInsertPartitionSpec(ctx);

        Assertions.assertFalse(spec.isStaticPartition());
        Assertions.assertTrue(spec.hasDynamicPartitionNames());
        Assertions.assertTrue(spec.isTemporary());
        Assertions.assertFalse(spec.isAutoDetect());
    }

    @Test
    public void testParseStaticPartitionStringValue() throws Exception {
        // Parse: INSERT OVERWRITE TABLE tbl PARTITION (dt='2025-01-25') SELECT ...
        String sql = "INSERT OVERWRITE TABLE tbl PARTITION (dt='2025-01-25') SELECT * FROM src";
        Object ctx = parsePartitionSpec(sql);

        InsertPartitionSpec spec = invokeParseInsertPartitionSpec(ctx);

        Assertions.assertTrue(spec.isStaticPartition());
        Assertions.assertFalse(spec.hasDynamicPartitionNames());
        Assertions.assertFalse(spec.isTemporary());
        Assertions.assertFalse(spec.isAutoDetect());

        Map<String, Expression> staticValues = spec.getStaticPartitionValues();
        Assertions.assertEquals(1, staticValues.size());
        Assertions.assertTrue(staticValues.containsKey("dt"));
        Assertions.assertTrue(staticValues.get("dt") instanceof StringLikeLiteral);
        Assertions.assertEquals("2025-01-25", ((StringLikeLiteral) staticValues.get("dt")).getStringValue());
    }

    @Test
    public void testParseStaticPartitionMultipleValues() throws Exception {
        // Parse: INSERT OVERWRITE TABLE tbl PARTITION (dt='2025-01-25', region='bj') SELECT
        // ...
        String sql = "INSERT OVERWRITE TABLE tbl PARTITION (dt='2025-01-25', region='bj') SELECT * FROM src";
        Object ctx = parsePartitionSpec(sql);

        InsertPartitionSpec spec = invokeParseInsertPartitionSpec(ctx);

        Assertions.assertTrue(spec.isStaticPartition());
        Assertions.assertFalse(spec.hasDynamicPartitionNames());

        Map<String, Expression> staticValues = spec.getStaticPartitionValues();
        Assertions.assertEquals(2, staticValues.size());
        Assertions.assertTrue(staticValues.containsKey("dt"));
        Assertions.assertTrue(staticValues.containsKey("region"));
        Assertions.assertEquals("2025-01-25", ((StringLikeLiteral) staticValues.get("dt")).getStringValue());
        Assertions.assertEquals("bj", ((StringLikeLiteral) staticValues.get("region")).getStringValue());
    }

    @Test
    public void testParseStaticPartitionIntegerValue() throws Exception {
        // Parse: INSERT OVERWRITE TABLE tbl PARTITION (year=2025) SELECT ...
        String sql = "INSERT OVERWRITE TABLE tbl PARTITION (year=2025) SELECT * FROM src";
        Object ctx = parsePartitionSpec(sql);

        InsertPartitionSpec spec = invokeParseInsertPartitionSpec(ctx);

        Assertions.assertTrue(spec.isStaticPartition());

        Map<String, Expression> staticValues = spec.getStaticPartitionValues();
        Assertions.assertEquals(1, staticValues.size());
        Assertions.assertTrue(staticValues.containsKey("year"));
        // Note: The parser may parse integers as IntegerLikeLiteral
        Expression yearExpr = staticValues.get("year");
        Assertions.assertNotNull(yearExpr);
    }

    @Test
    public void testParseNoPartition() throws Exception {
        // Parse: INSERT INTO tbl SELECT ... (no partition clause)
        String sql = "INSERT INTO tbl SELECT * FROM src";
        Object ctx = parsePartitionSpec(sql);

        InsertPartitionSpec spec = invokeParseInsertPartitionSpec(ctx);

        // ctx should be null for no partition
        Assertions.assertFalse(spec.isStaticPartition());
        Assertions.assertFalse(spec.hasDynamicPartitionNames());
        Assertions.assertFalse(spec.isTemporary());
        Assertions.assertFalse(spec.isAutoDetect());
    }

    @Test
    public void testParseStaticPartitionMixedTypes() throws Exception {
        // Parse: INSERT OVERWRITE TABLE tbl PARTITION (year=2025, month='01', day=25) SELECT
        // ...
        String sql = "INSERT OVERWRITE TABLE tbl PARTITION (year=2025, month='01', day=25) SELECT * FROM src";
        Object ctx = parsePartitionSpec(sql);

        InsertPartitionSpec spec = invokeParseInsertPartitionSpec(ctx);

        Assertions.assertTrue(spec.isStaticPartition());
        Assertions.assertFalse(spec.hasDynamicPartitionNames());

        Map<String, Expression> staticValues = spec.getStaticPartitionValues();
        Assertions.assertEquals(3, staticValues.size());
        Assertions.assertTrue(staticValues.containsKey("year"));
        Assertions.assertTrue(staticValues.containsKey("month"));
        Assertions.assertTrue(staticValues.containsKey("day"));

        // month should be string
        Assertions.assertTrue(staticValues.get("month") instanceof StringLikeLiteral);
        Assertions.assertEquals("01", ((StringLikeLiteral) staticValues.get("month")).getStringValue());
    }
}
