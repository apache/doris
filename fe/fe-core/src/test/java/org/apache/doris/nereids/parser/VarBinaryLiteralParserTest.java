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

import org.apache.doris.nereids.analyzer.UnboundOneRowRelation;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.VarBinaryLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;

import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Dedicated tests for parsing VARBINARY literals (X'...').
 */
public class VarBinaryLiteralParserTest {

    static {
        ConnectContext ctx = new ConnectContext();
        new MockUp<ConnectContext>() {
            @Mock
            public ConnectContext get() {
                return ctx;
            }
        };
    }

    private VarBinaryLiteral extract(String sql) {
        NereidsParser parser = new NereidsParser();
        Plan plan = parser.parseSingle(sql);
        LogicalPlan child = (LogicalPlan) plan.child(0);
        UnboundOneRowRelation one = (UnboundOneRowRelation) child;
        Expression expr = one.getProjects().get(0).child(0);
        Assertions.assertInstanceOf(VarBinaryLiteral.class, expr);
        return (VarBinaryLiteral) expr;
    }

    @Test
    public void testBasic() {
        VarBinaryLiteral v = extract("SELECT X'AB'");
        byte[] bytes = (byte[]) v.getValue();
        Assertions.assertEquals(1, bytes.length);
        Assertions.assertEquals((byte) 0xAB, bytes[0]);
        Assertions.assertEquals("AB", v.toString());
    }

    @Test
    public void testLowerCaseHex() {
        VarBinaryLiteral v = extract("SELECT X'abcd'");
        Assertions.assertArrayEquals(new byte[]{(byte) 0xAB, (byte) 0xCD}, (byte[]) v.getValue());
        Assertions.assertEquals("ABCD", v.toString());
    }

    @Test
    public void testOddLength() {
        VarBinaryLiteral v = extract("SELECT X'F'"); // should be 0F
        Assertions.assertArrayEquals(new byte[]{(byte) 0x0F}, (byte[]) v.getValue());
        Assertions.assertEquals("0F", v.toString());
    }

    @Test
    public void testEmpty() {
        VarBinaryLiteral v = extract("SELECT X''");
        Assertions.assertEquals(0, ((byte[]) v.getValue()).length);
        Assertions.assertEquals("", v.toString());
    }

    @Test
    public void testComparison() {
        VarBinaryLiteral v1 = extract("SELECT X'AB'");
        VarBinaryLiteral v2 = extract("SELECT X'AB00'");
        Assertions.assertEquals(0, v1.compareTo(v2)); // trailing 00 considered equal
        VarBinaryLiteral v3 = extract("SELECT X'AC'");
        Assertions.assertTrue(v3.compareTo(v1) > 0);
    }

    @Test
    public void testInvalidChar() {
        NereidsParser parser = new NereidsParser();
        Plan plan = parser.parseSingle("SELECT X'AG'");
        LogicalPlan child = (LogicalPlan) plan.child(0);
        UnboundOneRowRelation one = (UnboundOneRowRelation) child;
        Expression expr = one.getProjects().get(0).child(0);
        Assertions.assertFalse(expr instanceof VarBinaryLiteral, "Should NOT parse into VarBinaryLiteral for invalid hex");
        Assertions.assertTrue(expr instanceof UnboundSlot, "Expected fallback to UnboundSlot when hex content invalid");
    }

    @Test
    public void testInvalidChar2() {
        NereidsParser parser = new NereidsParser();
        Plan plan = parser.parseSingle("SELECT X'Z1'");
        LogicalPlan child = (LogicalPlan) plan.child(0);
        UnboundOneRowRelation one = (UnboundOneRowRelation) child;
        Expression expr = one.getProjects().get(0).child(0);
        Assertions.assertFalse(expr instanceof VarBinaryLiteral);
        Assertions.assertTrue(expr instanceof UnboundSlot);
    }

    @Test
    public void testSingleByte() {
        VarBinaryLiteral v = extract("SELECT X'01'");
        Assertions.assertArrayEquals(new byte[]{0x01}, (byte[]) v.getValue());
    }

    @Test
    public void testCreateTableVarbinaryDirect() {
        // expect parse or analysis failure (depending on where VARBINARY is rejected)
        Assertions.assertThrows(Throwable.class, () -> new NereidsParser().parseSingle(
                "CREATE TABLE t_vb (k1 INT, vb VARBINARY) DISTRIBUTED BY HASH(k1) BUCKETS 1"));
    }

    @Test
    public void testCreateTableVarbinaryWithLength() {
        Assertions.assertThrows(Throwable.class, () -> new NereidsParser().parseSingle(
                "CREATE TABLE t_vb2 (k1 INT, vb VARBINARY(10)) DISTRIBUTED BY HASH(k1) BUCKETS 1"));
    }

    @Test
    public void testCreateTableVarbinaryAsKey() {
        Assertions.assertThrows(Throwable.class, () -> new NereidsParser().parseSingle(
                "CREATE TABLE t_vb3 (vb VARBINARY, v2 INT) DISTRIBUTED BY HASH(vb) BUCKETS 1"));
    }

    @Test
    public void testCreateTableVarbinaryInComplex() {
        Assertions.assertThrows(Throwable.class, () -> new NereidsParser().parseSingle(
                "CREATE TABLE t_vb4 (id INT, arr ARRAY<VARBINARY>) DISTRIBUTED BY HASH(id) BUCKETS 1"));
    }

    @Test
    public void testCreateTableVarbinaryPartition() {
        Assertions.assertThrows(Throwable.class, () -> new NereidsParser().parseSingle(
                "CREATE TABLE t_vb5 (k1 INT, vb VARBINARY) PARTITION BY RANGE(k1)() DISTRIBUTED BY HASH(k1) BUCKETS 1"));
    }

    @Test
    public void testAlterAddVarbinary() {
        // Even adding via ALTER should fail
        Assertions.assertThrows(Throwable.class, () -> new NereidsParser().parseSingle(
                "ALTER TABLE some_tbl ADD COLUMN vb VARBINARY"));
    }

    @Test
    public void testAlterModifyToVarbinary() {
        Assertions.assertThrows(Throwable.class, () -> new NereidsParser().parseSingle(
                "ALTER TABLE some_tbl MODIFY COLUMN c1 VARBINARY"));
    }

    @Test
    public void testCreateTableLikeWithVarbinary() {
        // Statement referencing VARBINARY in a like/replace clause
        Assertions.assertThrows(Throwable.class, () -> new NereidsParser().parseSingle(
                "CREATE TABLE t_vb6 LIKE base_tbl PROPERTIES('replace_columns'='vb VARBINARY')"));
    }
}
