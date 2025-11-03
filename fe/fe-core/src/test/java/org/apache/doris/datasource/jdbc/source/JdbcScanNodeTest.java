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

package org.apache.doris.datasource.jdbc.source;

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.BinaryPredicate.Operator;
import org.apache.doris.analysis.BoolLiteral;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.analysis.FloatLiteral;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.IsNullPredicate;
import org.apache.doris.analysis.LikePredicate;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.catalog.JdbcTable;
import org.apache.doris.catalog.Type;
import org.apache.doris.thrift.TOdbcTableType;

import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

public class JdbcScanNodeTest {

    @Mocked
    private JdbcTable mockTable;

    @Test
    public void testSimpleBinaryPredicate() {
        new Expectations() {{
                mockTable.getProperRemoteColumnName((TOdbcTableType) any, anyString);
                result = new mockit.Delegate() {
                    String getProperColumnName(TOdbcTableType tableType, String colName) {
                        return "\"" + colName + "\"";
                    }
                };
            }};

        SlotRef idSlot = new SlotRef(null, "ID");
        IntLiteral intLiteral = new IntLiteral(1);
        BinaryPredicate predicate = new BinaryPredicate(Operator.EQ, idSlot, intLiteral);

        String result = JdbcScanNode.conjunctExprToString(TOdbcTableType.MYSQL, predicate, mockTable);
        Assert.assertEquals("(\"ID\" = 1)", result);

        result = JdbcScanNode.conjunctExprToString(TOdbcTableType.ORACLE, predicate, mockTable);
        Assert.assertEquals("(\"ID\" = 1)", result);
    }

    @Test
    public void testSimpleCompoundPredicate() {
        new Expectations() {{
                mockTable.getProperRemoteColumnName((TOdbcTableType) any, anyString);
                result = new mockit.Delegate() {
                    String getProperColumnName(TOdbcTableType tableType, String colName) {
                        return "\"" + colName + "\"";
                    }
                };
            }};

        SlotRef idSlot = new SlotRef(null, "ID");
        IntLiteral intLiteral = new IntLiteral(1);
        BinaryPredicate leftPred = new BinaryPredicate(Operator.EQ, idSlot, intLiteral);

        SlotRef nameSlot = new SlotRef(null, "NAME");
        StringLiteral stringLiteral = new StringLiteral("test");
        BinaryPredicate rightPred = new BinaryPredicate(Operator.EQ, nameSlot, stringLiteral);

        CompoundPredicate compoundPredicate = new CompoundPredicate(CompoundPredicate.Operator.OR, leftPred, rightPred);

        String result = JdbcScanNode.conjunctExprToString(TOdbcTableType.MYSQL, compoundPredicate, mockTable);
        Assert.assertEquals("((\"ID\" = 1) OR (\"NAME\" = 'test'))", result);
    }

    @Test
    public void testNestedCompoundPredicate() {
        new Expectations() {{
                mockTable.getProperRemoteColumnName((TOdbcTableType) any, anyString);
                result = new mockit.Delegate() {
                    String getProperColumnName(TOdbcTableType tableType, String colName) {
                        return "\"" + colName + "\"";
                    }
                };
            }};

        // ID = 1 OR (NAME = 'test' AND AGE > 18)
        SlotRef idSlot = new SlotRef(null, "ID");
        IntLiteral intLiteral = new IntLiteral(1);
        BinaryPredicate leftPred = new BinaryPredicate(Operator.EQ, idSlot, intLiteral);

        SlotRef nameSlot = new SlotRef(null, "NAME");
        StringLiteral stringLiteral = new StringLiteral("test");
        BinaryPredicate namePred = new BinaryPredicate(Operator.EQ, nameSlot, stringLiteral);

        SlotRef ageSlot = new SlotRef(null, "AGE");
        IntLiteral ageLiteral = new IntLiteral(18);
        BinaryPredicate agePred = new BinaryPredicate(Operator.GT, ageSlot, ageLiteral);

        CompoundPredicate innerComp = new CompoundPredicate(CompoundPredicate.Operator.AND, namePred, agePred);
        CompoundPredicate outerComp = new CompoundPredicate(CompoundPredicate.Operator.OR, leftPred, innerComp);

        String result = JdbcScanNode.conjunctExprToString(TOdbcTableType.MYSQL, outerComp, mockTable);
        Assert.assertEquals("((\"ID\" = 1) OR ((\"NAME\" = 'test') AND (\"AGE\" > 18)))", result);
    }

    @Test
    public void testComplexNestedCompoundPredicate() {

        new Expectations() {{
                mockTable.getProperRemoteColumnName((TOdbcTableType) any, anyString);
                result = new mockit.Delegate() {
                    String getProperColumnName(TOdbcTableType tableType, String colName) {
                        return "\"" + colName + "\"";
                    }
                };
            }};

        // (ID = 1 OR NAME = 'test') AND (AGE > 18 OR DEPT = 'HR')
        SlotRef idSlot = new SlotRef(null, "ID");
        IntLiteral intLiteral = new IntLiteral(1);
        BinaryPredicate idPred = new BinaryPredicate(Operator.EQ, idSlot, intLiteral);

        SlotRef nameSlot = new SlotRef(null, "NAME");
        StringLiteral nameLiteral = new StringLiteral("test");
        BinaryPredicate namePred = new BinaryPredicate(Operator.EQ, nameSlot, nameLiteral);

        SlotRef ageSlot = new SlotRef(null, "AGE");
        IntLiteral ageLiteral = new IntLiteral(18);
        BinaryPredicate agePred = new BinaryPredicate(Operator.GT, ageSlot, ageLiteral);

        SlotRef deptSlot = new SlotRef(null, "DEPT");
        StringLiteral deptLiteral = new StringLiteral("HR");
        BinaryPredicate deptPred = new BinaryPredicate(Operator.EQ, deptSlot, deptLiteral);

        CompoundPredicate leftComp = new CompoundPredicate(CompoundPredicate.Operator.OR, idPred, namePred);
        CompoundPredicate rightComp = new CompoundPredicate(CompoundPredicate.Operator.OR, agePred, deptPred);
        CompoundPredicate outerComp = new CompoundPredicate(CompoundPredicate.Operator.AND, leftComp, rightComp);

        String result = JdbcScanNode.conjunctExprToString(TOdbcTableType.MYSQL, outerComp, mockTable);
        Assert.assertEquals("(((\"ID\" = 1) OR (\"NAME\" = 'test')) AND ((\"AGE\" > 18) OR (\"DEPT\" = 'HR')))", result);
    }

    @Test
    public void testDateLiteralOracle() throws Exception {
        new Expectations() {{
                mockTable.getProperRemoteColumnName((TOdbcTableType) any, anyString);
                result = new mockit.Delegate() {
                    String getProperColumnName(TOdbcTableType tableType, String colName) {
                        return "\"" + colName + "\"";
                    }
                };
            }};

        DateLiteral dateLiteral = new DateLiteral("2023-01-01 12:34:56", Type.DATETIME);

        SlotRef dateSlot = new SlotRef(null, "CREATE_TIME");
        BinaryPredicate datePred = new BinaryPredicate(Operator.GE, dateSlot, dateLiteral);

        String result = JdbcScanNode.conjunctExprToString(TOdbcTableType.ORACLE, datePred, mockTable);
        Assert.assertTrue(result.contains("to_date('2023-01-01 12:34:56', 'yyyy-mm-dd hh24:mi:ss')"));
        Assert.assertTrue(result.startsWith("\"CREATE_TIME\" >= "));
    }

    @Test
    public void testDateLiteralTrino() throws Exception {
        new Expectations() {{
                mockTable.getProperRemoteColumnName((TOdbcTableType) any, anyString);
                result = new mockit.Delegate() {
                    String getProperColumnName(TOdbcTableType tableType, String colName) {
                        return "\"" + colName + "\"";
                    }
                };
            }};

        DateLiteral dateLiteral = new DateLiteral("2023-01-01 12:34:56", Type.DATETIME);

        SlotRef dateSlot = new SlotRef(null, "CREATE_TIME");
        BinaryPredicate datePred = new BinaryPredicate(Operator.GE, dateSlot, dateLiteral);

        String result = JdbcScanNode.conjunctExprToString(TOdbcTableType.TRINO, datePred, mockTable);
        Assert.assertTrue(result.contains("timestamp '2023-01-01 12:34:56'"));
        Assert.assertTrue(result.startsWith("\"CREATE_TIME\" >= "));
    }

    @Test
    public void testDateLiteralCompoundPredicateOracle() throws Exception {
        new Expectations() {{
                mockTable.getProperRemoteColumnName((TOdbcTableType) any, anyString);
                result = new mockit.Delegate() {
                    String getProperColumnName(TOdbcTableType tableType, String colName) {
                        return "\"" + colName + "\"";
                    }
                };
            }};

        // ID = 1 OR CREATE_TIME >= '2023-01-01'
        SlotRef idSlot = new SlotRef(null, "ID");
        IntLiteral intLiteral = new IntLiteral(1);
        BinaryPredicate idPred = new BinaryPredicate(Operator.EQ, idSlot, intLiteral);

        DateLiteral dateLiteral = new DateLiteral("2023-01-01 12:34:56", Type.DATETIME);

        SlotRef dateSlot = new SlotRef(null, "CREATE_TIME");
        BinaryPredicate datePred = new BinaryPredicate(Operator.GE, dateSlot, dateLiteral);

        CompoundPredicate compPred = new CompoundPredicate(CompoundPredicate.Operator.OR, idPred, datePred);

        String result = JdbcScanNode.conjunctExprToString(TOdbcTableType.ORACLE, compPred, mockTable);
        Assert.assertTrue(result.contains("to_date('2023-01-01 12:34:56', 'yyyy-mm-dd hh24:mi:ss')"));
        Assert.assertTrue(result.contains("\"ID\" = 1"));
        Assert.assertTrue(result.contains(" OR "));
    }

    @Test
    public void testBoolLiteral() {
        // 1 = 1 (true literal)
        BoolLiteral boolLiteral = new BoolLiteral(true);

        String result = JdbcScanNode.conjunctExprToString(TOdbcTableType.MYSQL, boolLiteral, mockTable);
        Assert.assertEquals("1 = 1", result);
    }

    @Test
    public void testComplexPredicateWithDateComparisons() throws Exception {
        new Expectations() {{
                mockTable.getProperRemoteColumnName((TOdbcTableType) any, anyString);
                result = new mockit.Delegate() {
                    String getProperColumnName(TOdbcTableType tableType, String colName) {
                        return "\"" + colName + "\"";
                    }
                };
            }};

        // (ID = 1 OR NAME = 'test') AND (CREATE_TIME >= '2023-01-01' AND UPDATE_TIME <= '2023-12-31')
        SlotRef idSlot = new SlotRef(null, "ID");
        IntLiteral intLiteral = new IntLiteral(1);
        BinaryPredicate idPred = new BinaryPredicate(Operator.EQ, idSlot, intLiteral);

        SlotRef nameSlot = new SlotRef(null, "NAME");
        StringLiteral nameLiteral = new StringLiteral("test");
        BinaryPredicate namePred = new BinaryPredicate(Operator.EQ, nameSlot, nameLiteral);

        DateLiteral startDateLiteral = new DateLiteral("2023-01-01 00:00:00", Type.DATETIME);
        DateLiteral endDateLiteral = new DateLiteral("2023-12-31 23:59:59", Type.DATETIME);

        SlotRef createTimeSlot = new SlotRef(null, "CREATE_TIME");
        BinaryPredicate createTimePred = new BinaryPredicate(Operator.GE, createTimeSlot, startDateLiteral);

        SlotRef updateTimeSlot = new SlotRef(null, "UPDATE_TIME");
        BinaryPredicate updateTimePred = new BinaryPredicate(Operator.LE, updateTimeSlot, endDateLiteral);

        CompoundPredicate leftComp = new CompoundPredicate(CompoundPredicate.Operator.OR, idPred, namePred);
        CompoundPredicate rightComp = new CompoundPredicate(CompoundPredicate.Operator.AND, createTimePred, updateTimePred);
        CompoundPredicate outerComp = new CompoundPredicate(CompoundPredicate.Operator.AND, leftComp, rightComp);

        // Test for Oracle
        String oracleResult = JdbcScanNode.conjunctExprToString(TOdbcTableType.ORACLE, outerComp, mockTable);
        Assert.assertTrue(oracleResult.contains("to_date('2023-01-01 00:00:00', 'yyyy-mm-dd hh24:mi:ss')"));
        Assert.assertTrue(oracleResult.contains("to_date('2023-12-31 23:59:59', 'yyyy-mm-dd hh24:mi:ss')"));

        // Test for Trino
        String trinoResult = JdbcScanNode.conjunctExprToString(TOdbcTableType.TRINO, outerComp, mockTable);
        Assert.assertTrue(trinoResult.contains("timestamp '2023-01-01 00:00:00'"));
        Assert.assertTrue(trinoResult.contains("timestamp '2023-12-31 23:59:59'"));

        // Test for MySQL (no special date formatting)
        String mysqlResult = JdbcScanNode.conjunctExprToString(TOdbcTableType.MYSQL, outerComp, mockTable);
        Assert.assertTrue(mysqlResult.contains("'2023-01-01 00:00:00'"));
        Assert.assertTrue(mysqlResult.contains("'2023-12-31 23:59:59'"));
    }

    @Test
    public void testDifferentComparisonOperators() {
        new Expectations() {{
                mockTable.getProperRemoteColumnName((TOdbcTableType) any, anyString);
                result = new mockit.Delegate() {
                    String getProperColumnName(TOdbcTableType tableType, String colName) {
                        return "\"" + colName + "\"";
                    }
                };
            }};

        SlotRef ageSlot = new SlotRef(null, "AGE");
        IntLiteral ageLiteral = new IntLiteral(30);

        // AGE < 30
        BinaryPredicate ltPred = new BinaryPredicate(Operator.LT, ageSlot, ageLiteral);
        String result = JdbcScanNode.conjunctExprToString(TOdbcTableType.MYSQL, ltPred, mockTable);
        Assert.assertEquals("(\"AGE\" < 30)", result);

        // AGE <= 30
        BinaryPredicate lePred = new BinaryPredicate(Operator.LE, ageSlot, ageLiteral);
        result = JdbcScanNode.conjunctExprToString(TOdbcTableType.MYSQL, lePred, mockTable);
        Assert.assertEquals("(\"AGE\" <= 30)", result);

        // AGE > 30
        BinaryPredicate gtPred = new BinaryPredicate(Operator.GT, ageSlot, ageLiteral);
        result = JdbcScanNode.conjunctExprToString(TOdbcTableType.MYSQL, gtPred, mockTable);
        Assert.assertEquals("(\"AGE\" > 30)", result);

        // AGE >= 30
        BinaryPredicate gePred = new BinaryPredicate(Operator.GE, ageSlot, ageLiteral);
        result = JdbcScanNode.conjunctExprToString(TOdbcTableType.MYSQL, gePred, mockTable);
        Assert.assertEquals("(\"AGE\" >= 30)", result);

        // AGE != 30
        BinaryPredicate nePred = new BinaryPredicate(Operator.NE, ageSlot, ageLiteral);
        result = JdbcScanNode.conjunctExprToString(TOdbcTableType.MYSQL, nePred, mockTable);
        Assert.assertEquals("(\"AGE\" != 30)", result);
    }

    @Test
    public void testIsNullPredicates() {
        new Expectations() {{
                mockTable.getProperRemoteColumnName((TOdbcTableType) any, anyString);
                result = new mockit.Delegate() {
                    String getProperColumnName(TOdbcTableType tableType, String colName) {
                        return "\"" + colName + "\"";
                    }
                };
            }};

        // NAME IS NULL
        SlotRef nameSlot = new SlotRef(null, "NAME");
        IsNullPredicate isNullPred = new IsNullPredicate(nameSlot, false);
        String result = JdbcScanNode.conjunctExprToString(TOdbcTableType.MYSQL, isNullPred, mockTable);
        Assert.assertEquals("\"NAME\" IS NULL", result);

        // NAME IS NOT NULL
        IsNullPredicate isNotNullPred = new IsNullPredicate(nameSlot, true);
        result = JdbcScanNode.conjunctExprToString(TOdbcTableType.MYSQL, isNotNullPred, mockTable);
        Assert.assertEquals("\"NAME\" IS NOT NULL", result);
    }

    @Test
    public void testCompoundIsNullPredicates() {
        new Expectations() {{
                mockTable.getProperRemoteColumnName((TOdbcTableType) any, anyString);
                result = new mockit.Delegate() {
                    String getProperColumnName(TOdbcTableType tableType, String colName) {
                        return "\"" + colName + "\"";
                    }
                };
            }};

        // ID = 1 AND NAME IS NULL
        SlotRef idSlot = new SlotRef(null, "ID");
        IntLiteral intLiteral = new IntLiteral(1);
        BinaryPredicate idPred = new BinaryPredicate(Operator.EQ, idSlot, intLiteral);

        SlotRef nameSlot = new SlotRef(null, "NAME");
        IsNullPredicate isNullPred = new IsNullPredicate(nameSlot, false);

        CompoundPredicate compPred = new CompoundPredicate(CompoundPredicate.Operator.AND, idPred, isNullPred);

        String result = JdbcScanNode.conjunctExprToString(TOdbcTableType.MYSQL, compPred, mockTable);
        Assert.assertEquals("((\"ID\" = 1) AND \"NAME\" IS NULL)", result);
    }

    @Test
    public void testLikePredicates() {
        new Expectations() {{
                mockTable.getProperRemoteColumnName((TOdbcTableType) any, anyString);
                result = new mockit.Delegate() {
                    String getProperColumnName(TOdbcTableType tableType, String colName) {
                        return "\"" + colName + "\"";
                    }
                };
            }};

        // NAME LIKE 'test%'
        SlotRef nameSlot = new SlotRef(null, "NAME");
        StringLiteral pattern = new StringLiteral("test%");
        LikePredicate likePred = new LikePredicate(LikePredicate.Operator.LIKE, nameSlot, pattern);

        String result = JdbcScanNode.conjunctExprToString(TOdbcTableType.MYSQL, likePred, mockTable);
        Assert.assertEquals("\"NAME\" LIKE 'test%'", result);

    }

    @Test
    public void testFloatLiteral() {
        new Expectations() {{
                mockTable.getProperRemoteColumnName((TOdbcTableType) any, anyString);
                result = new mockit.Delegate() {
                    String getProperColumnName(TOdbcTableType tableType, String colName) {
                        return "\"" + colName + "\"";
                    }
                };
            }};

        // SALARY > 5000.50
        SlotRef salarySlot = new SlotRef(null, "SALARY");
        FloatLiteral floatLiteral = new FloatLiteral(5000.50);
        BinaryPredicate salaryPred = new BinaryPredicate(Operator.GT, salarySlot, floatLiteral);

        String result = JdbcScanNode.conjunctExprToString(TOdbcTableType.MYSQL, salaryPred, mockTable);
        Assert.assertEquals("(\"SALARY\" > 5000.5)", result);
    }

    @Test
    public void testVeryComplexNestedPredicate() {

        new Expectations() {{
                mockTable.getProperRemoteColumnName((TOdbcTableType) any, anyString);
                result = new mockit.Delegate() {
                    String getProperColumnName(TOdbcTableType tableType, String colName) {
                        return "\"" + colName + "\"";
                    }
                };
            }};

        // (ID > 10 AND ID < 100) OR (NAME LIKE 'test%' AND (DEPT = 'HR' OR SALARY > 5000.0))
        SlotRef idSlot = new SlotRef(null, "ID");
        IntLiteral id10 = new IntLiteral(10);
        IntLiteral id100 = new IntLiteral(100);
        BinaryPredicate idGt10 = new BinaryPredicate(Operator.GT, idSlot, id10);
        BinaryPredicate idLt100 = new BinaryPredicate(Operator.LT, idSlot, id100);

        CompoundPredicate idRange = new CompoundPredicate(CompoundPredicate.Operator.AND, idGt10, idLt100);

        SlotRef nameSlot = new SlotRef(null, "NAME");
        StringLiteral pattern = new StringLiteral("test%");
        LikePredicate nameLike = new LikePredicate(LikePredicate.Operator.LIKE, nameSlot, pattern);

        SlotRef deptSlot = new SlotRef(null, "DEPT");
        StringLiteral deptLiteral = new StringLiteral("HR");
        BinaryPredicate deptEq = new BinaryPredicate(Operator.EQ, deptSlot, deptLiteral);

        SlotRef salarySlot = new SlotRef(null, "SALARY");
        FloatLiteral salaryLiteral = new FloatLiteral(5000.0);
        BinaryPredicate salaryGt = new BinaryPredicate(Operator.GT, salarySlot, salaryLiteral);

        CompoundPredicate deptOrSalary = new CompoundPredicate(CompoundPredicate.Operator.OR, deptEq, salaryGt);
        CompoundPredicate nameAndDeptSalary = new CompoundPredicate(CompoundPredicate.Operator.AND, nameLike, deptOrSalary);

        CompoundPredicate finalPred = new CompoundPredicate(CompoundPredicate.Operator.OR, idRange, nameAndDeptSalary);

        String result = JdbcScanNode.conjunctExprToString(TOdbcTableType.MYSQL, finalPred, mockTable);
        Assert.assertEquals("(((\"ID\" > 10) AND (\"ID\" < 100)) OR (\"NAME\" LIKE 'test%' AND ((\"DEPT\" = 'HR') OR (\"SALARY\" > 5000))))", result);
    }

    @Test
    public void testTripleNestedCompoundPredicate() {

        new Expectations() {{
                mockTable.getProperRemoteColumnName((TOdbcTableType) any, anyString);
                result = new mockit.Delegate() {
                    String getProperColumnName(TOdbcTableType tableType, String colName) {
                        return "\"" + colName + "\"";
                    }
                };
            }};

        // (ID = 1 OR (NAME = 'test' AND (AGE > 18 OR DEPT = 'HR')))
        SlotRef idSlot = new SlotRef(null, "ID");
        IntLiteral intLiteral = new IntLiteral(1);
        BinaryPredicate idPred = new BinaryPredicate(Operator.EQ, idSlot, intLiteral);

        SlotRef nameSlot = new SlotRef(null, "NAME");
        StringLiteral nameLiteral = new StringLiteral("test");
        BinaryPredicate namePred = new BinaryPredicate(Operator.EQ, nameSlot, nameLiteral);

        SlotRef ageSlot = new SlotRef(null, "AGE");
        IntLiteral ageLiteral = new IntLiteral(18);
        BinaryPredicate agePred = new BinaryPredicate(Operator.GT, ageSlot, ageLiteral);

        SlotRef deptSlot = new SlotRef(null, "DEPT");
        StringLiteral deptLiteral = new StringLiteral("HR");
        BinaryPredicate deptPred = new BinaryPredicate(Operator.EQ, deptSlot, deptLiteral);

        CompoundPredicate innerComp = new CompoundPredicate(CompoundPredicate.Operator.OR, agePred, deptPred);
        CompoundPredicate middleComp = new CompoundPredicate(CompoundPredicate.Operator.AND, namePred, innerComp);
        CompoundPredicate outerComp = new CompoundPredicate(CompoundPredicate.Operator.OR, idPred, middleComp);

        String result = JdbcScanNode.conjunctExprToString(TOdbcTableType.MYSQL, outerComp, mockTable);
        Assert.assertEquals("((\"ID\" = 1) OR ((\"NAME\" = 'test') AND ((\"AGE\" > 18) OR (\"DEPT\" = 'HR'))))", result);
    }
}
