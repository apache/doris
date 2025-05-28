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

package org.apache.doris.plugin.audit;

import org.apache.doris.analysis.EmptyStmt;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.plugin.AuditEvent;
import org.apache.doris.qe.AuditLogHelper;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.GlobalVariable;

import org.junit.Assert;
import org.junit.Test;

public class AuditLogBuilderTest {

    @Test
    public void testTimestampOutput() {
        AuditLogBuilder auditLogBuilder = new AuditLogBuilder();
        // 1 set a valid value
        long currentTime = 1741760376000L;
        AuditEvent auditEvent = new AuditEvent.AuditEventBuilder()
                .setTimestamp(currentTime).build();
        String result = Deencapsulation.invoke(auditLogBuilder, "getAuditLogString", auditEvent);
        Assert.assertTrue(result.contains("Timestamp=2025-03-12 14:19:36.000"));

        // 2 not set value
        auditEvent = new AuditEvent.AuditEventBuilder().build();
        result = Deencapsulation.invoke(auditLogBuilder, "getAuditLogString", auditEvent);
        Assert.assertTrue(result.contains("Timestamp=\\N"));
    }

    @Test
    public void testHandleStmtTruncationForNonInsertStmt() {
        // Save original values
        int originalMaxSqlLength = GlobalVariable.auditPluginMaxSqlLength;

        try {
            // Set smaller max length for testing
            GlobalVariable.auditPluginMaxSqlLength = 50;

            // Use EmptyStmt as representative of non-INSERT statements
            StatementBase nonInsertStmt = new EmptyStmt();

            // 1. Test null input
            String result = AuditLogHelper.handleStmt(null, nonInsertStmt);
            Assert.assertNull(result);

            // 2. Test short statement not truncated
            String shortStmt = "SELECT * FROM table1";
            result = AuditLogHelper.handleStmt(shortStmt, nonInsertStmt);
            Assert.assertEquals(shortStmt, result);

            // 3. Test long statement truncated (using audit_plugin_max_sql_length)
            String longStmt
                    = "SELECT * FROM very_long_table_name_that_exceeds_the_maximum_length_limit_for_audit_log";
            result = AuditLogHelper.handleStmt(longStmt, nonInsertStmt);
            Assert.assertTrue("Result should contain truncation message",
                    result.contains("/* truncated audit_plugin_max_sql_length=50 */"));
            Assert.assertTrue("Result should be shorter than original",
                    result.length() < longStmt.length() + 100); // Add length for truncation message

            // 4. Test statement with newlines, tabs, carriage returns
            String stmtWithSpecialChars = "SELECT *\nFROM table1\tWHERE id = 1\r";
            result = AuditLogHelper.handleStmt(stmtWithSpecialChars, nonInsertStmt);
            Assert.assertTrue("Should escape newlines", result.contains("\\n"));
            Assert.assertTrue("Should escape tabs", result.contains("\\t"));
            Assert.assertTrue("Should escape carriage returns", result.contains("\\r"));
            Assert.assertFalse("Should not contain actual newlines", result.contains("\n"));
            Assert.assertFalse("Should not contain actual tabs", result.contains("\t"));
            Assert.assertFalse("Should not contain actual carriage returns", result.contains("\r"));

            // 5. Test long statement with Chinese characters truncation
            String chineseStmt
                    = "SELECT * FROM 表名很长的中文表名字符测试表名很长的中文表名字符测试表名很长的中文表名字符测试";
            result = AuditLogHelper.handleStmt(chineseStmt, nonInsertStmt);
            Assert.assertTrue("Should contain truncation message for Chinese text",
                    result.contains("/* truncated audit_plugin_max_sql_length=50 */"));

            // 6. Test boundary case: exactly equal to max length
            // Create a string exactly equal to max length
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 50; i++) {
                sb.append("a");
            }
            String exactLengthStmt = sb.toString();
            result = AuditLogHelper.handleStmt(exactLengthStmt, nonInsertStmt);
            Assert.assertEquals("Should not be truncated when exactly at limit", exactLengthStmt, result);

            // 7. Test boundary case: exceeding max length by 1 character
            sb = new StringBuilder();
            for (int i = 0; i < 51; i++) {
                sb.append("a");
            }
            String overLimitStmt = sb.toString();
            result = AuditLogHelper.handleStmt(overLimitStmt, nonInsertStmt);
            Assert.assertTrue("Should be truncated when over limit by 1 char",
                    result.contains("/* truncated audit_plugin_max_sql_length=50 */"));

            // 8. Test empty string
            String emptyStmt = "";
            result = AuditLogHelper.handleStmt(emptyStmt, nonInsertStmt);
            Assert.assertEquals("Empty string should remain empty", "", result);

            // 9. Test statement with only special characters
            String specialCharsStmt = "\n\t\r\n\t\r";
            result = AuditLogHelper.handleStmt(specialCharsStmt, nonInsertStmt);
            Assert.assertEquals("Should escape all special characters", "\\n\\t\\r\\n\\t\\r", result);

        } finally {
            // Restore original values
            GlobalVariable.auditPluginMaxSqlLength = originalMaxSqlLength;
        }
    }

    @Test
    public void testHandleStmtTruncationForInsertStmt() {
        // Save original values
        int originalMaxSqlLength = GlobalVariable.auditPluginMaxSqlLength;
        int originalMaxInsertStmtLength = GlobalVariable.auditPluginMaxInsertStmtLength;
        ConnectContext ctx = new ConnectContext();
        ctx.changeDefaultCatalog("internal");
        ctx.setDatabase("db1");
        try {
            ctx.setThreadLocalInfo();
            // Set different length limits to test INSERT statement special handling
            GlobalVariable.auditPluginMaxSqlLength = 200;        // Regular statement limit
            GlobalVariable.auditPluginMaxInsertStmtLength = 80;  // INSERT statement limit (smaller)

            NereidsParser parser = new NereidsParser();

            // 1. Test real INSERT statement correctly identified and uses INSERT-specific length limit
            String longInsertStmt
                    = "INSERT INTO table_with_very_long_name VALUES (1, 'test_value_1'), (2, 'test_value_2'), (3, 'test_value_3')";
            StatementBase insertStmt = parser.parseSQL(longInsertStmt).get(0);
            String result = AuditLogHelper.handleStmt(longInsertStmt, insertStmt);

            // Should use audit_plugin_max_insert_stmt_length=80 for truncation
            Assert.assertTrue("Should contain insert stmt length truncation message", 
                result.contains("/* truncated audit_plugin_max_insert_stmt_length=80 */"));
            Assert.assertFalse("Should not contain sql length truncation message", 
                result.contains("/* truncated audit_plugin_max_sql_length="));

            // 2. Test short INSERT statement not truncated
            String shortInsertStmt = "INSERT INTO tbl VALUES (1, 'a')";
            insertStmt = parser.parseSQL(shortInsertStmt).get(0);
            result = AuditLogHelper.handleStmt(shortInsertStmt, insertStmt);

            // Should not be truncated, and special characters should be properly escaped
            Assert.assertFalse("Short INSERT should not be truncated", 
                result.contains("/* truncated"));
            Assert.assertEquals("Short INSERT should remain unchanged", shortInsertStmt, result);

            // 3. Test special character handling in INSERT statements
            String insertWithSpecialChars = "INSERT INTO tbl\nVALUES\t(1,\r'test')";
            insertStmt = parser.parseSQL(insertWithSpecialChars).get(0);
            result = AuditLogHelper.handleStmt(insertWithSpecialChars, insertStmt);

            // Verify special characters are properly escaped
            Assert.assertTrue("Should escape newlines in INSERT", result.contains("\\n"));
            Assert.assertTrue("Should escape tabs in INSERT", result.contains("\\t"));
            Assert.assertTrue("Should escape carriage returns in INSERT", result.contains("\\r"));
            Assert.assertFalse("Should not contain actual newlines", result.contains("\n"));
            Assert.assertFalse("Should not contain actual tabs", result.contains("\t"));
            Assert.assertFalse("Should not contain actual carriage returns", result.contains("\r"));

            // 4. Test comparison: same length statements, different handling for INSERT vs non-INSERT
            // Create a statement with length between 80-200
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 120; i++) {
                sb.append("x");
            }
            String testContent = sb.toString();

            // INSERT statement
            String insertStmtStr = "INSERT INTO tbl VALUES (1, '" + testContent + "')";
            StatementBase parsedInsertStmt = parser.parseSQL(insertStmtStr).get(0);
            String insertResult = AuditLogHelper.handleStmt(insertStmtStr, parsedInsertStmt);

            // Non-INSERT statement
            String selectStmt = "SELECT '" + testContent + "' FROM tbl";
            StatementBase parsedSelectStmt = parser.parseSQL(selectStmt).get(0);
            String selectResult = AuditLogHelper.handleStmt(selectStmt, parsedSelectStmt);

            // INSERT should be truncated (using limit of 80)
            Assert.assertTrue("INSERT should be truncated with insert length limit", 
                insertResult.contains("/* truncated audit_plugin_max_insert_stmt_length=80 */"));

            // SELECT should not be truncated (using limit of 200)
            Assert.assertFalse("SELECT should not be truncated with sql length limit", 
                selectResult.contains("/* truncated"));

            // 5. Test boundary case: INSERT statement exactly equal to limit length
            // Create a statement exactly equal to INSERT limit length
            sb = new StringBuilder("INSERT INTO tbl VALUES (1, '");
            while (sb.length() < 78) { // Leave 2 characters for ending ')
                sb.append("a");
            }
            sb.append("')");
            String exactLengthInsert = sb.toString();

            insertStmt = parser.parseSQL(exactLengthInsert).get(0);
            result = AuditLogHelper.handleStmt(exactLengthInsert, insertStmt);

            // Should not be truncated
            Assert.assertFalse("INSERT at exact limit should not be truncated", 
                result.contains("/* truncated"));
            Assert.assertEquals("INSERT at exact limit should remain unchanged", 
                exactLengthInsert, result);

        } finally {
            // Restore original values
            GlobalVariable.auditPluginMaxSqlLength = originalMaxSqlLength;
            GlobalVariable.auditPluginMaxInsertStmtLength = originalMaxInsertStmtLength;
            ConnectContext.remove();
        }
    }

    @Test
    public void testHandleStmtTruncationWithDifferentLengths() {
        // Save original values
        int originalMaxSqlLength = GlobalVariable.auditPluginMaxSqlLength;

        try {
            // Test different max length settings
            int[] testLengths = {10, 100, 1000, 4096};
            StatementBase nonInsertStmt = new EmptyStmt();

            for (int maxLength : testLengths) {
                GlobalVariable.auditPluginMaxSqlLength = maxLength;

                // Create a statement exceeding current max length
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < maxLength + 100; i++) {
                    sb.append("x");
                }
                String longStmt = sb.toString();

                String result = AuditLogHelper.handleStmt(longStmt, nonInsertStmt);

                Assert.assertTrue("Should contain truncation message for length " + maxLength,
                        result.contains("/* truncated audit_plugin_max_sql_length=" + maxLength + " */"));

                // Verify truncated length is reasonable (original part + truncation info)
                String expectedTruncationMsg = " ... /* truncated audit_plugin_max_sql_length=" + maxLength + " */";
                Assert.assertTrue("Truncated result should be reasonable length",
                        result.length() <= maxLength + expectedTruncationMsg.length() + 10); // Allow some UTF-8 encoding error margin
            }

        } finally {
            // Restore original values
            GlobalVariable.auditPluginMaxSqlLength = originalMaxSqlLength;
        }
    }

    @Test
    public void testHandleStmtUtf8Truncation() {
        // Save original values
        int originalMaxSqlLength = GlobalVariable.auditPluginMaxSqlLength;

        try {
            // Set a smaller character length limit
            GlobalVariable.auditPluginMaxSqlLength = 20;
            StatementBase nonInsertStmt = new EmptyStmt();

            // Test truncation with Chinese characters (calculated by character count, not byte count)
            String utf8Stmt = "SELECT * FROM 测试表名"; // Chinese characters calculated by character count
            String result = AuditLogHelper.handleStmt(utf8Stmt, nonInsertStmt);

            // Verify result is a valid string
            Assert.assertNotNull("Result should not be null", result);

            // If truncated, should contain truncation info
            if (utf8Stmt.length() > 20) {
                Assert.assertTrue("Should contain truncation message for UTF-8 text",
                        result.contains("/* truncated audit_plugin_max_sql_length=20 */"));
            } else {
                // If not exceeding character limit, should not be truncated
                Assert.assertEquals("Should not be truncated if within character limit", utf8Stmt, result);
            }

            // Test a definitely truncated long Chinese string
            String longUtf8Stmt = "SELECT * FROM 这是一个很长的中文表名用来测试字符截断功能是否正常工作";
            String longResult = AuditLogHelper.handleStmt(longUtf8Stmt, nonInsertStmt);

            Assert.assertTrue("Long UTF-8 string should be truncated",
                    longResult.contains("/* truncated audit_plugin_max_sql_length=20 */"));

        } finally {
            // Restore original values
            GlobalVariable.auditPluginMaxSqlLength = originalMaxSqlLength;
        }
    }

    @Test
    public void testHandleStmtInsertVsNonInsertBehavior() {
        // Save original values
        int originalMaxSqlLength = GlobalVariable.auditPluginMaxSqlLength;
        int originalMaxInsertStmtLength = GlobalVariable.auditPluginMaxInsertStmtLength;

        try {
            // Set different length limits to highlight INSERT vs non-INSERT differences
            GlobalVariable.auditPluginMaxSqlLength = 100;        // Regular statement limit
            GlobalVariable.auditPluginMaxInsertStmtLength = 60;  // INSERT statement limit

            StatementBase nonInsertStmt = new EmptyStmt();

            // Create a statement with length between 60-100
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 80; i++) {
                sb.append("a");
            }
            String testStmt = sb.toString();

            // 1. Test non-INSERT statement behavior
            String result = AuditLogHelper.handleStmt(testStmt, nonInsertStmt);
            // Should use audit_plugin_max_sql_length=100, so not truncated
            Assert.assertFalse("Non-INSERT statement should not be truncated with sql length limit",
                    result.contains("/* truncated"));
            Assert.assertEquals("Non-INSERT statement should remain unchanged", testStmt, result);

            // 2. Test behavior when statement exceeds regular limit
            StringBuilder longSb = new StringBuilder();
            for (int i = 0; i < 120; i++) {
                longSb.append("b");
            }
            String longTestStmt = longSb.toString();

            result = AuditLogHelper.handleStmt(longTestStmt, nonInsertStmt);
            // Should use audit_plugin_max_sql_length=100 for truncation
            Assert.assertTrue("Long non-INSERT statement should be truncated",
                    result.contains("/* truncated audit_plugin_max_sql_length=100 */"));
            Assert.assertFalse("Should not use insert stmt length limit",
                    result.contains("audit_plugin_max_insert_stmt_length"));

        } finally {
            // Restore original values
            GlobalVariable.auditPluginMaxSqlLength = originalMaxSqlLength;
            GlobalVariable.auditPluginMaxInsertStmtLength = originalMaxInsertStmtLength;
        }
    }

}
