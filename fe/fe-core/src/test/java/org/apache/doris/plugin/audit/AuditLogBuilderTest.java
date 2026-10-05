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

import org.apache.doris.analysis.StatementBase;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.commands.EmptyCommand;
import org.apache.doris.plugin.AuditEvent;
import org.apache.doris.qe.AuditLogHelper;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.GlobalVariable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AuditLogBuilderTest {

    @Test
    public void testTimestampOutput() {
        AuditLogBuilder auditLogBuilder = new AuditLogBuilder();
        // 1 set a valid value
        long currentTime = 1741760376000L;
        AuditEvent auditEvent = new AuditEvent.AuditEventBuilder()
                .setTimestamp(currentTime).build();
        String result = Deencapsulation.invoke(auditLogBuilder, "getAuditLogString", auditEvent);
        Assertions.assertTrue(result.contains("Timestamp=2025-03-12 14:19:36.000"));

        // 2 not set value
        auditEvent = new AuditEvent.AuditEventBuilder().build();
        result = Deencapsulation.invoke(auditLogBuilder, "getAuditLogString", auditEvent);
        Assertions.assertTrue(result.contains("Timestamp=\\N"));
    }

    @Test
    public void testHandleStmtTruncationForNonInsertStmt() {
        // Save original values
        int originalMaxSqlLength = GlobalVariable.auditPluginMaxSqlLength;

        try {
            // Set smaller max length for testing
            GlobalVariable.auditPluginMaxSqlLength = 50;

            // Use EmptyCommand as representative of non-INSERT statements
            Command nonInsertCommand = new EmptyCommand();

            // 1. Test null input
            String result = AuditLogHelper.handleCommand(null, nonInsertCommand);
            Assertions.assertNull(result);

            // 2. Test short statement not truncated
            String shortStmt = "SELECT * FROM table1";
            result = AuditLogHelper.handleCommand(shortStmt, nonInsertCommand);
            Assertions.assertEquals(shortStmt, result);

            // 3. Test long statement truncated (using audit_plugin_max_sql_length)
            String longStmt
                    = "SELECT * FROM very_long_table_name_that_exceeds_the_maximum_length_limit_for_audit_log";
            result = AuditLogHelper.handleCommand(longStmt, nonInsertCommand);
            Assertions.assertTrue(result.contains("/* truncated. audit_plugin_max_sql_length=50 */"), "Result should contain truncation message");
            Assertions.assertTrue(result.getBytes().length < longStmt.getBytes().length + 100, "Result should be shorter than original"); // Add length for truncation message

            // 4. Test statement with newlines, tabs, carriage returns
            String stmtWithSpecialChars = "SELECT *\nFROM table1\tWHERE id = 1\r";
            result = AuditLogHelper.handleCommand(stmtWithSpecialChars, nonInsertCommand);
            Assertions.assertTrue(result.contains("\n"), "Should contain actual newlines");
            Assertions.assertTrue(result.contains("\t"), "Should contain actual tabs");
            Assertions.assertTrue(result.contains("\r"), "Should contain actual carriage returns");

            // 5. Test long statement with Chinese characters truncation
            String chineseStmt
                    = "SELECT * FROM 表名很长的中文表名字符测试表名很长的中文表名字符测试表名很长的中文表名字符测试";
            result = AuditLogHelper.handleCommand(chineseStmt, nonInsertCommand);
            Assertions.assertTrue(result.contains("/* truncated. audit_plugin_max_sql_length=50 */"), "Should contain truncation message for Chinese text");

            // 6. Test boundary case: exactly equal to max length
            // Create a string exactly equal to max length
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 50; i++) {
                sb.append("a");
            }
            String exactLengthStmt = sb.toString();
            result = AuditLogHelper.handleCommand(exactLengthStmt, nonInsertCommand);
            Assertions.assertEquals(exactLengthStmt, result, "Should not be truncated when exactly at limit");

            // 7. Test boundary case: exceeding max length by 1 character
            sb = new StringBuilder();
            for (int i = 0; i < 51; i++) {
                sb.append("a");
            }
            String overLimitStmt = sb.toString();
            result = AuditLogHelper.handleCommand(overLimitStmt, nonInsertCommand);
            Assertions.assertTrue(result.contains("/* truncated. audit_plugin_max_sql_length=50 */"), "Should be truncated when over limit by 1 char");

            // 8. Test empty string
            String emptyStmt = "";
            result = AuditLogHelper.handleCommand(emptyStmt, nonInsertCommand);
            Assertions.assertEquals("", result, "Empty string should remain empty");
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
            Assertions.assertTrue(result.contains("/* total 3 rows, truncated. audit_plugin_max_insert_stmt_length=80 */"), "Should contain insert stmt length truncation message");

            // 2. Test short INSERT statement not truncated
            String shortInsertStmt = "INSERT INTO tbl VALUES (1, 'a')";
            insertStmt = parser.parseSQL(shortInsertStmt).get(0);
            result = AuditLogHelper.handleStmt(shortInsertStmt, insertStmt);

            // Should not be truncated, and special characters should be properly escaped
            Assertions.assertFalse(result.contains("/* truncated."), "Short INSERT should not be truncated");
            Assertions.assertEquals(shortInsertStmt, result, "Short INSERT should remain unchanged");

            // 3. Test special character handling in INSERT statements
            String insertWithSpecialChars = "INSERT INTO tbl\nVALUES\t(1,\r'test')";
            insertStmt = parser.parseSQL(insertWithSpecialChars).get(0);
            result = AuditLogHelper.handleStmt(insertWithSpecialChars, insertStmt);

            // Verify special characters are properly escaped
            Assertions.assertTrue(result.contains("\n"), "Should contain actual newlines");
            Assertions.assertTrue(result.contains("\t"), "Should contain actual tabs");
            Assertions.assertTrue(result.contains("\r"), "Should contain actual carriage returns");

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
            Assertions.assertTrue(insertResult.contains("/* total 1 rows, truncated. audit_plugin_max_insert_stmt_length=80 */"), "INSERT should be truncated with insert length limit");

            // SELECT should not be truncated (using limit of 200)
            Assertions.assertFalse(selectResult.contains("/* truncated."), "SELECT should not be truncated with sql length limit");

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
            Assertions.assertFalse(result.contains("/* truncated."), "INSERT at exact limit should not be truncated");
            Assertions.assertEquals(exactLengthInsert, result, "INSERT at exact limit should remain unchanged");

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
            Command nonInsertCommand = new EmptyCommand();

            for (int maxLength : testLengths) {
                GlobalVariable.auditPluginMaxSqlLength = maxLength;

                // Create a statement exceeding current max length
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < maxLength + 100; i++) {
                    sb.append("x");
                }
                String longStmt = sb.toString();

                String result = AuditLogHelper.handleCommand(longStmt, nonInsertCommand);

                Assertions.assertTrue(result.contains("/* truncated. audit_plugin_max_sql_length=" + maxLength + " */"), "Should contain truncation message for length " + maxLength);

                // Verify truncated length is reasonable (original part + truncation info)
                String expectedTruncationMsg = " ... /* truncated audit_plugin_max_sql_length=" + maxLength + " */";
                Assertions.assertTrue(result.getBytes().length <= maxLength + expectedTruncationMsg.getBytes().length + 10, "Truncated. result should be reasonable length"); // Allow some UTF-8 encoding error margin
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
            Command nonInsertCommand = new EmptyCommand();

            // Test truncation with Chinese characters (calculated by character count, not byte count)
            String utf8Stmt = "SELECT * FROM 测试表名"; // Chinese characters calculated by character count
            String result = AuditLogHelper.handleCommand(utf8Stmt, nonInsertCommand);

            // Verify result is a valid string
            Assertions.assertNotNull(result, "Result should not be null");

            // If truncated, should contain truncation info
            if (utf8Stmt.getBytes().length > 20) {
                Assertions.assertTrue(result.contains("/* truncated. audit_plugin_max_sql_length=20 */"), "Should contain truncation message for UTF-8 text");
            } else {
                // If not exceeding character limit, should not be truncated
                Assertions.assertEquals(utf8Stmt, result, "Should not be truncated if within character limit");
            }

            // Test a definitely truncated long Chinese string
            String longUtf8Stmt = "SELECT * FROM 这是一个很长的中文表名用来测试字符截断功能是否正常工作";
            String longResult = AuditLogHelper.handleCommand(longUtf8Stmt, nonInsertCommand);

            Assertions.assertTrue(longResult.contains("/* truncated. audit_plugin_max_sql_length=20 */"), "Long UTF-8 string should be truncated");

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

            Command nonInsertCommand = new EmptyCommand();

            // Create a statement with length between 60-100
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 80; i++) {
                sb.append("a");
            }
            String testStmt = sb.toString();

            // 1. Test non-INSERT statement behavior
            String result = AuditLogHelper.handleCommand(testStmt, nonInsertCommand);
            // Should use audit_plugin_max_sql_length=100, so not truncated
            Assertions.assertFalse(result.contains("/* truncated."), "Non-INSERT statement should not be truncated with sql length limit");
            Assertions.assertEquals(testStmt, result, "Non-INSERT statement should remain unchanged");

            // 2. Test behavior when statement exceeds regular limit
            StringBuilder longSb = new StringBuilder();
            for (int i = 0; i < 120; i++) {
                longSb.append("b");
            }
            String longTestStmt = longSb.toString();

            result = AuditLogHelper.handleCommand(longTestStmt, nonInsertCommand);
            // Should use audit_plugin_max_sql_length=100 for truncation
            Assertions.assertTrue(result.contains("/* truncated. audit_plugin_max_sql_length=100 */"), "Long non-INSERT statement should be truncated");
            Assertions.assertFalse(result.contains("audit_plugin_max_insert_stmt_length"), "Should not use insert stmt length limit");

        } finally {
            // Restore original values
            GlobalVariable.auditPluginMaxSqlLength = originalMaxSqlLength;
            GlobalVariable.auditPluginMaxInsertStmtLength = originalMaxInsertStmtLength;
        }
    }

    @Test
    public void testHandleStmtInsertLengthLimitLogic() {
        // Save original values
        int originalMaxSqlLength = GlobalVariable.auditPluginMaxSqlLength;
        int originalMaxInsertStmtLength = GlobalVariable.auditPluginMaxInsertStmtLength;
        ConnectContext ctx = new ConnectContext();
        ctx.changeDefaultCatalog("internal");
        ctx.setDatabase("db1");

        try {
            ctx.setThreadLocalInfo();
            NereidsParser parser = new NereidsParser();

            // Test 1: auditPluginMaxInsertStmtLength < auditPluginMaxSqlLength
            // Should use the smaller INSERT limit
            GlobalVariable.auditPluginMaxSqlLength = 200;
            GlobalVariable.auditPluginMaxInsertStmtLength = 80;

            String insertStmt = "INSERT INTO test_table VALUES " + generateValueList(50); // Generate a long INSERT
            StatementBase parsedStmt = parser.parseSQL(insertStmt).get(0);
            String result = AuditLogHelper.handleStmt(insertStmt, parsedStmt);

            if (insertStmt.getBytes().length > 80) {
                Assertions.assertTrue(result.contains("audit_plugin_max_insert_stmt_length=80"), "Should use insert stmt length limit (80) when it's smaller");
                Assertions.assertFalse(result.contains("audit_plugin_max_sql_length=200"), "Should not use sql length limit when insert limit is smaller");
            }

            // Test 2: auditPluginMaxInsertStmtLength > auditPluginMaxSqlLength
            // Should use the smaller SQL limit
            GlobalVariable.auditPluginMaxSqlLength = 60;
            GlobalVariable.auditPluginMaxInsertStmtLength = 150;

            result = AuditLogHelper.handleStmt(insertStmt, parsedStmt);

            if (insertStmt.getBytes().length > 60) {
                Assertions.assertTrue(result.contains("audit_plugin_max_insert_stmt_length=60"), "Should use insert stmt length limit (60) when sql limit is smaller");
            }

            // Test 3: auditPluginMaxInsertStmtLength = auditPluginMaxSqlLength
            // Should use the same limit value
            GlobalVariable.auditPluginMaxSqlLength = 100;
            GlobalVariable.auditPluginMaxInsertStmtLength = 100;

            result = AuditLogHelper.handleStmt(insertStmt, parsedStmt);

            if (insertStmt.getBytes().length > 100) {
                Assertions.assertTrue(result.contains("audit_plugin_max_insert_stmt_length=100"), "Should use limit (100) when both limits are equal");
            }

            // Test 4: Test with very small but valid limits
            GlobalVariable.auditPluginMaxSqlLength = 10;
            GlobalVariable.auditPluginMaxInsertStmtLength = 15;

            // Create a short INSERT that will exceed the 10-char SQL limit
            String shortInsert = "INSERT INTO t VALUES (1, 'data')";
            parsedStmt = parser.parseSQL(shortInsert).get(0);
            result = AuditLogHelper.handleStmt(shortInsert, parsedStmt);

            // Math.max(0, Math.min(15, 10)) = Math.max(0, 10) = 10
            if (shortInsert.getBytes().length > 10) {
                Assertions.assertTrue(result.contains("audit_plugin_max_insert_stmt_length=10"), "Should use the smaller limit (10) when sql limit is smaller");
            }

            // Test 5: Test with small INSERT limit but larger SQL limit
            GlobalVariable.auditPluginMaxSqlLength = 100;
            GlobalVariable.auditPluginMaxInsertStmtLength = 25;

            result = AuditLogHelper.handleStmt(shortInsert, parsedStmt);

            // Math.max(0, Math.min(25, 100)) = Math.max(0, 25) = 25
            if (shortInsert.getBytes().length > 25) {
                Assertions.assertTrue(result.contains("audit_plugin_max_insert_stmt_length=25"), "Should use the insert limit (25) when it's smaller");
            }

            // Test 6: Verify the exact boundary behavior
            GlobalVariable.auditPluginMaxSqlLength = 100;
            GlobalVariable.auditPluginMaxInsertStmtLength = 50;

            // Create an INSERT statement with exactly 50 characters
            String exactLengthInsert = createExactLengthInsertStatement(50);
            parsedStmt = parser.parseSQL(exactLengthInsert).get(0);
            result = AuditLogHelper.handleStmt(exactLengthInsert, parsedStmt);

            Assertions.assertFalse(result.contains("truncated"), "Statement with exactly max length should not be truncated");

            // Create an INSERT statement with 51 characters (1 over limit)
            String overLimitInsert = createExactLengthInsertStatement(51);
            parsedStmt = parser.parseSQL(overLimitInsert).get(0);
            result = AuditLogHelper.handleStmt(overLimitInsert, parsedStmt);

            Assertions.assertTrue(result.contains("audit_plugin_max_insert_stmt_length=50"), "Statement exceeding max length by 1 should be truncated");

            // Test 7: Test the Math.min logic with different combinations
            GlobalVariable.auditPluginMaxSqlLength = 120;
            GlobalVariable.auditPluginMaxInsertStmtLength = 80;

            String mediumInsert = "INSERT INTO table_name VALUES " + generateValueList(10);
            parsedStmt = parser.parseSQL(mediumInsert).get(0);
            result = AuditLogHelper.handleStmt(mediumInsert, parsedStmt);

            // Should use Math.max(0, Math.min(80, 120)) = 80
            if (mediumInsert.getBytes().length > 80) {
                Assertions.assertTrue(result.contains("audit_plugin_max_insert_stmt_length=80"), "Should use the smaller insert limit (80)");
            }

        } finally {
            // Restore original values
            GlobalVariable.auditPluginMaxSqlLength = originalMaxSqlLength;
            GlobalVariable.auditPluginMaxInsertStmtLength = originalMaxInsertStmtLength;
            ConnectContext.remove();
        }
    }

    /**
     * Helper method to generate a VALUES list with specified number of rows
     */
    private String generateValueList(int rowCount) {
        StringBuilder sb = new StringBuilder("(1, 'value1')");
        for (int i = 2; i <= rowCount; i++) {
            sb.append(", (").append(i).append(", 'value").append(i).append("')");
        }
        return sb.toString();
    }

    /**
     * Helper method to create INSERT statement with exact character length
     */
    private String createExactLengthInsertStatement(int targetLength) {
        String prefix = "INSERT INTO t VALUES (1, '";
        String suffix = "')";
        int dataLength = targetLength - prefix.getBytes().length - suffix.getBytes().length;

        if (dataLength <= 0) {
            // If target length is too small, create minimal valid INSERT
            return "INSERT INTO t VALUES (1)";
        }

        StringBuilder data = new StringBuilder();
        for (int i = 0; i < dataLength; i++) {
            data.append("a");
        }

        return prefix + data.toString() + suffix;
    }

    @Test
    public void testHandleStmtWithAbnormalLengthLimits() {
        // Save original values
        int originalMaxSqlLength = GlobalVariable.auditPluginMaxSqlLength;
        int originalMaxInsertStmtLength = GlobalVariable.auditPluginMaxInsertStmtLength;
        ConnectContext ctx = new ConnectContext();
        ctx.changeDefaultCatalog("internal");
        ctx.setDatabase("db1");

        try {
            ctx.setThreadLocalInfo();
            NereidsParser parser = new NereidsParser();

            // Test INSERT statement that will be used across multiple test cases
            String testInsertStmt = "INSERT INTO test_table VALUES (1, 'test_data')";
            StatementBase parsedInsertStmt = parser.parseSQL(testInsertStmt).get(0);

            // Test non-INSERT statement
            Command nonInsertCommand = new EmptyCommand();

            String testSelectStmt = "SELECT * FROM test_table WHERE id = 1";

            // Test Case 1: auditPluginMaxSqlLength = 0, auditPluginMaxInsertStmtLength > 0
            // Expected: Math.max(0, Math.min(positive, 0)) = Math.max(0, 0) = 0
            GlobalVariable.auditPluginMaxSqlLength = 0;
            GlobalVariable.auditPluginMaxInsertStmtLength = 50;

            String result = AuditLogHelper.handleStmt(testInsertStmt, parsedInsertStmt);
            Assertions.assertNotNull(result, "Result should not be null when sql length limit is 0");
            // When maxLen = 0, the statement should be heavily truncated
            if (testInsertStmt.getBytes().length > 0) {
                Assertions.assertTrue(result.contains("audit_plugin_max_insert_stmt_length=0") || result.isEmpty(), "Should be truncated when effective limit is 0");
            }

            // Test Case 2: auditPluginMaxSqlLength > 0, auditPluginMaxInsertStmtLength = 0
            // Expected: Math.max(0, Math.min(0, positive)) = Math.max(0, 0) = 0
            GlobalVariable.auditPluginMaxSqlLength = 50;
            GlobalVariable.auditPluginMaxInsertStmtLength = 0;

            result = AuditLogHelper.handleStmt(testInsertStmt, parsedInsertStmt);
            Assertions.assertNotNull(result, "Result should not be null when insert length limit is 0");
            if (testInsertStmt.getBytes().length > 0) {
                Assertions.assertTrue(result.contains("audit_plugin_max_insert_stmt_length=0") || result.isEmpty(), "Should be truncated when effective limit is 0");
            }

            // Test Case 3: Both limits are 0
            // Expected: Math.max(0, Math.min(0, 0)) = Math.max(0, 0) = 0
            GlobalVariable.auditPluginMaxSqlLength = 0;
            GlobalVariable.auditPluginMaxInsertStmtLength = 0;

            result = AuditLogHelper.handleStmt(testInsertStmt, parsedInsertStmt);
            Assertions.assertNotNull(result, "Result should not be null when both limits are 0");
            if (testInsertStmt.getBytes().length > 0) {
                Assertions.assertTrue(result.contains("audit_plugin_max_insert_stmt_length=0") || result.isEmpty(), "Should be truncated when both limits are 0");
            }

            // Test Case 4: Negative auditPluginMaxSqlLength
            // Expected: Math.max(0, Math.min(positive, negative)) = Math.max(0, negative) = 0
            GlobalVariable.auditPluginMaxSqlLength = -10;
            GlobalVariable.auditPluginMaxInsertStmtLength = 50;

            result = AuditLogHelper.handleStmt(testInsertStmt, parsedInsertStmt);
            Assertions.assertNotNull(result, "Result should not be null when sql length limit is negative");
            if (testInsertStmt.getBytes().length > 0) {
                Assertions.assertTrue(result.contains("audit_plugin_max_insert_stmt_length=0") || result.isEmpty(), "Should be truncated when sql limit is negative");
            }

            // Test Case 5: Negative auditPluginMaxInsertStmtLength
            // Expected: Math.max(0, Math.min(negative, positive)) = Math.max(0, negative) = 0
            GlobalVariable.auditPluginMaxSqlLength = 50;
            GlobalVariable.auditPluginMaxInsertStmtLength = -20;

            result = AuditLogHelper.handleStmt(testInsertStmt, parsedInsertStmt);
            Assertions.assertNotNull(result, "Result should not be null when insert length limit is negative");
            if (testInsertStmt.getBytes().length > 0) {
                Assertions.assertTrue(result.contains("audit_plugin_max_insert_stmt_length=0") || result.isEmpty(), "Should be truncated when insert limit is negative");
            }

            // Test Case 6: Both limits are negative
            // Expected: Math.max(0, Math.min(negative1, negative2)) = Math.max(0, min(neg1,neg2)) = 0
            GlobalVariable.auditPluginMaxSqlLength = -15;
            GlobalVariable.auditPluginMaxInsertStmtLength = -25;

            result = AuditLogHelper.handleStmt(testInsertStmt, parsedInsertStmt);
            Assertions.assertNotNull(result, "Result should not be null when both limits are negative");
            if (testInsertStmt.getBytes().length > 0) {
                Assertions.assertTrue(result.contains("audit_plugin_max_insert_stmt_length=0") || result.isEmpty(), "Should be truncated when both limits are negative");
            }

            // Test Case 7: Test non-INSERT statement with abnormal limits
            // Non-INSERT statements should use auditPluginMaxSqlLength directly in truncateByBytes
            GlobalVariable.auditPluginMaxSqlLength = 0;
            GlobalVariable.auditPluginMaxInsertStmtLength = 100; // This should be ignored for non-INSERT

            result = AuditLogHelper.handleCommand(testSelectStmt, nonInsertCommand);
            Assertions.assertNotNull(result, "Result should not be null for non-INSERT with zero sql limit");
            // Non-INSERT statements bypass the Math.max(0, Math.min(...)) logic and go directly to truncateByBytes

            // Test Case 8: Very large negative numbers
            GlobalVariable.auditPluginMaxSqlLength = Integer.MIN_VALUE;
            GlobalVariable.auditPluginMaxInsertStmtLength = Integer.MIN_VALUE;

            result = AuditLogHelper.handleStmt(testInsertStmt, parsedInsertStmt);
            Assertions.assertNotNull(result, "Result should not be null with very large negative values");

            // Test Case 9: Mixed extreme values
            GlobalVariable.auditPluginMaxSqlLength = Integer.MAX_VALUE;
            GlobalVariable.auditPluginMaxInsertStmtLength = Integer.MIN_VALUE;

            result = AuditLogHelper.handleStmt(testInsertStmt, parsedInsertStmt);
            Assertions.assertNotNull(result, "Result should not be null with mixed extreme values");
            // Expected: Math.max(0, Math.min(MIN_VALUE, MAX_VALUE)) = Math.max(0, MIN_VALUE) = 0

            // Test Case 10: Edge case with very small positive numbers
            GlobalVariable.auditPluginMaxSqlLength = 1;
            GlobalVariable.auditPluginMaxInsertStmtLength = 1;

            result = AuditLogHelper.handleStmt(testInsertStmt, parsedInsertStmt);
            Assertions.assertNotNull(result, "Result should not be null with very small positive limits");
            // Expected: Math.max(0, Math.min(1, 1)) = Math.max(0, 1) = 1
            if (testInsertStmt.getBytes().length > 1) {
                Assertions.assertTrue(result.contains("audit_plugin_max_insert_stmt_length=1"), "Should be truncated with limit of 1");
            }

            // Test Case 11: Test empty string with abnormal limits
            String emptyStmt = "";
            GlobalVariable.auditPluginMaxSqlLength = -5;
            GlobalVariable.auditPluginMaxInsertStmtLength = -10;

            result = AuditLogHelper.handleStmt(emptyStmt, parsedInsertStmt);
            Assertions.assertNotNull(result, "Result should not be null for empty string");
            Assertions.assertEquals("", result, "Empty string should remain empty");

        } catch (Exception e) {
            // If any exception occurs, we want to log it but not fail the test immediately
            // This helps us identify which specific abnormal values cause issues
            Assertions.fail("Unexpected exception with abnormal length limits: " + e.getMessage()
                    + ". sqlLength=" + GlobalVariable.auditPluginMaxSqlLength
                    + ", insertLength=" + GlobalVariable.auditPluginMaxInsertStmtLength);
        } finally {
            // Restore original values
            GlobalVariable.auditPluginMaxSqlLength = originalMaxSqlLength;
            GlobalVariable.auditPluginMaxInsertStmtLength = originalMaxInsertStmtLength;
            ConnectContext.remove();
        }
    }

}
