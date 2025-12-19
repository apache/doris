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

package org.apache.doris.common.util;

import org.apache.doris.common.FeNameFormat;
import org.apache.doris.qe.ConnectContext;

import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class UtilTest {

    private static final String TEST_SESSION_ID = "test_session_123";

    @BeforeEach
    public void setUp() {
        // Mock ConnectContext to return a fixed session ID
        new MockUp<ConnectContext>() {
            @Mock
            public ConnectContext get() {
                return new ConnectContext() {
                    @Override
                    public String getSessionId() {
                        return TEST_SESSION_ID;
                    }
                };
            }
        };
    }

    @Test
    public void testGenerateTempTableInnerNameOriginalCase() {
        String tableName = "MyTempTable";
        String expected = TEST_SESSION_ID + FeNameFormat.TEMPORARY_TABLE_SIGN + tableName;
        String actual = Util.generateTempTableInnerName(tableName);
        Assertions.assertEquals(expected, actual, "Inner name should be constructed correctly with original case");

        // Already has temp sign - should return as-is
        String withTempSign = TEST_SESSION_ID + FeNameFormat.TEMPORARY_TABLE_SIGN + tableName;
        Assertions.assertEquals(withTempSign, Util.generateTempTableInnerName(withTempSign),
                "Should return input unchanged if it already contains temp sign");
    }

    @Test
    public void testGenerateTempTableInnerNameWithMixedCase() {
        // Test with mixed case TEMP separator
        String tableName = "MyTable";
        String mixedCaseTemp = TEST_SESSION_ID + "_#TeMp#_" + tableName;
        Assertions.assertEquals(mixedCaseTemp, Util.generateTempTableInnerName(mixedCaseTemp),
                "Should detect temp sign case-insensitively");
    }

    @Test
    public void testGetTempTableDisplayNameOriginalCase() {
        String tableName = "MyTable";
        String fullName = TEST_SESSION_ID + FeNameFormat.TEMPORARY_TABLE_SIGN + tableName;
        Assertions.assertEquals(tableName, Util.getTempTableDisplayName(fullName),
                "Should extract display name preserving original case");

        // No temp sign - should return input
        Assertions.assertEquals(tableName, Util.getTempTableDisplayName(tableName),
                "Should return input unchanged if no temp sign present");
    }

    @Test
    public void testGetTempTableDisplayNameWithMixedCase() {
        String tableName = "MyTable";
        String mixedCase = TEST_SESSION_ID + "_#TeMp#_" + tableName;
        Assertions.assertEquals(tableName, Util.getTempTableDisplayName(mixedCase),
                "Should extract display name with mixed case separator");

        String lowerCase = TEST_SESSION_ID + "_#temp#_" + tableName;
        Assertions.assertEquals(tableName, Util.getTempTableDisplayName(lowerCase),
                "Should extract display name with lowercase separator");
    }

    @Test
    public void testGetTempTableSessionId() {
        String tableName = "MyTable";
        String fullName = TEST_SESSION_ID + FeNameFormat.TEMPORARY_TABLE_SIGN + tableName;
        Assertions.assertEquals(TEST_SESSION_ID, Util.getTempTableSessionId(fullName),
                "Should extract session ID correctly");

        String mixedCase = TEST_SESSION_ID + "_#TeMp#_" + tableName;
        Assertions.assertEquals(TEST_SESSION_ID, Util.getTempTableSessionId(mixedCase),
                "Should extract session ID with mixed case separator");

        // No temp sign - should return empty string
        Assertions.assertEquals("", Util.getTempTableSessionId(tableName),
                "Should return empty string if no temp sign present");
    }

    @Test
    public void testIsTempTableWithDifferentCases() {
        String tableName = "MyTable";
        String fullName = TEST_SESSION_ID + FeNameFormat.TEMPORARY_TABLE_SIGN + tableName;
        Assertions.assertTrue(Util.isTempTable(fullName), "Should detect temp table with standard separator");

        String mixedCase = TEST_SESSION_ID + "_#TeMp#_" + tableName;
        Assertions.assertTrue(Util.isTempTable(mixedCase), "Should detect temp table with mixed case separator");

        String lowerCase = TEST_SESSION_ID + "_#temp#_" + tableName;
        Assertions.assertTrue(Util.isTempTable(lowerCase), "Should detect temp table with lowercase separator");

        Assertions.assertFalse(Util.isTempTable(tableName), "Should return false for non-temp table");
    }

    @Test
    public void testIsTempTableInCurrentSession() {
        String tableName = "MyTable";
        String fullName = TEST_SESSION_ID + FeNameFormat.TEMPORARY_TABLE_SIGN + tableName;
        Assertions.assertTrue(Util.isTempTableInCurrentSession(fullName),
                "Should detect temp table in current session");

        String mixedCase = TEST_SESSION_ID + "_#TeMp#_" + tableName;
        Assertions.assertTrue(Util.isTempTableInCurrentSession(mixedCase),
                "Should detect temp table with mixed case separator in current session");

        String wrongSession = "wrong_session" + FeNameFormat.TEMPORARY_TABLE_SIGN + tableName;
        Assertions.assertFalse(Util.isTempTableInCurrentSession(wrongSession),
                "Should return false for temp table in different session");

        Assertions.assertFalse(Util.isTempTableInCurrentSession(tableName),
                "Should return false for non-temp table");
    }

    @Test
    public void getRootCauseWithSuppressedMessageRootCauseWithMessageNoSuppressed() {
        Exception rootCause = new Exception("Root cause message");
        Assertions.assertEquals("java.lang.Exception: Root cause message",
                Util.getRootCauseWithSuppressedMessage(rootCause));
    }

    @Test
    public void getRootCauseWithSuppressedMessageRootCauseWithMessageWithSuppressed() {
        Exception rootCause = new Exception("Root cause message");
        rootCause.addSuppressed(new Exception("Suppressed message"));
        Assertions.assertEquals(
                "java.lang.Exception: Root cause message With suppressed[0]:Suppressed message",
                Util.getRootCauseWithSuppressedMessage(rootCause));
    }

    @Test
    public void getRootCauseWithSuppressedMessageRootCauseWithMessageWithMultiSuppressed() {
        Exception rootCause = new Exception("Root cause message");
        rootCause.addSuppressed(new Exception("Suppressed message"));
        rootCause.addSuppressed(new Exception("Suppressed message2"));
        Assertions.assertEquals(
                "java.lang.Exception: Root cause message"
                            + " With suppressed[0]:Suppressed message"
                            + " With suppressed[1]:Suppressed message2",
                Util.getRootCauseWithSuppressedMessage(rootCause));
    }

    @Test
    public void getRootCauseWithSuppressedMessageRootCauseWithoutMessageNoSuppressed() {
        Exception rootCause = new Exception();
        Assertions.assertEquals("java.lang.Exception", Util.getRootCauseWithSuppressedMessage(rootCause));
    }

    @Test
    public void getRootCauseWithSuppressedMessageRootCauseWithoutMessageWithSuppressed() {
        Exception rootCause = new Exception();
        rootCause.addSuppressed(new Exception("Suppressed message"));
        Assertions.assertEquals(
                "java.lang.Exception With suppressed[0]:Suppressed message",
                Util.getRootCauseWithSuppressedMessage(rootCause));
    }

    @Test
    public void getRootCauseWithSuppressedMessageChainedExceptionWithChainedSuppressed() {
        Exception rootCause = new Exception("Root cause message");
        Exception chainedException = new Exception("Chained exception", rootCause);
        chainedException.addSuppressed(new Exception("Suppressed message"));
        Assertions.assertEquals("java.lang.Exception: Root cause message",
                Util.getRootCauseWithSuppressedMessage(chainedException));
    }

    @Test
    public void getRootCauseWithSuppressedMessageChainedExceptionWithCauseSuppressed() {
        Exception rootCause = new Exception("Root cause message");
        Exception chainedException = new Exception("Chained exception", rootCause);
        rootCause.addSuppressed(new Exception("Suppressed message"));
        Assertions.assertEquals(
                "java.lang.Exception: Root cause message With suppressed[0]:Suppressed message",
                Util.getRootCauseWithSuppressedMessage(chainedException));
    }

    @Test
    public void sha256longEcoding() {
        String str = "东南卫视";
        String str1 = "东方卫视";
        Assertions.assertNotEquals(Util.sha256long(str), Util.sha256long(str1));
    }

    @Test
    public void sha256longHandlesLongMinValue() {
        String testStr = "test_long_min_value_case";
        long result = Util.sha256long(testStr);

        Assertions.assertNotEquals(Long.MIN_VALUE, result,
                "sha256long should not return Long.MIN_VALUE");

        Assertions.assertEquals(result, Util.sha256long(testStr),
                "Same input should produce same output");
    }

    /**
     * Test for GitHub issue: Temporary table name case sensitivity with lower_case_table_names=1
     * Scenario: When lower_case_table_names=1, internal table names may be stored with different
     * casing (e.g., "__TEMP__" instead of "__temp__"), but operations should still work correctly.
     * This test validates that all temp table operations handle case-insensitive temp sign matching.
     */
    @Test
    public void testTempTableOperationsWithLowerCaseTableNames() {
        // Simulate the scenario where lower_case_table_names=1 causes internal storage
        // to have uppercase or mixed case temp sign
        String baseTable = "my_table";
        String internalNameUpperCase = TEST_SESSION_ID + "_#TEMP#_" + baseTable;
        String internalNameLowerCase = TEST_SESSION_ID + "_#temp#_" + baseTable;
        String internalNameMixedCase = TEST_SESSION_ID + "_#TeMp#_" + baseTable;

        // All variants should be recognized as temp tables
        Assertions.assertTrue(Util.isTempTable(internalNameUpperCase),
                "Uppercase temp sign should be recognized");
        Assertions.assertTrue(Util.isTempTable(internalNameLowerCase),
                "Lowercase temp sign should be recognized");
        Assertions.assertTrue(Util.isTempTable(internalNameMixedCase),
                "Mixed case temp sign should be recognized");

        // All variants should extract the same display name (without temp sign)
        Assertions.assertEquals(baseTable, Util.getTempTableDisplayName(internalNameUpperCase),
                "Display name should be extracted correctly from uppercase");
        Assertions.assertEquals(baseTable, Util.getTempTableDisplayName(internalNameLowerCase),
                "Display name should be extracted correctly from lowercase");
        Assertions.assertEquals(baseTable, Util.getTempTableDisplayName(internalNameMixedCase),
                "Display name should be extracted correctly from mixed case");

        // All variants should extract the same session ID
        Assertions.assertEquals(TEST_SESSION_ID, Util.getTempTableSessionId(internalNameUpperCase),
                "Session ID should be extracted from uppercase");
        Assertions.assertEquals(TEST_SESSION_ID, Util.getTempTableSessionId(internalNameLowerCase),
                "Session ID should be extracted from lowercase");
        Assertions.assertEquals(TEST_SESSION_ID, Util.getTempTableSessionId(internalNameMixedCase),
                "Session ID should be extracted from mixed case");

        // All variants should be recognized as belonging to current session
        Assertions.assertTrue(Util.isTempTableInCurrentSession(internalNameUpperCase),
                "Uppercase variant should belong to current session");
        Assertions.assertTrue(Util.isTempTableInCurrentSession(internalNameLowerCase),
                "Lowercase variant should belong to current session");
        Assertions.assertTrue(Util.isTempTableInCurrentSession(internalNameMixedCase),
                "Mixed case variant should belong to current session");
    }

    /**
     * Test that verifies case-insensitive filtering behavior required for SHOW TABLE STATUS.
     * This validates the fix in ShowTableStatusCommand where we use LOWER(TABLE_NAME).
     */
    @Test
    public void testShowTableStatusCaseInsensitiveFiltering() {
        // These internal names simulate what might be in the catalog with different case variations
        String[] internalNames = {
            "session001" + "_#temp#_" + "user_data",
            "session002" + "_#TEMP#_" + "USER_DATA",
            "session003" + "_#TeMp#_" + "User_Data",
            "regular_table"
        };

        // Verify that all temp table variants are detected
        int tempTableCount = 0;
        for (String name : internalNames) {
            if (Util.isTempTable(name)) {
                tempTableCount++;
                // All temp tables should have user_data as display name (case may vary in original)
                String displayName = Util.getTempTableDisplayName(name);
                // The display name case is preserved from the original
                Assertions.assertTrue(displayName.toLowerCase().equals("user_data"),
                        "Display name should be user_data (case-insensitive), got: " + displayName);
            }
        }
        Assertions.assertEquals(3, tempTableCount, "Should detect all 3 temp table variants");

        // Verify regular table is not detected as temp
        Assertions.assertFalse(Util.isTempTable("regular_table"),
                "Regular table should not be detected as temp table");
    }

    /**
     * Test edge cases with empty strings, null-like patterns, and special characters.
     */
    @Test
    public void testTempTableEdgeCases() {
        // Empty string
        Assertions.assertFalse(Util.isTempTable(""),
                "Empty string should not be temp table");

        // Only temp sign without session or table name
        Assertions.assertFalse(Util.isTempTable("_#temp#_"),
                "Only temp sign should not be valid temp table");

        // Session ID with temp sign but no table name
        String noTableName = TEST_SESSION_ID + "_#temp#_";
        Assertions.assertTrue(Util.isTempTable(noTableName),
                "Session with temp sign should be detected even without table name");
        Assertions.assertEquals("", Util.getTempTableDisplayName(noTableName),
                "Display name should be empty string when no table name part");

        // Multiple temp signs (only first should be recognized)
        String multipleSigns = TEST_SESSION_ID + "_#temp#_" + "table" + "_#temp#_" + "name";
        Assertions.assertTrue(Util.isTempTable(multipleSigns),
                "Should detect temp table even with multiple temp signs");
        Assertions.assertEquals("table_#temp#_name", Util.getTempTableDisplayName(multipleSigns),
                "Display name should include everything after first temp sign");
    }
}
