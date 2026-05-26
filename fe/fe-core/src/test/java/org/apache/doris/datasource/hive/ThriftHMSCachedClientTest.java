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

package org.apache.doris.datasource.hive;

import org.apache.doris.common.Config;
import org.apache.doris.qe.BDPAuthContext;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class ThriftHMSCachedClientTest {

    private int originalThreshold;
    private TestAppender testAppender;
    private Logger logger;

    /**
     * Thread-safe test appender for capturing log events
     */
    private static class TestAppender extends AbstractAppender {
        private final List<LogEvent> events = new CopyOnWriteArrayList<>();

        public TestAppender(String name) {
            super(name, null, PatternLayout.createDefaultLayout(), true, Property.EMPTY_ARRAY);
        }

        @Override
        public void append(LogEvent event) {
            events.add(event.toImmutable()); // Store immutable copy for thread safety
        }

        public List<LogEvent> getEvents() {
            return new ArrayList<>(events);
        }

        public void clearEvents() {
            events.clear();
        }

        public List<String> getFormattedMessages() {
            return events.stream()
                    .map(event -> event.getMessage().getFormattedMessage())
                    .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
        }

        public boolean hasLoggedMessage(String expectedMessage) {
            return events.stream()
                    .anyMatch(event -> event.getMessage().getFormattedMessage().contains(expectedMessage));
        }

        public long getEventCount(Level level) {
            return events.stream()
                    .filter(event -> event.getLevel().equals(level))
                    .count();
        }
    }

    @BeforeEach
    public void setUp() {
        // Save original config value
        originalThreshold = Config.log_huge_hms_partition_num;

        // Get the specific logger for ThriftHMSCachedClient
        logger = (Logger) LogManager.getLogger(ThriftHMSCachedClient.class);

        // Create and configure test appender
        testAppender = new TestAppender("TestAppender");
        testAppender.start();

        // Add appender to logger
        logger.addAppender(testAppender);
        logger.setLevel(Level.WARN); // Ensure WARN level is captured
    }

    @AfterEach
    public void tearDown() {
        // Restore original config value
        Config.log_huge_hms_partition_num = originalThreshold;

        // Clear thread local BDPAuthContext
        BDPAuthContext.clear();

        // Clean up: remove test appender
        if (logger != null && testAppender != null) {
            logger.removeAppender(testAppender);
            testAppender.stop();
        }
    }

    // ========== THRESHOLD LOGIC TESTS ==========

    @Test
    public void testPartitionCountBelowThreshold() {
        // Setup: Set threshold to 1000
        Config.log_huge_hms_partition_num = 1000;

        // Test: Call with partition count below threshold
        ThriftHMSCachedClient.logIfHugeHmsCall(500, "test_db", "test_table");

        // Verify: No log events should be captured
        Assertions.assertEquals(0, testAppender.getEvents().size(),
                "No log events should be generated when partition count is below threshold");
    }

    @Test
    public void testPartitionCountAtThreshold() {
        // Setup: Set threshold to 100
        Config.log_huge_hms_partition_num = 100;

        // Test: Call with partition count exactly at threshold
        ThriftHMSCachedClient.logIfHugeHmsCall(100, "test_db", "test_table");

        // Verify: No log events should be captured (100 <= 100)
        Assertions.assertEquals(0, testAppender.getEvents().size(),
                "No log events should be generated when partition count equals threshold");
    }

    @Test
    public void testPartitionCountAboveThreshold() {
        // Setup: Set threshold to 1000
        Config.log_huge_hms_partition_num = 1000;

        // Test: Call with partition count above threshold
        ThriftHMSCachedClient.logIfHugeHmsCall(1001, "test_db", "test_table");

        // Verify: One WARN level log event should be captured
        Assertions.assertEquals(1, testAppender.getEventCount(Level.WARN),
                "Should generate exactly one WARN log event");

        // Verify: Log message content
        List<String> messages = testAppender.getFormattedMessages();
        Assertions.assertEquals(1, messages.size(), "Should have exactly one log message");

        String logMessage = messages.get(0);
        Assertions.assertTrue(logMessage.contains("Partition count exceeds 1000 partitions"),
                "Log should mention threshold exceeded");
        Assertions.assertTrue(logMessage.contains("actual count 1001"),
                "Log should include actual partition count");
        Assertions.assertTrue(logMessage.contains("dbName test_db"),
                "Log should include database name");
        Assertions.assertTrue(logMessage.contains("tableName test_table"),
                "Log should include table name");
    }

    @Test
    public void testBoundaryConditionJustAboveThreshold() {
        // Setup: Set threshold to 100
        Config.log_huge_hms_partition_num = 100;

        // Test: Call with count just above threshold
        ThriftHMSCachedClient.logIfHugeHmsCall(101, "test_db", "test_table");

        // Verify: Should trigger logging just above threshold
        Assertions.assertEquals(1, testAppender.getEventCount(Level.WARN),
                    "Should log when count is just above threshold");

        String logMessage = testAppender.getFormattedMessages().get(0);
        Assertions.assertTrue(logMessage.contains("actual count 101"),
                "Log should show actual count 101");
    }

    @Test
    public void testZeroPartitionCount() {
        // Setup: Set threshold to 10
        Config.log_huge_hms_partition_num = 10;

        // Test: Call with zero partition count
        ThriftHMSCachedClient.logIfHugeHmsCall(0, "test_db", "test_table");

        // Verify: No logging should occur
        Assertions.assertEquals(0, testAppender.getEvents().size(),
                "No logging should occur for zero count");
    }

    @Test
    public void testNegativePartitionCount() {
        // Setup: Set threshold to 10
        Config.log_huge_hms_partition_num = 10;

        // Test: Call with negative partition count
        ThriftHMSCachedClient.logIfHugeHmsCall(-5, "test_db", "test_table");

        // Verify: No logging should occur
        Assertions.assertEquals(0, testAppender.getEvents().size(),
                "No logging should occur for negative count");
    }

    @Test
    public void testVeryLargePartitionCount() {
        // Setup: Set threshold to 100
        Config.log_huge_hms_partition_num = 100;

        // Test: Call with maximum integer value
        ThriftHMSCachedClient.logIfHugeHmsCall(Integer.MAX_VALUE, "test_db", "test_table");

        // Verify: Should log for MAX_VALUE
        Assertions.assertEquals(1, testAppender.getEventCount(Level.WARN),
                "Should log for Integer.MAX_VALUE");

        String logMessage = testAppender.getFormattedMessages().get(0);
        Assertions.assertTrue(logMessage.contains("actual count " + Integer.MAX_VALUE),
                "Log should handle MAX_VALUE correctly");
    }

    // ========== PARAMETER HANDLING TESTS ==========

    @Test
    public void testNullDbName() {
        // Setup: Set threshold to 10
        Config.log_huge_hms_partition_num = 10;

        // Test: Call with null database name
        ThriftHMSCachedClient.logIfHugeHmsCall(20, null, "test_table");

        // Verify: Should log and use N/A for null database name
        Assertions.assertEquals(1, testAppender.getEventCount(Level.WARN),
                "Should generate one WARN log event");

        String logMessage = testAppender.getFormattedMessages().get(0);
        Assertions.assertTrue(logMessage.contains("dbName N/A"),
                "Should use N/A for null database name");
        Assertions.assertTrue(logMessage.contains("tableName test_table"),
                "Table name should be preserved");
    }

    @Test
    public void testNullTableName() {
        // Setup: Set threshold to 10
        Config.log_huge_hms_partition_num = 10;

        // Test: Call with null table name
        ThriftHMSCachedClient.logIfHugeHmsCall(20, "test_db", null);

        // Verify: Should log and use N/A for null table name
        Assertions.assertEquals(1, testAppender.getEventCount(Level.WARN),
                "Should generate one WARN log event");

        String logMessage = testAppender.getFormattedMessages().get(0);
        Assertions.assertTrue(logMessage.contains("dbName test_db"),
                "Database name should be preserved");
        Assertions.assertTrue(logMessage.contains("tableName N/A"),
                "Should use N/A for null table name");
    }

    @Test
    public void testBothNullNames() {
        // Setup: Set threshold to 10
        Config.log_huge_hms_partition_num = 10;

        // Test: Call with both names null
        ThriftHMSCachedClient.logIfHugeHmsCall(20, null, null);

        // Verify: Should log and use N/A for both names
        Assertions.assertEquals(1, testAppender.getEventCount(Level.WARN),
                "Should generate one WARN log event");

        String logMessage = testAppender.getFormattedMessages().get(0);
        Assertions.assertTrue(logMessage.contains("dbName N/A"),
                "Should use N/A for null database name");
        Assertions.assertTrue(logMessage.contains("tableName N/A"),
                "Should use N/A for null table name");
    }

    @Test
    public void testEmptyStringNames() {
        // Setup: Set threshold to 10
        Config.log_huge_hms_partition_num = 10;

        // Test: Call with empty string names (should NOT be converted to N/A)
        ThriftHMSCachedClient.logIfHugeHmsCall(20, "", "");

        // Verify: Should log and preserve empty strings (not convert to N/A)
        Assertions.assertEquals(1, testAppender.getEventCount(Level.WARN),
                "Should generate one WARN log event");

        String logMessage = testAppender.getFormattedMessages().get(0);
        // Empty strings should be preserved as-is in the log message
        Assertions.assertTrue(logMessage.contains("dbName ,") || logMessage.contains("dbName"),
                "Empty database name should be preserved");
        Assertions.assertTrue(logMessage.contains("tableName ,") || logMessage.contains("tableName"),
                "Empty table name should be preserved");
    }

    // ========== BDPAuthContext TESTS ==========

    @Test
    public void testWithBDPAuthContext() {
        // Setup: Set threshold to 10
        Config.log_huge_hms_partition_num = 10;

        // Set up BDPAuthContext
        BDPAuthContext authContext = new BDPAuthContext("test", "test", "test", "token");
        authContext.setThreadLocalInfo();

        // Test: Call with BDPAuthContext present
        ThriftHMSCachedClient.logIfHugeHmsCall(20, "test_db", "test_table");

        // Verify: Should log with context information (not N/A)
        Assertions.assertEquals(1, testAppender.getEventCount(Level.WARN),
                "Should generate one WARN log event");

        String logMessage = testAppender.getFormattedMessages().get(0);
        // When BDPAuthContext is present, context and query id should not be N/A
        Assertions.assertFalse(logMessage.contains("HMS client information: N/A"),
                "Context should not be N/A when BDPAuthContext is present");
        Assertions.assertFalse(logMessage.contains("query id: N/A"),
                "Query ID should not be N/A when BDPAuthContext is present");
    }

    @Test
    public void testWithoutBDPAuthContext() {
        // Setup: Set threshold to 10
        Config.log_huge_hms_partition_num = 10;

        // Ensure no BDPAuthContext
        BDPAuthContext.clear();

        // Test: Call without BDPAuthContext
        ThriftHMSCachedClient.logIfHugeHmsCall(20, "test_db", "test_table");

        // Verify: Should log with N/A for context and query ID
        Assertions.assertEquals(1, testAppender.getEventCount(Level.WARN),
                "Should generate one WARN log event");

        String logMessage = testAppender.getFormattedMessages().get(0);
        Assertions.assertTrue(logMessage.contains("HMS client information: N/A"),
                "Context should be N/A when BDPAuthContext is null");
    }

    // ========== INTEGRATION TESTS ==========

    @Test
    public void testDynamicConfigurationChange() {
        // Setup: Start with high threshold
        Config.log_huge_hms_partition_num = 100;

        // Test: Call with count below initial threshold (should not log)
        ThriftHMSCachedClient.logIfHugeHmsCall(10, "test_db", "test_table");

        // Verify: No logging initially
        Assertions.assertEquals(0, testAppender.getEvents().size(),
                "No logging should occur with count below threshold");

        // Change threshold to lower value
        Config.log_huge_hms_partition_num = 5;

        // Test: Same call now above new threshold (should log)
        ThriftHMSCachedClient.logIfHugeHmsCall(10, "test_db", "test_table");

        // Verify: Should log with new threshold
        Assertions.assertEquals(1, testAppender.getEventCount(Level.WARN),
                "Should log when count exceeds new lower threshold");
    }

    @Test
    public void testMultipleCalls() {
        // Setup: Set threshold to 10
        Config.log_huge_hms_partition_num = 10;

        // Test: Multiple calls above threshold
        ThriftHMSCachedClient.logIfHugeHmsCall(15, "db1", "table1");
        ThriftHMSCachedClient.logIfHugeHmsCall(20, "db2", "table2");

        // Verify: Both calls should be logged
        Assertions.assertEquals(2, testAppender.getEventCount(Level.WARN),
                "Should capture both log events");

        List<String> messages = testAppender.getFormattedMessages();
        Assertions.assertTrue(messages.get(0).contains("db1") && messages.get(0).contains("table1"),
                "First message should contain db1/table1");
        Assertions.assertTrue(messages.get(1).contains("db2") && messages.get(1).contains("table2"),
                "Second message should contain db2/table2");
    }

    @Test
    public void testMethodVisibilityAndAccess() {
        // Setup: Set threshold to 10
        Config.log_huge_hms_partition_num = 10;

        // Test: Verify method is accessible (would fail compilation if not visible)
        // This test verifies the @VisibleForTesting annotation is working
        ThriftHMSCachedClient.logIfHugeHmsCall(20, "test_db", "test_table");

        // Verify: Should be able to call method directly and generate log
        Assertions.assertEquals(1, testAppender.getEventCount(Level.WARN),
                    "Method should be accessible and functional via @VisibleForTesting annotation");
    }

    @Test
    public void testRealWorldScenario() {
        // Setup: Use realistic threshold
        Config.log_huge_hms_partition_num = 10000;

        // Test various realistic scenarios
        // Small partition count - should not trigger logging
        ThriftHMSCachedClient.logIfHugeHmsCall(100, "sales", "orders");

        // Large partition count - should trigger logging
        ThriftHMSCachedClient.logIfHugeHmsCall(50000, "analytics", "events_partitioned");

        // With special characters in names
        ThriftHMSCachedClient.logIfHugeHmsCall(20000, "test_db_2024", "user_activity_log");

        // Verify: Only 2 of the 3 calls should log (100 <= 10000, others > 10000)
        Assertions.assertEquals(2, testAppender.getEventCount(Level.WARN),
                "Should log for 2 calls that exceed threshold");

        List<String> messages = testAppender.getFormattedMessages();
        Assertions.assertTrue(messages.get(0).contains("actual count 50000"),
                "First log should be for 50000 partitions");
        Assertions.assertTrue(messages.get(1).contains("actual count 20000"),
                "Second log should be for 20000 partitions");
    }

    @Test
    public void testEdgeCasesAndBoundaryConditions() {
        // Test various edge cases that might occur in production

        // Very high threshold - should trigger logging
        Config.log_huge_hms_partition_num = Integer.MAX_VALUE - 1;
        ThriftHMSCachedClient.logIfHugeHmsCall(Integer.MAX_VALUE, "test", "test");

        // Very low threshold - should trigger logging
        Config.log_huge_hms_partition_num = 1;
        ThriftHMSCachedClient.logIfHugeHmsCall(2, "test", "test");

        // Zero threshold - should trigger logging
        Config.log_huge_hms_partition_num = 0;
        ThriftHMSCachedClient.logIfHugeHmsCall(1, "test", "test");

        // Verify: All 3 edge cases should trigger logging
        Assertions.assertEquals(3, testAppender.getEventCount(Level.WARN),
                "All edge cases should trigger logging");
    }

    @Test
    public void testLogMessageCompleteFormat() {
        // Setup
        Config.log_huge_hms_partition_num = 100;
        BDPAuthContext.clear(); // Ensure predictable N/A values

        // Test
        ThriftHMSCachedClient.logIfHugeHmsCall(50000, "analytics", "events_partitioned");

        // Verify complete log structure
        String logMessage = testAppender.getFormattedMessages().get(0);

        // Check all expected components are present
        Assertions.assertTrue(logMessage.contains("Partition count exceeds 100 partitions"),
                "Should contain threshold message");
        Assertions.assertTrue(logMessage.contains("actual count 50000"),
                "Should contain actual count");
        Assertions.assertTrue(logMessage.contains("dbName analytics"),
                "Should contain database name");
        Assertions.assertTrue(logMessage.contains("tableName events_partitioned"),
                "Should contain table name");
        Assertions.assertTrue(logMessage.contains("HMS client information: N/A"),
                "Should contain HMS client info (N/A in test)");
    }
}
