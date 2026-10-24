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

package org.apache.doris.common;

import org.apache.doris.thrift.TUniqueId;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.ThreadContext;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.impl.Log4jLogEvent;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.message.SimpleMessage;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

public class Log4jConfigTest {

    @Test
    public void testQueryIdAppearsOnceWithAndWithoutThePrefix() {
        boolean savedEnabled = Config.sys_log_enable_query_id;
        String savedQueryId = ThreadContext.get(QueryLogContext.QUERY_ID);
        TUniqueId queryId = new TUniqueId(1, 2);
        try {
            for (boolean enabled : new boolean[] {true, false}) {
                Config.sys_log_enable_query_id = enabled;
                ThreadContext.remove(QueryLogContext.QUERY_ID);
                PatternLayout layout = PatternLayout.newBuilder()
                        .withPattern(Log4jConfig.getQueryLogPattern() + "%m").build();
                LogEvent captured;
                try (QueryLogContext ignored = QueryLogContext.open(queryId)) {
                    captured = Log4jLogEvent.newBuilder().setLevel(Level.INFO)
                            .setMessage(new ParameterizedMessage("Query{} finished",
                                    QueryLogContext.queryIdSuffix(queryId))).build().toImmutable();
                }
                // The async formatter can run after this worker has started a different query.
                QueryLogContext.setQueryId(new TUniqueId(3, 4));
                Assertions.assertEquals(enabled ? "[1-2] Query finished" : "Query [1-2] finished",
                        layout.toSerializable(captured));

                ThreadContext.remove(QueryLogContext.QUERY_ID);
                LogEvent missingContext = Log4jLogEvent.newBuilder().setLevel(Level.INFO)
                        .setMessage(new ParameterizedMessage("Query{} finished",
                                QueryLogContext.queryIdSuffix(queryId))).build().toImmutable();
                Assertions.assertEquals("Query [1-2] finished", layout.toSerializable(missingContext));
            }
        } finally {
            Config.sys_log_enable_query_id = savedEnabled;
            if (savedQueryId == null) {
                ThreadContext.remove(QueryLogContext.QUERY_ID);
            } else {
                ThreadContext.put(QueryLogContext.QUERY_ID, savedQueryId);
            }
        }
    }

    @Test
    public void testQueryLogPatternFormatsTheEventSnapshot() {
        boolean savedEnabled = Config.sys_log_enable_query_id;
        String savedQueryId = ThreadContext.get(QueryLogContext.QUERY_ID);
        try {
            Config.sys_log_enable_query_id = true;
            ThreadContext.remove(QueryLogContext.QUERY_ID);
            PatternLayout layout = PatternLayout.newBuilder()
                    .withPattern(Log4jConfig.getQueryLogPattern() + "%m").build();
            LogEvent withoutQuery = Log4jLogEvent.newBuilder().setLevel(Level.INFO)
                    .setMessage(new SimpleMessage("message")).build();
            Assertions.assertEquals("message", layout.toSerializable(withoutQuery));

            LogEvent captured;
            try (QueryLogContext ignored = QueryLogContext.open(new TUniqueId(0x1234, 0xabcd))) {
                captured = Log4jLogEvent.newBuilder().setLevel(Level.INFO)
                        .setMessage(new SimpleMessage("message")).build().toImmutable();
            }
            QueryLogContext.setQueryId(new TUniqueId(5, 6));
            // Delayed formatting must use the event's ID rather than the thread's next query.
            Assertions.assertEquals("[1234-abcd] message", layout.toSerializable(captured));

            Config.sys_log_enable_query_id = false;
            Assertions.assertEquals("", Log4jConfig.getQueryLogPattern());
            PatternLayout disabledLayout = PatternLayout.newBuilder()
                    .withPattern(Log4jConfig.getQueryLogPattern() + "%m").build();
            Assertions.assertEquals("message", disabledLayout.toSerializable(captured));
        } finally {
            Config.sys_log_enable_query_id = savedEnabled;
            if (savedQueryId == null) {
                ThreadContext.remove(QueryLogContext.QUERY_ID);
            } else {
                ThreadContext.put(QueryLogContext.QUERY_ID, savedQueryId);
            }
        }
    }

    /**
     * Test that getXmlConfByStrategy correctly reads Config.log_rollover_strategy
     * and generates the corresponding XML snippet.
     *
     * This is the core of the console mode bug fix: previously Log4jConfig's static
     * block called getXmlConfByStrategy() before Config.init(), so it always saw
     * the default "age" value. After the fix, the static block runs after Config.init(),
     * ensuring the correct strategy is used.
     */
    @Test
    public void testGetXmlConfByStrategyReadsConfig() throws Exception {
        Field builderField = Log4jConfig.class.getDeclaredField("xmlConfTemplateBuilder");
        builderField.setAccessible(true);
        StringBuilder originalBuilder = (StringBuilder) builderField.get(null);

        Method method = Log4jConfig.class.getDeclaredMethod(
                "getXmlConfByStrategy", String.class, String.class);
        method.setAccessible(true);

        String origStrategy = Config.log_rollover_strategy;
        try {
            // Test size strategy
            Config.log_rollover_strategy = "size";
            StringBuilder sizeBuilder = new StringBuilder();
            builderField.set(null, sizeBuilder);
            method.invoke(null, "info_sys_accumulated_file_size", "sys_log_delete_age");
            String sizeResult = sizeBuilder.toString();
            Assertions.assertTrue(sizeResult.contains("IfAccumulatedFileSize"), "Size strategy should use IfAccumulatedFileSize");
            Assertions.assertFalse(sizeResult.contains("IfLastModified"), "Size strategy should not use IfLastModified");

            // Test age strategy
            Config.log_rollover_strategy = "age";
            StringBuilder ageBuilder = new StringBuilder();
            builderField.set(null, ageBuilder);
            method.invoke(null, "info_sys_accumulated_file_size", "sys_log_delete_age");
            String ageResult = ageBuilder.toString();
            Assertions.assertTrue(ageResult.contains("IfLastModified"), "Age strategy should use IfLastModified");
            Assertions.assertFalse(ageResult.contains("IfAccumulatedFileSize"), "Age strategy should not use IfAccumulatedFileSize");
        } finally {
            // Restore original state
            Config.log_rollover_strategy = origStrategy;
            builderField.set(null, originalBuilder);
        }
    }
}
