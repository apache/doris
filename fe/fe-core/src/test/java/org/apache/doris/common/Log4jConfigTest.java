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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

public class Log4jConfigTest {

    @Test
    public void testAlwaysOnRetainsAuditFileAppender() {
        boolean originalFileLogger = Config.enable_file_logger;
        boolean originalAlwaysOn = Config.query_audit_always_on;
        String template = "<!--REPLACED BY LOG APPENDER-->\n<!--REPLACED BY WARN_LOG_APPENDER-->\n"
                + "<!--REPLACED BY AUDIT FILE LOGGER-->";
        try {
            Config.enable_file_logger = false;
            Config.query_audit_always_on = false;
            Assertions.assertEquals(template, Log4jConfig.addFileAppenderReferences(template));

            Config.query_audit_always_on = true;
            String auditOnly = Log4jConfig.addFileAppenderReferences(template);
            Assertions.assertTrue(auditOnly.contains("ref=\"AuditFile\""));
            Assertions.assertFalse(auditOnly.contains("ref=\"Sys\""));
            Assertions.assertFalse(auditOnly.contains("level=\"WARN\""));

            Config.enable_file_logger = true;
            Config.query_audit_always_on = false;
            String allFiles = Log4jConfig.addFileAppenderReferences(template);
            Assertions.assertTrue(allFiles.contains("ref=\"AuditFile\""));
            Assertions.assertTrue(allFiles.contains("ref=\"Sys\""));
            Assertions.assertTrue(allFiles.contains("level=\"WARN\""));
        } finally {
            Config.enable_file_logger = originalFileLogger;
            Config.query_audit_always_on = originalAlwaysOn;
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
