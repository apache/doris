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

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

public class Log4jConfigTest {

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
            Assert.assertTrue("Size strategy should use IfAccumulatedFileSize",
                    sizeResult.contains("IfAccumulatedFileSize"));
            Assert.assertFalse("Size strategy should not use IfLastModified",
                    sizeResult.contains("IfLastModified"));

            // Test age strategy
            Config.log_rollover_strategy = "age";
            StringBuilder ageBuilder = new StringBuilder();
            builderField.set(null, ageBuilder);
            method.invoke(null, "info_sys_accumulated_file_size", "sys_log_delete_age");
            String ageResult = ageBuilder.toString();
            Assert.assertTrue("Age strategy should use IfLastModified",
                    ageResult.contains("IfLastModified"));
            Assert.assertFalse("Age strategy should not use IfAccumulatedFileSize",
                    ageResult.contains("IfAccumulatedFileSize"));
        } finally {
            // Restore original state
            Config.log_rollover_strategy = origStrategy;
            builderField.set(null, originalBuilder);
        }
    }
}
