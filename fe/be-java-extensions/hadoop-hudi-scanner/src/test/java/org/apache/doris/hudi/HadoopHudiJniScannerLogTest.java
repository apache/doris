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

package org.apache.doris.hudi;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

public class HadoopHudiJniScannerLogTest {
    @Test
    public void logsQueryIdAndParameterNamesWithoutCredentialValues() throws Exception {
        Map<String, String> hadoopProperties = new HashMap<>();
        hadoopProperties.put("fs.s3a.access.key", "synthetic-access-key");
        hadoopProperties.put("fs.s3a.secret.key", "synthetic-secret-key");
        hadoopProperties.put("fs.s3a.session.token", "synthetic-session-token");
        hadoopProperties.put("fs.s3a.bucket.test-bucket.access.key", "synthetic-bucket-access-key");
        hadoopProperties.put("fs.s3a.bucket.test-bucket.secret.key", "synthetic-bucket-secret-key");
        hadoopProperties.put("fs.s3a.bucket.test-bucket.session.token", "synthetic-bucket-session-token");
        hadoopProperties.put("custom.setting", "synthetic-custom-value");

        Map<String, String> params = new HashMap<>();
        params.put("query_id", "0123456789abcdef-0123456789abcdef");
        params.put("base_path", "file:///unused");
        params.put("data_file_path", "file:///unused/data.parquet");
        params.put("data_file_length", "0");
        params.put("hudi_column_names", "id");
        params.put("hudi_column_types", "int");
        params.put("columns_types", "int");
        params.put("required_fields", "id");
        params.put("time_zone", "UTC");
        params.put("hoodie.custom.token", "synthetic-hudi-token");
        hadoopProperties.forEach((key, value) -> params.put("hadoop_conf." + key, value));

        try (CapturedLog capture = new CapturedLog()) {
            Assert.assertTrue("The test must exercise DEBUG parameter logging",
                    LoggerFactory.getLogger(HadoopHudiJniScanner.class).isDebugEnabled());
            HadoopHudiJniScanner scanner = new HadoopHudiJniScanner(16, params);
            String text = String.join("\n", capture.messages);
            Assert.assertTrue("Keep the query ID for diagnostics", text.contains(params.get("query_id")));
            for (String key : params.keySet()) {
                Assert.assertTrue("Missing parameter name: " + key, text.contains(key));
            }
            for (String value : hadoopProperties.values()) {
                Assert.assertFalse("Hadoop parameter values must not be logged", text.contains(value));
            }
            Assert.assertFalse("Non-Hadoop credentials must not be logged either",
                    text.contains(params.get("hoodie.custom.token")));

            Field properties = HadoopHudiJniScanner.class.getDeclaredField("fsOptionsProps");
            properties.setAccessible(true);
            Assert.assertEquals("Removing values from logs must preserve the actual Hadoop configuration",
                    hadoopProperties, properties.get(scanner));
        }
    }

    private static class CapturedLog implements AutoCloseable {
        private final List<String> messages = new CopyOnWriteArrayList<>();
        private final Logger logger = (Logger) LogManager.getLogger(HadoopHudiJniScanner.class);
        private final org.apache.log4j.Logger reload4j =
                org.apache.log4j.Logger.getLogger(HadoopHudiJniScanner.class);
        private final org.apache.log4j.Level previousReload4jLevel = reload4j.getLevel();
        private final boolean previousReload4jAdditive = reload4j.getAdditivity();
        private final org.apache.log4j.AppenderSkeleton reload4jAppender = new org.apache.log4j.AppenderSkeleton() {
            @Override
            protected void append(org.apache.log4j.spi.LoggingEvent event) {
                messages.add(event.getRenderedMessage());
            }

            @Override
            public void close() {
            }

            @Override
            public boolean requiresLayout() {
                return false;
            }
        };
        private final Level previousLevel = logger.getLevel();
        private final boolean previousAdditive = logger.isAdditive();
        private final AbstractAppender appender = new AbstractAppender("hudi-parameter-test", null,
                null, true, Property.EMPTY_ARRAY) {
            @Override
            public void append(LogEvent event) {
                messages.add(event.getMessage().getFormattedMessage());
            }
        };

        CapturedLog() {
            reload4j.setLevel(org.apache.log4j.Level.DEBUG);
            reload4j.setAdditivity(false);
            reload4j.addAppender(reload4jAppender);
            appender.start();
            logger.setLevel(Level.DEBUG);
            logger.setAdditive(false);
            logger.addAppender(appender);
        }

        @Override
        public void close() {
            logger.removeAppender(appender);
            logger.setLevel(previousLevel);
            logger.setAdditive(previousAdditive);
            appender.stop();
            reload4j.removeAppender(reload4jAppender);
            reload4j.setLevel(previousReload4jLevel);
            reload4j.setAdditivity(previousReload4jAdditive);
            reload4jAppender.close();
        }
    }
}
