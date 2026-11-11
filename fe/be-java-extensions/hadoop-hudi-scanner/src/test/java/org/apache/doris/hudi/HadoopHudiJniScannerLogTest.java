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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

class HadoopHudiJniScannerLogTest {
    @Test
    void logsQueryIdAndParameterNamesWithoutCredentialValues() throws Exception {
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
            Assertions.assertTrue(LoggerFactory.getLogger(HadoopHudiJniScanner.class).isDebugEnabled(),
                    "The test must exercise DEBUG parameter logging");
            HadoopHudiJniScanner scanner = new HadoopHudiJniScanner(16, params);
            String text = String.join("\n", capture.messages);
            Assertions.assertTrue(text.contains(params.get("query_id")),
                    "Keep the query ID for diagnostics");
            for (String key : params.keySet()) {
                Assertions.assertTrue(text.contains(key), "Missing parameter name: " + key);
            }
            for (String value : hadoopProperties.values()) {
                Assertions.assertFalse(text.contains(value), "Hadoop parameter values must not be logged");
            }
            Assertions.assertFalse(text.contains(params.get("hoodie.custom.token")),
                    "Non-Hadoop credentials must not be logged either");

            Field properties = HadoopHudiJniScanner.class.getDeclaredField("fsOptionsProps");
            properties.setAccessible(true);
            Assertions.assertEquals(hadoopProperties, properties.get(scanner),
                    "Removing values from logs must preserve the actual Hadoop configuration");
        }
    }

    private static class CapturedLog implements AutoCloseable {
        private final List<String> messages = new CopyOnWriteArrayList<>();
        private final Logger logger = Logger.getLogger(HadoopHudiJniScanner.class.getName());
        private final Level previousLevel = logger.getLevel();
        private final boolean previousUseParentHandlers = logger.getUseParentHandlers();
        private final Handler handler = new Handler() {
            @Override
            public void publish(LogRecord record) {
                messages.add(record.getMessage());
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };

        CapturedLog() {
            logger.setLevel(Level.FINE);
            logger.setUseParentHandlers(false);
            logger.addHandler(handler);
        }

        @Override
        public void close() {
            logger.removeHandler(handler);
            logger.setLevel(previousLevel);
            logger.setUseParentHandlers(previousUseParentHandlers);
            handler.close();
        }
    }
}
