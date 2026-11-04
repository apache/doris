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

package org.apache.doris.lakesoul;

import com.dmetasoul.lakesoul.lakesoul.io.NativeIOReader;
import com.lakesoul.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LakeSoulJniScannerTest {
    @Test
    public void testOpenDoesNotLogCredentialsAtInfo() throws Exception {
        assertSafeOpen(Level.INFO, "test-query-id");
    }

    @Test
    public void testOpenDoesNotLogCredentialsAtDebug() throws Exception {
        assertSafeOpen(Level.DEBUG, "test-query-id");
    }

    @Test
    public void testOpenWithoutQueryIdDoesNotLogCredentials() throws Exception {
        assertSafeOpen(Level.DEBUG, null);
    }

    private void assertSafeOpen(Level level, String queryId) throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(LakeSoulUtils.FS_S3A_ACCESS_KEY, "FAKE_ACCESS_KEY");
        options.put(LakeSoulUtils.FS_S3A_SECRET_KEY, "FAKE_SECRET_KEY");
        options.put(LakeSoulUtils.FS_S3A_ENDPOINT, "https://object-store.invalid");
        options.put(LakeSoulUtils.FS_S3A_PATH_STYLE_ACCESS, "true");
        Map<String, String> params = new HashMap<>();
        if (queryId != null) {
            params.put("query_id", queryId);
        }
        params.put(LakeSoulUtils.OPTIONS, new ObjectMapper().writeValueAsString(options));
        params.put(LakeSoulUtils.FILE_NAMES, "s3://test/file?signature=FAKE_SIGNATURE");
        params.put(LakeSoulUtils.SCHEMA_JSON, "{\"fields\":[]}");
        params.put(LakeSoulUtils.REQUIRED_FIELDS, "");
        params.put("future_auth_option", "FAKE_FUTURE_TOKEN");
        Map<String, String> originalParams = new HashMap<>(params);
        IOException stopBeforeNativeIo = new IOException("stop before native initialization");

        Logger logger = (Logger) LogManager.getLogger(LakeSoulJniScanner.class);
        Level originalLevel = logger.getLevel();
        boolean originalAdditive = logger.isAdditive();
        CapturingAppender appender = new CapturingAppender();
        appender.start();
        logger.addAppender(appender);
        logger.setLevel(level);
        logger.setAdditive(false);
        try (MockedConstruction<NativeIOReader> readers = Mockito.mockConstruction(
                NativeIOReader.class, (reader, context) ->
                        Mockito.doThrow(stopBeforeNativeIo).when(reader).initializeReader())) {
            LakeSoulJniScanner scanner = new LakeSoulJniScanner(128, params);
            Assert.assertSame(stopBeforeNativeIo, Assert.assertThrows(IOException.class, scanner::open));
            Assert.assertEquals(1, readers.constructed().size());
            NativeIOReader reader = readers.constructed().get(0);
            Mockito.verify(reader).setBatchSize(128);
            for (Map.Entry<String, String> option : options.entrySet()) {
                Mockito.verify(reader).setObjectStoreOption(option.getKey(), option.getValue());
            }
            Assert.assertEquals(originalParams, params);

            String logs = String.join("\n", appender.messages);
            Assert.assertTrue(logs.contains("opening LakeSoulJniScanner"));
            Assert.assertFalse("Credentials must not appear in scanner logs", logs.contains("FAKE_"));
            Assert.assertTrue(logs.contains("queryId=" + queryId));
            Assert.assertTrue(logs.contains("batchSize=128"));
            Assert.assertFalse(logs.contains("fs.s3a."));
            Assert.assertFalse(logs.contains("options="));
            Assert.assertFalse(logs.contains("future_auth_option"));
        } finally {
            logger.removeAppender(appender);
            appender.stop();
            logger.setLevel(originalLevel);
            logger.setAdditive(originalAdditive);
        }
    }

    private static class CapturingAppender extends AbstractAppender {
        private final List<String> messages = new ArrayList<>();

        CapturingAppender() {
            super("lakesoul-scanner-test-capture", null, null, false, Property.EMPTY_ARRAY);
        }

        @Override
        public void append(LogEvent event) {
            messages.add(event.getMessage().getFormattedMessage());
        }
    }
}
