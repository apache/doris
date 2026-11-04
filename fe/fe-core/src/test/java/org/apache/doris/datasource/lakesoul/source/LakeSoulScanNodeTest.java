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

package org.apache.doris.datasource.lakesoul.source;

import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.TableFormatType;
import org.apache.doris.datasource.lakesoul.LakeSoulUtils;
import org.apache.doris.thrift.TFileRangeDesc;

import com.alibaba.fastjson.JSON;
import com.dmetasoul.lakesoul.meta.LakeSoulOptions;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LakeSoulScanNodeTest {
    @Test
    public void testScanOptionsPreserveCredentialsWithoutLoggingThem() throws Exception {
        Map<String, String> catalogProps = new HashMap<>();
        catalogProps.put("AWS_ENDPOINT", "https://object-store.invalid");
        catalogProps.put("AWS_ACCESS_KEY", "FAKE_ACCESS_KEY");
        catalogProps.put("AWS_SECRET_KEY", "FAKE_SECRET_KEY");
        catalogProps.put("AWS_REGION", "test-region");
        catalogProps.put("future_auth_option", "FAKE_FUTURE_TOKEN");
        Map<String, String> originalProps = new HashMap<>(catalogProps);
        ExternalCatalog catalog = Mockito.mock(ExternalCatalog.class);
        Mockito.when(catalog.getProperties()).thenReturn(catalogProps);
        LakeSoulScanNode scanNode = Mockito.mock(LakeSoulScanNode.class, Mockito.CALLS_REAL_METHODS);
        Mockito.doReturn(catalog).when(scanNode).getCatalog();
        scanNode.readType = LakeSoulOptions.ReadType$.MODULE$.FULL_READ();
        LakeSoulSplit split = new LakeSoulSplit(Collections.singletonList("s3://test/file"),
                Collections.emptyList(), Collections.emptyMap(), "{\"fields\":[]}",
                0, 0, 0, new String[0], Collections.emptyList());
        split.setTableFormatType(TableFormatType.LAKESOUL);
        TFileRangeDesc range = new TFileRangeDesc();

        Logger logger = (Logger) LogManager.getLogger(LakeSoulScanNode.class);
        Level originalLevel = logger.getLevel();
        boolean originalAdditive = logger.isAdditive();
        CapturingAppender appender = new CapturingAppender();
        appender.start();
        logger.addAppender(appender);
        logger.setLevel(Level.DEBUG);
        logger.setAdditive(false);
        // Predicate translation is independent of the catalog options being tested.
        try (MockedStatic<LakeSoulUtils> ignored = Mockito.mockStatic(LakeSoulUtils.class)) {
            scanNode.setScanParams(range, split);
            Map<String, Object> options = JSON.parseObject(
                    range.getTableFormatParams().getLakesoulParams().getOptions());
            Assert.assertEquals("FAKE_ACCESS_KEY", options.get(LakeSoulUtils.FS_S3A_ACCESS_KEY));
            Assert.assertEquals("FAKE_SECRET_KEY", options.get(LakeSoulUtils.FS_S3A_SECRET_KEY));
            Assert.assertEquals("https://object-store.invalid", options.get(LakeSoulUtils.FS_S3A_ENDPOINT));
            Assert.assertEquals("test-region", options.get(LakeSoulUtils.FS_S3A_REGION));
            Assert.assertEquals(originalProps, catalogProps);

            Assert.assertFalse(appender.messages.isEmpty());
            String logs = String.join("\n", appender.messages);
            Assert.assertFalse("Catalog credentials must not appear in logs", logs.contains("FAKE_"));
            Assert.assertFalse(logs.contains("AWS_ACCESS_KEY"));
            Assert.assertFalse(logs.contains("AWS_SECRET_KEY"));
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
            super("lakesoul-scan-node-test-capture", null, null, false, Property.EMPTY_ARRAY);
        }

        @Override
        public void append(LogEvent event) {
            messages.add(event.getMessage().getFormattedMessage());
        }
    }
}
