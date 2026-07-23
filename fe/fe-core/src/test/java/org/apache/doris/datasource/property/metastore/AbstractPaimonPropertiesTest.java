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

package org.apache.doris.datasource.property.metastore;

import org.apache.doris.datasource.property.storage.StorageProperties;

import org.apache.paimon.catalog.Catalog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AbstractPaimonPropertiesTest {

    private static class TestPaimonProperties extends AbstractPaimonProperties {


        protected TestPaimonProperties(Map<String, String> props) {
            super(props);
        }

        @Override
        public String getPaimonCatalogType() {
            return "test";
        }

        @Override
        public Catalog initializeCatalog(String catalogName, List<StorageProperties> storagePropertiesList) {
            return null;
        }

        @Override
        protected void appendCustomCatalogOptions() {

        }

        @Override
        protected String getMetastoreType() {
            return "test";
        }
    }

    TestPaimonProperties props;

    @BeforeEach
    void setup() {
        Map<String, String> input = new HashMap<>();
        input.put("warehouse", "s3://tmp/warehouse");
        input.put("paimon.metastore", "filesystem");
        input.put("paimon.s3.access-key", "AK");
        input.put("paimon.s3.secret-key", "SK");
        input.put("paimon.custom.key", "value");
        props = new TestPaimonProperties(input);
    }

    @Test
    void testNormalizeS3Config() {
        Map<String, String> input = new HashMap<>();
        input.put("paimon.s3.list.version", "1");
        input.put("paimon.s3.paging.maximum", "100");
        input.put("paimon.fs.s3.read.ahead.buffer.size", "1");
        input.put("paimon.s3a.replication.factor", "3");
        TestPaimonProperties testProps = new TestPaimonProperties(input);
        Map<String, String> result = testProps.normalizeS3Config();
        Assertions.assertTrue("1".equals(result.get("fs.s3a.list.version")));
        Assertions.assertTrue("100".equals(result.get("fs.s3a.paging.maximum")));
        Assertions.assertTrue("1".equals(result.get("fs.s3a.read.ahead.buffer.size")));
        Assertions.assertTrue("3".equals(result.get("fs.s3a.replication.factor")));
    }

    @Test
    void testExtractAndValidateTableOptions() {
        Map<String, String> input = new HashMap<>();
        input.put("warehouse", "s3://tmp/warehouse");
        input.put("paimon.table-option.read.batch-size", "4096");
        input.put("paimon.table-option.file.compression.per.level.0", "lz4");
        TestPaimonProperties testProps = new TestPaimonProperties(input);

        testProps.initNormalizeAndCheckProps();
        testProps.buildCatalogOptions();

        Assertions.assertEquals("4096", testProps.getTableOptionsMap().get("read.batch-size"));
        Assertions.assertEquals(
                "lz4", testProps.getTableOptionsMap().get("file.compression.per.level.0"));
        Assertions.assertFalse(testProps.getCatalogOptionsMap().containsKey("table-option.read.batch-size"));
    }

    @Test
    void testRejectUnknownTableOption() {
        Map<String, String> input = new HashMap<>();
        input.put("warehouse", "s3://tmp/warehouse");
        input.put("paimon.table-option.option-does-not-exist", "value");
        TestPaimonProperties testProps = new TestPaimonProperties(input);

        IllegalArgumentException exception = Assertions.assertThrows(
                IllegalArgumentException.class, testProps::initNormalizeAndCheckProps);

        Assertions.assertTrue(exception.getMessage().contains("option-does-not-exist"));
    }

    @Test
    void testRejectInvalidTableOptionValue() {
        Map<String, String> input = new HashMap<>();
        input.put("warehouse", "s3://tmp/warehouse");
        input.put("paimon.table-option.read.batch-size", "not-an-integer");
        TestPaimonProperties testProps = new TestPaimonProperties(input);

        IllegalArgumentException exception = Assertions.assertThrows(
                IllegalArgumentException.class, testProps::initNormalizeAndCheckProps);

        Assertions.assertTrue(exception.getMessage().contains("read.batch-size"));
    }

}
