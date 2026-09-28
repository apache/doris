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

import org.apache.doris.datasource.CatalogProperty;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.persist.gson.GsonUtils;

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
        input.put("paimon.jni.enable_jni_io_manager", "true");
        input.put("paimon.table-option.read.batch-size", "4096");
        input.put("paimon.table-option.file-reader-async-threshold", "16 MB");
        input.put("paimon.table-option.file-index.read.enabled", "false");
        input.put("paimon.table-option.source.split.target-size", "64 MB");
        input.put("paimon.table-option.source.split.open-file-cost", "1 MB");
        input.put("paimon.table-option.scan.manifest.parallelism", "1");
        input.put("paimon.table-option.scan.plan-sort-partition", "true");
        TestPaimonProperties testProps = new TestPaimonProperties(input);

        testProps.initNormalizeAndCheckProps();
        testProps.buildCatalogOptions();

        Assertions.assertEquals("4096", testProps.getTableOptionsMap().get("read.batch-size"));
        Assertions.assertEquals("16 MB",
                testProps.getTableOptionsMap().get("file-reader-async-threshold"));
        Assertions.assertEquals("false", testProps.getTableOptionsMap().get("file-index.read.enabled"));
        Assertions.assertEquals("64 MB", testProps.getTableOptionsMap().get("source.split.target-size"));
        Assertions.assertEquals("1 MB", testProps.getTableOptionsMap().get("source.split.open-file-cost"));
        Assertions.assertEquals("1", testProps.getTableOptionsMap().get("scan.manifest.parallelism"));
        Assertions.assertEquals("true", testProps.getTableOptionsMap().get("scan.plan-sort-partition"));
        Assertions.assertFalse(testProps.getCatalogOptionsMap().containsKey("table-option.read.batch-size"));
        Assertions.assertFalse(testProps.getCatalogOptionsMap().containsKey("jni.enable_jni_io_manager"));
    }

    @Test
    void testCatalogReaderOptionsTakePrecedenceOverPhysicalTableOptions() {
        Map<String, String> input = new HashMap<>();
        input.put("warehouse", "s3://tmp/warehouse");
        input.put("paimon.table-option.read.batch-size", "4096");
        input.put("paimon.table-option.file-reader-async-threshold", "16 MB");
        TestPaimonProperties testProps = new TestPaimonProperties(input);
        testProps.initNormalizeAndCheckProps();

        Map<String, String> optionsForCopy = testProps.getTableOptionsForCopy();

        Assertions.assertEquals("4096", optionsForCopy.get("read.batch-size"));
        Assertions.assertEquals("16 MB", optionsForCopy.get("file-reader-async-threshold"));
    }

    @Test
    void testCatalogTableOptionsFillMissingPaimonTableOptions() {
        Map<String, String> input = new HashMap<>();
        input.put("warehouse", "s3://tmp/warehouse");
        input.put("paimon.table-option.read.batch-size", "4096");
        TestPaimonProperties testProps = new TestPaimonProperties(input);
        testProps.initNormalizeAndCheckProps();

        Map<String, String> optionsForCopy = testProps.getTableOptionsForCopy();

        Assertions.assertEquals("4096", optionsForCopy.get("read.batch-size"));
    }

    @Test
    void testPersistedUnknownTableOptionDoesNotPreventCatalogLoading() {
        Map<String, String> input = new HashMap<>();
        input.put("warehouse", "s3://tmp/warehouse");
        input.put("paimon.table-option.option-does-not-exist", "value");
        TestPaimonProperties testProps = new TestPaimonProperties(input);

        testProps.initNormalizeAndCheckProps();

        Assertions.assertTrue(testProps.getTableOptionsMap().isEmpty());
    }

    @Test
    void testPersistedPrefixMapTableOptionDoesNotPreventCatalogLoading() {
        Map<String, String> input = new HashMap<>();
        input.put("warehouse", "s3://tmp/warehouse");
        input.put("paimon.table-option.file.compression.per.level.0", "lz4");
        TestPaimonProperties testProps = new TestPaimonProperties(input);

        testProps.initNormalizeAndCheckProps();

        Assertions.assertTrue(testProps.getTableOptionsMap().isEmpty());
    }

    @Test
    void testPersistedInvalidReaderOptionIsIgnoredDuringCatalogLoading() {
        Map<String, String> input = new HashMap<>();
        input.put("warehouse", "s3://tmp/warehouse");
        input.put("paimon.table-option.read.batch-size", "not-an-integer");
        TestPaimonProperties testProps = new TestPaimonProperties(input);

        testProps.initNormalizeAndCheckProps();

        Assertions.assertTrue(testProps.getTableOptionsMap().isEmpty());
    }

    @Test
    void testPersistedUnsafeTableOptionsAreIgnoredDuringCatalogLoading() {
        for (String option : new String[] {
                "branch", "path", "scan.tag-name", "scan.snapshot-id",
                "write.batch-size", "file.compression.per.level"
        }) {
            Map<String, String> input = new HashMap<>();
            input.put("warehouse", "s3://tmp/warehouse");
            input.put("paimon.table-option." + option, "1");

            TestPaimonProperties testProps = new TestPaimonProperties(input);
            testProps.initNormalizeAndCheckProps();
            Assertions.assertTrue(testProps.getTableOptionsMap().isEmpty(), option);
        }
    }

    @Test
    void testPersistedOutOfRangeReaderOptionsAreIgnoredDuringCatalogLoading() {
        Map<String, String> invalidOptions = new HashMap<>();
        invalidOptions.put("read.batch-size", "0");
        invalidOptions.put("read.batch-size-negative", "-1");
        invalidOptions.put("read.batch-size-too-large", "65537");
        invalidOptions.put("file-reader-async-threshold", "0 B");
        invalidOptions.put("file-reader-async-threshold-too-small", "512 KB");
        invalidOptions.put("file-reader-async-threshold-too-large", "2 GB");

        invalidOptions.forEach((caseName, value) -> {
            String option = caseName.startsWith("read.batch-size")
                    ? "read.batch-size" : "file-reader-async-threshold";
            Map<String, String> input = new HashMap<>();
            input.put("warehouse", "s3://tmp/warehouse");
            input.put("paimon.table-option." + option, value);

            TestPaimonProperties testProps = new TestPaimonProperties(input);
            testProps.initNormalizeAndCheckProps();
            Assertions.assertTrue(testProps.getTableOptionsMap().isEmpty(), caseName);
        });
    }

    @Test
    void testImageRoundTripKeepsLegacyCatalogLoadableButDoesNotApplyUnsafeOption() {
        Map<String, String> input = new HashMap<>();
        input.put("warehouse", "s3://tmp/warehouse");
        input.put("paimon.table-option.write.batch-size", "2048");
        CatalogProperty persisted = new CatalogProperty(null, input);

        CatalogProperty restored = GsonUtils.GSON.fromJson(
                GsonUtils.GSON.toJson(persisted), CatalogProperty.class);
        TestPaimonProperties testProps = new TestPaimonProperties(restored.getProperties());
        testProps.initNormalizeAndCheckProps();

        Assertions.assertTrue(testProps.getTableOptionsMap().isEmpty());
    }

    @Test
    void testForwardTableDefaultOptionsToPaimonCatalog() {
        Map<String, String> input = new HashMap<>();
        input.put("paimon.table-default.scan.mode", "latest");
        input.put("paimon.table-default.scan.snapshot-id", "7");
        TestPaimonProperties testProps = new TestPaimonProperties(input);

        testProps.buildCatalogOptions();

        Assertions.assertEquals(
                "latest", testProps.getCatalogOptionsMap().get("table-default.scan.mode"));
        Assertions.assertEquals(
                "7", testProps.getCatalogOptionsMap().get("table-default.scan.snapshot-id"));
    }

    @Test
    public void testWeightGovernanceDisablesSdkMetadataCacheUnlessUserConfigured() {
        // Doris weight governance owns retention: Paimon's own CachingCatalog would retain
        // snapshot/statistics/manifest caches outside the budget, so it is disabled by default.
        Map<String, String> props = new HashMap<>();
        props.put("warehouse", "file:///tmp/warehouse");
        TestPaimonProperties governed = new TestPaimonProperties(props);
        governed.setDisableSdkMetadataCacheByDefault(true);
        governed.buildCatalogOptions();
        Assertions.assertEquals("false", governed.getCatalogOptionsMap().get("cache-enabled"));

        // An explicit user choice wins over the default.
        Map<String, String> userProps = new HashMap<>(props);
        userProps.put("paimon.cache-enabled", "true");
        TestPaimonProperties userConfigured = new TestPaimonProperties(userProps);
        userConfigured.setDisableSdkMetadataCacheByDefault(true);
        userConfigured.buildCatalogOptions();
        Assertions.assertEquals("true", userConfigured.getCatalogOptionsMap().get("cache-enabled"));

        // Without weight governance the SDK default stays untouched.
        TestPaimonProperties ungoverned = new TestPaimonProperties(new HashMap<>(props));
        ungoverned.buildCatalogOptions();
        Assertions.assertFalse(ungoverned.getCatalogOptionsMap().containsKey("cache-enabled"));
    }
}
