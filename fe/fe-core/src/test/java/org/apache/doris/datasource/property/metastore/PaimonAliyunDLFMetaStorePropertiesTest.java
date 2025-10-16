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

import org.apache.doris.common.UserException;
import org.apache.doris.datasource.property.storage.StorageProperties;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PaimonAliyunDLFMetaStorePropertiesTest {
    private Map<String, String> createValidProps() {
        Map<String, String> props = new HashMap<>();
        props.put("type", "paimon");
        props.put("paimon.catalog.type", "dlf");
        props.put("dlf.access_key", "ak");
        props.put("dlf.secret_key", "sk");
        props.put("dlf.endpoint", "dlf.cn-hangzhou.aliyuncs.com");
        props.put("dlf.region", "cn-hangzhou");
        props.put("dlf.catalog.id", "catalogId");
        props.put("dlf.uid", "uid");
        props.put("warehouse", "oss://bucket/warehouse");
        return props;
    }

    @Test
    void testInitNormalizeAndCheckProps() {
        Map<String, String> props = createValidProps();
        PaimonAliyunDLFMetaStoreProperties dlfProps =
                new PaimonAliyunDLFMetaStoreProperties(props);

        dlfProps.initNormalizeAndCheckProps();

        Assertions.assertEquals(
                "dlf",
                dlfProps.getPaimonCatalogType(),
                "Catalog type should be PAIMON_DLF"
        );
        Assertions.assertEquals(
                "hive",
                dlfProps.getMetastoreType(),
                "Metastore type should be hive"
        );
    }

    @Test
    void testInitializeCatalogWithValidOssProperties() throws UserException {
        Map<String, String> props = createValidProps();
        PaimonAliyunDLFMetaStoreProperties dlfProps =
                new PaimonAliyunDLFMetaStoreProperties(props);
        dlfProps.initNormalizeAndCheckProps();

        // Prepare OSSProperties mock
        Map<String, String> ossProps = new HashMap<>();
        ossProps.put("oss.access_key", "ak");
        ossProps.put("oss.secret_key", "sk");
        ossProps.put("oss.endpoint", "oss-cn-hangzhou.aliyuncs.com");


        List<StorageProperties> storageProperties = StorageProperties.createAll(ossProps);

        Catalog mockCatalog = Mockito.mock(Catalog.class);

        try (MockedStatic<CatalogFactory> mocked = Mockito.mockStatic(CatalogFactory.class)) {
            mocked.when(() -> CatalogFactory.createCatalog(Mockito.any(CatalogContext.class)))
                    .thenReturn(mockCatalog);

            Catalog catalog = dlfProps.initializeCatalog("testCatalog", storageProperties);

            Assertions.assertNotNull(catalog, "Catalog should not be null");
            Assertions.assertEquals(mockCatalog, catalog, "Catalog should be the mocked one");

            mocked.verify(() -> CatalogFactory.createCatalog(Mockito.any(CatalogContext.class)));
        }
    }

    @Test
    void testInitializeCatalogWithoutOssPropertiesThrows() {
        Map<String, String> props = createValidProps();
        PaimonAliyunDLFMetaStoreProperties dlfProps =
                new PaimonAliyunDLFMetaStoreProperties(props);
        dlfProps.initNormalizeAndCheckProps();

        List<StorageProperties> storageProperties = new ArrayList<>(); // No OSS properties

        IllegalStateException ex = Assertions.assertThrows(
                IllegalStateException.class,
                () -> dlfProps.initializeCatalog("testCatalog", storageProperties)
        );

        Assertions.assertTrue(ex.getMessage().contains("OSS storage properties"));
    }

    @Test
    void testInitializeCatalogWithNonOssTypeThrows() {
        Map<String, String> props = createValidProps();
        PaimonAliyunDLFMetaStoreProperties dlfProps =
                new PaimonAliyunDLFMetaStoreProperties(props);
        dlfProps.initNormalizeAndCheckProps();

        StorageProperties nonOssProps = Mockito.mock(StorageProperties.class);
        Mockito.when(nonOssProps.getType()).thenReturn(StorageProperties.Type.HDFS);

        List<StorageProperties> storageProperties = Collections.singletonList(nonOssProps);

        IllegalStateException ex = Assertions.assertThrows(
                IllegalStateException.class,
                () -> dlfProps.initializeCatalog("testCatalog", storageProperties)
        );

        Assertions.assertTrue(ex.getMessage().contains("Paimon DLF metastore requires OSS storage properties."));
    }
}
