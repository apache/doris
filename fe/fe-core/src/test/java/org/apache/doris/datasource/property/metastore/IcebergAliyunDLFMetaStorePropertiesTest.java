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

import org.apache.doris.datasource.iceberg.dlf.DLFCatalog;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.datasource.property.storage.exception.StoragePropertiesException;

import org.apache.iceberg.catalog.Catalog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class IcebergAliyunDLFMetaStorePropertiesTest {
    @Test
    void testGetIcebergCatalogType() {
        Map<String, String> props = new HashMap<>();
        props.put("dlf.access_key", "ak");
        props.put("dlf.secret_key", "sk");
        props.put("dlf.endpoint", "custom-endpoint");
        props.put("dlf.catalog.uid", "123");

        IcebergAliyunDLFMetaStoreProperties properties =
                new IcebergAliyunDLFMetaStoreProperties(props);

        Assertions.assertEquals("dlf", properties.getIcebergCatalogType());
    }

    @Test
    void testInitCatalog() {
        Map<String, String> props = new HashMap<>();
        props.put("dlf.access_key", "ak");
        props.put("dlf.secret_key", "sk");
        props.put("dlf.endpoint", "dlf-vpc.cn-beijing.aliyuncs.com");
        props.put("dlf.region", "cn-hz");
        props.put("dlf.catalog.uid", "uid-123");
        props.put("dlf.catalog.id", "id-456");
        props.put("dlf.proxy.mode", "DLF_ONLY");

        IcebergAliyunDLFMetaStoreProperties properties =
                new IcebergAliyunDLFMetaStoreProperties(props);
        // Replace DLFCatalog with a mock
        Catalog catalog = properties.initCatalog("test_catalog", props,
                Collections.singletonList(StorageProperties.createPrimary(props)));
        Assertions.assertEquals(DLFCatalog.class, catalog.getClass());
    }

    @Test
    void testAliyunDLFBasePropertiesSuccessWithPublicEndpoint() {
        Map<String, String> props = new HashMap<>();
        props.put("dlf.access_key", "ak");
        props.put("dlf.secret_key", "sk");
        props.put("dlf.region", "cn-shanghai");
        props.put("dlf.access.public", "true");
        props.put("dlf.uid", "uid-001");

        AliyunDLFBaseProperties base = AliyunDLFBaseProperties.of(props);

        Assertions.assertEquals("ak", base.dlfAccessKey);
        Assertions.assertEquals("sk", base.dlfSecretKey);
        Assertions.assertEquals("dlf.cn-shanghai.aliyuncs.com", base.dlfEndpoint);
        Assertions.assertEquals("uid-001", base.dlfCatalogId); // defaulted to uid
    }

    @Test
    void testAliyunDLFBasePropertiesSuccessWithVpcEndpoint() {
        Map<String, String> props = new HashMap<>();
        props.put("dlf.access_key", "ak");
        props.put("dlf.secret_key", "sk");
        props.put("dlf.region", "cn-hangzhou");
        props.put("dlf.access.public", "false");
        props.put("dlf.uid", "uid-002");
        AliyunDLFBaseProperties base = AliyunDLFBaseProperties.of(props);
        Assertions.assertEquals("dlf-vpc.cn-hangzhou.aliyuncs.com", base.dlfEndpoint);
        Assertions.assertEquals("uid-002", base.dlfCatalogId);
    }

    @Test
    void testAliyunDLFBasePropertiesThrowsWhenEndpointMissing() {
        Map<String, String> props = new HashMap<>();
        props.put("dlf.access_key", "ak");
        props.put("dlf.secret_key", "sk");
        // No endpoint and no region

        Assertions.assertThrows(StoragePropertiesException.class,
                () -> AliyunDLFBaseProperties.of(props));
    }
}
