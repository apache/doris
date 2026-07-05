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

package org.apache.doris.connector.metastore.iceberg.dlf;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Parity for the iceberg DLF backend: it reuses the shared {@link
 * org.apache.doris.connector.metastore.spi.AbstractDlfMetaStoreProperties} AK/SK/endpoint connection rules
 * but — unlike paimon — requires NEITHER warehouse NOR OSS storage (legacy
 * {@code IcebergAliyunDLFMetaStoreProperties} → {@code AliyunDLFBaseProperties.of}; §4 of the P6-T10
 * design). Conf ({@code toDlfCatalogConf}, used by the connector via {@code bindForType("dlf")}) comes
 * from the shared base unchanged.
 */
public class IcebergDlfMetaStorePropertiesTest {

    private static Map<String, String> raw(String... kv) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    private static IcebergDlfMetaStoreProperties of(Map<String, String> raw) {
        return IcebergDlfMetaStoreProperties.of(raw, Collections.emptyMap());
    }

    private static String validateError(Map<String, String> raw) {
        return Assertions.assertThrows(IllegalArgumentException.class,
                () -> of(raw).validate()).getMessage();
    }

    @Test
    public void requiredInOrderAccessKeySecretKeyEndpoint() {
        Assertions.assertEquals("dlf.access_key is required", validateError(raw()));
        Assertions.assertEquals("dlf.secret_key is required", validateError(raw("dlf.access_key", "ak")));
        // both endpoint and region blank => cannot derive endpoint.
        Assertions.assertEquals("dlf.endpoint is required.",
                validateError(raw("dlf.access_key", "ak", "dlf.secret_key", "sk")));
    }

    @Test
    public void validWithoutWarehouseOrOssStorage() {
        // KEY iceberg-vs-paimon difference: iceberg DLF requires neither warehouse nor OSS storage.
        // MUTATION: if IcebergDlf.validate() ran requireWarehouse()/requireOssStorage() (paimon's rules),
        // these would throw.
        of(raw("dlf.access_key", "ak", "dlf.secret_key", "sk", "dlf.region", "cn-hangzhou")).validate();
        of(raw("dlf.access_key", "ak", "dlf.secret_key", "sk", "dlf.endpoint", "dlf-vpc.cn.aliyuncs.com")).validate();
        Assertions.assertEquals("DLF",
                of(raw("dlf.access_key", "ak", "dlf.secret_key", "sk", "dlf.region", "cn")).providerName());
    }

    @Test
    public void confComesFromSharedBaseUnchanged() {
        // toDlfCatalogConf (bindForType("dlf")) is the shared base's — identical to paimon's. Pin a key.
        Map<String, String> conf = of(raw("dlf.access_key", "ak", "dlf.secret_key", "sk",
                "dlf.region", "cn-hangzhou")).toDlfCatalogConf();
        Assertions.assertEquals("ak", conf.get("dlf.catalog.accessKeyId"));
        Assertions.assertEquals("dlf-vpc.cn-hangzhou.aliyuncs.com", conf.get("dlf.catalog.endpoint"));
    }
}
