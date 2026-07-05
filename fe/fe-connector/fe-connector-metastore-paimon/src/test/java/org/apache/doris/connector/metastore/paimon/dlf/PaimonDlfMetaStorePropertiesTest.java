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

package org.apache.doris.connector.metastore.paimon.dlf;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** T2 parity for the DLF backend (legacy {@code PaimonAliyunDLFMetaStoreProperties}/{@code buildDlfHiveConf}). */
public class PaimonDlfMetaStorePropertiesTest {

    private static Map<String, String> raw(String... kv) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    @Test
    public void toDlfCatalogConfDerivesVpcEndpointAndCatalogIdFromUid() {
        PaimonDlfMetaStoreProperties props = PaimonDlfMetaStoreProperties.of(raw(
                "dlf.access_key", "ak", "dlf.secret_key", "sk", "dlf.region", "cn-hangzhou",
                "dlf.catalog.uid", "123456", "warehouse", "wh"), Collections.emptyMap());

        Assertions.assertEquals("DLF", props.providerName());
        Assertions.assertTrue(props.needsStorage());

        Map<String, String> conf = props.toDlfCatalogConf();
        Assertions.assertEquals("ak", conf.get("dlf.catalog.accessKeyId"));
        Assertions.assertEquals("sk", conf.get("dlf.catalog.accessKeySecret"));
        // accessPublic defaults false -> VPC endpoint.
        Assertions.assertEquals("dlf-vpc.cn-hangzhou.aliyuncs.com", conf.get("dlf.catalog.endpoint"));
        Assertions.assertEquals("cn-hangzhou", conf.get("dlf.catalog.region"));
        Assertions.assertEquals("", conf.get("dlf.catalog.securityToken"));
        Assertions.assertEquals("123456", conf.get("dlf.catalog.uid"));
        Assertions.assertEquals("123456", conf.get("dlf.catalog.id"));
        Assertions.assertEquals("DLF_ONLY", conf.get("dlf.catalog.proxyMode"));
    }

    @Test
    public void publicEndpointWhenAccessPublicTrue() {
        PaimonDlfMetaStoreProperties props = PaimonDlfMetaStoreProperties.of(raw(
                "dlf.access_key", "ak", "dlf.secret_key", "sk", "dlf.region", "cn-hangzhou",
                "dlf.access.public", "true", "warehouse", "wh"), Collections.emptyMap());
        Assertions.assertEquals("dlf.cn-hangzhou.aliyuncs.com", props.toDlfCatalogConf().get("dlf.catalog.endpoint"));
    }

    @Test
    public void explicitEndpointAndCatalogIdWin() {
        PaimonDlfMetaStoreProperties props = PaimonDlfMetaStoreProperties.of(raw(
                "dlf.access_key", "ak", "dlf.secret_key", "sk", "dlf.endpoint", "dlf.my.endpoint",
                "dlf.catalog.uid", "u", "dlf.catalog.id", "cid", "warehouse", "wh"), Collections.emptyMap());
        Map<String, String> conf = props.toDlfCatalogConf();
        Assertions.assertEquals("dlf.my.endpoint", conf.get("dlf.catalog.endpoint"));
        Assertions.assertEquals("cid", conf.get("dlf.catalog.id"));
    }

    @Test
    public void overlaysStorageHadoopConfig() {
        Map<String, String> storage = new HashMap<>();
        storage.put("fs.oss.endpoint", "oss-cn-hangzhou.aliyuncs.com");
        PaimonDlfMetaStoreProperties props = PaimonDlfMetaStoreProperties.of(raw(
                "dlf.access_key", "ak", "dlf.secret_key", "sk", "dlf.endpoint", "e", "warehouse", "wh"), storage);
        Assertions.assertEquals("oss-cn-hangzhou.aliyuncs.com", props.toDlfCatalogConf().get("fs.oss.endpoint"));
    }

    @Test
    public void validateInOrderWarehouseAkSkEndpointOss() {
        Assertions.assertEquals("Property warehouse is required.",
                Assertions.assertThrows(IllegalArgumentException.class,
                        () -> PaimonDlfMetaStoreProperties.of(raw("dlf.access_key", "ak"), Collections.emptyMap())
                                .validate()).getMessage());
        Assertions.assertEquals("dlf.access_key is required",
                Assertions.assertThrows(IllegalArgumentException.class,
                        () -> PaimonDlfMetaStoreProperties.of(raw("warehouse", "wh"), Collections.emptyMap())
                                .validate()).getMessage());
        Assertions.assertEquals("dlf.secret_key is required",
                Assertions.assertThrows(IllegalArgumentException.class,
                        () -> PaimonDlfMetaStoreProperties.of(raw("warehouse", "wh", "dlf.access_key", "ak"),
                                Collections.emptyMap()).validate()).getMessage());
        Assertions.assertEquals("dlf.endpoint is required.",
                Assertions.assertThrows(IllegalArgumentException.class,
                        () -> PaimonDlfMetaStoreProperties.of(raw("warehouse", "wh", "dlf.access_key", "ak",
                                "dlf.secret_key", "sk"), Collections.emptyMap()).validate()).getMessage());
        Assertions.assertEquals("Paimon DLF metastore requires OSS storage properties.",
                Assertions.assertThrows(IllegalArgumentException.class,
                        () -> PaimonDlfMetaStoreProperties.of(raw("warehouse", "wh", "dlf.access_key", "ak",
                                "dlf.secret_key", "sk", "dlf.region", "cn-hangzhou"), Collections.emptyMap())
                                .validate()).getMessage());
        // valid: OSS storage key present
        PaimonDlfMetaStoreProperties.of(raw("warehouse", "wh", "dlf.access_key", "ak", "dlf.secret_key", "sk",
                "dlf.region", "cn-hangzhou", "oss.endpoint", "oss-cn-hangzhou.aliyuncs.com"),
                Collections.emptyMap()).validate();
    }

    @Test
    public void rejectsS3OnlyStorageButAcceptsPaimonFsOss() {
        // Documented anti-regression: an S3-only DLF catalog (no oss.* key) must be REJECTED.
        Assertions.assertEquals("Paimon DLF metastore requires OSS storage properties.",
                Assertions.assertThrows(IllegalArgumentException.class,
                        () -> PaimonDlfMetaStoreProperties.of(raw("warehouse", "wh", "dlf.access_key", "ak",
                                "dlf.secret_key", "sk", "dlf.region", "cn", "fs.s3a.access.key", "x"),
                                Collections.emptyMap()).validate()).getMessage());
        // paimon.fs.oss.* counts as OSS storage -> accepted.
        PaimonDlfMetaStoreProperties.of(raw("warehouse", "wh", "dlf.access_key", "ak", "dlf.secret_key", "sk",
                "dlf.region", "cn", "paimon.fs.oss.endpoint", "oss-ep"), Collections.emptyMap()).validate();
    }

    @Test
    public void proxyModeUserOverrideCarriesThrough() {
        PaimonDlfMetaStoreProperties props = PaimonDlfMetaStoreProperties.of(raw(
                "dlf.access_key", "ak", "dlf.secret_key", "sk", "dlf.endpoint", "e",
                "dlf.proxy.mode", "DLF_AND_HMS", "warehouse", "wh"), Collections.emptyMap());
        Assertions.assertEquals("DLF_AND_HMS", props.toDlfCatalogConf().get("dlf.catalog.proxyMode"));
    }
}
