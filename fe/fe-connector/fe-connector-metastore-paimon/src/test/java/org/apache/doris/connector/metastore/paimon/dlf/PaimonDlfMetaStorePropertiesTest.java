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

public class PaimonDlfMetaStorePropertiesTest {

    private static Map<String, String> raw(String... kv) {
        Map<String, String> result = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            result.put(kv[i], kv[i + 1]);
        }
        return result;
    }

    @Test
    public void derivesPublicEndpointAndCatalogId() {
        PaimonDlfMetaStoreProperties props = PaimonDlfMetaStoreProperties.of(raw(
                "dlf.access_key", "ak",
                "dlf.secret_key", "sk",
                "dlf.region", "cn-hangzhou",
                "dlf.access.public", "true",
                "dlf.catalog.uid", "uid",
                "warehouse", "oss://bucket/wh"), Collections.emptyMap());

        props.validate();
        Map<String, String> conf = props.toDlfCatalogConf();
        Assertions.assertEquals("dlf.cn-hangzhou.aliyuncs.com", conf.get("dlf.catalog.endpoint"));
        Assertions.assertEquals("uid", conf.get("dlf.catalog.id"));
    }

    @Test
    public void defersOssRequirementUntilResolvedStorageIsAvailable() {
        PaimonDlfMetaStoreProperties props = PaimonDlfMetaStoreProperties.of(raw(
                "dlf.access_key", "ak",
                "dlf.secret_key", "sk",
                "dlf.region", "cn-hangzhou",
                "warehouse", "oss://bucket/wh"), Collections.emptyMap());

        Assertions.assertDoesNotThrow(props::validate);
        Assertions.assertTrue(props.needsStorage());
    }

    @Test
    public void providerExposesEveryCredentialAliasAsSensitive() {
        Assertions.assertTrue(new PaimonDlfMetaStoreProvider().sensitivePropertyKeys().containsAll(
                java.util.Arrays.asList("dlf.access_key", "dlf.catalog.accessKeyId", "dlf.secret_key",
                        "dlf.catalog.secret_key", "dlf.catalog.accessKeySecret", "dlf.session_token",
                        "dlf.catalog.sessionToken", "dlf.catalog.securityToken")));
    }
}
