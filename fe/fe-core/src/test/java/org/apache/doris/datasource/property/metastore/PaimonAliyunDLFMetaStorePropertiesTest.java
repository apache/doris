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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
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
    }
}
