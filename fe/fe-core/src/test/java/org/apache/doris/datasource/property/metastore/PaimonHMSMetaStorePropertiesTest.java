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

import org.apache.doris.datasource.property.storage.HdfsProperties;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class PaimonHMSMetaStorePropertiesTest {

    @Test
    public void testNonKerberosCatalog() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put(HdfsProperties.FS_HDFS_SUPPORT, "true");
        props.put("fs.defaultFS", "file:///tmp");
        props.put("type", "paimon");
        props.put("paimon.catalog.type", "hms");
        props.put("hive.metastore.uris", "thrift://localhost:9083");
        props.put("warehouse", "file:///tmp");
        PaimonHMSMetaStoreProperties paimonProps = (PaimonHMSMetaStoreProperties) MetastoreProperties.create(props);
        Assertions.assertEquals("hms", paimonProps.getPaimonCatalogType());
        // Parity: only the REST flavor vends credentials; non-REST paimon flavors keep building the
        // static storage map (the former provider gated on instanceof PaimonRestMetaStoreProperties).
        Assertions.assertFalse(paimonProps.isVendedCredentialsEnabled());
        //should mock connection to hms
    }

}
