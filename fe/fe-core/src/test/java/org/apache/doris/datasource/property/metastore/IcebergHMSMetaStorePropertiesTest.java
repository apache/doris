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

import org.apache.doris.common.security.authentication.HadoopExecutionAuthenticator;
import org.apache.doris.datasource.property.storage.HdfsProperties;
import org.apache.doris.datasource.property.storage.StorageProperties;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IcebergHMSMetaStorePropertiesTest {
    @Test
    public void testKerberosCatalog() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put(HdfsProperties.FS_HDFS_SUPPORT, "true");
        props.put("fs.defaultFS", "hdfs://mycluster_test");
        props.put("hadoop.security.authentication", "kerberos");
        props.put("hadoop.kerberos.principal", "myprincipal");
        props.put("hadoop.kerberos.keytab", "mykeytab");
        props.put("type", "iceberg");
        props.put("hive.metastore.uris", "thrift://localhost:12345");
        props.put("iceberg.catalog.type", "hms");
        props.put("warehouse", "hdfs://mycluster_test/ice");
        IcebergHMSMetaStoreProperties icebergProps = (IcebergHMSMetaStoreProperties) MetastoreProperties.create(props);
        List<StorageProperties> storagePropertiesList = Collections.singletonList(StorageProperties.createPrimary(props));
        //We expect a Kerberos-related exception, but because the messages vary by environment, we’re only doing a simple check.
        Assertions.assertThrows(RuntimeException.class,
                () -> icebergProps.initializeCatalog("iceberg", storagePropertiesList));
    }

    @Test
    public void testNonKerberosCatalog() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put(HdfsProperties.FS_HDFS_SUPPORT, "true");
        props.put("fs.defaultFS", "file:///tmp");
        props.put("type", "iceberg");
        props.put("iceberg.catalog.type", "hms");
        props.put("hive.metastore.uris", "thrift://localhost:9083");
        props.put("warehouse", "file:///tmp");
        IcebergHMSMetaStoreProperties paimonProps = (IcebergHMSMetaStoreProperties) MetastoreProperties.create(props);
        Assertions.assertEquals("hms", paimonProps.getIcebergCatalogType());

        List<StorageProperties> storagePropertiesList = Collections.singletonList(StorageProperties.createPrimary(props));
        Assertions.assertDoesNotThrow(() -> paimonProps.initializeCatalog("iceberg", storagePropertiesList));
        Assertions.assertEquals(HadoopExecutionAuthenticator.class, paimonProps.getExecutionAuthenticator().getClass());
    }

}
