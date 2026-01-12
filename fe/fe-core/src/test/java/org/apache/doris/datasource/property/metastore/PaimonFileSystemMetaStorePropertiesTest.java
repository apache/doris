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

import org.apache.paimon.catalog.FileSystemCatalogFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class PaimonFileSystemMetaStorePropertiesTest {

    @Test
    public void testKerberosCatalog() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put(HdfsProperties.FS_HDFS_SUPPORT, "true");
        props.put("fs.defaultFS", "hdfs://mycluster_test");
        props.put("hadoop.security.authentication", "kerberos");
        props.put("hadoop.kerberos.principal", "myprincipal");
        props.put("hadoop.kerberos.keytab", "mykeytab");
        props.put("type", "paimon");
        props.put("paimon.catalog.type", "filesystem");
        props.put("warehouse", "hdfs://mycluster_test/paimon");
        PaimonFileSystemMetaStoreProperties paimonProps = (PaimonFileSystemMetaStoreProperties) MetastoreProperties.create(props);
        //We expect a Kerberos-related exception, but because the messages vary by environment, we’re only doing a simple check.
        Assertions.assertThrows(RuntimeException.class, () -> paimonProps.initializeCatalog("paimon", StorageProperties.createAll(props))
        );
    }

    @Test
    public void testNonKerberosCatalog() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("fs.defaultFS", "file:///tmp");
        props.put("type", "paimon");
        props.put("paimon.catalog.type", "filesystem");
        props.put("warehouse", "file:///tmp");
        PaimonFileSystemMetaStoreProperties paimonProps = (PaimonFileSystemMetaStoreProperties) MetastoreProperties.create(props);
        Assertions.assertEquals(FileSystemCatalogFactory.IDENTIFIER, paimonProps.getMetastoreType());
        Assertions.assertEquals("filesystem", paimonProps.getPaimonCatalogType());
        Assertions.assertDoesNotThrow(() -> paimonProps.initializeCatalog("paimon", StorageProperties.createAll(props)));
        Assertions.assertEquals(HadoopExecutionAuthenticator.class, paimonProps.getExecutionAuthenticator().getClass());
    }
}
