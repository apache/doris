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

import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.kerberos.HadoopExecutionAuthenticator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class PaimonJdbcMetaStorePropertiesTest {

    @Test
    public void testInitExecutionAuthenticatorWiresHdfsAuthenticatorWithoutInitializeCatalog() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("type", "paimon");
        props.put("paimon.catalog.type", "jdbc");
        props.put("uri", "jdbc:mysql://localhost:3306/paimon");
        props.put("warehouse", "file:///tmp");
        props.put("fs.defaultFS", "file:///tmp");
        PaimonJdbcMetaStoreProperties jdbcProps =
                (PaimonJdbcMetaStoreProperties) MetastoreProperties.create(props);
        // M-8: like filesystem, the jdbc flavor only set the real authenticator inside the legacy
        // initializeCatalog() (removed with the legacy catalog-build path), so the cutover path kept the
        // base no-op and lost doAs over Kerberized HDFS. This assertion pins the bug.
        Assertions.assertNotEquals(HadoopExecutionAuthenticator.class,
                jdbcProps.getExecutionAuthenticator().getClass());
        // The fix wires the HDFS authenticator from the storage props at catalog-init time.
        // MUTATION: removing the jdbc initExecutionAuthenticator override leaves the no-op -> red.
        jdbcProps.initExecutionAuthenticator(StorageProperties.createAll(props));
        Assertions.assertEquals(HadoopExecutionAuthenticator.class,
                jdbcProps.getExecutionAuthenticator().getClass());
    }

    @Test
    public void testFactoryCreateJdbcType() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("type", "paimon");
        props.put("paimon.catalog.type", "jdbc");
        props.put("uri", "jdbc:mysql://localhost:3306/paimon");
        props.put("warehouse", "s3://warehouse/path");

        MetastoreProperties properties = MetastoreProperties.create(props);
        Assertions.assertEquals(PaimonJdbcMetaStoreProperties.class, properties.getClass());
    }

    @Test
    public void testMissingWarehouse() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("type", "paimon");
        props.put("paimon.catalog.type", "jdbc");
        props.put("uri", "jdbc:mysql://localhost:3306/paimon");

        Assertions.assertThrows(IllegalArgumentException.class, () -> MetastoreProperties.create(props));
    }

    @Test
    public void testMissingUri() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("type", "paimon");
        props.put("paimon.catalog.type", "jdbc");
        props.put("warehouse", "s3://warehouse/path");

        Assertions.assertThrows(IllegalArgumentException.class, () -> MetastoreProperties.create(props));
    }
}
