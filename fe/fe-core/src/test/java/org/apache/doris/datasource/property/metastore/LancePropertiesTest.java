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

public class LancePropertiesTest {
    @Test
    public void testDefaultFilesystemProperties() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "lance");
        properties.put(LanceFileSystemMetastoreProperties.WAREHOUSE, "/tmp/lance");

        AbstractLanceProperties lanceProperties =
                (AbstractLanceProperties) MetastoreProperties.create(properties);

        Assertions.assertInstanceOf(LanceFileSystemMetastoreProperties.class, lanceProperties);
        Assertions.assertEquals(AbstractLanceProperties.LANCE_FILESYSTEM,
                lanceProperties.getLanceCatalogType());
        Assertions.assertEquals(AbstractLanceProperties.DEFAULT_DELIMITER,
                lanceProperties.getNamespaceDelimiter());
        Assertions.assertEquals(AbstractLanceProperties.DEFAULT_ROOT_DATABASE,
                lanceProperties.getRootDatabase());
    }

    @Test
    public void testOssFilesystemProperties() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "lance");
        properties.put(LanceFileSystemMetastoreProperties.WAREHOUSE,
                "oss://bucket/lance");

        AbstractLanceProperties lanceProperties =
                (AbstractLanceProperties) MetastoreProperties.create(properties);

        Assertions.assertInstanceOf(LanceFileSystemMetastoreProperties.class, lanceProperties);
        Assertions.assertEquals("oss://bucket/lance",
                ((LanceFileSystemMetastoreProperties) lanceProperties).getWarehouse());
    }

    @Test
    public void testRestProperties() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "lance");
        properties.put(AbstractLanceProperties.LANCE_CATALOG_TYPE,
                AbstractLanceProperties.LANCE_REST);
        properties.put(LanceRestMetastoreProperties.REST_URI, "http://localhost:8080/");
        properties.put(LanceRestMetastoreProperties.REST_SECURITY_TYPE, "bearer");
        properties.put(LanceRestMetastoreProperties.REST_BEARER_TOKEN, "token");

        AbstractLanceProperties lanceProperties =
                (AbstractLanceProperties) MetastoreProperties.create(properties);

        Assertions.assertInstanceOf(LanceRestMetastoreProperties.class, lanceProperties);
        LanceRestMetastoreProperties restProperties = (LanceRestMetastoreProperties) lanceProperties;
        Assertions.assertEquals("http://localhost:8080", restProperties.getRestUri());
        Assertions.assertEquals("bearer", restProperties.getSecurityType());
    }
}
