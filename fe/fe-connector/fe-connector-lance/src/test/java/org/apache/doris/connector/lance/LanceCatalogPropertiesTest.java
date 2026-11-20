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

package org.apache.doris.connector.lance;

import org.apache.doris.connector.spi.DorisConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class LanceCatalogPropertiesTest {

    private static Map<String, String> filesystem(String warehouse) {
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "lance");
        properties.put("warehouse", warehouse);
        return properties;
    }

    @Test
    void defaultsDescribeAFilesystemCatalogRootedAtTheWarehouse() {
        LanceCatalogProperties properties = LanceCatalogProperties.of(filesystem("/tmp/lance"));
        Assertions.assertEquals("filesystem", properties.getCatalogType());
        Assertions.assertEquals("/tmp/lance", properties.getWarehouse());
        Assertions.assertEquals("$", properties.getNamespaceDelimiter());
        Assertions.assertEquals("default", properties.getRootDatabase());
        Assertions.assertEquals(Collections.emptyList(), properties.getParentNamespace());
        properties.requireFilesystem();
    }

    /** The maps 4.1.x persisted for its Lance catalogs (fe-core's upgrade/415 fixture) still bind unchanged. */
    @Test
    void catalogsWrittenByEarlierVersionsStillBind() {
        Map<String, String> s3 = filesystem("s3://warehouse/lance");
        s3.put("lance.catalog.type", "filesystem");
        s3.put("s3.endpoint", "http://127.0.0.1:19001");
        s3.put("s3.access_key", "admin");
        s3.put("s3.secret_key", "password");
        s3.put("s3.region", "us-east-1");
        s3.put("use_path_style", "true");
        s3.put("use_meta_cache", "true");
        Assertions.assertEquals("s3://warehouse/lance", LanceCatalogProperties.of(s3).getWarehouse());

        Map<String, String> local = filesystem("file:///tmp/lance_wh");
        local.put("use_meta_cache", "true");
        Assertions.assertEquals("file:///tmp/lance_wh", LanceCatalogProperties.of(local).getWarehouse());

        Map<String, String> rest = new HashMap<>();
        rest.put("type", "lance");
        rest.put("lance.catalog.type", "rest");
        rest.put("lance.rest.uri", "http://127.0.0.1:19102");
        rest.put("lance.rest.security.type", "bearer");
        rest.put("lance.rest.bearer-token", "doris-lance-rest-test-token");
        rest.put("lance.namespace.delimiter", ".");
        rest.put("lance.namespace.root_database", "default");
        rest.put("use_meta_cache", "true");
        LanceCatalogProperties restProperties = LanceCatalogProperties.of(rest);
        Assertions.assertEquals("rest", restProperties.getCatalogType());
        // Bound so it replays and stays masked, but this build cannot read it.
        Assertions.assertEquals("Lance rest catalogs are not supported yet; only 'lance.catalog.type'='filesystem'"
                        + " catalogs can be read",
                Assertions.assertThrows(DorisConnectorException.class, restProperties::requireFilesystem)
                        .getMessage());
    }

    @Test
    void catalogTypeIsMatchedTrimmedAndCaseInsensitively() {
        Map<String, String> properties = filesystem("/tmp/lance");
        properties.put("lance.catalog.type", " FileSystem ");
        Assertions.assertEquals("filesystem", LanceCatalogProperties.of(properties).getCatalogType());
        properties.put("lance.catalog.type", "hive");
        Assertions.assertEquals("Property 'lance.catalog.type' must be 'filesystem' or 'rest', but was 'hive'",
                Assertions.assertThrows(IllegalArgumentException.class,
                        () -> LanceCatalogProperties.of(properties)).getMessage());
    }

    @Test
    void filesystemCatalogNeedsAnAbsoluteOrS3Warehouse() {
        Map<String, String> missing = new HashMap<>();
        missing.put("type", "lance");
        Assertions.assertEquals("Missing required property 'warehouse' for Lance filesystem catalog",
                Assertions.assertThrows(IllegalArgumentException.class,
                        () -> LanceCatalogProperties.of(missing)).getMessage());
        Assertions.assertEquals("Local Lance warehouse must be an absolute path: relative/dir",
                Assertions.assertThrows(IllegalArgumentException.class,
                        () -> LanceCatalogProperties.of(filesystem("relative/dir"))).getMessage());
        Assertions.assertTrue(Assertions.assertThrows(IllegalArgumentException.class,
                () -> LanceCatalogProperties.of(filesystem("hdfs://nn/lance"))).getMessage()
                .startsWith("Unsupported Lance filesystem warehouse scheme 'hdfs'"));
        Assertions.assertTrue(Assertions.assertThrows(IllegalArgumentException.class,
                () -> LanceCatalogProperties.of(filesystem("s3:/lance"))).getMessage()
                .contains("must name a bucket"));
    }

    @Test
    void restKeysAreRejectedOnAFilesystemCatalog() {
        Map<String, String> properties = filesystem("/tmp/lance");
        properties.put("lance.rest.uri", "http://127.0.0.1:19102");
        Assertions.assertEquals("Property 'lance.rest.uri' is not valid for Lance filesystem catalog",
                Assertions.assertThrows(IllegalArgumentException.class,
                        () -> LanceCatalogProperties.of(properties)).getMessage());
    }

    /** Namespace values keep their whitespace and explicit emptiness; both matter to the escaping rules. */
    @Test
    void namespaceKeysAreValidatedAsWritten() {
        Map<String, String> properties = filesystem("/tmp/lance");
        properties.put("lance.namespace.parent", "company.analytics");
        properties.put("lance.namespace.delimiter", ".");
        properties.put("lance.namespace.root_database", "root");
        LanceCatalogProperties parsed = LanceCatalogProperties.of(properties);
        Assertions.assertEquals(Arrays.asList("company", "analytics"), parsed.getParentNamespace());
        Assertions.assertEquals("root", parsed.getRootDatabase());

        properties.put("lance.namespace.delimiter", "");
        Assertions.assertTrue(Assertions.assertThrows(IllegalArgumentException.class,
                () -> LanceCatalogProperties.of(properties)).getMessage().contains("cannot be empty"));
        properties.put("lance.namespace.delimiter", ".");
        properties.put("lance.namespace.root_database", " ");
        Assertions.assertEquals("Property 'lance.namespace.root_database' must be non-empty",
                Assertions.assertThrows(IllegalArgumentException.class,
                        () -> LanceCatalogProperties.of(properties)).getMessage());
        properties.put("lance.namespace.root_database", "default");
        properties.put("lance.namespace.parent", "a..b");
        Assertions.assertEquals("Empty Lance namespace component in 'a..b'",
                Assertions.assertThrows(IllegalArgumentException.class,
                        () -> LanceCatalogProperties.of(properties)).getMessage());
    }

    /** ALTER CATALOG merges and cannot remove a key, so a key nobody here knows must not be refused. */
    @Test
    void unknownKeysAreAccepted() {
        Map<String, String> properties = filesystem("/tmp/lance");
        properties.put("lance.table_access_cache_ttl_seconds", "0");
        properties.put("meta.cache.lance.anything", "1");
        properties.put("some.future.key", "x");
        LanceCatalogProperties.of(properties);
    }

    @Test
    void toStringNeverPrintsCredentials() {
        Map<String, String> properties = filesystem("/tmp/lance");
        properties.put("s3.secret_key", "sentinel-secret");
        Assertions.assertFalse(LanceCatalogProperties.of(properties).toString().contains("sentinel-secret"));
    }
}
