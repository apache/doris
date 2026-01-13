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

package org.apache.doris.datasource.fluss;

import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.ExternalCatalog;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class FlussExternalCatalogTest {

    @Test
    public void testCreateCatalogWithBootstrapServers() throws DdlException {
        Map<String, String> props = new HashMap<>();
        props.put(FlussExternalCatalog.FLUSS_BOOTSTRAP_SERVERS, "localhost:9123");

        ExternalCatalog catalog = FlussExternalCatalogFactory.createCatalog(
                1L, "test_fluss_catalog", null, props, "test catalog");

        Assert.assertNotNull(catalog);
        Assert.assertEquals("test_fluss_catalog", catalog.getName());
        Assert.assertTrue(catalog instanceof FlussExternalCatalog);
    }

    @Test
    public void testCreateCatalogWithMultipleServers() throws DdlException {
        Map<String, String> props = new HashMap<>();
        props.put(FlussExternalCatalog.FLUSS_BOOTSTRAP_SERVERS, "host1:9123,host2:9123,host3:9123");

        ExternalCatalog catalog = FlussExternalCatalogFactory.createCatalog(
                1L, "test_fluss_catalog", null, props, "test catalog");

        Assert.assertNotNull(catalog);
        Assert.assertEquals("test_fluss_catalog", catalog.getName());
    }

    @Test
    public void testCheckPropertiesMissingBootstrapServers() {
        Map<String, String> props = new HashMap<>();
        FlussExternalCatalog catalog = new FlussExternalCatalog(
                1L, "test", null, props, "");

        try {
            catalog.checkProperties();
            Assert.fail("Should throw DdlException for missing bootstrap servers");
        } catch (DdlException e) {
            Assert.assertTrue(e.getMessage().contains(FlussExternalCatalog.FLUSS_BOOTSTRAP_SERVERS));
        }
    }

    @Test
    public void testCheckPropertiesWithBootstrapServers() throws DdlException {
        Map<String, String> props = new HashMap<>();
        props.put(FlussExternalCatalog.FLUSS_BOOTSTRAP_SERVERS, "localhost:9123");

        FlussExternalCatalog catalog = new FlussExternalCatalog(
                1L, "test", null, props, "");
        catalog.checkProperties();
    }

    @Test
    public void testCatalogProperties() {
        Map<String, String> props = new HashMap<>();
        props.put(FlussExternalCatalog.FLUSS_BOOTSTRAP_SERVERS, "localhost:9123");
        props.put("fluss.client.timeout", "30000");

        FlussExternalCatalog catalog = new FlussExternalCatalog(
                1L, "test", null, props, "");
        Assert.assertEquals("localhost:9123",
                catalog.getCatalogProperty().getOrDefault(FlussExternalCatalog.FLUSS_BOOTSTRAP_SERVERS, null));
    }

    @Test
    public void testCatalogSecurityProperties() {
        Map<String, String> props = new HashMap<>();
        props.put(FlussExternalCatalog.FLUSS_BOOTSTRAP_SERVERS, "localhost:9123");
        props.put(FlussExternalCatalog.FLUSS_SECURITY_PROTOCOL, "SASL_PLAINTEXT");
        props.put(FlussExternalCatalog.FLUSS_SASL_MECHANISM, "PLAIN");
        props.put(FlussExternalCatalog.FLUSS_SASL_USERNAME, "user");
        props.put(FlussExternalCatalog.FLUSS_SASL_PASSWORD, "password");

        FlussExternalCatalog catalog = new FlussExternalCatalog(
                1L, "test", null, props, "");
        Assert.assertEquals("SASL_PLAINTEXT",
                catalog.getCatalogProperty().getOrDefault(FlussExternalCatalog.FLUSS_SECURITY_PROTOCOL, null));
    }

    @Test
    public void testCacheTtlProperty() throws DdlException {
        Map<String, String> props = new HashMap<>();
        props.put(FlussExternalCatalog.FLUSS_BOOTSTRAP_SERVERS, "localhost:9123");
        props.put(FlussExternalCatalog.FLUSS_TABLE_META_CACHE_TTL_SECOND, "300");

        FlussExternalCatalog catalog = new FlussExternalCatalog(
                1L, "test", null, props, "");
        catalog.checkProperties();
    }

    @Test
    public void testInvalidCacheTtlProperty() {
        Map<String, String> props = new HashMap<>();
        props.put(FlussExternalCatalog.FLUSS_BOOTSTRAP_SERVERS, "localhost:9123");
        props.put(FlussExternalCatalog.FLUSS_TABLE_META_CACHE_TTL_SECOND, "-1");

        FlussExternalCatalog catalog = new FlussExternalCatalog(
                1L, "test", null, props, "");
        try {
            catalog.checkProperties();
            Assert.fail("Should throw DdlException for negative cache TTL");
        } catch (DdlException e) {
            Assert.assertTrue(e.getMessage().contains("non-negative"));
        }
    }
}
