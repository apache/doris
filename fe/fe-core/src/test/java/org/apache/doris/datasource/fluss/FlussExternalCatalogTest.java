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
    public void testCreateCatalogWithCoordinatorUri() throws DdlException {
        Map<String, String> props = new HashMap<>();
        props.put(FlussExternalCatalog.FLUSS_COORDINATOR_URI, "localhost:9123");

        ExternalCatalog catalog = FlussExternalCatalogFactory.createCatalog(
                1L, "test_fluss_catalog", null, props, "test catalog");

        Assert.assertNotNull(catalog);
        Assert.assertEquals("test_fluss_catalog", catalog.getName());
        Assert.assertTrue(catalog instanceof FlussExternalCatalog);
    }

    @Test
    public void testCreateCatalogWithBootstrapServers() throws DdlException {
        Map<String, String> props = new HashMap<>();
        props.put(FlussExternalCatalog.FLUSS_BOOTSTRAP_SERVERS, "localhost:9123");

        ExternalCatalog catalog = FlussExternalCatalogFactory.createCatalog(
                1L, "test_fluss_catalog", null, props, "test catalog");

        Assert.assertNotNull(catalog);
        Assert.assertEquals("test_fluss_catalog", catalog.getName());
    }

    @Test
    public void testCheckPropertiesMissingUri() {
        Map<String, String> props = new HashMap<>();
        FlussExternalCatalog catalog = new FlussExternalCatalog(
                1L, "test", null, props, "");

        try {
            catalog.checkProperties();
            Assert.fail("Should throw DdlException for missing coordinator URI");
        } catch (DdlException e) {
            Assert.assertTrue(e.getMessage().contains(FlussExternalCatalog.FLUSS_COORDINATOR_URI)
                    || e.getMessage().contains(FlussExternalCatalog.FLUSS_BOOTSTRAP_SERVERS));
        }
    }

    @Test
    public void testCheckPropertiesWithCoordinatorUri() throws DdlException {
        Map<String, String> props = new HashMap<>();
        props.put(FlussExternalCatalog.FLUSS_COORDINATOR_URI, "localhost:9123");

        FlussExternalCatalog catalog = new FlussExternalCatalog(
                1L, "test", null, props, "");
        catalog.checkProperties();
        // Should not throw exception
    }

    @Test
    public void testCheckPropertiesWithBootstrapServers() throws DdlException {
        Map<String, String> props = new HashMap<>();
        props.put(FlussExternalCatalog.FLUSS_BOOTSTRAP_SERVERS, "localhost:9123");

        FlussExternalCatalog catalog = new FlussExternalCatalog(
                1L, "test", null, props, "");
        catalog.checkProperties();
        // Should not throw exception
    }

    @Test
    public void testCatalogProperties() {
        Map<String, String> props = new HashMap<>();
        props.put(FlussExternalCatalog.FLUSS_COORDINATOR_URI, "localhost:9123");
        props.put("fluss.client.timeout", "30000");

        FlussExternalCatalog catalog = new FlussExternalCatalog(
                1L, "test", null, props, "");
        Assert.assertEquals("localhost:9123", 
                catalog.getCatalogProperty().getOrDefault(FlussExternalCatalog.FLUSS_COORDINATOR_URI, null));
    }
}

