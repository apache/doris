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

public class FlussExternalCatalogFactoryTest {

    @Test
    public void testCreateCatalog() throws DdlException {
        Map<String, String> props = new HashMap<>();
        props.put(FlussExternalCatalog.FLUSS_COORDINATOR_URI, "localhost:9123");

        ExternalCatalog catalog = FlussExternalCatalogFactory.createCatalog(
                1L, "test_catalog", null, props, "test");

        Assert.assertNotNull(catalog);
        Assert.assertTrue(catalog instanceof FlussExternalCatalog);
        Assert.assertEquals("test_catalog", catalog.getName());
    }

    @Test
    public void testCreateCatalogWithBootstrapServers() throws DdlException {
        Map<String, String> props = new HashMap<>();
        props.put(FlussExternalCatalog.FLUSS_BOOTSTRAP_SERVERS, "localhost:9123");

        ExternalCatalog catalog = FlussExternalCatalogFactory.createCatalog(
                2L, "test_catalog2", null, props, "");

        Assert.assertNotNull(catalog);
        Assert.assertTrue(catalog instanceof FlussExternalCatalog);
    }

    @Test
    public void testCreateCatalogWithAdditionalProperties() throws DdlException {
        Map<String, String> props = new HashMap<>();
        props.put(FlussExternalCatalog.FLUSS_COORDINATOR_URI, "localhost:9123");
        props.put("fluss.client.timeout", "30000");
        props.put("fluss.client.retry", "3");

        ExternalCatalog catalog = FlussExternalCatalogFactory.createCatalog(
                3L, "test_catalog3", null, props, "catalog with extra props");

        Assert.assertNotNull(catalog);
    }
}

