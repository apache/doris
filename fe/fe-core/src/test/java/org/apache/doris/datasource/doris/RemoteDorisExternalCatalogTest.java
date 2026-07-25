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

package org.apache.doris.datasource.doris;

import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.property.constants.RemoteDorisProperties;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Verifies that a {@code type=doris} catalog SSRF-checks its remote FE host properties.
 *
 * <p>These endpoints (fe_http_hosts / fe_thrift_hosts / fe_arrow_hosts) are plain
 * comma-separated host:port properties rather than {@code @ConnectorProperty} fields, and
 * {@code doris} is not a valid metastore type, so they are only reachable through the
 * catalog-specific {@code getSsrfCheckEndpointUris()} override exercised by checkSsrf().
 */
public class RemoteDorisExternalCatalogTest {

    @Test
    public void testCheckSsrfRejectsLoopbackFeHttpHost() {
        Map<String, String> props = new HashMap<>();
        props.put(RemoteDorisProperties.FE_HTTP_HOSTS, "127.0.0.1:8030");
        RemoteDorisExternalCatalog catalog = new RemoteDorisExternalCatalog(1L, "doris_catalog", null, props, "");

        DdlException exception = Assert.assertThrows(DdlException.class, catalog::checkSsrf);
        Assert.assertTrue(exception.getMessage().contains("127.0.0.1"));
    }

    @Test
    public void testCheckSsrfRejectsLoopbackFeThriftHost() {
        Map<String, String> props = new HashMap<>();
        props.put(RemoteDorisProperties.FE_THRIFT_HOSTS, "127.0.0.1:9020");
        RemoteDorisExternalCatalog catalog = new RemoteDorisExternalCatalog(1L, "doris_catalog", null, props, "");

        DdlException exception = Assert.assertThrows(DdlException.class, catalog::checkSsrf);
        Assert.assertTrue(exception.getMessage().contains("127.0.0.1"));
    }

    @Test
    public void testCheckSsrfRejectsLoopbackFeArrowHost() {
        Map<String, String> props = new HashMap<>();
        props.put(RemoteDorisProperties.FE_ARROW_HOSTS, "127.0.0.1:9090");
        RemoteDorisExternalCatalog catalog = new RemoteDorisExternalCatalog(1L, "doris_catalog", null, props, "");

        DdlException exception = Assert.assertThrows(DdlException.class, catalog::checkSsrf);
        Assert.assertTrue(exception.getMessage().contains("127.0.0.1"));
    }
}
