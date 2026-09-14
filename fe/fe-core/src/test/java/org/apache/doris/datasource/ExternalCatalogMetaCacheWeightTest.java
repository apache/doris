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

package org.apache.doris.datasource;

import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.doris.RemoteDorisExternalCatalog;
import org.apache.doris.datasource.property.constants.RemoteDorisProperties;
import org.apache.doris.datasource.test.TestExternalCatalog;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class ExternalCatalogMetaCacheWeightTest {

    @Test
    public void coreSchemaWeightIsValidatedAtTheDdlDoor() {
        Map<String, String> properties = testCatalogProperties();
        properties.put("meta.cache.default.schema.max-weight", "invalid");
        TestExternalCatalog invalidWeight = new TestExternalCatalog(1L, "test", "", properties, "");
        Assertions.assertThrows(DdlException.class, invalidWeight::checkProperties);

        properties = testCatalogProperties();
        properties.put("meta.cache.default.table.max-weight", "64MB");
        TestExternalCatalog unknownEntry = new TestExternalCatalog(2L, "test", "", properties, "");
        Assertions.assertDoesNotThrow(() -> {
            unknownEntry.checkProperties();
        });
    }

    @Test
    public void remoteDorisValidatesItsOwnConsumedEntries() {
        Map<String, String> properties = remoteDorisProperties();
        properties.put("meta.cache.doris.backends.max-weight", "invalid");
        RemoteDorisExternalCatalog invalidWeight = new RemoteDorisExternalCatalog(
                1L, "remote", "", properties, "");
        Assertions.assertThrows(DdlException.class, invalidWeight::checkProperties);

        properties = remoteDorisProperties();
        properties.put("meta.cache.doris.unknown.max-weight", "64MB");
        RemoteDorisExternalCatalog unknownEntry = new RemoteDorisExternalCatalog(
                2L, "remote", "", properties, "");
        Assertions.assertDoesNotThrow(() -> {
            unknownEntry.checkProperties();
        });
    }

    private static Map<String, String> testCatalogProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put("catalog_provider.class", RefreshCatalogTest.RefreshCatalogProvider.class.getName());
        return properties;
    }

    private static Map<String, String> remoteDorisProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put(RemoteDorisProperties.FE_THRIFT_HOSTS, "127.0.0.1:9020");
        properties.put(RemoteDorisProperties.FE_HTTP_HOSTS, "127.0.0.1:8030");
        properties.put(RemoteDorisProperties.FE_ARROW_HOSTS, "127.0.0.1:8070");
        properties.put(RemoteDorisProperties.USER, "root");
        properties.put(RemoteDorisProperties.PASSWORD, "");
        properties.put(RemoteDorisProperties.USE_ARROW_FLIGHT, "true");
        return properties;
    }
}
