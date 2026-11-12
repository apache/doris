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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.OptionalLong;

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

    @Test
    public void submittedCoreKeysMustNameAKnownOptionWithAValueOfItsType() throws DdlException {
        ExternalCatalog catalog = new TestExternalCatalog(3L, "test", "", testCatalogProperties(), "");
        DdlException misspelled = Assertions.assertThrows(DdlException.class,
                () -> catalog.checkSubmittedMetaCacheProperties(testCatalogProperties(),
                        Collections.singletonMap("meta.cache.default.schema.max-weigth", "64MB"),
                        OptionalLong.empty()));
        Assertions.assertTrue(misspelled.getMessage().contains(
                "Unknown external meta cache property: meta.cache.default.schema.max-weigth"), misspelled.getMessage());
        for (String[] bad : new String[][] {{"meta.cache.default.schema.enable", "1"},
                {"meta.cache.default.schema.ttl-second", "-2"}, {"meta.cache.default.schema.capacity", "-1"}}) {
            Assertions.assertThrows(DdlException.class, () -> catalog.checkSubmittedMetaCacheProperties(
                    testCatalogProperties(), Collections.singletonMap(bad[0], bad[1]), OptionalLong.empty()), bad[0]);
        }

        // An entry this version does not know stays accepted; a connector validates the keys of its own engine.
        Map<String, String> accepted = new HashMap<>();
        accepted.put("meta.cache.default.future_entry.max-weight", "64MB");
        accepted.put("meta.cache.hive.file.enable", "1");
        catalog.checkSubmittedMetaCacheProperties(testCatalogProperties(), accepted, OptionalLong.empty());
        // A key an earlier statement stored is not inspected again.
        Map<String, String> stored = testCatalogProperties();
        stored.put("meta.cache.default.schema.max-weigth", "64MB");
        catalog.checkSubmittedMetaCacheProperties(stored, Collections.singletonMap("comment", "updated"),
                OptionalLong.empty());
    }

    @Test
    public void submittedWeightsMayNotExceedTheGlobalLimitOfThisFrontend() throws DdlException {
        ExternalCatalog catalog = new TestExternalCatalog(4L, "test", "", testCatalogProperties(), "");
        OptionalLong global = OptionalLong.of(64L * 1024 * 1024);
        Map<String, String> catalogAbove = Collections.singletonMap("meta.cache.max-weight", "128MB");
        DdlException e = Assertions.assertThrows(DdlException.class,
                () -> catalog.checkSubmittedMetaCacheProperties(catalogAbove, catalogAbove, global));
        Assertions.assertTrue(e.getMessage().contains("meta.cache.max-weight (134217728 bytes) can not exceed the FE"
                + " global max weight external_meta_cache_max_weight (67108864 bytes)"), e.getMessage());
        Map<String, String> entryAbove = Collections.singletonMap("meta.cache.default.schema.max-weight", "128MB");
        Assertions.assertThrows(DdlException.class,
                () -> catalog.checkSubmittedMetaCacheProperties(entryAbove, entryAbove, global));

        catalog.checkSubmittedMetaCacheProperties(catalogAbove, Collections.singletonMap("comment", "updated"), global);
        catalog.checkSubmittedMetaCacheProperties(catalogAbove, catalogAbove, OptionalLong.empty());
    }

    @Test
    public void remoteDorisChecksTheKeysOfItsOwnEngine() throws DdlException {
        ExternalCatalog catalog = new RemoteDorisExternalCatalog(3L, "remote", "", remoteDorisProperties(), "");
        Assertions.assertThrows(DdlException.class, () -> catalog.checkSubmittedMetaCacheProperties(
                remoteDorisProperties(), Collections.singletonMap("meta.cache.doris.backends.enable", "1"),
                OptionalLong.empty()));
        catalog.checkSubmittedMetaCacheProperties(remoteDorisProperties(),
                Collections.singletonMap("meta.cache.doris.future_entry.ttl-second", "0"), OptionalLong.empty());
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
