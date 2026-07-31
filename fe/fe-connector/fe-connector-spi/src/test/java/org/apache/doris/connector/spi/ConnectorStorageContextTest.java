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

package org.apache.doris.connector.spi;

import org.apache.doris.filesystem.properties.StorageProperties;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

/**
 * The defaults a connector gets when the engine manages no storage for its catalog. These are the same
 * assertions that used to be made on {@code ConnectorContext} before the storage services moved onto their
 * own interface: moving them must not have changed a single answer, because a connector reaching a service
 * that is not there has to keep getting the benign result it got before.
 */
public class ConnectorStorageContextTest {

    @Test
    public void getStorageProperties_defaultsToEmptyList() {
        // fe-core overrides this to hand the connector the catalog's typed fe-filesystem StorageProperties.
        // Every OTHER connector keeps the default empty list -- and it must never return null.
        List<StorageProperties> storage = ConnectorStorageContext.NOOP.getStorageProperties();
        Assertions.assertNotNull(storage, "getStorageProperties() must never return null");
        Assertions.assertTrue(storage.isEmpty(),
                "default getStorageProperties() must be empty so connectors without storage are unaffected");
    }

    @Test
    public void getBackendFileType_defaultDerivesFromScheme() {
        // fe-core overrides it (LocationPath, broker-aware); the default has no storage machinery and derives
        // the BE file type from the URI scheme alone, returning the TFileType enum NAME so the SPI stays
        // Thrift-free (like normalizeStorageUri).
        ConnectorStorageContext ctx = ConnectorStorageContext.NOOP;
        Assertions.assertEquals("FILE_S3", ctx.getBackendFileType("s3://bucket/data", null));
        Assertions.assertEquals("FILE_S3", ctx.getBackendFileType("oss://bucket/data", null));
        Assertions.assertEquals("FILE_HDFS", ctx.getBackendFileType("hdfs://ns/data", null));
        Assertions.assertEquals("FILE_HDFS", ctx.getBackendFileType("viewfs://ns/data", null));
        Assertions.assertEquals("FILE_LOCAL", ctx.getBackendFileType("file:///tmp/data", null));
        Assertions.assertEquals("FILE_LOCAL", ctx.getBackendFileType("/no/scheme", null));
        Assertions.assertEquals("FILE_LOCAL", ctx.getBackendFileType(null, null));
    }

    @Test
    public void remainingDefaultsAreBenign() {
        // The other seven, pinned together because each one's default is what makes the storage split safe
        // for a connector that has no storage: it gets "nothing", never a failure and never a null it has to
        // check. MUTATION: making any of these throw or return null -> red.
        ConnectorStorageContext ctx = ConnectorStorageContext.NOOP;
        Assertions.assertTrue(ctx.vendStorageCredentials(null).isEmpty(),
                "no vending machinery -> no vended credentials");
        Assertions.assertEquals("oss://bucket/f", ctx.normalizeStorageUri("oss://bucket/f"),
                "no normalization machinery -> the URI passes through unchanged");
        Assertions.assertEquals("oss://bucket/f", ctx.normalizeStorageUri("oss://bucket/f", null),
                "the vended-aware overload falls back to the single-arg form");
        UnaryOperator<String> normalizer = ctx.newStorageUriNormalizer(null);
        Assertions.assertNotNull(normalizer, "the batch normalizer must never be null");
        Assertions.assertEquals("oss://bucket/f", normalizer.apply("oss://bucket/f"),
                "each application must match the per-call form");
        Assertions.assertTrue(ctx.getBrokerAddresses().isEmpty(), "no broker machinery -> no brokers");
        Assertions.assertTrue(ctx.getBackendStorageProperties().isEmpty(),
                "no normalization machinery -> no BE storage properties");
        Assertions.assertDoesNotThrow(
                () -> ctx.testBackendStorageConnectivity(0, Map.of()),
                "no backend fleet to ask -> the probe is skipped, not failed");
        Assertions.assertNull(ctx.getFileSystem(null), "no engine-managed filesystem");
        Assertions.assertDoesNotThrow(() -> ctx.cleanupEmptyManagedLocation("s3://bucket/db/t", List.of()),
                "cleanup is cosmetic and must never fail a drop");
    }
}
