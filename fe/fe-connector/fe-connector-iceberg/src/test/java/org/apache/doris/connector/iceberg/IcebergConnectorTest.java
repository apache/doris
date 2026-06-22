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

package org.apache.doris.connector.iceberg;

import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.filesystem.properties.StorageProperties;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Connector-level tests for {@link IcebergConnector} that can run offline (no live catalog / no AWS call). The
 * live s3tables catalog construction (hand-built {@code S3TablesClient} + {@code S3TablesCatalog.initialize}) is
 * exercised only at the P6.6 docker plugin-zip gate; here we lock the FAIL-LOUD routing invariants that guard it.
 * No Mockito — the {@link RecordingConnectorContext} fail-loud fake is used.
 */
public class IcebergConnectorTest {

    @Test
    public void s3TablesWithoutStorageFailsLoud() {
        // WHY: legacy IcebergS3TablesMetaStoreProperties always derives from S3Properties.of(origProps); the
        // connector needs a bound S3-compatible storage to derive the region + credentials for the control-plane
        // S3TablesClient. Missing storage must fail loud (a clear DorisConnectorException), NOT silently route
        // s3tables through the generic CatalogUtil path or fall back to an anonymous client. MUTATION: dropping
        // the chosenS3 presence check -> a NullPointer / wrong-path error instead of this message -> red.
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        IcebergConnector connector = new IcebergConnector(
                Map.of("iceberg.catalog.type", "s3tables",
                        "warehouse", "arn:aws:s3tables:us-east-1:1:bucket/b"),
                ctx);
        DorisConnectorException ex =
                Assertions.assertThrows(DorisConnectorException.class, () -> connector.getMetadata(null));
        Assertions.assertTrue(ex.getMessage().contains("S3-compatible storage"),
                "expected a fail-loud message naming the missing S3-compatible storage, got: " + ex.getMessage());
    }

    @Test
    public void s3TablesWithoutRegionFailsLoud() {
        // WHY: Region.of("") would yield an invalid AWS region that only blows up deep in the SDK; the connector
        // must reject a region-less s3tables storage up front (legacy getRegion() is validated non-blank at
        // property-binding time). MUTATION: passing a blank region straight to Region.of -> a cryptic SDK error
        // instead of this message -> red.
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        List<StorageProperties> storages = Collections.singletonList(
                new FakeS3CompatibleStorageProperties("S3").accessKey("AK").secretKey("SK"));
        ctx.storageProperties = storages;
        IcebergConnector connector = new IcebergConnector(
                Map.of("iceberg.catalog.type", "s3tables",
                        "warehouse", "arn:aws:s3tables:us-east-1:1:bucket/b"),
                ctx);
        DorisConnectorException ex =
                Assertions.assertThrows(DorisConnectorException.class, () -> connector.getMetadata(null));
        Assertions.assertTrue(ex.getMessage().contains("region"),
                "expected a fail-loud message naming the missing region, got: " + ex.getMessage());
    }
}
