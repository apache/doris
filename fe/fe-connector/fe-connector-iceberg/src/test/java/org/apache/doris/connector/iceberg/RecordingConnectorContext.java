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

import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.filesystem.properties.StorageProperties;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Hand-written {@link ConnectorContext} test double (no Mockito), adapted verbatim from the paimon
 * connector's {@code RecordingConnectorContext}.
 *
 * <p>{@link IcebergConnectorMetadata} takes a context (ctor {@code (IcebergCatalogOps, Map,
 * ConnectorContext)}) and wraps every remote read in {@link #executeAuthenticated}; the read tests use
 * this double to assert one wrap per op via {@link #authCount}, and that {@link #getStorageProperties} /
 * {@link #loadHiveConfResources} are threaded through. When {@link #failAuth} is set,
 * {@link #executeAuthenticated} throws WITHOUT invoking the task, which proves the seam call sits INSIDE
 * the authenticator.
 */
final class RecordingConnectorContext implements ConnectorContext {

    int authCount;
    boolean failAuth;

    /** Map the fake returns from {@link #loadHiveConfResources} (the "resolved" hive-site.xml keys). */
    Map<String, String> hiveConfResources = Collections.emptyMap();
    /** Whether the connector invoked {@link #loadHiveConfResources}. */
    boolean hiveConfResourcesCalled;
    /** The {@code resources} string the connector passed to {@link #loadHiveConfResources}. */
    String lastHiveConfResourcesArg;

    /** Storage properties the fake returns from {@link #getStorageProperties()} (default: none). */
    List<StorageProperties> storageProperties = Collections.emptyList();

    /** Raw URIs the connector routed through {@link #normalizeStorageUri(String)} (delete-path normalization). */
    final List<String> normalizedUris = new ArrayList<>();

    @Override
    public String getCatalogName() {
        return "test";
    }

    @Override
    public String normalizeStorageUri(String rawUri) {
        normalizedUris.add(rawUri);
        // Canonicalize the scheme the way DefaultConnectorContext does for native paths
        // (oss/cos/obs/s3a -> s3), so a test can prove the connector routes delete paths through this seam.
        // Identity for already-canonical s3:// paths, so the existing scan/read tests are unaffected.
        return rawUri == null ? null : rawUri.replaceFirst("^(oss|cos|obs|s3a)://", "s3://");
    }

    @Override
    public List<StorageProperties> getStorageProperties() {
        return storageProperties;
    }

    @Override
    public Map<String, String> loadHiveConfResources(String resources) {
        hiveConfResourcesCalled = true;
        lastHiveConfResourcesArg = resources;
        return hiveConfResources;
    }

    @Override
    public long getCatalogId() {
        return 0;
    }

    @Override
    public <T> T executeAuthenticated(Callable<T> task) throws Exception {
        authCount++;
        if (failAuth) {
            // Deliberately do NOT call task -> the wrapped seam call must not run.
            throw new RuntimeException("auth failed");
        }
        return task.call();
    }
}
