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

package org.apache.doris.connector.hive;

import org.apache.doris.connector.spi.ConnectorBrokerAddress;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.filesystem.properties.StorageProperties;
import org.apache.doris.thrift.TFileType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Hand-written {@link ConnectorContext} test double (no Mockito) for the hive write-plan tests, adapted from
 * the iceberg connector's {@code RecordingConnectorContext}. Resolves the BE file type / normalized write
 * path / broker addresses / static storage creds that {@link HiveWritePlanProvider} consults, and passes
 * {@link #executeAuthenticated} through so the table load runs inside the (fake) auth context.
 */
final class RecordingConnectorContext implements ConnectorContext {

    int authCount;

    /** BE file type the fake returns from {@link #getBackendFileType} (drives in-place vs staging write path). */
    TFileType backendFileType = TFileType.FILE_S3;

    /** Raw URIs the connector routed through {@link #normalizeStorageUri}. */
    final List<String> normalizedUris = new ArrayList<>();

    /** Static storage properties the fake returns from {@link #getStorageProperties()} (BE-canonical creds via
     * {@code sp.toBackendProperties().toMap()}); default none. */
    List<StorageProperties> storageProperties = Collections.emptyList();

    /** Broker addresses the fake returns from {@link #getBrokerAddresses()}; default none, so a FILE_BROKER
     * write fails loud ("No alive broker.") unless a test populates it. */
    List<ConnectorBrokerAddress> brokerAddresses = Collections.emptyList();

    @Override
    public String getCatalogName() {
        return "test";
    }

    @Override
    public long getCatalogId() {
        return 0;
    }

    @Override
    public String getBackendFileType(String rawUri, Map<String, String> rawVendedCredentials) {
        return backendFileType.name();
    }

    @Override
    public String normalizeStorageUri(String rawUri) {
        return normalizeStorageUri(rawUri, null);
    }

    @Override
    public String normalizeStorageUri(String rawUri, Map<String, String> rawVendedCredentials) {
        normalizedUris.add(rawUri);
        // Canonicalize the scheme the way DefaultConnectorContext does for native object-store paths
        // (oss/cos/obs/s3a -> s3), so a test can prove the connector routes the location through this seam.
        return rawUri == null ? null : rawUri.replaceFirst("^(oss|cos|obs|s3a)://", "s3://");
    }

    @Override
    public List<StorageProperties> getStorageProperties() {
        return storageProperties;
    }

    @Override
    public List<ConnectorBrokerAddress> getBrokerAddresses() {
        return brokerAddresses;
    }

    @Override
    public <T> T executeAuthenticated(Callable<T> task) throws Exception {
        authCount++;
        return task.call();
    }
}
