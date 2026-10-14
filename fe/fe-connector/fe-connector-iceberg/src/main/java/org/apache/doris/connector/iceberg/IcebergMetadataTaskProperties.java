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

import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.filesystem.Location;

import com.azure.core.util.UrlBuilder;
import com.azure.storage.blob.BlobUrlParts;
import com.azure.storage.common.sas.CommonSasQueryParameters;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ScanTaskParser;
import org.apache.iceberg.azure.AzureProperties;
import org.apache.iceberg.azure.adlsv2.ADLSFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.ResolvingFileIO;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.util.JsonUtil;
import org.apache.iceberg.util.PropertyUtil;

import java.net.URI;
import java.time.DateTimeException;
import java.util.Map;
import java.util.Set;

/** Non-secret execution metadata for the unchanged, serialized Iceberg FileIO task. */
final class IcebergMetadataTaskProperties {
    private static final Set<String> REMOTE_TASKS = Set.of(
            "org.apache.iceberg.BaseFilesTable$ManifestReadTask",
            "org.apache.iceberg.BaseEntriesTable$ManifestReadTask",
            "org.apache.iceberg.AllManifestsTable$ManifestListReadTask");

    private IcebergMetadataTaskProperties() {
    }

    static Long fixedSasExpiryMs(FileIO fileIO, FileScanTask task) {
        // Materialized rows do not use credentials at execution. Custom FileIOs retain their
        // serialization contract: unlike these two SDK implementations, properties() is optional.
        if ((fileIO.getClass() != ADLSFileIO.class && fileIO.getClass() != ResolvingFileIO.class)
                || !REMOTE_TASKS.contains(task.getClass().getName())) {
            return null;
        }
        // The SDK's public JSON view exposes the remote task location without opening FileIO.
        // In particular, all-manifests task.file() would issue HEAD to populate the file size.
        // Never deserialize this JSON: it cannot preserve Hadoop configuration or scoped credentials.
        String location = JsonUtil.parse(ScanTaskParser.toJson(task), node -> {
            if ("all-manifests-table-task".equals(JsonUtil.getString("task-type", node))) {
                return JsonUtil.getString("manifest-list-Location", node);
            }
            return JsonUtil.getString("path", node.get("manifest-file"));
        });
        // ResolvingFileIO.io() falls back to Hadoop when an optional provider such as GCS is
        // absent, whereas ioClass() throws. Only probe the schemes it routes to ADLSFileIO;
        // non-Azure tasks must retain their own FileIO resolution and credential lifecycle.
        if (fileIO instanceof ResolvingFileIO
                && (!Set.of("abfs", "abfss", "wasb", "wasbs").contains(Location.of(location).scheme())
                        || ((ResolvingFileIO) fileIO).ioClass(location) != ADLSFileIO.class)) {
            return null;
        }
        Map<String, String> properties = fileIO.properties();
        String refreshEndpoint = RESTUtil.resolveEndpoint(properties.get(CatalogProperties.URI),
                properties.get(AzureProperties.ADLS_REFRESH_CREDENTIALS_ENDPOINT));
        if (PropertyUtil.propertyAsBoolean(properties, AzureProperties.ADLS_REFRESH_CREDENTIALS_ENABLED, true)
                && refreshEndpoint != null && !refreshEndpoint.isEmpty()) {
            return null;
        }
        // Only the authority is relevant. Do not parse or rewrite raw ABFS object names (which
        // may contain spaces, literal percent signs or additional :// sequences).
        int pathStart = location.indexOf('/', location.indexOf("://") + 3);
        String host = URI.create(pathStart < 0 ? location : location.substring(0, pathStart)).getHost();
        String endpoint = properties.get(AzureProperties.ADLS_CONNECTION_STRING_PREFIX + host);
        if (endpoint != null) {
            CommonSasQueryParameters endpointSas;
            try {
                endpointSas = BlobUrlParts.parse(endpoint).getCommonSasQueryParameters();
            } catch (IllegalArgumentException | DateTimeException e) {
                throw new DorisConnectorException("Invalid Iceberg Azure FileIO endpoint");
            }
            // The Azure SDK applies the endpoint last; an embedded SAS replaces the property SAS.
            if (endpointSas.getSignature() != null && !endpointSas.getSignature().isEmpty()) {
                return sasExpiryMillis(endpointSas);
            }
        }
        String sas = properties.get(AzureProperties.ADLS_SAS_TOKEN_PREFIX + host);
        if (sas == null || sas.isEmpty()) {
            return null;
        }
        Long expiry;
        try {
            expiry = sasExpiryMillis(new BlobUrlParts().parseSasQueryParameters(sas).getCommonSasQueryParameters());
        } catch (IllegalArgumentException | DateTimeException e) {
            throw new DorisConnectorException("Invalid Iceberg FileIO SAS expiry");
        }
        String explicit = properties.get(AzureProperties.ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX + host);
        if (explicit != null) {
            final long explicitExpiry;
            try {
                explicitExpiry = Long.parseLong(explicit);
            } catch (NumberFormatException e) {
                throw new DorisConnectorException("Invalid Iceberg FileIO SAS expiry");
            }
            expiry = expiry == null ? explicitExpiry : Math.min(expiry, explicitExpiry);
        }
        return expiry;
    }

    private static Long sasExpiryMillis(CommonSasQueryParameters sas) {
        // The SDK supports stored-access-policy SAS without se, but getExpiryTime() dereferences
        // null in that case. Its encoded form omits absent fields; use the SDK query parser to
        // check presence before calling the date accessor, without reimplementing SAS date formats.
        return new UrlBuilder().setQuery(sas.encode()).getQuery().containsKey("se")
                ? sas.getExpiryTime().toInstant().toEpochMilli() : null;
    }
}
