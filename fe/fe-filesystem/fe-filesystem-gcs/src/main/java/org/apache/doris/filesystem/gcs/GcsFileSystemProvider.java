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

package org.apache.doris.filesystem.gcs;

import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.auth.ObjectStorageAuthentication;
import org.apache.doris.filesystem.gcs.auth.GcpCredential;
import org.apache.doris.filesystem.gcs.auth.GcsAuthResolver;
import org.apache.doris.filesystem.s3.S3CompatSignals;
import org.apache.doris.filesystem.s3.S3FileSystem;
import org.apache.doris.filesystem.s3.S3FileSystemProperties;
import org.apache.doris.filesystem.spi.FileSystemProvider;
import org.apache.doris.foundation.property.ConnectorPropertiesUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * SPI provider for Google Cloud Storage over the S3 interoperability endpoint (HMAC keys).
 *
 * <p>Registered via META-INF/services/org.apache.doris.filesystem.spi.FileSystemProvider.
 *
 * <p>Selected explicitly by {@code provider=GCS}/{@code GCP} or {@code fs.gcs.support=true};
 * otherwise, when the user declared no filesystem explicitly, by
 * {@link S3CompatSignals#guessIsGcs} (a {@code gs.endpoint}, or an endpoint on
 * {@code storage.googleapis.com}). Delegates I/O to {@link S3FileSystem}.
 */
public class GcsFileSystemProvider implements FileSystemProvider<GcsFileSystemProperties> {

    private static final String STORAGE_TYPE_GCS = "GCS";
    private static final String FS_GCS_SUPPORT = "fs.gcs.support";

    private static final Map<String, String> RESOURCE_ALIASES = Map.ofEntries(
            Map.entry("gs.endpoint", "s3.endpoint"),
            Map.entry("gs.access_key", "s3.access_key"),
            Map.entry("gs.secret_key", "s3.secret_key"),
            Map.entry("gs.session_token", "s3.session_token"),
            Map.entry("gs.connection.maximum", "s3.connection.maximum"),
            Map.entry("gs.connection.request.timeout", "s3.connection.request.timeout"),
            Map.entry("gs.connection.timeout", "s3.connection.timeout"),
            Map.entry("gs.use_path_style", "use_path_style"),
            Map.entry("gs.force_parsing_by_standard_uri", "force_parsing_by_standard_uri"));

    @Override
    public Map<String, String> normalizeProperties(Map<String, String> properties, Map<String, String> context) {
        GcsAuthResolver.resolve(properties);
        Map<String, String> normalized = new HashMap<>(properties);
        String selected = context.get("provider");
        if (selected == null || selected.isBlank()) {
            if (!GcsAuthResolver.guessIsGcs(context)) {
                return normalized;
            }
            selected = "GCP";
            normalized.put("provider", selected);
        }
        if ("GCP".equalsIgnoreCase(selected)) {
            RESOURCE_ALIASES.forEach((alias, key) -> {
                String value = normalized.remove(alias);
                if (value != null && !value.isBlank()) {
                    normalized.put(key, value);
                }
            });
        }
        return normalized;
    }

    @Override
    public Optional<ObjectStorageAuthentication> resolveAuthentication(Map<String, String> properties) {
        return GcsAuthResolver.resolve(properties).map(auth -> {
            Map<String, String> credential = new HashMap<>();
            Map<String, String> updates = new HashMap<>();
            auth.getNativeCredential().ifPresent(nativeCredential -> {
                credential.put("credential_provider_type", nativeCredential.getCredentialProviderType().name());
                if (!nativeCredential.getImpersonationServiceAccount().isEmpty()) {
                    credential.put("impersonation_service_account", nativeCredential.getImpersonationServiceAccount());
                }
                if (properties.containsKey(GcpCredential.CREDENTIAL_PROVIDER_TYPE)) {
                    updates.put("credential_provider_type", nativeCredential.getCredentialProviderType().name());
                }
                if (properties.containsKey(GcpCredential.IMPERSONATION_SERVICE_ACCOUNT)) {
                    updates.put("impersonation_service_account", nativeCredential.getImpersonationServiceAccount());
                }
            });
            return new ObjectStorageAuthentication("GCP", auth.isAnonymous(), credential, updates);
        });
    }

    @Override
    public Map<String, String> credentialToProperties(Map<String, String> credential) {
        Map<String, String> properties = new HashMap<>();
        properties.put(GcpCredential.CREDENTIAL_PROVIDER_TYPE, credential.get("credential_provider_type"));
        String principal = credential.get("impersonation_service_account");
        if (principal != null && !principal.isEmpty()) {
            properties.put(GcpCredential.IMPERSONATION_SERVICE_ACCOUNT, principal);
        }
        return properties;
    }

    @Override
    public Set<String> modifiableCredentialPropertyKeys() {
        return Set.of(GcpCredential.CREDENTIAL_PROVIDER_TYPE, GcpCredential.IMPERSONATION_SERVICE_ACCOUNT);
    }

    @Override
    public Set<String> clearablePropertyKeys() {
        return Set.of(GcpCredential.IMPERSONATION_SERVICE_ACCOUNT);
    }

    @Override
    public boolean supports(Map<String, String> properties) {
        String providerValue = properties.get(S3CompatSignals.PROVIDER_KEY);
        // "GCP" is the value legacy GCSProperties stamps into getBackendConfigProperties().
        if (STORAGE_TYPE_GCS.equalsIgnoreCase(providerValue)
                || "GCP".equalsIgnoreCase(providerValue)
                || S3CompatSignals.isFsSupport(properties, FS_GCS_SUPPORT)) {
            return true;
        }
        return S3CompatSignals.guessAllowed(properties) && S3CompatSignals.guessIsGcs(properties);
    }

    @Override
    public GcsFileSystemProperties bind(Map<String, String> properties) {
        return GcsFileSystemProperties.of(properties);
    }

    @Override
    public FileSystem create(GcsFileSystemProperties properties) throws IOException {
        Map<String, String> delegateProperties = new HashMap<>(properties.toS3CompatibleKv());
        if (properties.getAuth().getNativeCredential().isPresent()) {
            // The delegate only supplies URI/connection settings. GcsObjStorage supplies OAuth.
            delegateProperties.remove(GcpCredential.CREDENTIAL_PROVIDER_TYPE);
            delegateProperties.remove(GcpCredential.IMPERSONATION_SERVICE_ACCOUNT);
            delegateProperties.put("AWS_CREDENTIALS_PROVIDER_TYPE", "ANONYMOUS");
        }
        S3FileSystemProperties delegate = S3FileSystemProperties.of(delegateProperties);
        return new S3FileSystem(delegate,
                new GcsObjStorage(delegate, properties));
    }

    @Override
    public boolean supportsExplicit(Map<String, String> properties) {
        return S3CompatSignals.isFsSupport(properties, FS_GCS_SUPPORT);
    }

    @Override
    public boolean supportsGuess(Map<String, String> properties) {
        return !S3CompatSignals.hasExplicitS3Request(properties) && S3CompatSignals.guessIsGcs(properties);
    }

    @Override
    public FileSystem create(Map<String, String> properties) throws IOException {
        return create(bind(properties));
    }

    @Override
    public String name() {
        return "GCS";
    }

    @Override
    public Set<String> sensitivePropertyKeys() {
        return ConnectorPropertiesUtils.getSensitiveKeys(GcsFileSystemProperties.class);
    }
}
