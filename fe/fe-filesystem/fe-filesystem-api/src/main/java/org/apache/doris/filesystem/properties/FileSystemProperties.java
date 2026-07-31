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

package org.apache.doris.filesystem.properties;

import org.apache.doris.filesystem.FileSystemType;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Parsed and validated FileSystem properties owned by a specific provider.
 *
 * <p>The API layer exposes this interface so framework code can pass typed
 * configuration between filesystem modules without depending on provider
 * implementations. Each provider is responsible for binding raw key-value
 * properties, validating them, and creating runtime clients from typed accessors.</p>
 */
public interface FileSystemProperties extends StorageProperties {

    /**
     * Returns the provider name, such as S3, OSS, COS, or OBS.
     */
    String providerName();

    /**
     * Returns the generic storage kind used for provider selection and routing.
     */
    StorageKind kind();

    /**
     * Returns the Doris filesystem type represented by this property model.
     */
    FileSystemType type();

    /**
     * Returns the original raw properties passed to FileSystemProvider.bind().
     *
     * <p>The returned map is intended for diagnostics and compatibility paths.
     * Callers should prefer typed accessors or conversion methods for runtime
     * behavior.</p>
     */
    Map<String, String> rawProperties();

    /**
     * Returns raw key-value pairs that matched provider-declared property aliases during binding.
     *
     * <p>If a provider accepts multiple aliases for the same setting, this map
     * records which input keys were actually consumed. This is useful for auditing
     * and for detecting unused or misspelled options without exposing provider
     * implementation details.</p>
     */
    Map<String, String> matchedProperties();

    /**
     * Returns the URI schemes this provider accepts, lower-cased (e.g. {@code {hdfs, viewfs}}).
     *
     * <p>Single source of truth for the provider's scheme identity: URI parsing and
     * scheme-to-storage routing both read this set. The default is empty for providers
     * without a scheme identity (e.g. Broker, Local); every scheme-addressable provider
     * overrides it.</p>
     */
    default Set<String> getSupportedSchemes() {
        return Collections.emptySet();
    }

    /**
     * Storage family name reported to fe-core consumers and persisted in e.g. backup
     * Repository metadata (the legacy {@code getStorageName()} contract). Defaults to the
     * provider name; families with a legacy spelling override it (every S3-compatible
     * dialect reports {@code "S3"}, HDFS-family {@code "HDFS"}, Local {@code "local"}).
     */
    default String storageFamilyName() {
        return providerName();
    }

    /**
     * The exact scheme set the legacy fe-core {@code schemas()} contract exposed for this
     * provider — feeds per-scheme {@code fs.<schema>.impl.disable.cache} handling. This is
     * deliberately NOT {@link #getSupportedSchemes()}: that set advertises broader routing
     * aliases (e.g. OSS accepts {@code {oss, s3, s3a}}) while the legacy disable-cache loop
     * only ever saw the narrow per-type sets (e.g. OSS = {@code {oss}}, HDFS family =
     * {@code {hdfs}} only).
     */
    default Set<String> legacyCacheSchemes() {
        return Collections.emptySet();
    }

    /**
     * Validates and normalizes a single storage URI against this provider's configuration.
     *
     * <p>This is pure configuration logic (no I/O) and lives on the properties model rather than
     * on {@link org.apache.doris.filesystem.FileSystem} on purpose: many callers validate a URI
     * before — or without ever — creating a FileSystem (e.g. HTTP has no FileSystem). Providers
     * with scheme/endpoint/path-style specific rules override this; the default returns the URI
     * unchanged.
     *
     * @throws IllegalArgumentException if the URI is invalid for this provider
     */
    default String validateAndNormalizeUri(String uri) {
        return uri;
    }

    /**
     * Extracts the storage URI from the given load properties and validates it via
     * {@link #validateAndNormalizeUri(String)}.
     *
     * <p>The default looks up the {@code "uri"} key. Providers whose URI lives under a different
     * key, or that need extra extraction logic (e.g. HDFS nameservices), override this.
     *
     * @throws IllegalArgumentException if the URI is missing or invalid for this provider
     */
    default String validateAndGetUri(Map<String, String> loadProperties) {
        return validateAndNormalizeUri(loadProperties == null ? null : loadProperties.get("uri"));
    }

    /**
     * Converts to backend storage properties if this provider supports BE access.
     */
    default Optional<BackendStorageProperties> toBackendProperties() {
        return Optional.empty();
    }

    /**
     * Converts to Hadoop configuration properties if this provider supports Hadoop access.
     */
    default Optional<HadoopStorageProperties> toHadoopProperties() {
        return Optional.empty();
    }
}
