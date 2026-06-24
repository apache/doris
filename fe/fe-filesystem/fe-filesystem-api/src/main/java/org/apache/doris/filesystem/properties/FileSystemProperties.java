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

import java.util.Map;
import java.util.Optional;

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
