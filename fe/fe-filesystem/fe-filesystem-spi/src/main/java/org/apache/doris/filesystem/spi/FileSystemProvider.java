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

package org.apache.doris.filesystem.spi;

import org.apache.doris.extension.spi.Plugin;
import org.apache.doris.extension.spi.PluginFactory;
import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.properties.FileSystemProperties;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * SPI interface for filesystem provider discovery via Java ServiceLoader.
 *
 * <p>Extends {@link PluginFactory} to allow {@link
 * org.apache.doris.extension.loader.DirectoryPluginRuntimeManager} to load filesystem
 * providers from plugin directories at runtime, decoupling fe-core from concrete
 * storage backend implementations at the Maven dependency level.
 *
 * <p>Implementations must:
 * 1. Have a public no-arg constructor.
 * 2. Register in META-INF/services/org.apache.doris.filesystem.spi.FileSystemProvider.
 * 3. Have NO dependency on fe-core, fe-common, or fe-catalog.
 */
public interface FileSystemProvider<P extends FileSystemProperties> extends PluginFactory {

    /**
     * Returns true if this provider can handle the given properties.
     * Must be cheap (no network calls) and deterministic.
     *
     * @param properties key-value storage configuration
     * @return true if this provider supports the configuration
     */
    boolean supports(Map<String, String> properties);

    /**
     * Binds raw key-value storage configuration into a provider-owned typed properties model.
     *
     * <p>Providers that have been migrated to typed properties should override this method and
     * return a validated immutable properties object. Legacy providers can continue to implement
     * {@link #create(Map)} directly during the migration period.
     */
    default P bind(Map<String, String> properties) {
        throw new UnsupportedOperationException(
                name() + " does not support typed FileSystemProperties binding yet.");
    }

    /**
     * Creates a FileSystem instance from validated typed properties.
     *
     * <p>Typed providers should override this method and construct the runtime client
     * directly from typed accessors. The migration-compatible map entry remains
     * {@link #create(Map)}.
     */
    default FileSystem create(P properties) throws IOException {
        throw new UnsupportedOperationException(
                name() + " does not support typed FileSystem creation yet.");
    }

    /**
     * Creates a FileSystem instance from a properties object whose static type is not known
     * at the registry or factory call site.
     */
    @SuppressWarnings("unchecked")
    default FileSystem createUntyped(FileSystemProperties properties) throws IOException {
        return create((P) properties);
    }

    /**
     * Creates a FileSystem instance from the given properties.
     * Called only after {@link #supports(Map)} returns true.
     *
     * @param properties key-value storage configuration
     * @return a ready-to-use FileSystem
     * @throws IOException if the filesystem cannot be initialized
     */
    FileSystem create(Map<String, String> properties) throws IOException;

    /**
     * Returns the raw property key aliases this provider treats as sensitive credentials.
     *
     * <p>Framework code (e.g. {@code DatasourcePrintableMap}) aggregates these from all loaded
     * providers to mask credential values when printing property maps (SHOW CREATE, error logs),
     * without fe-core needing a compile-time dependency on provider implementations. Providers with
     * typed properties should return {@code ConnectorPropertiesUtils.getSensitiveKeys(XxxProperties.class)}
     * so the {@code @ConnectorProperty(sensitive = true)} annotation stays the single source of truth.
     *
     * @return sensitive property key aliases; empty if the provider has no credentials to mask
     */
    default Set<String> sensitivePropertyKeys() {
        return Collections.emptySet();
    }

    /**
     * Human-readable name for logging/diagnostics (e.g., "S3", "HDFS", "Azure").
     */
    @Override
    default String name() {
        return getClass().getSimpleName().replace("FileSystemProvider", "");
    }

    /**
     * Not used by DirectoryPluginRuntimeManager (it only discovers factories via ServiceLoader).
     * Provided to satisfy {@link PluginFactory} contract.
     */
    @Override
    default Plugin create() {
        throw new UnsupportedOperationException(
                "FileSystemProvider does not support no-arg create(). Use create(Map) instead.");
    }
}
