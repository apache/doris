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

/**
 * FE-facing storage parameter model for creating file systems.
 *
 * <p>This API is the common contract for all storage providers. Implementations
 * own raw parameter binding, validation, redaction metadata, and conversion to
 * runtime-specific configuration maps such as BE or Hadoop properties.</p>
 */
public interface StorageProperties {

    /**
     * Returns the provider name, such as S3, HDFS, Broker, Local, or OBS.
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
     * Returns whether the registry added this binding as a fallback without matching the user's
     * storage configuration. The origin is fixed at binding creation, not inferred from raw keys.
     * Normal provider bindings, including heuristic matches, return false.
     */
    default boolean isSyntheticDefault() {
        return false;
    }

    /**
     * Validates the format and required fields of the already-bound property model.
     * This may run during metadata replay, so time-dependent checks belong in
     * {@link #validateForAccess()} instead.
     */
    default void validate() {
    }

    /**
     * Checks whether the bound credentials can be used now, for example against a known expiry.
     * Callers must invoke this on each credential access, including when returning a cached
     * configuration map. The default preserves providers without time-dependent validation.
     *
     * <p>This is local validation only: implementations must not perform I/O, refresh credentials,
     * or mutate the binding. Invalid credentials must fail explicitly without exposing secrets.</p>
     */
    default void validateForAccess() {
    }

    /**
     * Returns the original raw properties passed to the provider.
     */
    Map<String, String> rawProperties();

    /**
     * Returns raw key-value pairs that matched provider-declared property aliases during binding.
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

    /**
     * Hadoop configuration needed by Iceberg's default REST FileIO selection. Unlike a
     * filesystem-backed catalog or an explicitly selected Hadoop/custom FileIO, that path
     * may use a native FileIO whose credentials must be selected from the load response first.
     * Providers can omit an unused Hadoop view without accessing an obsolete credential.
     * The default preserves every other provider's existing Hadoop configuration.
     *
     * <p>This does not affect native data readers, ordinary Hadoop consumers or credential
     * validity checks when the selected FileIO configuration is emitted.</p>
     */
    default Optional<HadoopStorageProperties> toIcebergHadoopProperties() {
        return toHadoopProperties();
    }

    /**
     * Connection-only properties for the official Iceberg FileIO, without authentication or
     * FileIO selection. A response may replace an obsolete credential while retaining the
     * provider's endpoint, so this view must not access credentials or validate their expiry.
     * Implementations must still validate connection fields and reject embedded credentials.
     *
     * <p>This performs no I/O and returns an immutable map. The default contributes nothing;
     * credential emission and access validation remain in {@link #toIcebergFileIOProperties()}.</p>
     */
    default Map<String, String> toIcebergFileIOConnectionProperties() {
        return Collections.emptyMap();
    }

    /**
     * Storage authentication and connection properties for the official Iceberg FileIO.
     * This is a separate consumer dialect from the native backend and Hadoop maps. The
     * provider owns translation; no Iceberg or SDK objects cross the plugin boundary.
     *
     * <p>Emitting credentials is an access point, so implementations must validate their
     * validity here. The default contributes nothing and preserves existing FileIO selection.
     * Returned maps must be immutable and must not be logged.</p>
     *
     * <p>A provider may include {@code io-impl} when its authentication requires a specific
     * default FileIO. Consumers must reconcile this requirement with explicit configuration,
     * REST server overrides and vended credentials before constructing FileIO; they must not
     * blindly overwrite those choices or silently substitute another identity. Hadoop
     * authentication still comes from {@link #toHadoopProperties()}, not this map.</p>
     */
    default Map<String, String> toIcebergFileIOProperties() {
        return Collections.emptyMap();
    }
}
