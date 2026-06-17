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

package org.apache.doris.connector.metastore;

import java.util.Map;

/**
 * Public contract for a connector's bound-and-validated metastore connection properties —
 * the metastore-side counterpart of fe-filesystem's {@code StorageProperties}.
 *
 * <p>Following the same thin-interface principle as fe-filesystem-api, this API exposes only
 * neutral {@code Map}/scalar facts and never leaks {@code HiveConf}/Hadoop/engine-SDK types;
 * the concrete {@code HiveConf} (and any SDK catalog) is assembled on the connector side.
 *
 * <p>The backend is identified by a {@link #providerName()} string and cross-cutting behaviour
 * is expressed through capability methods (e.g. {@link #needsStorage()}), deliberately avoiding a
 * per-backend {@code MetaStoreType} enum and the central {@code switch} statements that come with
 * it (D-006). Backend discovery/dispatch is done by {@code MetaStoreProvider.supports(Map)} +
 * ServiceLoader in fe-connector-metastore-spi.
 */
public interface MetaStoreProperties {

    /** Stable backend identifier, e.g. "HMS", "DLF", "REST", "JDBC", "FILESYSTEM". */
    String providerName();

    /**
     * Whether this backend needs the bound {@code List<StorageProperties>} to be supplied during
     * parsing (FileSystem/DLF do; HMS/REST/JDBC do not). Replaces a per-backend enum switch.
     */
    default boolean needsStorage() {
        return false;
    }

    /** Whether this backend uses vended credentials (replaces {@code VendedCredentialsFactory} type switch). */
    default boolean needsVendedCredentials() {
        return false;
    }

    /** Validates the bound facts; the default is a no-op for backends with no extra invariants. */
    default void validate() {
    }

    /** The raw, unmodified properties the catalog was created with. */
    Map<String, String> rawProperties();

    /** The subset of raw properties actually matched by the backend's {@code @ConnectorProperty} aliases. */
    Map<String, String> matchedProperties();
}
