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

package org.apache.doris.connector.metastore.spi;

import org.apache.doris.connector.metastore.MetaStoreProperties;
import org.apache.doris.extension.spi.Plugin;
import org.apache.doris.extension.spi.PluginFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Backend discovery SPI for metastore connection properties — the metastore-side counterpart of
 * fe-filesystem's {@code FileSystemProvider}. Adding a backend = a new provider + one line in
 * {@code META-INF/services}; the API/SPI need no change and there is no central {@code switch}
 * (D-006).
 *
 * <p>A provider self-identifies via {@link #supports(Map)} (reads a cheap, deterministic signal
 * such as {@code paimon.catalog.type}) and, when selected, parses the raw properties into its typed
 * {@link MetaStoreProperties} facts via {@link #bind(Map, Map)}. Discovery is via
 * {@link java.util.ServiceLoader}; a catalog has exactly one metastore, so the dispatcher
 * ({@link MetaStoreProviders#bind}) selects the FIRST supporting provider.
 *
 * @param <P> the concrete {@link MetaStoreProperties} subtype produced
 */
public interface MetaStoreProvider<P extends MetaStoreProperties> extends PluginFactory {

    /** Cheap, deterministic self-identification (e.g. reads {@code paimon.catalog.type}). */
    boolean supports(Map<String, String> properties);

    /**
     * Parses the raw properties (plus a pre-computed neutral storage Hadoop-config map, used by the
     * backends whose {@link MetaStoreProperties#needsStorage()} is true to overlay storage into the
     * conf in the parity-critical order) into the typed facts.
     *
     * @param properties          the raw CREATE-CATALOG properties
     * @param storageHadoopConfig the canonical object-store Hadoop config (may be empty; never null),
     *                            pre-computed by the FE/connector from the bound storage properties;
     *                            kept neutral so this module stays hadoop/fs-free (DV-007)
     */
    P bind(Map<String, String> properties, Map<String, String> storageHadoopConfig);

    /** Alias keys of sensitive properties (for masking in logs); empty by default. */
    default Set<String> sensitivePropertyKeys() {
        return Collections.emptySet();
    }

    @Override
    default String name() {
        return getClass().getSimpleName().replace("MetaStoreProvider", "");
    }

    @Override
    default Plugin create() {
        throw new UnsupportedOperationException(
                "MetaStoreProvider does not support no-arg create(); use bind(Map, Map) instead.");
    }
}
