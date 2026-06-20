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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

/**
 * Dispatches a raw property map to the first {@link MetaStoreProvider} that
 * {@link MetaStoreProvider#supports(Map) supports} it (a catalog has exactly one metastore), mirroring
 * the first-hit semantics of {@code FileSystemPluginManager.createFileSystem}. Providers are
 * discovered via {@link ServiceLoader} (the {@code META-INF/services} entries), so there is no
 * central {@code switch} and no per-backend enum (D-006).
 */
public final class MetaStoreProviders {

    private static final List<MetaStoreProvider> PROVIDERS = load();

    private MetaStoreProviders() {
    }

    private static List<MetaStoreProvider> load() {
        List<MetaStoreProvider> list = new ArrayList<>();
        // Use the SPI interface's own defining classloader, not the thread-context classloader.
        // At CREATE CATALOG time this static initializer is first triggered from
        // PaimonConnectorProvider.validateProperties, which runs on an FE worker thread whose TCCL is
        // the FE app loader. fe-core does not depend on fe-connector-metastore-spi, so the providers and
        // their META-INF/services file live only inside the connector plugin's (child) classloader; a
        // 1-arg ServiceLoader.load (TCCL) therefore finds nothing and caches an empty list process-wide.
        // MetaStoreProvider.class.getClassLoader() is the plugin loader that defined this interface, so it
        // can see the service file and the impls regardless of the caller's TCCL.
        ServiceLoader.load(MetaStoreProvider.class, MetaStoreProvider.class.getClassLoader()).forEach(list::add);
        return list;
    }

    /**
     * Binds {@code properties} to the typed facts of the first supporting backend.
     *
     * @param properties          the raw CREATE-CATALOG properties
     * @param storageHadoopConfig the pre-computed neutral storage Hadoop config (may be empty; never null)
     * @return the bound {@link MetaStoreProperties}
     * @throws IllegalArgumentException if no registered provider supports {@code properties}
     */
    public static MetaStoreProperties bind(Map<String, String> properties,
            Map<String, String> storageHadoopConfig) {
        for (MetaStoreProvider provider : PROVIDERS) {
            if (provider.supports(properties)) {
                return provider.bind(properties, storageHadoopConfig);
            }
        }
        throw new IllegalArgumentException(
                "No MetaStoreProvider supports the given properties; registered providers: " + registeredNames());
    }

    /** Names of the registered providers (for diagnostics). */
    public static List<String> registeredNames() {
        return PROVIDERS.stream().map(MetaStoreProvider::name).collect(Collectors.toList());
    }
}
