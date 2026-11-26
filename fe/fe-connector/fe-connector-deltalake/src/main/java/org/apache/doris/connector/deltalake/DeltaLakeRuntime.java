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


package org.apache.doris.connector.deltalake;

import org.apache.doris.connector.hms.HmsConfHelper;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.filesystem.properties.StorageProperties;
import org.apache.doris.kerberos.HadoopAuthenticator;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.delta.kernel.Snapshot;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

/** Lazily built plugin resources; constructing catalog properties never opens storage or contacts a catalog. */
final class DeltaLakeRuntime implements AutoCloseable {
    final Engine engine;
    final DeltaLakeTableResolver resolver;
    final ConnectorContext storageContext;
    private final DeltaLakeFileIO fileIO;
    // Record raw encodings, never session-resolved instants: every reader of this snapshot must normalize
    // columns with empty null markers or offsetless TIMESTAMPs before applying their predicates.
    // Weak keys give the observation the same lifetime as the cached or in-flight Kernel snapshot.
    private final Cache<Snapshot, Set<String>> normalizedPartitionColumns = Caffeine.newBuilder().weakKeys().build();

    DeltaLakeRuntime(DeltaLakeCatalogProperties properties, ConnectorContext context) {
        HmsConfHelper.initializeHadoopConfigDir(context);
        Configuration configuration = storageConfiguration(properties.getRaw(), context);
        HadoopAuthenticator storageAuth = storageAuthenticator(configuration);
        storageContext = new TcclPinningConnectorContext(context, () -> storageAuth);
        fileIO = new DeltaLakeFileIO(configuration, storageContext);
        engine = DefaultEngine.create(fileIO);
        resolver = properties.getCatalogType().create(properties.getMetastore(), fileIO, configuration, storageContext);
    }

    static Configuration storageConfiguration(Map<String, String> properties, ConnectorContext context) {
        Map<String, String> merged = new HashMap<>();
        for (StorageProperties storage : context.getStorageContext().getStorageProperties()) {
            storage.toHadoopProperties().ifPresent(value -> merged.putAll(value.toHadoopConfigurationMap()));
        }
        // Keep the same precedence as Paimon: normalized storage settings first, explicit Hadoop keys last.
        properties.forEach((key, value) -> {
            if (key.startsWith("fs.") || key.startsWith("dfs.") || key.startsWith("hadoop.")) {
                merged.put(key, value);
            }
        });
        Configuration configuration = HmsConfHelper.createHadoopConfWithResources(
                merged.get("hadoop.config.resources"), merged);
        configuration.setClassLoader(DeltaLakeRuntime.class.getClassLoader());
        return configuration;
    }

    static HadoopAuthenticator storageAuthenticator(Configuration configuration) {
        // The plugin owns its Hadoop UGI copy. Explicit SIMPLE users need the same treatment as
        // Kerberos principals; otherwise an app-loader doAs leaves HDFS using the FE process user.
        boolean kerberos = "kerberos".equalsIgnoreCase(configuration.get("hadoop.security.authentication"));
        boolean explicitUser = !configuration.getTrimmed("hadoop.username", "").isEmpty();
        return kerberos || explicitUser ? HadoopAuthenticator.getHadoopAuthenticator(configuration) : null;
    }

    Engine engine(DeltaLakeTableReference reference) {
        return resolver.engine(reference, engine);
    }

    void recordNormalizedPartitionColumns(Snapshot snapshot, Set<String> columns) {
        normalizedPartitionColumns.put(snapshot, Set.copyOf(columns));
    }

    Collection<String> partitionColumnsToNormalize(Snapshot snapshot) {
        Set<String> observed = normalizedPartitionColumns.getIfPresent(snapshot);
        return observed != null ? observed : snapshot.getPartitionColumnNames();
    }

    Map<String, String> backendProperties(DeltaLakeTableReference reference) throws IOException {
        Map<String, String> configured = storageContext.getStorageContext().getBackendStorageProperties();
        return resolver.backendProperties(reference, configured);
    }

    <T> T execute(Callable<T> action) {
        try {
            return storageContext.executeAuthenticated(action);
        } catch (RuntimeException e) {
            throw e;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DorisConnectorException("Delta metadata operation interrupted for catalog '"
                    + storageContext.getCatalogName() + "'", e);
        } catch (Exception e) {
            throw new DorisConnectorException("Delta metadata operation failed for catalog '"
                    + storageContext.getCatalogName() + "'", e);
        }
    }

    @Override
    public void close() throws IOException {
        try (DeltaLakeTableResolver owned = resolver) {
            fileIO.close();
        }
    }
}
