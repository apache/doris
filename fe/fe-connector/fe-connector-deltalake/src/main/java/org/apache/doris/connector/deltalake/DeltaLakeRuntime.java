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

import org.apache.doris.connector.hms.HmsClient;
import org.apache.doris.connector.hms.HmsClientConfig;
import org.apache.doris.connector.hms.HmsConfHelper;
import org.apache.doris.connector.hms.ThriftHmsClient;
import org.apache.doris.connector.metastore.FileSystemMetaStoreProperties;
import org.apache.doris.connector.metastore.spi.AbstractHmsMetaStoreProperties;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.filesystem.properties.StorageProperties;
import org.apache.doris.kerberos.AuthType;
import org.apache.doris.kerberos.AuthenticationConfig;
import org.apache.doris.kerberos.HadoopAuthenticator;
import org.apache.doris.kerberos.KerberosAuthSpec;
import org.apache.doris.kerberos.KerberosAuthenticationConfig;

import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;

/** Lazily built plugin resources; constructing catalog properties never opens storage or HMS. */
final class DeltaLakeRuntime implements AutoCloseable {
    final Engine engine;
    final DeltaLakeTableResolver resolver;
    final ConnectorContext storageContext;
    private final DeltaLakeFileIO fileIO;
    private final HmsClient hms;

    DeltaLakeRuntime(DeltaLakeCatalogProperties properties, ConnectorContext context) {
        HmsConfHelper.initializeHadoopConfigDir(context);
        Configuration configuration = storageConfiguration(properties.getRaw(), context);
        HadoopAuthenticator storageAuth = storageAuthenticator(configuration);
        storageContext = new TcclPinningConnectorContext(context, () -> storageAuth);
        fileIO = new DeltaLakeFileIO(configuration, storageContext);
        engine = DefaultEngine.create(fileIO);
        if (DeltaLakeCatalogProperties.HMS.equals(properties.getCatalogType())) {
            AbstractHmsMetaStoreProperties metastore = (AbstractHmsMetaStoreProperties) properties.getMetastore();
            HadoopAuthenticator hmsAuth = hmsAuthenticator(metastore);
            ConnectorContext hmsContext = new TcclPinningConnectorContext(context, () -> hmsAuth);
            HmsClientConfig config = new HmsClientConfig(metastore.getConfResources(),
                    HmsConfHelper.mergeCatalogProperties(properties.getRaw(),
                            metastore.toHiveConfOverrides(HmsConfHelper.metastoreClientTimeoutSecond(context))),
                    properties.getHmsClientPoolSize());
            hms = new ThriftHmsClient(config, new ThriftHmsClient.AuthAction() {
                @Override
                public <T> T execute(Callable<T> action) throws Exception {
                    return hmsContext.executeAuthenticated(action);
                }
            });
            resolver = new DeltaLakeHmsTableResolver(hms);
        } else {
            hms = null;
            resolver = new DeltaLakeFileSystemTableResolver(
                    ((FileSystemMetaStoreProperties) properties.getMetastore()).getWarehouse(), fileIO);
        }
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

    static HadoopAuthenticator hmsAuthenticator(AbstractHmsMetaStoreProperties properties) {
        Configuration configuration = HmsConfHelper.createHadoopConfWithResources(
                properties.getConfResources(), properties.toHiveConfOverrides(""));
        Optional<KerberosAuthSpec> kerberos = properties.kerberos();
        if (kerberos.isPresent() && kerberos.get().hasCredentials()) {
            KerberosAuthSpec spec = kerberos.get();
            configuration.set("hadoop.security.authentication", "kerberos");
            configuration.set("hive.metastore.sasl.enabled", "true");
            return HadoopAuthenticator.getHadoopAuthenticator(
                    new KerberosAuthenticationConfig(spec.getPrincipal(), spec.getKeytab(), configuration));
        }
        if (properties.getAuthType() == AuthType.KERBEROS) {
            return null;
        }
        // HMS SIMPLE user selection belongs to the metastore, not to the storage identity.
        return HadoopAuthenticator.getHadoopAuthenticator(
                AuthenticationConfig.getSimpleAuthenticationConfig(configuration));
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
        // A null HMS client is valid for a filesystem catalog. Try-with-resources also preserves
        // both failures if closing the filesystem and the HMS pool each reports an error.
        try (HmsClient ownedHms = hms) {
            fileIO.close();
        }
    }
}
