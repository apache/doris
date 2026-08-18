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

package org.apache.doris.connector.hudi;

import org.apache.doris.connector.hms.CachingHmsClient;
import org.apache.doris.connector.hms.HmsClient;
import org.apache.doris.connector.hms.HmsClientConfig;
import org.apache.doris.connector.hms.HmsConfHelper;
import org.apache.doris.connector.hms.ThriftHmsClient;
import org.apache.doris.connector.metastore.spi.AbstractHmsMetaStoreProperties;
import org.apache.doris.connector.metastore.spi.MetaStoreProviders;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.scan.ConnectorScanPlanProvider;
import org.apache.doris.kerberos.AuthType;
import org.apache.doris.kerberos.AuthenticationConfig;
import org.apache.doris.kerberos.HadoopAuthenticator;
import org.apache.doris.kerberos.KerberosAuthSpec;
import org.apache.doris.kerberos.KerberosAuthenticationConfig;

import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;

/**
 * Hudi connector implementation. Manages the lifecycle of an
 * {@link HmsClient} for HMS table discovery and provides Hadoop
 * configuration for building {@code HoodieTableMetaClient}.
 *
 * <p>Phase 1 provides read-only metadata operations (list databases,
 * list tables, get schema via Hudi's Avro schema). Phase 2 adds scan
 * planning for COW and MOR tables (snapshot reads).</p>
 *
 * <p>Built only as an embedded <em>sibling</em> of the hive {@code hms} gateway (via
 * {@code ConnectorContext.createSiblingConnector("hudi", ...)}), never as a standalone {@code type=hudi}
 * catalog — see {@link HudiConnectorProvider}.</p>
 */
public class HudiConnector implements Connector {

    private static final Logger LOG = LogManager.getLogger(HudiConnector.class);

    private final HudiCatalogProperties props;
    private final Map<String, String> properties;
    private final ConnectorContext context;
    private volatile HmsClient hmsClient;

    // HMS and storage deliberately have separate authenticators: hive.metastore.username must affect set_ugi
    // without changing the UGI used by HoodieTableMetaClient and FileIO.
    private volatile HadoopAuthenticator hmsAuth;
    private volatile boolean hmsAuthComputed;
    private volatile HadoopAuthenticator storageAuth;
    private volatile boolean storageAuthComputed;

    public HudiConnector(Map<String, String> properties, ConnectorContext context) {
        this.props = HudiCatalogProperties.of(properties);
        this.properties = props.getRaw();
        this.context = context;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session) {
        return new HudiConnectorMetadata(getOrCreateClient(), props, metaClientExecutor(),
                HudiScanPlanProvider.storageHadoopConfig(context));
    }

    /**
     * Builds the metaClient execute-wrapper the metadata partition/snapshot methods run their
     * {@code HoodieTableMetaClient}-touching work inside: a TCCL pin to the hudi plugin classloader (so
     * hudi-bundled reflection resolves the plugin's child-first copies) around the plugin UGI {@code doAs}
     * (Kerberos) — or the FE-injected {@code context.executeAuthenticated} when storage is non-Kerberos —
     * restoring the previous TCCL in a {@code finally}. The storage-specific choice is intentionally separate
     * from HMS auth; the TCCL pin is added because — unlike the HMS thrift RPC (which pins the
     * system loader) — building a metaClient / listing partitions off the (unpinned) planning thread needs the
     * plugin loader. See {@link HudiMetaClientExecutor} and memory
     * {@code catalog-spi-plugin-tccl-classloader-gotcha}.
     */
    private HudiMetaClientExecutor metaClientExecutor() {
        return new HudiMetaClientExecutor() {
            @Override
            public <T> T execute(Callable<T> action) {
                ClassLoader previous = Thread.currentThread().getContextClassLoader();
                Thread.currentThread().setContextClassLoader(HudiConnector.class.getClassLoader());
                try {
                    HadoopAuthenticator auth = storageAuthenticator();
                    if (auth != null) {
                        return auth.doAs(action::call);
                    }
                    return context.executeAuthenticated(action);
                } catch (Exception e) {
                    throw new DorisConnectorException("Hudi metadata operation failed for catalog '"
                            + context.getCatalogName() + "'", e);
                } finally {
                    Thread.currentThread().setContextClassLoader(previous);
                }
            }
        };
    }

    /**
     * True for a handle this connector produced (a {@link HudiTableHandle}). Tested against this connector's OWN
     * in-loader type, so a heterogeneous hms gateway that embeds this connector as a sibling can route a foreign
     * hudi handle here without casting it across the plugin classloader split. Returns false for any other
     * connector's handle (e.g. an iceberg sibling's), so the gateway keeps looking.
     */
    @Override
    public boolean ownsHandle(ConnectorTableHandle handle) {
        return handle instanceof HudiTableHandle;
    }

    @Override
    public ConnectorScanPlanProvider getScanPlanProvider() {
        return new HudiScanPlanProvider(properties, context);
    }

    /**
     * REFRESH TABLE hook: flush this table's cached HMS metadata ({@link CachingHmsClient#flush}: table info +
     * partition names) so the next query re-reads it live. Reads the client field WITHOUT building it
     * (getOrCreateClient would force a real client just to flush an empty cache; a never-queried catalog has no
     * cache to flush). hudi is a leaf sibling (no siblings of its own) holding no file/partition-view caches, so
     * the metastore flush is the only layer. The hive gateway forwards REFRESH to this sibling via
     * {@code forEachBuiltSibling}, so this override is what makes REFRESH reach the sibling's own client
     * (fe-core routes REFRESH TABLE to {@code connector.invalidateTable} for a plugin-driven catalog).
     */
    @Override
    public void invalidateTable(String dbName, String tableName) {
        invalidateTable(hmsClient, dbName, tableName);
    }

    // Package-private seam: a unit test can pass an observable CachingHmsClient (the hmsClient field is
    // otherwise only set by getOrCreateClient building a real pooled client).
    void invalidateTable(HmsClient client, String dbName, String tableName) {
        if (client instanceof CachingHmsClient) {
            ((CachingHmsClient) client).flush(dbName, tableName);
        }
    }

    /**
     * REFRESH DATABASE hook: flush every cached table in this database ({@link CachingHmsClient#flushDb}). Same
     * no-force-build read of the client as {@link #invalidateTable(String, String)}.
     */
    @Override
    public void invalidateDb(String dbName) {
        invalidateDb(hmsClient, dbName);
    }

    // Package-private seam (see invalidateTable above).
    void invalidateDb(HmsClient client, String dbName) {
        if (client instanceof CachingHmsClient) {
            ((CachingHmsClient) client).flushDb(dbName);
        }
    }

    /**
     * REFRESH CATALOG hook: flush this catalog's entire HMS metadata cache ({@link CachingHmsClient#flushAll}).
     * Same no-force-build read of the client as {@link #invalidateTable(String, String)}.
     */
    @Override
    public void invalidateAll() {
        invalidateAll(hmsClient);
    }

    // Package-private seam (see invalidateTable above).
    void invalidateAll(HmsClient client) {
        if (client instanceof CachingHmsClient) {
            ((CachingHmsClient) client).flushAll();
        }
    }

    private HmsClient getOrCreateClient() {
        if (hmsClient == null) {
            synchronized (this) {
                if (hmsClient == null) {
                    hmsClient = createClient();
                }
            }
        }
        return hmsClient;
    }

    private HmsClient createClient() {
        // The URI (either spelling) and the pool size were checked when this connector was constructed --
        // HudiCatalogProperties.of throws for a catalog without a metastore URI, so there is nothing left
        // to re-check here.
        int poolSize = props.getHmsClientPoolSize();

        AbstractHmsMetaStoreProperties hms = (AbstractHmsMetaStoreProperties) MetaStoreProviders.bindForType(
                HmsClientConfig.METASTORE_TYPE_HMS, properties, Collections.emptyMap());
        HmsClientConfig config = new HmsClientConfig(hms.getConfResources(),
                HmsConfHelper.mergeCatalogProperties(properties, hms.toHiveConfOverrides("")),
                poolSize);
        LOG.info("Creating Hudi connector HMS client for catalog='{}', uri={}, poolSize={}",
                context.getCatalogName(), config.getMetastoreUri(), poolSize);

        // HMS SIMPLE and Kerberos both need the plugin's UGI at set_ugi/client-RPC time; the separate storage
        // authenticator is deliberately not reused because hive.metastore.username is not a FileIO identity.
        // AuthAction.execute is generic, so it cannot be a lambda. ThriftHmsClient.doAs pins the RPC TCCL; the
        // plugin authenticator here only adds the UGI doAs.
        HadoopAuthenticator auth = hmsAuthenticator();
        ThriftHmsClient.AuthAction authAction;
        if (auth != null) {
            authAction = new ThriftHmsClient.AuthAction() {
                @Override
                public <T> T execute(Callable<T> callable) throws Exception {
                    return auth.doAs(callable::call);
                }
            };
        } else {
            authAction = context::executeAuthenticated;
        }
        return wrapWithCache(new ThriftHmsClient(config, authAction));
    }

    /**
     * Wraps the raw pooled client in the shared {@link CachingHmsClient} (mirrors {@code HiveConnector}):
     * {@code getTable} and {@code listPartitionNames} become {@code (db,table)}-keyed and TTL-bounded
     * ({@code meta.cache.hive.*}, default 24h), so repeated queries against the same hudi table stop re-hitting
     * HMS; {@code tableExists}/{@code listTables} stay pass-through. Freshness is preserved two ways: the
     * SHOW-PARTITIONS / {@code partition_values} path lists FRESH (bypasses the cache &mdash; see
     * {@link HudiConnectorMetadata}{@code .collectPartitions}), and REFRESH flushes it (see
     * {@link #invalidateTable(String, String)}). Package-private so a unit test can wrap an observable fake and
     * assert the cache decoration.
     */
    HmsClient wrapWithCache(HmsClient raw) {
        return new CachingHmsClient(raw, properties);
    }

    /**
     * Lazily builds and memoizes the plugin-side authenticator that {@link #createClient()} wraps the
     * metastore RPC under, so the RPC uses the PLUGIN's own {@code UserGroupInformation} copy (hadoop +
     * fe-kerberos are bundled child-first in the hudi plugin). Construction is cheap — a keytab login is lazy
     * in {@code getUGI()} on the first {@code doAs}. Mirrors {@code HiveConnector.pluginAuthenticator}.
     */
    private HadoopAuthenticator hmsAuthenticator() {
        if (!hmsAuthComputed) {
            synchronized (this) {
                if (!hmsAuthComputed) {
                    hmsAuth = buildHmsAuthenticator(properties);
                    hmsAuthComputed = true;
                }
            }
        }
        return hmsAuth;
    }

    private HadoopAuthenticator storageAuthenticator() {
        if (!storageAuthComputed) {
            synchronized (this) {
                if (!storageAuthComputed) {
                    storageAuth = buildPluginAuthenticator(properties);
                    storageAuthComputed = true;
                }
            }
        }
        return storageAuth;
    }

    /**
     * Resolves the plugin-side HMS authenticator. Explicit HMS SIMPLE/KERBEROS wins over storage fallback, so
     * the metastore identity remains independent from the Hudi data-path identity. Package-visible + static for
     * KDC-free unit testing.
     */
    static HadoopAuthenticator buildHmsAuthenticator(Map<String, String> properties) {
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(HudiConnector.class.getClassLoader());
            AbstractHmsMetaStoreProperties hms = (AbstractHmsMetaStoreProperties) MetaStoreProviders.bindForType(
                    HmsClientConfig.METASTORE_TYPE_HMS, properties, Collections.emptyMap());
            Optional<KerberosAuthSpec> spec = hms.kerberos();
            if (spec.isPresent() && spec.get().hasCredentials()) {
                Configuration conf = buildHmsConf(hms);
                conf.set("hadoop.security.authentication", "kerberos");
                conf.set("hive.metastore.sasl.enabled", "true");
                return HadoopAuthenticator.getHadoopAuthenticator(
                        new KerberosAuthenticationConfig(
                                spec.get().getPrincipal(), spec.get().getKeytab(), conf));
            }
            if (hms.getAuthType() == AuthType.KERBEROS) {
                return null;
            }
            // HMS set_ugi uses the current UGI; scope this user to the metastore instead of Hudi FileIO.
            return HadoopAuthenticator.getHadoopAuthenticator(
                    AuthenticationConfig.getSimpleAuthenticationConfig(buildHmsConf(hms)));
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
    }

    /** Resolves only the storage Kerberos identity used by Hudi metadata and file operations. */
    static HadoopAuthenticator buildPluginAuthenticator(Map<String, String> properties) {
        if (!"kerberos".equalsIgnoreCase(properties.get("hadoop.security.authentication"))) {
            return null;
        }
        Configuration conf = new Configuration();
        conf.setClassLoader(HudiConnector.class.getClassLoader());
        properties.forEach(conf::set);
        return HadoopAuthenticator.getHadoopAuthenticator(conf);
    }

    /**
     * Builds a plain Hadoop {@link Configuration} from the catalog properties for the authenticator. A plain
     * {@code new Configuration()} (NOT {@code HiveConf}) is used deliberately: HiveConf static-init would drag
     * hadoop-mapreduce onto the unit-test classpath. The classloader is pinned to the plugin loader so the
     * child-first (plugin) copy of the auth classes is resolved. Mirrors {@code HiveConnector.buildHadoopConf}.
     */
    private static Configuration buildHmsConf(AbstractHmsMetaStoreProperties hms) {
        return HmsConfHelper.createHadoopConfWithResources(hms.getConfResources(), hms.toHiveConfOverrides(""));
    }

    @Override
    public void close() throws IOException {
        HmsClient c = hmsClient;
        if (c != null) {
            c.close();
            hmsClient = null;
        }
    }
}
