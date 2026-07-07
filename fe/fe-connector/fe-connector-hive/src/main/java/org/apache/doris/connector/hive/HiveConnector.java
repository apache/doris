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

package org.apache.doris.connector.hive;

import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorCapability;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.api.write.ConnectorWritePlanProvider;
import org.apache.doris.connector.hms.HmsClient;
import org.apache.doris.connector.hms.HmsClientConfig;
import org.apache.doris.connector.hms.ThriftHmsClient;
import org.apache.doris.connector.metastore.HmsMetaStoreProperties;
import org.apache.doris.connector.metastore.spi.MetaStoreProviders;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.kerberos.HadoopAuthenticator;
import org.apache.doris.kerberos.KerberosAuthSpec;
import org.apache.doris.kerberos.KerberosAuthenticationConfig;

import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;

/**
 * Hive connector implementation. Manages the lifecycle of
 * an {@link HmsClient} for HMS operations and provides scan planning.
 */
public class HiveConnector implements Connector {

    private static final Logger LOG = LogManager.getLogger(HiveConnector.class);

    // Catalog property key gating the plugin-side Kerberos authenticator (value matches AuthType.KERBEROS).
    private static final String HADOOP_SECURITY_AUTHENTICATION = "hadoop.security.authentication";

    private final Map<String, String> properties;
    private final ConnectorContext context;
    private volatile HmsClient hmsClient;

    // Lazily-built plugin-side Kerberos authenticator (single-owner auth), null for a non-Kerberos catalog.
    // Its doAs acts on the PLUGIN's UserGroupInformation copy — the one the plugin's ThriftHmsClient RPC reads
    // (hadoop + fe-kerberos bundled child-first) — not the app-loader copy the FE-injected context would use.
    private volatile HadoopAuthenticator pluginAuth;
    private volatile boolean pluginAuthComputed;

    // Read-transaction manager for transactional (ACID) Hive scans. One per connector, keyed by query.
    // Plugin-owned and dormant until the read cutover wires its query-finish commit (see the manager).
    private final HiveReadTransactionManager readTxnManager = new HiveReadTransactionManager();

    public HiveConnector(Map<String, String> properties, ConnectorContext context) {
        this.properties = Collections.unmodifiableMap(properties);
        this.context = context;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session) {
        return new HiveConnectorMetadata(getOrCreateClient(), properties, context);
    }

    @Override
    public ConnectorScanPlanProvider getScanPlanProvider() {
        return new HiveScanPlanProvider(getOrCreateClient(), properties, readTxnManager);
    }

    @Override
    public ConnectorWritePlanProvider getWritePlanProvider() {
        return new HiveWritePlanProvider(getOrCreateClient(), properties, context);
    }

    @Override
    public Set<ConnectorCapability> getCapabilities() {
        // SUPPORTS_VIEW: legacy HMSExternalTable resolves isView() from the remote table's view text and hive
        // views are queryable/droppable/visible in SHOW TABLES. The generic plugin-driven path reproduces this
        // ONLY under this capability: PluginDrivenExternalTable.isView() then consults the connector's
        // viewExists, PluginDrivenExternalCatalog.dropTable routes a view DROP to dropView, and
        // listTableNamesFromRemote merges listViewNames into SHOW TABLES — hive returns an EMPTY listViewNames
        // (its listTableNames already includes views), so the merge is a no-op and each view is listed once
        // (legacy parity). Inert until hms enters SPI_READY_TYPES. Other connector-wide capabilities
        // (auto-analyze, show-create-ddl, metadata-preload) land in their own substeps; SUPPORTS_MVCC_SNAPSHOT
        // is intentionally withheld (hive is non-MVCC), and Top-N / nested-prune are per-table markers, not
        // connector-wide flags.
        return EnumSet.of(ConnectorCapability.SUPPORTS_VIEW);
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
        String metastoreUri = properties.get(HiveConnectorProperties.HIVE_METASTORE_URIS);
        if (metastoreUri == null || metastoreUri.isEmpty()) {
            // Also check the "uri" short form
            metastoreUri = properties.get("uri");
        }
        if (metastoreUri == null || metastoreUri.isEmpty()) {
            throw new DorisConnectorException(
                    "HMS URI ('" + HiveConnectorProperties.HIVE_METASTORE_URIS + "') is required");
        }

        int poolSize = HiveConnectorProperties.getInt(
                properties, HiveConnectorProperties.HMS_CLIENT_POOL_SIZE,
                HiveConnectorProperties.DEFAULT_HMS_CLIENT_POOL_SIZE);

        HmsClientConfig config = new HmsClientConfig(properties, poolSize);
        LOG.info("Creating Hive connector client for catalog='{}', uri={}, type={}, poolSize={}",
                context.getCatalogName(), config.getMetastoreUri(),
                config.getMetastoreType(), poolSize);

        // For a Kerberos catalog run the metastore RPC under the PLUGIN's UGI doAs (buildPluginAuthenticator),
        // NOT the FE-injected context: after the catalog flip that context resolves to NOOP (SIMPLE) auth, which
        // would silently downgrade a Kerberos HMS. AuthAction.execute is a generic method (<T> T execute(...)),
        // so it cannot be a lambda — use an anonymous class. ThriftHmsClient.doAs already pins the RPC's TCCL to
        // the system classloader; the plugin's HadoopAuthenticator only wraps it in a UGI doAs (no TCCL change).
        HadoopAuthenticator auth = pluginAuthenticator();
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
        return new ThriftHmsClient(config, authAction);
    }

    /**
     * Lazily builds and memoizes the plugin-side Kerberos authenticator that {@link #createClient()} wraps the
     * metastore RPC under, so the RPC uses the PLUGIN's own {@code UserGroupInformation} copy (hadoop +
     * fe-kerberos are bundled child-first in the hive plugin). Returns {@code null} for a non-Kerberos catalog
     * so the FE-injected auth path is preserved unchanged. Construction is cheap — the keytab login is lazy in
     * {@code getUGI()} on the first {@code doAs}.
     */
    private HadoopAuthenticator pluginAuthenticator() {
        if (!pluginAuthComputed) {
            synchronized (this) {
                if (!pluginAuthComputed) {
                    pluginAuth = buildPluginAuthenticator(properties);
                    pluginAuthComputed = true;
                }
            }
        }
        return pluginAuth;
    }

    /**
     * Resolves the plugin-side Kerberos authenticator for the catalog, or {@code null} for a non-Kerberos
     * catalog. Two Kerberos sources are covered, in precedence order (mirroring the legacy
     * {@code HMSBaseProperties.initHadoopAuthenticator}):
     * <ol>
     *   <li><b>Storage</b> Kerberos — the raw {@code hadoop.security.authentication=kerberos} passthrough
     *       (HDFS login), built from the catalog Hadoop configuration. When storage is Kerberos this single
     *       login also carries the HMS metastore RPC (same UGI).</li>
     *   <li><b>HMS-metastore</b> Kerberos with non-Kerberos storage — a secured Hive Metastore whose data
     *       storage is simple (e.g. a Kerberized HMS over S3). The HMS client principal/keytab facts
     *       ({@link HmsMetaStoreProperties#kerberos()}, resolved through the shared metastore-spi parser) feed a
     *       {@link KerberosAuthenticationConfig}, so the {@code doAs} logs in the same client identity fe-core
     *       used. The HMS <em>service</em> principal / SASL settings ride the catalog's own HiveConf, not the
     *       login.</li>
     * </ol>
     * Package-visible + static for KDC-free unit testing.
     */
    static HadoopAuthenticator buildPluginAuthenticator(Map<String, String> properties) {
        if ("kerberos".equalsIgnoreCase(properties.get(HADOOP_SECURITY_AUTHENTICATION))) {
            return HadoopAuthenticator.getHadoopAuthenticator(buildHadoopConf(properties));
        }
        HmsMetaStoreProperties hms = (HmsMetaStoreProperties) MetaStoreProviders.bindForType(
                HmsClientConfig.METASTORE_TYPE_HMS, properties, Collections.emptyMap());
        Optional<KerberosAuthSpec> spec = hms.kerberos();
        if (spec.isPresent() && spec.get().hasCredentials()) {
            Configuration conf = buildHadoopConf(properties);
            conf.set("hadoop.security.authentication", "kerberos");
            conf.set("hive.metastore.sasl.enabled", "true");
            return HadoopAuthenticator.getHadoopAuthenticator(
                    new KerberosAuthenticationConfig(spec.get().getPrincipal(), spec.get().getKeytab(), conf));
        }
        return null;
    }

    /**
     * Builds a plain Hadoop {@link Configuration} from the catalog properties for the authenticator. A plain
     * {@code new Configuration()} (NOT {@code HiveConf}) is used deliberately: HiveConf static-init would drag
     * hadoop-mapreduce onto the unit-test classpath. The classloader is pinned to the plugin loader so the
     * child-first (plugin) copy of the auth classes is resolved.
     */
    private static Configuration buildHadoopConf(Map<String, String> properties) {
        Configuration conf = new Configuration();
        conf.setClassLoader(HiveConnector.class.getClassLoader());
        properties.forEach(conf::set);
        return conf;
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
