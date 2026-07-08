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

import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.scan.ConnectorScanPlanProvider;
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

    // Catalog property key gating the plugin-side Kerberos authenticator (value matches AuthType.KERBEROS).
    private static final String HADOOP_SECURITY_AUTHENTICATION = "hadoop.security.authentication";

    private final Map<String, String> properties;
    private final ConnectorContext context;
    private volatile HmsClient hmsClient;

    // Lazily-built plugin-side Kerberos authenticator (single-owner auth), null for a non-Kerberos catalog.
    // Its doAs acts on the PLUGIN's UserGroupInformation copy — the one this connector's ThriftHmsClient RPC
    // reads (hadoop + fe-kerberos bundled child-first in the hudi plugin zip) — not the app-loader copy the
    // FE-injected context would use. Mirrors HiveConnector: after the catalog flip the sibling shares the hms
    // gateway's context whose executeAuthenticated resolves to NOOP (SIMPLE), which would silently downgrade a
    // Kerberos HMS. A hudi sibling runs in its OWN classloader, so it must own its authenticator (sharing the
    // gateway's hive-loader authenticator would split the UGI copy across loaders).
    private volatile HadoopAuthenticator pluginAuth;
    private volatile boolean pluginAuthComputed;

    public HudiConnector(Map<String, String> properties, ConnectorContext context) {
        this.properties = Collections.unmodifiableMap(properties);
        this.context = context;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session) {
        return new HudiConnectorMetadata(getOrCreateClient(), properties);
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
        String metastoreUri = properties.get(HudiConnectorProperties.HIVE_METASTORE_URIS);
        if (metastoreUri == null || metastoreUri.isEmpty()) {
            metastoreUri = properties.get("uri");
        }
        if (metastoreUri == null || metastoreUri.isEmpty()) {
            throw new DorisConnectorException(
                    "HMS URI ('" + HudiConnectorProperties.HIVE_METASTORE_URIS + "') is required for Hudi connector");
        }

        int poolSize = HudiConnectorProperties.getInt(
                properties, HudiConnectorProperties.HMS_CLIENT_POOL_SIZE,
                HudiConnectorProperties.DEFAULT_HMS_CLIENT_POOL_SIZE);

        HmsClientConfig config = new HmsClientConfig(properties, poolSize);
        LOG.info("Creating Hudi connector HMS client for catalog='{}', uri={}, poolSize={}",
                context.getCatalogName(), config.getMetastoreUri(), poolSize);

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
     * fe-kerberos are bundled child-first in the hudi plugin). Returns {@code null} for a non-Kerberos catalog
     * so the FE-injected auth path is preserved unchanged. Construction is cheap — the keytab login is lazy in
     * {@code getUGI()} on the first {@code doAs}. Mirrors {@code HiveConnector.pluginAuthenticator}.
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
     * catalog. Byte-faithful mirror of {@code HiveConnector.buildPluginAuthenticator} — two Kerberos sources in
     * precedence order (mirroring the legacy {@code HMSBaseProperties.initHadoopAuthenticator}):
     * <ol>
     *   <li><b>Storage</b> Kerberos — the raw {@code hadoop.security.authentication=kerberos} passthrough (HDFS
     *       login). When storage is Kerberos this single login also carries the HMS metastore RPC (same UGI).</li>
     *   <li><b>HMS-metastore</b> Kerberos with non-Kerberos storage — a secured Hive Metastore whose data
     *       storage is simple (e.g. a Kerberized HMS over S3). The HMS client principal/keytab facts
     *       ({@link HmsMetaStoreProperties#kerberos()}, resolved through the shared metastore-spi parser) feed a
     *       {@link KerberosAuthenticationConfig}, so the {@code doAs} logs in the same client identity fe-core
     *       used.</li>
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
     * child-first (plugin) copy of the auth classes is resolved. Mirrors {@code HiveConnector.buildHadoopConf}.
     */
    private static Configuration buildHadoopConf(Map<String, String> properties) {
        Configuration conf = new Configuration();
        conf.setClassLoader(HudiConnector.class.getClassLoader());
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
