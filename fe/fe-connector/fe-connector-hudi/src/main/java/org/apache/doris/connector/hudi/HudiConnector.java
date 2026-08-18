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
import org.apache.doris.connector.hms.ThriftHmsClient;
import org.apache.doris.connector.metastore.HmsMetaStoreProperties;
import org.apache.doris.connector.metastore.spi.MetaStoreProviders;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.scan.ConnectorScanPlanProvider;
import org.apache.doris.kerberos.HadoopAuthenticator;
import org.apache.doris.kerberos.KerberosAuthSpec;
import org.apache.doris.kerberos.KerberosAuthenticationConfig;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

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
    // Deliberately NOT a HudiCatalogProperties field: it is a raw hadoop storage key that buildHadoopConf
    // below hands to the Configuration wholesale along with every other passthrough key, and what happens
    // here is a peek at it, not this connector interpreting a property of its own.
    private static final String HADOOP_SECURITY_AUTHENTICATION = "hadoop.security.authentication";

    // fs.s3a.impl.disable.cache and its per-scheme siblings, as the FE emits them.
    private static final Pattern FS_DISABLE_CACHE = Pattern.compile("fs\\..+\\.impl\\.disable\\.cache");

    private final HudiCatalogProperties props;
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

    // One UGI per distinct catalog configuration, which is what keys Hadoop's FileSystem cache to the
    // credentials that opened it. See fileSystemScope(). Static and never evicted on purpose: an entry is
    // one UGI, there is one per catalog, and dropping one would strand the filesystems cached under it -
    // a REFRESH CATALOG rebuilds this connector and must land on the SAME scope, not a fresh one.
    private static final ConcurrentHashMap<String, UserGroupInformation> FS_SCOPES = new ConcurrentHashMap<>();
    private volatile UserGroupInformation fsScope;
    private volatile boolean fsScopeComputed;

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
     * (Kerberos) — or the FE-injected {@code context.executeAuthenticated} when this is a non-Kerberos
     * catalog — restoring the previous TCCL in a {@code finally}. Mirrors the {@link #createClient()} auth
     * choice; the TCCL pin is added because — unlike the HMS thrift RPC ({@code ThriftHmsClient.doAs} pins the
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
                    HadoopAuthenticator auth = pluginAuthenticator();
                    if (auth != null) {
                        return auth.doAs(action::call);
                    }
                    UserGroupInformation scope = fileSystemScope();
                    if (scope != null) {
                        return scope.doAs((PrivilegedExceptionAction<T>) action::call);
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
     * Lazily builds and memoizes the plugin-side Kerberos authenticator that {@link #createClient()} wraps the
     * metastore RPC under, so the RPC uses the PLUGIN's own {@code UserGroupInformation} copy (hadoop +
     * fe-kerberos are bundled child-first in the hudi plugin). Returns {@code null} for a non-Kerberos catalog
     * so the FE-injected auth path is preserved unchanged. Construction is cheap — the keytab login is lazy in
     * {@code getUGI()} on the first {@code doAs}. Mirrors {@code HiveConnector.pluginAuthenticator}.
     */
    /**
     * The {@link UserGroupInformation} this catalog's metadata reads run under, so that Hadoop's
     * {@code FileSystem} cache can stay ON without letting two catalogs share one filesystem.
     *
     * <p>The problem this exists for: Hadoop's cache is keyed on (scheme, authority, ugi) and ignores
     * credentials, and every non-Kerberos catalog arrives under the SAME ugi,
     * {@code HadoopSimpleAuthenticator}'s {@code createRemoteUser(hadoop.username)}. Two catalogs on one
     * bucket would otherwise read through whichever S3AFileSystem was built first. Turning the cache off
     * per scheme is the FE's historical answer and is still what the HDFS family gets
     * ({@code fs.hdfs.impl.disable.cache=true} and its siblings; asserted by
     * {@code HdfsConfigBuilderTest}) - the S3 side has since moved to the credential fingerprint below
     * ({@code FsCacheKeys}) and no longer emits it.
     *
     * <p>Disabling the cache does stop that, and leaks instead. Every {@code FileSystem.get()} behind a
     * {@code HoodieTableMetaClient} then builds a new S3AFileSystem and a new AWS SDK client that nobody
     * closes; the filesystems are collected but each SDK client's scheduled executor is kept alive by its
     * own worker threads. Measured at about 62 threads per query, until the FE cannot start a thread:
     * {@code OutOfMemoryError: unable to create native thread}, and every session dies with it.
     *
     * <p>So the cache goes back on ({@code buildHadoopConf} clears the flag) and the separation it was
     * disabled for moves into the cache key: one UGI per catalog configuration. The UGI carries the name
     * the simple authenticator would have used - a second Subject for one user, not another user - so an
     * {@code hdfs://} warehouse still sees the identity it always saw. A Kerberos catalog never reaches
     * here: its own authenticator runs the doAs above with credentials this cannot reproduce, and that
     * UGI already partitions the cache per principal.
     */
    private UserGroupInformation fileSystemScope() {
        if (!fsScopeComputed) {
            synchronized (this) {
                if (!fsScopeComputed) {
                    fsScope = buildFileSystemScope();
                    fsScopeComputed = true;
                }
            }
        }
        return fsScope;
    }

    private UserGroupInformation buildFileSystemScope() {
        try {
            String userName = UserGroupInformation.getCurrentUser().getUserName();
            return FS_SCOPES.computeIfAbsent(fileSystemScopeKey(properties),
                    key -> UserGroupInformation.createRemoteUser(userName));
        } catch (Exception e) {
            LOG.warn("failed to derive a FileSystem scope for catalog '{}', keeping the shared one",
                    context.getCatalogName(), e);
            return null;
        }
    }

    /**
     * Turns Hadoop's {@code FileSystem} cache back on for a configuration the FE built with it off.
     * Without this every {@code FileSystem.get()} behind a metaClient returns a filesystem nobody closes;
     * with it, this catalog holds one. Safe for either auth path because both run under a UGI that is
     * stable for the catalog and unique to it - {@link #fileSystemScope()} for a non-Kerberos catalog, the
     * connector's own lazily-built authenticator for a Kerberos one - so the cache key separates catalogs
     * exactly where the flag was separating them.
     */
    static void enableFileSystemCache(Configuration conf) {
        List<String> disabled = new ArrayList<>();
        for (Map.Entry<String, String> entry : conf) {
            if (FS_DISABLE_CACHE.matcher(entry.getKey()).matches()) {
                disabled.add(entry.getKey());
            }
        }
        // Collected first: Configuration's iterator walks a live view, and set() during the walk trips it.
        disabled.forEach(key -> conf.set(key, "false"));
    }

    /**
     * Identifies a catalog configuration so {@link #FS_SCOPES} hands the same UGI to two connectors exactly
     * when they may share a filesystem. Digested rather than used directly because the properties hold
     * credentials and a map key is easy to print by accident.
     */
    static String fileSystemScopeKey(Map<String, String> properties) throws NoSuchAlgorithmException {
        StringBuilder canonical = new StringBuilder();
        new TreeMap<>(properties).forEach((k, v) -> canonical.append(k).append('=').append(v).append('\n'));
        byte[] digest = MessageDigest.getInstance("SHA-256")
                .digest(canonical.toString().getBytes(StandardCharsets.UTF_8));
        StringBuilder hex = new StringBuilder(digest.length * 2);
        for (byte b : digest) {
            hex.append(Character.forDigit((b >> 4) & 0xF, 16)).append(Character.forDigit(b & 0xF, 16));
        }
        return hex.toString();
    }

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
