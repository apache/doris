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

package org.apache.doris.connector.iceberg;

import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorCapability;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorTestResult;
import org.apache.doris.connector.api.ConnectorValidationContext;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.procedure.ConnectorProcedureOps;
import org.apache.doris.connector.api.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.api.write.ConnectorWritePlanProvider;
import org.apache.doris.connector.metastore.HmsMetaStoreProperties;
import org.apache.doris.connector.metastore.spi.JdbcDriverSupport;
import org.apache.doris.connector.metastore.spi.MetaStoreProviders;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.filesystem.properties.S3CompatibleFileSystemProperties;
import org.apache.doris.filesystem.properties.StorageProperties;
import org.apache.doris.kerberos.HadoopAuthenticator;
import org.apache.doris.kerberos.KerberosAuthSpec;
import org.apache.doris.kerberos.KerberosAuthenticationConfig;
import org.apache.doris.thrift.TStorageBackendType;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.BaseViewSessionCatalog;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.RESTSessionCatalog;
import org.apache.iceberg.util.ThreadPools;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3tables.S3TablesClient;
import software.amazon.awssdk.services.s3tables.S3TablesClientBuilder;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.s3tables.iceberg.S3TablesCatalog;
import software.amazon.s3tables.iceberg.S3TablesProperties;
import software.amazon.s3tables.iceberg.imports.HttpClientProperties;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Iceberg connector implementation. Manages the lifecycle of an Iceberg SDK
 * {@link Catalog} instance for all metadata operations.
 *
 * <p>Supports all Iceberg catalog backends: REST, HMS, Glue, JDBC,
 * Hadoop, and S3Tables. The backend is determined by the {@code iceberg.catalog.type}
 * property. The per-flavor catalog-property assembly lives in the pure
 * {@link IcebergCatalogFactory} (mirroring {@code PaimonCatalogFactory}); this class drives the
 * live catalog creation: it resolves the chosen storage + Hadoop {@code Configuration} / {@code HiveConf}
 * sinks, registers the JDBC driver, and wraps {@code CatalogUtil.buildIcebergCatalog} in the FE-injected
 * authentication context with the thread-context classloader pinned to the plugin loader.</p>
 *
 * <p>Phase 1 provides read-only metadata operations (list databases, list tables,
 * get schema). Write operations, scan planning, actions (compaction, snapshot
 * management), and transaction support remain in fe-core temporarily. {@code s3tables} uses its bespoke
 * 3-arg {@code S3TablesCatalog.initialize(name, opts, client)} path (P6-T06).</p>
 */
public class IcebergConnector implements Connector {

    private static final Logger LOG = LogManager.getLogger(IcebergConnector.class);

    /**
     * Caches {@link ClassLoader}s keyed by resolved driver URL so a given JDBC driver jar is loaded at
     * most once across catalogs, and tracks the (url#class) keys already registered with the
     * {@link java.sql.DriverManager}. Ported verbatim from the legacy
     * {@code IcebergJdbcMetaStoreProperties} (mirrors {@code PaimonConnector}).
     */
    private static final Map<URL, ClassLoader> DRIVER_CLASS_LOADER_CACHE = new ConcurrentHashMap<>();
    private static final Set<String> REGISTERED_DRIVER_KEYS = ConcurrentHashMap.newKeySet();

    // Guards the one-per-JVM pinning of iceberg's shared worker-pool threads to the plugin classloader (see
    // pinIcebergWorkerPoolToPluginClassLoader). The iceberg connector provider is loaded once, so every iceberg
    // catalog shares the single ThreadPools.getWorkerPool(); pinning it once covers them all.
    private static final AtomicBoolean ICEBERG_WORKER_POOL_PINNED = new AtomicBoolean(false);

    // T08 latest-snapshot cache knobs (mirror PaimonConnector). The TTL pins a STABLE snapshot across queries;
    // <= 0 means "no-cache catalog" (always live). Defaults mirror the legacy iceberg table cache
    // (Config.external_cache_expire_time_seconds_after_access = 24h, Config.max_external_table_cache_num).
    static final String TABLE_CACHE_TTL_SECOND = "meta.cache.iceberg.table.ttl-second";
    static final long DEFAULT_TABLE_CACHE_TTL_SECOND = 86400L;
    static final int DEFAULT_TABLE_CACHE_CAPACITY = 1000;

    // Doris storage property keys (mirror StorageProperties without a fe-core dependency).
    private static final String S3_ACCESS_KEY = "s3.access_key";
    private static final String S3_SECRET_KEY = "s3.secret_key";
    private static final String S3_ENDPOINT = "s3.endpoint";
    private static final String S3_REGION = "s3.region";
    // Catalog property key gating the plugin-side Kerberos authenticator (value matches AuthType.KERBEROS).
    private static final String HADOOP_SECURITY_AUTHENTICATION = "hadoop.security.authentication";
    // Polaris REST catalog exposes its object-store base location under this key when the
    // "warehouse" property is a catalog name rather than an s3:// location.
    private static final String REST_DEFAULT_BASE_LOCATION = "default-base-location";

    /** The key BE looks up (case-sensitively) to learn which location to probe. */
    private static final String BE_TEST_LOCATION = "test_location";

    private final Map<String, String> properties;
    private final ConnectorContext context;
    private volatile Catalog icebergCatalog;
    // Session-aware REST catalog, built for every REST catalog (plain or iceberg.rest.session=user). A SINGLE
    // shared instance, held as the ReauthenticatingRestSessionCatalog wrapper (BaseViewSessionCatalog) that
    // recovers from a 401 on the catalog's own identity by rebuilding the client (upstream #64966). For
    // session=user the adapter attaches the querying user's delegated credential per request (#63068). Null for
    // every non-REST catalog; sessionCatalogAdapter is null unless session=user.
    private volatile BaseViewSessionCatalog restSessionCatalog;
    private volatile IcebergSessionCatalogAdapter sessionCatalogAdapter;
    // T08 connector-internal caches (D6, 0 SPI). Final per-catalog fields: a REFRESH CATALOG rebuilds the
    // connector (PluginDrivenExternalCatalog.onClose nulls + recreates it) and thus drops both caches. The
    // manifest cache is path-keyed, no-TTL, capacity-bounded; it is consumed only when
    // meta.cache.iceberg.manifest.enable is set (default off → scan uses the SDK planFiles path).
    private final IcebergLatestSnapshotCache latestSnapshotCache;
    // PERF-01: cross-query cache of the RAW iceberg Table (restores the legacy IcebergExternalMetaCache table
    // cache that the SPI cutover dropped). null when the catalog's credentials are query-dependent
    // (iceberg.rest.session=user / REST vended-credentials) — see the constructor. The per-statement scope
    // (ConnectorStatementScope) shares one loaded table across a statement's read/scan/write regardless of this
    // field, and is what a credential-gated catalog (this field null) relies on within a statement.
    private final IcebergTableCache tableCache;
    // PERF-02: cross-query partition-view cache (the raw PARTITIONS-scan result, keyed by (table, snapshotId)).
    // Built unconditionally — the cached value is pure metadata with no FileIO/credential, so no credential gate
    // (unlike tableCache); only the TTL knob disables it.
    private final IcebergPartitionCache partitionCache;
    // PERF-03: cross-query inferred-file-format cache (the whole-table planFiles() fallback result, keyed by
    // (table, snapshotId)). Like partitionCache, the value is pure metadata (a format-name string) with no
    // FileIO/credential, so no gate; only the TTL knob disables it.
    private final IcebergFormatCache formatCache;
    // PERF-05: cross-query table-comment cache (value = the 'comment' property string). Built ONLY for a REST
    // vended-credentials catalog that is NOT session=user -- plain catalogs already reuse tableCache (PERF-01) for
    // the comment path, and session=user must stay live because the loadTable itself carries per-user
    // authorization a shared cache would bypass. null for every other flavor.
    private final IcebergCommentCache commentCache;
    private final IcebergManifestCache manifestCache = new IcebergManifestCache();

    // Lazily-built plugin-side Kerberos authenticator (single-owner auth; see TcclPinningConnectorContext).
    // null for a non-Kerberos catalog. Its doAs acts on the PLUGIN's UserGroupInformation copy — the one the
    // plugin's HDFS FileSystem reads — not the app-loader copy the FE-injected authenticator logs in.
    private volatile HadoopAuthenticator pluginAuth;
    private volatile boolean pluginAuthComputed;

    public IcebergConnector(Map<String, String> properties, ConnectorContext context) {
        this.properties = Collections.unmodifiableMap(properties);
        // Pin the thread-context classloader to the plugin loader for the duration of every
        // executeAuthenticated call (see TcclPinningConnectorContext). The injected context is fanned out to
        // the metadata / transaction / procedure ops below; wrapping it once here is what extends the
        // "TCCL pinned to the plugin loader" guard (already applied on the scan + catalog-build paths) to the
        // write/DDL/procedure commits, whose lazy iceberg-aws S3-client build otherwise ClassCasts
        // ApacheHttpClientConfigurations across the app/child loader split. For a Kerberos catalog it ALSO runs
        // each op under a plugin-side UGI doAs (pluginAuthenticator): the plugin's FileSystem reads the plugin's
        // own UserGroupInformation copy (hadoop bundled child-first), which the FE-injected app-side
        // authenticator never logs in — so without this the DDL/read hits secured HDFS as SIMPLE auth.
        this.context = new TcclPinningConnectorContext(context, getClass().getClassLoader(),
                this::pluginAuthenticator);
        this.latestSnapshotCache = new IcebergLatestSnapshotCache(
                resolveTableCacheTtlSecond(this.properties), DEFAULT_TABLE_CACHE_CAPACITY);
        // PERF-01 cross-query RAW-table cache. Disabled (null) when the catalog's credentials are
        // query-dependent, because a cached raw Table carries its FileIO's credentials:
        //   - iceberg.rest.session=user: per-user delegated FileIO -> sharing across users leaks credentials.
        //   - REST vended-credentials: the FileIO carries a server-vended token that expires within ~an hour;
        //     iceberg keeps it fresh by reloading the table each query, so a 24h-TTL hit would hand BE an
        //     expired token (403 mid-scan). Both gates are independent; either one disables this layer.
        // The query-scoped fat handle stays on in all cases (its token is fresh within the one query). Same
        // TTL/capacity as the snapshot cache (the single meta.cache.iceberg.table.ttl-second knob).
        this.tableCache = (isUserSessionEnabled()
                || IcebergScanPlanProvider.restVendedCredentialsEnabled(this.properties))
                ? null
                : new IcebergTableCache(
                        resolveTableCacheTtlSecond(this.properties), DEFAULT_TABLE_CACHE_CAPACITY);
        // PERF-02: partition-view cache. Pure metadata (no credentials) -> no gate; same TTL/capacity as above.
        this.partitionCache = new IcebergPartitionCache(
                resolveTableCacheTtlSecond(this.properties), DEFAULT_TABLE_CACHE_CAPACITY);
        // PERF-03: inferred-file-format cache. Pure metadata (no credentials) -> no gate; same TTL/capacity.
        this.formatCache = new IcebergFormatCache(
                resolveTableCacheTtlSecond(this.properties), DEFAULT_TABLE_CACHE_CAPACITY);
        // PERF-05: table-comment cache, built ONLY for a REST vended-credentials catalog that is NOT session=user.
        // Plain catalogs (tableCache on) already serve the comment path from tableCache; session=user is excluded
        // because a shared comment cache would bypass the per-user loadTable authorization (a metadata disclosure).
        // Comment is pure metadata (no credential) so no gate on the VALUE -- the flavor gate is an authorization
        // decision, not a credential-leak one. Same TTL/capacity (ttl<=0 still disables internally).
        this.commentCache = (IcebergScanPlanProvider.restVendedCredentialsEnabled(this.properties)
                && !isUserSessionEnabled())
                ? new IcebergCommentCache(
                        resolveTableCacheTtlSecond(this.properties), DEFAULT_TABLE_CACHE_CAPACITY)
                : null;
    }

    /**
     * Resolves {@code meta.cache.iceberg.table.ttl-second} (default 24h); a blank/unparseable value falls back
     * to the default rather than failing catalog creation (best-effort, mirrors PaimonConnector). A value
     * {@code <= 0} disables caching (the no-cache catalog reads the latest snapshot live every query).
     */
    static long resolveTableCacheTtlSecond(Map<String, String> properties) {
        String raw = properties.get(TABLE_CACHE_TTL_SECOND);
        if (StringUtils.isBlank(raw)) {
            return DEFAULT_TABLE_CACHE_TTL_SECOND;
        }
        try {
            return Long.parseLong(raw.trim());
        } catch (NumberFormatException e) {
            LOG.warn("Invalid {}='{}', falling back to default {}s", TABLE_CACHE_TTL_SECOND, raw,
                    DEFAULT_TABLE_CACHE_TTL_SECOND);
            return DEFAULT_TABLE_CACHE_TTL_SECOND;
        }
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session) {
        return new IcebergConnectorMetadata(newCatalogBackedOps(session), properties, context,
                latestSnapshotCache, tableCache, partitionCache, commentCache);
    }

    /**
     * True for a handle this connector produced (an {@link IcebergTableHandle}). Tested against this connector's
     * OWN in-loader type, so a heterogeneous hms gateway that embeds this connector as a sibling can route a
     * foreign iceberg handle here without casting it across the plugin classloader split. Returns false for any
     * other connector's handle (e.g. a hudi sibling's), so the gateway keeps looking.
     */
    @Override
    public boolean ownsHandle(ConnectorTableHandle handle) {
        return handle instanceof IcebergTableHandle;
    }

    /**
     * Eagerly validates connectivity during CREATE CATALOG (when {@code test_connection=true}).
     * Runs two probes so bad configurations fail fast instead of at first query:
     * <ul>
     *   <li><b>Metastore</b> (any {@code iceberg.catalog.type}): lists namespaces, forcing a real
     *       round-trip that validates the URI, auth (OAuth2/SigV4, Kerberos) and warehouse config.</li>
     *   <li><b>Storage</b>: HEADs the warehouse location with the user-declared S3 credentials,
     *       mirroring fe-core's S3ConnectivityTester so wrong keys are rejected up front.</li>
     * </ul>
     */
    @Override
    public ConnectorTestResult testConnection(ConnectorSession session) {
        String catalogType = properties.getOrDefault(
                IcebergConnectorProperties.ICEBERG_CATALOG_TYPE, "");

        // -- Metastore probe (REST, HMS, Glue, S3Tables) --
        // Listing databases forces a real round-trip that validates the URI, auth and warehouse config.
        // This used to be REST-only here, which silently dropped the HMS/Glue/S3Tables coverage the legacy
        // fe-core Iceberg{HMS,Glue,S3Tables}ConnectivityTester family provided.
        if (probesMetastore(catalogType)) {
            try {
                getMetadata(session).listDatabaseNames(session);
            } catch (Exception e) {
                LOG.warn("Iceberg metastore connectivity test failed for catalog '{}'",
                        context.getCatalogName(), e);
                return ConnectorTestResult.failure(metaFailureMessage(catalogType, e));
            }
        }

        // -- Storage probe (only when the user supplied S3 credentials) --
        ConnectorTestResult storageResult = probeStorage(catalogType);
        if (storageResult != null) {
            return storageResult;
        }
        return ConnectorTestResult.success();
    }

    /**
     * Probes the object store with the user-declared S3 credentials. Returns a failure result if
     * the store is unreachable or the credentials are rejected, or {@code null} when the check
     * passes or is not applicable (no S3 credentials, or no resolvable s3:// location).
     */
    private ConnectorTestResult probeStorage(String catalogType) {
        String accessKey = properties.get(S3_ACCESS_KEY);
        String endpoint = properties.get(S3_ENDPOINT);
        if (isBlank(accessKey) || isBlank(endpoint)) {
            // No S3 credentials supplied: nothing to probe.
            return null;
        }
        String location = resolveS3TestLocation(catalogType);
        if (location == null) {
            // Could not determine an s3:// location to probe (e.g. a non-REST catalog whose
            // warehouse is not an s3:// path). Skip rather than fail a check we cannot perform.
            LOG.info("Skipping Iceberg storage connectivity probe for catalog '{}': "
                    + "no s3:// warehouse location resolved", context.getCatalogName());
            return null;
        }

        // Map Doris s3.* keys to Iceberg S3FileIO keys and force static credentials (disable
        // remote/vended signing) so the probe validates exactly what the user configured.
        Map<String, String> ioProps = new HashMap<>();
        ioProps.put("s3.endpoint", endpoint);
        ioProps.put("s3.access-key-id", accessKey);
        ioProps.put("s3.secret-access-key", properties.getOrDefault(S3_SECRET_KEY, ""));
        ioProps.put("s3.path-style-access", "true");
        ioProps.put("s3.remote-signing-enabled", "false");
        String region = properties.get(S3_REGION);
        if (!isBlank(region)) {
            ioProps.put("client.region", region);
        }

        // Load S3FileIO reflectively via CatalogUtil so this module needs no compile-time AWS SDK
        // dependency; the AWS SDK is resolved from the shared runtime classpath at execution time.
        // Pin the TCCL to the plugin loader for the probe: iceberg-aws builds its S3 client lazily and
        // resolves org.apache.iceberg.aws.ApacheHttpClientConfigurations via DynMethods (default loader =
        // TCCL). This CREATE-CATALOG thread runs under the default 'app' TCCL, which would return the
        // fe-core copy of the class and ClassCast against the child-loaded plugin copy the rest of the
        // iceberg-aws stack uses — the SAME split-brain TcclPinningConnectorContext guards on the commit
        // path. Unlike the metastore probe (which routes through executeAuthenticated), this path is not
        // pinned by the context, so it must pin here.
        FileIO io = null;
        ClassLoader previousTccl = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
            io = CatalogUtil.loadFileIO("org.apache.iceberg.aws.s3.S3FileIO", ioProps, null);
            // exists() issues a HEAD: a 404 (missing object) returns false and is fine, but
            // endpoint/credential failures (e.g. 403) throw — which is what we want to catch.
            io.newInputFile(location).exists();
        } catch (Exception e) {
            LOG.warn("Iceberg storage connectivity test failed for catalog '{}'",
                    context.getCatalogName(), e);
            return ConnectorTestResult.failure(storageFailureMessage(e));
        } finally {
            Thread.currentThread().setContextClassLoader(previousTccl);
            if (io != null) {
                try {
                    io.close();
                } catch (Exception ignored) {
                    // best-effort cleanup
                }
            }
        }

        // FE can reach the warehouse — now make sure BE can too. FE and BE routinely sit on different
        // networks, so a warehouse that only FE reaches would pass CREATE CATALOG and then fail every
        // scan. The engine owns the round-trip (it needs the backend registry); we only hand it the
        // BE-facing credentials and the location to try.
        return probeStorageFromBackend(location);
    }

    /**
     * Asks a backend to reach {@code location} with the catalog's BE-facing credentials. Returns a failure
     * result if the backend rejects it, or {@code null} when it passes (or when there is no backend to ask,
     * or the catalog has no static storage credentials to send — e.g. a REST catalog with vended ones).
     */
    ConnectorTestResult probeStorageFromBackend(String location) {
        Map<String, String> backendProps = new HashMap<>(context.getBackendStorageProperties());
        if (backendProps.isEmpty()) {
            return null;
        }
        // BE reads the bucket out of this key and dereferences it unconditionally — never omit it.
        backendProps.put(BE_TEST_LOCATION, location);
        try {
            context.testBackendStorageConnectivity(TStorageBackendType.S3.getValue(), backendProps);
            return null;
        } catch (Exception e) {
            LOG.warn("Iceberg storage connectivity test failed on the compute node for catalog '{}'",
                    context.getCatalogName(), e);
            return ConnectorTestResult.failure(storageBackendFailureMessage(e));
        }
    }

    /**
     * Resolves an {@code s3://} location to probe. Prefers an explicit S3 {@code warehouse} in the
     * catalog properties; for REST catalogs falls back to the server-merged warehouse location
     * (Iceberg {@code warehouse} or Polaris {@code default-base-location}).
     */
    private String resolveS3TestLocation(String catalogType) {
        String location = toS3Location(properties.get(IcebergConnectorProperties.WAREHOUSE));
        if (location != null) {
            return location;
        }
        if (IcebergConnectorProperties.TYPE_REST.equalsIgnoreCase(catalogType)) {
            Catalog catalog = getOrCreateCatalog();
            // The server-merged config (Iceberg warehouse / Polaris default-base-location) lives on the REST
            // session catalog we now hold (the wrapped RESTSessionCatalog; its properties() delegates to the raw
            // one, exactly what RESTCatalog.properties() returned). Fall back to a bare RESTCatalog defensively.
            Map<String, String> merged = null;
            BaseViewSessionCatalog sc = restSessionCatalog;
            if (sc != null) {
                merged = sc.properties();
            } else if (catalog instanceof RESTCatalog) {
                merged = ((RESTCatalog) catalog).properties();
            }
            if (merged != null) {
                location = toS3Location(merged.get(CatalogProperties.WAREHOUSE_LOCATION));
                if (location == null) {
                    location = toS3Location(merged.get(REST_DEFAULT_BASE_LOCATION));
                }
            }
        }
        return location;
    }

    /**
     * Builds the metastore-connectivity failure message. The wording deliberately contains both
     * the catalog-type tag (e.g. {@code "Iceberg REST"}) and the phrase
     * {@code "connectivity test failed"} so CREATE CATALOG surfaces a stable, actionable error.
     */
    /**
     * True for the catalog types that talk to a remote metastore, which is exactly the set the legacy
     * fe-core coordinator built a {@code MetaConnectivityTester} for (Iceberg HMS / Glue / REST / S3Tables).
     * Filesystem-backed catalogs ({@code hadoop}, and an unset type) had no tester there and get none here:
     * their "metastore" is the warehouse itself, which the storage probe below covers.
     */
    static boolean probesMetastore(String catalogType) {
        return IcebergConnectorProperties.TYPE_REST.equalsIgnoreCase(catalogType)
                || IcebergConnectorProperties.TYPE_HMS.equalsIgnoreCase(catalogType)
                || IcebergConnectorProperties.TYPE_GLUE.equalsIgnoreCase(catalogType)
                || IcebergConnectorProperties.TYPE_S3_TABLES.equalsIgnoreCase(catalogType);
    }

    static String metaFailureMessage(String catalogType, Throwable cause) {
        String tag = isBlank(catalogType) ? "Iceberg" : "Iceberg " + catalogType.toUpperCase(Locale.ROOT);
        return tag + " connectivity test failed: " + rootCauseMessage(cause);
    }

    static String storageFailureMessage(Throwable cause) {
        return "Storage connectivity test failed: " + rootCauseMessage(cause);
    }

    /**
     * The compute-node variant. The {@code (compute node)} tag is what tells an operator the warehouse is
     * fine from FE and the problem is BE-side (a different network or credential set) — the legacy fe-core
     * coordinator drew the same distinction.
     */
    static String storageBackendFailureMessage(Throwable cause) {
        return "Storage connectivity test failed (compute node): " + rootCauseMessage(cause);
    }

    /** Normalizes and returns {@code value} if it is an s3/s3a/s3n URI, otherwise {@code null}. */
    static String toS3Location(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        if (trimmed.matches("^(s3|s3a|s3n)://.+")) {
            return trimmed.replaceFirst("^s3[an]://", "s3://");
        }
        return null;
    }

    /** Returns the message of the deepest cause, falling back to its simple class name. */
    static String rootCauseMessage(Throwable t) {
        Throwable root = t;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        String msg = root.getMessage();
        return msg != null ? msg : root.getClass().getSimpleName();
    }

    private static boolean isBlank(String s) {
        return s == null || s.trim().isEmpty();
    }

    /**
     * Build the {@link IcebergCatalogOps} seam over the lazily-built live catalog, threading the listing-parity
     * gating mirrored from legacy {@code IcebergMetadataOps} (a single per-catalog ops that carried this for ALL
     * of metadata/scan/write/procedure): nested-namespace recursion is REST-only and flag-gated; view filtering
     * is REST-flag-gated; a configured {@code external_catalog.name} roots namespaces (REST 3-level
     * {@code <db>.<table>} living under {@code [<db>, <cat>]}). ALL FOUR call sites (getMetadata + the three
     * provider getters) share this so they resolve namespaces identically — in particular {@code loadTable} (the
     * only seam method scan/write/procedure use) must honour {@code external_catalog.name} or 3-level REST
     * catalogs resolve to the wrong namespace.
     */
    private IcebergCatalogOps newCatalogBackedOps() {
        String flavor = IcebergCatalogFactory.resolveFlavor(properties);
        boolean restFlavor = IcebergConnectorProperties.TYPE_REST.equals(flavor);
        boolean nestedNamespaceEnabled = Boolean.parseBoolean(properties.getOrDefault(
                IcebergConnectorProperties.REST_NESTED_NAMESPACE_ENABLED, "false"));
        boolean viewEnabled = Boolean.parseBoolean(properties.getOrDefault(
                IcebergConnectorProperties.REST_VIEW_ENABLED, "true"));
        Optional<String> externalCatalogName =
                Optional.ofNullable(properties.get(IcebergConnectorProperties.EXTERNAL_CATALOG_NAME));
        Catalog sharedCatalog = getOrCreateCatalog();
        // Plain REST now holds the bare (wrapped) RESTSessionCatalog: its asCatalog(empty) is a Catalog +
        // SupportsNamespaces but NOT a ViewCatalog, so inject the view facet asViewCatalog(empty) explicitly (what
        // the all-in-one RESTCatalog used to carry inline). session=user routes views per-user elsewhere, so the
        // shared path keeps its existing behavior (no injection here).
        BaseViewSessionCatalog sc = restSessionCatalog;
        if (sc != null && !isUserSessionEnabled()) {
            ViewCatalog sharedViewCatalog = sc.asViewCatalog(SessionCatalog.SessionContext.createEmpty());
            return new IcebergCatalogOps.CatalogBackedIcebergCatalogOps(sharedCatalog, sharedViewCatalog,
                    restFlavor, nestedNamespaceEnabled, viewEnabled, externalCatalogName);
        }
        return new IcebergCatalogOps.CatalogBackedIcebergCatalogOps(sharedCatalog,
                restFlavor, nestedNamespaceEnabled, viewEnabled, externalCatalogName);
    }

    /**
     * Session-aware variant used by {@link #getMetadata}: for a {@code iceberg.rest.session=user} catalog it
     * routes through the per-request delegated catalog + view catalog (FAIL-CLOSED — a session that carries no
     * delegated credential is rejected by {@code IcebergSessionCatalogAdapter.delegatedCatalog}, never served a
     * shared identity), so metadata reads are authorized as the querying user. For every other catalog it is
     * identical to {@link #newCatalogBackedOps()} (the shared catalog). getMetadata is invoked per operation with
     * the current session, so resolving the per-user catalog here covers each metadata call (#63068 parity).
     */
    private IcebergCatalogOps newCatalogBackedOps(ConnectorSession session) {
        if (!isUserSessionEnabled()) {
            return newCatalogBackedOps();
        }
        getOrCreateCatalog();  // ensure the shared RESTSessionCatalog + adapter are built
        IcebergSessionCatalogAdapter adapter = sessionCatalogAdapter;
        Catalog perUserCatalog = adapter.delegatedCatalog(session);
        ViewCatalog perUserViewCatalog = adapter.delegatedViewCatalog(session);
        boolean nestedNamespaceEnabled = Boolean.parseBoolean(properties.getOrDefault(
                IcebergConnectorProperties.REST_NESTED_NAMESPACE_ENABLED, "false"));
        boolean viewEnabled = Boolean.parseBoolean(properties.getOrDefault(
                IcebergConnectorProperties.REST_VIEW_ENABLED, "true"));
        Optional<String> externalCatalogName =
                Optional.ofNullable(properties.get(IcebergConnectorProperties.EXTERNAL_CATALOG_NAME));
        // restFlavor is unconditionally true here (isUserSessionEnabled() ⇒ a REST catalog).
        return new IcebergCatalogOps.CatalogBackedIcebergCatalogOps(perUserCatalog, perUserViewCatalog,
                true, nestedNamespaceEnabled, viewEnabled, externalCatalogName);
    }

    /**
     * REFRESH TABLE hook: drop the cached latest snapshot for one table so the next query re-pins live
     * (mirrors PaimonConnector). The names are the REMOTE db/table (RefreshManager passes remote names, the
     * same form {@link IcebergConnectorMetadata#beginQuerySnapshot} keys on). The manifest cache is path-keyed
     * and intentionally NOT cleared here (legacy IcebergExternalMetaCache parity — db/table invalidation keeps
     * manifest entries; only a REFRESH CATALOG, i.e. connector rebuild, drops them).
     */
    @Override
    public void invalidateTable(String dbName, String tableName) {
        latestSnapshotCache.invalidate(TableIdentifier.of(dbName, tableName));
        if (tableCache != null) {
            tableCache.invalidate(TableIdentifier.of(dbName, tableName));
        }
        partitionCache.invalidate(TableIdentifier.of(dbName, tableName));
        formatCache.invalidate(TableIdentifier.of(dbName, tableName));
        if (commentCache != null) {
            commentCache.invalidate(TableIdentifier.of(dbName, tableName));
        }
    }

    /**
     * REFRESH DATABASE hook (also reached by a Doris-issued {@code DROP DATABASE} via the generic
     * {@code PluginDrivenExternalCatalog} dropDb hook, and by the hive gateway's
     * {@code forEachBuiltSibling} for an iceberg-on-HMS sibling): drop the cached latest snapshot for
     * EVERY table in one database so the next query re-pins live. Db-scoped analogue of
     * {@link #invalidateTable}; the name is the REMOTE db name (RefreshManager / the dropDb hook pass
     * remote names). Without this override iceberg inherited the SPI no-op default, so REFRESH DATABASE
     * and DROP DATABASE (incl. its FORCE table cascade, which bypasses per-table invalidateTable) left
     * the snapshot pins stale up to the TTL. The path-keyed manifest cache is intentionally NOT cleared
     * (legacy parity — only REFRESH CATALOG drops manifests).
     */
    @Override
    public void invalidateDb(String dbName) {
        latestSnapshotCache.invalidateDb(dbName);
        if (tableCache != null) {
            tableCache.invalidateDb(dbName);
        }
        partitionCache.invalidateDb(dbName);
        formatCache.invalidateDb(dbName);
        if (commentCache != null) {
            commentCache.invalidateDb(dbName);
        }
    }

    /**
     * REFRESH CATALOG hook: drop ALL of this catalog's connector-owned caches. Clears both the latest-snapshot
     * cache and the (path-keyed) manifest cache — mirroring legacy {@code IcebergExternalMetaCache}'s
     * catalog-wide {@code group.invalidateAll()}, which dropped table (latest-snapshot projection) AND manifest
     * entries. Unlike {@link #invalidateTable} (REFRESH TABLE, which keeps manifest entries), the catalog-level
     * invalidation flushes manifests too.
     */
    @Override
    public void invalidateAll() {
        latestSnapshotCache.invalidateAll();
        if (tableCache != null) {
            tableCache.invalidateAll();
        }
        partitionCache.invalidateAll();
        formatCache.invalidateAll();
        if (commentCache != null) {
            commentCache.invalidateAll();
        }
        manifestCache.invalidateAll();
    }

    /**
     * Restore the legacy single-knob semantics: {@code meta.cache.iceberg.table.ttl-second} also governs the FE
     * schema cache (the SPI routes iceberg schema to the generic schema cache keyed by
     * {@code schema.cache.ttl-second}), so a no-cache catalog ({@code ttl-second=0}) serves FRESH schema after
     * external DDL (mirrors {@code PaimonConnector.schemaCacheTtlSecondOverride}). Absent -> no override (engine
     * default TTL). Do NOT reuse {@link #resolveTableCacheTtlSecond}, which substitutes the 24h default for a
     * blank value and would defeat the engine default.
     */
    @Override
    public OptionalLong schemaCacheTtlSecondOverride() {
        String raw = properties.get(TABLE_CACHE_TTL_SECOND);
        if (raw == null || raw.trim().isEmpty()) {
            return OptionalLong.empty();
        }
        try {
            return OptionalLong.of(Long.parseLong(raw.trim()));
        } catch (NumberFormatException e) {
            return OptionalLong.empty();
        }
    }

    /** Test-only: the manifest cache, so cache tests can assert REFRESH CATALOG ({@link #invalidateAll}) drops it. */
    IcebergManifestCache manifestCacheForTest() {
        return manifestCache;
    }

    /** Test-only: the cross-query table cache, or {@code null} when disabled by the credential gate (PERF-01). */
    IcebergTableCache tableCacheForTest() {
        return tableCache;
    }

    /** Test-only: the cross-query partition-view cache (PERF-02; always built, no credential gate). */
    IcebergPartitionCache partitionCacheForTest() {
        return partitionCache;
    }

    /** Test-only: the cross-query inferred-file-format cache (PERF-03; always built, no credential gate). */
    IcebergFormatCache formatCacheForTest() {
        return formatCache;
    }

    /** Test-only: the table-comment cache (PERF-05), or {@code null} unless vended-credentials and non-session. */
    IcebergCommentCache commentCacheForTest() {
        return commentCache;
    }

    @Override
    public ConnectorScanPlanProvider getScanPlanProvider() {
        // Mirrors PaimonConnector.getScanPlanProvider: build a fresh provider per call over the lazily-built
        // live catalog. Scan planning resolves the table via catalogOps.loadTable, which honours
        // external_catalog.name (REST 3-level catalogs), so it must share getMetadata's fully-threaded ops
        // (newCatalogBackedOps) — the listing-only flags (nested-namespace / view) are inert on this path but
        // threaded for parity with the legacy single per-catalog IcebergMetadataOps.
        return new IcebergScanPlanProvider(properties,
                this::newCatalogBackedOps, context, manifestCache,
                tableCache, formatCache);
    }

    @Override
    public ConnectorWritePlanProvider getWritePlanProvider() {
        // Mirrors getScanPlanProvider: a fresh provider per call over the lazily-built live catalog. The
        // provider builds the TIcebergTableSink and binds the write to the executor-opened
        // IcebergConnectorTransaction. It resolves the target via catalogOps.loadTable, so it shares the
        // fully-threaded ops (newCatalogBackedOps) — external_catalog.name must apply to INSERT/DELETE/MERGE.
        return new IcebergWritePlanProvider(properties,
                this::newCatalogBackedOps, context);
    }

    @Override
    public ConnectorProcedureOps getProcedureOps() {
        // Mirrors getWritePlanProvider: a fresh provider per call over the lazily-built live catalog. The
        // provider loadTable()s the target and runs the procedure body (P6.4-T03/T04). It resolves the target
        // via catalogOps.loadTable, so it shares the fully-threaded ops (newCatalogBackedOps) —
        // external_catalog.name must apply to ALTER TABLE ... EXECUTE on REST 3-level catalogs.
        return new IcebergProcedureOps(properties,
                this::newCatalogBackedOps, context);
    }

    /**
     * Iceberg exposes point-in-time snapshots, so it declares {@code SUPPORTS_MVCC_SNAPSHOT} (the gate for the
     * generic {@code PluginDrivenMvccExternalTable}, which drives {@code beginQuerySnapshot}/{@code
     * resolveTimeTravel}/{@code applySnapshot}). Inert pre-cutover — the capability is consumed only on the
     * plugin-driven path, which iceberg does not use until it enters {@code SPI_READY_TYPES} (P6.6).
     */
    @Override
    public Set<ConnectorCapability> getCapabilities() {
        // SUPPORTS_COLUMN_AUTO_ANALYZE: legacy IcebergExternalTable is in the auto-analyze whitelist and is
        // forced to FULL analyze; the generic statistics collector reproduces both ONLY under this capability,
        // so post-cutover iceberg keeps background per-column stats (CBO quality). Inert pre-cutover (P6.6).
        // SUPPORTS_TOPN_LAZY_MATERIALIZE: legacy IcebergExternalTable.class is in MaterializeProbeVisitor's
        // supported set; the generic probe reproduces that ONLY under this capability, so post-cutover iceberg
        // keeps Top-N lazy materialization (query latency). The BE rowid plumbing is already generic. Inert
        // pre-cutover (P6.6).
        // SUPPORTS_SHOW_CREATE_DDL: legacy IcebergExternalTable rendered LOCATION + PROPERTIES + PARTITION BY
        // + ORDER BY in SHOW CREATE TABLE (and IcebergExternalDatabase rendered LOCATION in SHOW CREATE
        // DATABASE); the generic plugin-driven render arm reproduces that ONLY under this capability
        // (the connector pre-renders the partition/sort clauses under the show.* reserved keys, and getDatabase
        // surfaces the namespace location). Inert pre-cutover (P6.6).
        // SUPPORTS_VIEW: legacy IcebergExternalTable resolves isView() from catalog.viewExists and
        // IcebergExternalCatalog merges listViewNames back into SHOW TABLES; the generic plugin-driven path
        // reproduces both ONLY under this capability (PluginDrivenExternalTable.isView() consults the connector,
        // and listTableNamesFromRemote re-merges the connector's listViewNames), so post-cutover iceberg views
        // remain visible/queryable/droppable. Inert pre-cutover (P6.6).
        // SUPPORTS_NESTED_COLUMN_PRUNE: legacy IcebergExternalTable.class returns true from
        // LogicalFileScan.supportPruneNestedColumn (and SlotTypeReplacer rewrites the nested access path to
        // iceberg field-ids); the generic plugin-driven path reproduces both ONLY under this capability, so
        // post-cutover iceberg keeps reading just the accessed STRUCT/ARRAY/MAP sub-fields (read-amplification
        // avoidance). Correct only because the connector also carries per-field ids down its column tree
        // (parseSchema withUniqueId + IcebergTypeMapping withChildrenFieldIds), which the BE field-id scan
        // path matches nested leaves by; without them a nested leaf reads NULL. Inert pre-cutover (P6.6).
        // SUPPORTS_METADATA_PRELOAD: legacy IcebergExternalTable.supportsExternalMetadataPreload returns true so
        // the planner async pre-warms schema/snapshot before taking the read lock; the generic plugin-driven
        // path reproduces this ONLY under this capability (PluginDrivenExternalTable.supportsExternalMetadataPreload),
        // so post-cutover iceberg keeps async pre-load instead of degrading to synchronous bind-time load. Pure
        // lock-latency optimization, opt-in via enable_preload_external_metadata. Inert pre-cutover (P6.6).
        EnumSet<ConnectorCapability> capabilities = EnumSet.of(ConnectorCapability.SUPPORTS_MVCC_SNAPSHOT,
                ConnectorCapability.SUPPORTS_COLUMN_AUTO_ANALYZE,
                ConnectorCapability.SUPPORTS_TOPN_LAZY_MATERIALIZE,
                ConnectorCapability.SUPPORTS_SHOW_CREATE_DDL,
                ConnectorCapability.SUPPORTS_VIEW,
                ConnectorCapability.SUPPORTS_NESTED_COLUMN_PRUNE,
                ConnectorCapability.SUPPORTS_METADATA_PRELOAD);
        // SUPPORTS_USER_SESSION: only a REST catalog configured iceberg.rest.session=user projects the querying
        // user's delegated credential onto a per-request Iceberg REST SessionCatalog (#63068 re-migration). This
        // gates FE credential injection + shared-cache bypass; every other flavor/config authenticates with a
        // single static catalog identity and must NOT declare it (least-privilege).
        if (isUserSessionEnabled()) {
            capabilities.add(ConnectorCapability.SUPPORTS_USER_SESSION);
        }
        return capabilities;
    }

    /**
     * Whether this catalog is a REST catalog configured {@code iceberg.rest.session=user} — the single gate for
     * the per-user session machinery (capability declaration, the shared {@code RESTSessionCatalog} build, and
     * the session-aware catalog routing). {@code IcebergRestMetaStoreProperties.validate} has already enforced
     * that {@code session=user} implies {@code security.type=oauth2}.
     */
    boolean isUserSessionEnabled() {
        return IcebergConnectorProperties.SESSION_USER.equalsIgnoreCase(
                        properties.get(IcebergConnectorProperties.REST_SESSION))
                && IcebergConnectorProperties.TYPE_REST.equals(IcebergCatalogFactory.resolveFlavor(properties));
    }

    private Catalog getOrCreateCatalog() {
        if (icebergCatalog == null) {
            synchronized (this) {
                if (icebergCatalog == null) {
                    icebergCatalog = createCatalog();
                }
            }
        }
        return icebergCatalog;
    }

    private Catalog createCatalog() {
        String flavor = IcebergCatalogFactory.resolveFlavor(properties);
        if (flavor == null) {
            throw new DorisConnectorException(
                    "Missing '" + IcebergConnectorProperties.ICEBERG_CATALOG_TYPE + "' property");
        }

        Optional<S3CompatibleFileSystemProperties> chosenS3 =
                IcebergCatalogFactory.chooseS3Compatible(context.getStorageProperties());
        String catalogName = IcebergCatalogFactory.resolveCatalogName(properties, flavor, context.getCatalogName());

        // s3tables is bespoke: it is NOT built via CatalogUtil.buildIcebergCatalog. Legacy
        // IcebergS3TablesMetaStoreProperties hand-builds an S3TablesClient and calls the 3-arg
        // S3TablesCatalog.initialize(name, opts, client). Routed before the CatalogUtil flavor switch.
        if (IcebergConnectorProperties.TYPE_S3_TABLES.equals(flavor)) {
            return createS3TablesCatalog(catalogName, chosenS3);
        }

        Map<String, String> catalogProps =
                IcebergCatalogFactory.buildCatalogProperties(properties, flavor, chosenS3);
        Map<String, String> storageHadoopConfig = buildStorageHadoopConfig();

        Configuration conf;
        switch (flavor) {
            case IcebergConnectorProperties.TYPE_HMS: {
                // Reuse the shared metastore-spi parser (Q2=B bindForType): iceberg passes its own flavor token
                // so the metastore-spi never learns iceberg.catalog.type. Only toHiveConfOverrides is used
                // (iceberg HMS does NOT call paimon's validate(); it does not require a warehouse). The external
                // hive.conf.resources hive-site.xml is resolved by the connector itself (addConfResources).
                HmsMetaStoreProperties hms = (HmsMetaStoreProperties) MetaStoreProviders.bindForType(
                        IcebergConnectorProperties.TYPE_HMS, properties, storageHadoopConfig);
                conf = IcebergCatalogFactory.assembleHiveConf(
                        IcebergCatalogFactory.firstNonBlank(properties, "hive.conf.resources"),
                        hms.toHiveConfOverrides(context.getEnvironment()
                                .getOrDefault("hive_metastore_client_timeout_second", "10")));
                break;
            }
            case IcebergConnectorProperties.TYPE_GLUE:
                // Legacy IcebergGlueMetaStoreProperties builds the catalog with conf=null.
                conf = null;
                break;
            case IcebergConnectorProperties.TYPE_JDBC:
                maybeRegisterJdbcDriver();
                conf = IcebergCatalogFactory.buildHadoopConfiguration(properties, storageHadoopConfig);
                break;
            default:
                // rest / hadoop: a storage Configuration from the fe-filesystem-bound storage + raw
                // fs./dfs./hadoop. passthrough.
                conf = IcebergCatalogFactory.buildHadoopConfiguration(properties, storageHadoopConfig);
                break;
        }

        // Every REST catalog (plain or iceberg.rest.session=user) is built as a session-aware RESTSessionCatalog
        // (not the all-in-one RESTCatalog) wrapped for 401 re-authentication. The default (non-delegated) catalog
        // is asCatalog(empty), identical to what a RESTCatalog exposes; wrapping it recovers a catalog-identity
        // token expiry (#64966), and for session=user the shared session catalog + adapter also drive per-request
        // asCatalog(ctx)/asViewCatalog(ctx) delegated-credential routing (#63068).
        if (IcebergConnectorProperties.TYPE_REST.equals(flavor)) {
            return buildRestSessionCatalogDefault(catalogName, catalogProps, conf);
        }

        LOG.info("Creating Iceberg catalog '{}' flavor='{}' impl='{}'",
                catalogName, flavor, catalogProps.get(CatalogProperties.CATALOG_IMPL));
        return buildCatalogAuthenticated(flavor,
                () -> CatalogUtil.buildIcebergCatalog(catalogName, catalogProps, conf));
    }

    /**
     * Builds the default catalog for a REST catalog (plain or {@code iceberg.rest.session=user}): a SINGLE shared
     * {@link RESTSessionCatalog} (built directly — NOT via {@code CatalogUtil.buildIcebergCatalog}, which returns
     * the all-in-one {@code RESTCatalog} and hides the session catalog behind a private field), wrapped in a
     * {@link ReauthenticatingRestSessionCatalog} so an expired/rejected catalog-identity token (HTTP 401) rebuilds
     * the client and retries once instead of wedging until the FE restarts (upstream #64966). Its
     * {@code asCatalog(empty)} is the default (non-delegated) catalog; for {@code session=user} its
     * {@code asCatalog(ctx)} / {@code asViewCatalog(ctx)} are used per request by {@link #sessionCatalogAdapter}.
     * Memoizes {@link #restSessionCatalog} (always) and {@link #sessionCatalogAdapter} (session=user only) as a
     * side effect. The optional {@code iceberg.rest.session-timeout} maps to the iceberg AuthSession timeout.
     */
    private Catalog buildRestSessionCatalogDefault(String catalogName, Map<String, String> catalogProps,
            Configuration conf) {
        Map<String, String> sessionProps = new HashMap<>(catalogProps);
        // Built directly via new RESTSessionCatalog(), so the CatalogUtil catalog-impl key is neither needed nor
        // valid here; drop it.
        sessionProps.remove(CatalogProperties.CATALOG_IMPL);
        String sessionTimeout = properties.get(IcebergConnectorProperties.REST_SESSION_TIMEOUT);
        if (StringUtils.isNotBlank(sessionTimeout)) {
            sessionProps.put(CatalogProperties.AUTH_SESSION_TIMEOUT_MS, sessionTimeout);
        }
        boolean userSession = isUserSessionEnabled();
        IcebergSessionCatalogAdapter.DelegatedTokenMode tokenMode =
                IcebergSessionCatalogAdapter.DelegatedTokenMode.fromString(properties.getOrDefault(
                        IcebergConnectorProperties.REST_DELEGATED_TOKEN_MODE,
                        IcebergConnectorProperties.DELEGATED_TOKEN_MODE_ACCESS_TOKEN));
        // Frozen catalog-identity properties for the 401-recovery rebuild: an unmodifiable copy so the rebuild
        // always re-resolves from the catalog's OWN credential and can never capture a per-user delegated token.
        Map<String, String> frozenProps = Collections.unmodifiableMap(new HashMap<>(sessionProps));
        LOG.info("Creating Iceberg REST catalog '{}' (userSession={}, delegated-token-mode={})",
                catalogName, userSession, tokenMode);
        return buildCatalogAuthenticated(IcebergConnectorProperties.TYPE_REST, () -> {
            RESTSessionCatalog rawSessionCatalog = newRestSessionCatalog(catalogName, sessionProps, conf);
            // Wrap so a 401 on the catalog's own identity rebuilds the client and retries once. The wrapper is a
            // thin BaseViewSessionCatalog: asCatalog(empty)/asViewCatalog(empty) and the per-user asCatalog(ctx)
            // all call back into it, so every path inherits recovery; per-user requests are excluded by the
            // wrapper's own request-level gate (a request carrying a delegated credential is never recovered).
            ReauthenticatingRestSessionCatalog sessionCatalog = new ReauthenticatingRestSessionCatalog(
                    rawSessionCatalog, () -> rebuildRestSessionCatalog(catalogName, frozenProps, conf));
            Catalog defaultCatalog = sessionCatalog.asCatalog(SessionCatalog.SessionContext.createEmpty());
            this.restSessionCatalog = sessionCatalog;
            if (userSession) {
                this.sessionCatalogAdapter =
                        new IcebergSessionCatalogAdapter(defaultCatalog, sessionCatalog, tokenMode);
            }
            return defaultCatalog;
        });
    }

    /** Builds and initializes a bare Iceberg {@link RESTSessionCatalog} (fresh REST client + OAuth2 token fetch). */
    private RESTSessionCatalog newRestSessionCatalog(String catalogName, Map<String, String> props,
            Configuration conf) {
        RESTSessionCatalog sessionCatalog = new RESTSessionCatalog();
        CatalogUtil.configureHadoopConf(sessionCatalog, conf);
        sessionCatalog.initialize(catalogName, props);
        return sessionCatalog;
    }

    /**
     * Rebuilds the REST session catalog for 401 re-authentication, under the plugin classloader pin. The initial
     * build runs inside {@link #buildCatalogAuthenticated} (i.e. {@code context.executeAuthenticated}); this rebuild
     * fires later on whatever thread hit the 401, so it must re-apply the same pin or iceberg's reflective REST /
     * iceberg-aws client build split-brains against the app loader (see {@code pinIcebergWorkerPoolToPluginClassLoader}
     * and {@code TcclPinningConnectorContext}). Uses the frozen catalog-identity props, so it can never mint the
     * shared client with a per-user token.
     */
    private RESTSessionCatalog rebuildRestSessionCatalog(String catalogName, Map<String, String> props,
            Configuration conf) {
        try {
            return context.executeAuthenticated(() -> newRestSessionCatalog(catalogName, props, conf));
        } catch (Exception e) {
            throw new DorisConnectorException("Failed to rebuild Iceberg REST client for re-authentication (catalog="
                    + catalogName + "): " + e.getMessage(), e);
        }
    }

    /**
     * Creates the bespoke {@code s3tables} catalog, mirroring legacy {@code IcebergS3TablesMetaStoreProperties}: a
     * hand-built {@link S3TablesClient} (region + credentials + optional {@code s3tables.endpoint} override + the
     * s3tables-SDK http config) is passed to the 3-arg {@code S3TablesCatalog.initialize(name, opts, client)} —
     * NOT to {@code CatalogUtil.buildIcebergCatalog}. The 2-arg {@code initialize(name, opts)} is intentionally
     * avoided: its {@code DefaultS3TablesAwsClientFactory} honors only a {@code client.credentials-provider} class
     * and would silently drop static {@code s3.access-key-id}/{@code s3.secret-access-key} (falling back to the
     * SDK default chain). A region is required (from the bound storage or the raw props); credentials come from
     * the bound storage when present, else the SDK default chain — e.g. an EC2 instance-profile s3tables catalog
     * with only region + warehouse ARN and no static creds. Only a missing region fails loud here, before any
     * AWS call (legacy IcebergS3TablesMetaStoreProperties used the DefaultCredentialsProvider chain likewise).
     */
    private Catalog createS3TablesCatalog(String catalogName, Optional<S3CompatibleFileSystemProperties> chosenS3) {
        String region = resolveS3TablesRegion(chosenS3, properties);
        Map<String, String> catalogProps =
                IcebergCatalogFactory.buildS3TablesCatalogProperties(properties, chosenS3);
        LOG.info("Creating Iceberg s3tables catalog '{}' region='{}' boundStorage={}",
                catalogName, region, chosenS3.isPresent());
        return buildCatalogAuthenticated(IcebergConnectorProperties.TYPE_S3_TABLES, () -> {
            S3TablesClient client = buildS3TablesClient(chosenS3, region);
            S3TablesCatalog catalog = new S3TablesCatalog();
            catalog.initialize(catalogName, catalogProps, client);
            return catalog;
        });
    }

    /**
     * Resolves the s3tables control-plane region: the bound fe-filesystem storage's region when present, else
     * the raw catalog props (the widened S3 region-alias set, via {@link IcebergCatalogFactory#resolveS3Region}).
     * A region is the SOLE hard requirement for s3tables; credentials fall back to the SDK default chain when no
     * storage is bound. Fails loud only when NEITHER storage nor props supply a region. Static / package-visible
     * so the gate is unit-testable offline without a live {@link S3TablesClient}.
     */
    static String resolveS3TablesRegion(
            Optional<S3CompatibleFileSystemProperties> chosenS3, Map<String, String> props) {
        String region = chosenS3.map(S3CompatibleFileSystemProperties::getRegion)
                .filter(StringUtils::isNotBlank)
                .orElseGet(() -> IcebergCatalogFactory.resolveS3Region(props));
        if (StringUtils.isBlank(region)) {
            throw new DorisConnectorException(
                    "Iceberg s3tables catalog requires a region (set s3.region or a region-bearing endpoint)");
        }
        return region;
    }


    /**
     * Hand-builds the control-plane {@link S3TablesClient}, mirroring legacy
     * {@code IcebergS3TablesMetaStoreProperties.buildS3TablesClient}: region + credentials provider + the optional
     * {@code s3tables.endpoint} override + the s3tables-SDK http-client tuning ({@link HttpClientProperties}). The
     * credentials provider is derived from the typed fe-filesystem storage by {@link #buildAwsCredentialsProvider}
     * when one is bound, else the SDK default chain ({@link DefaultCredentialsProvider}); the region is the value
     * already resolved by {@link #resolveS3TablesRegion}.
     */
    private S3TablesClient buildS3TablesClient(Optional<S3CompatibleFileSystemProperties> chosenS3, String region) {
        AwsCredentialsProvider credentialsProvider = chosenS3
                .map(s3 -> buildAwsCredentialsProvider(s3, properties))
                .orElseGet(DefaultCredentialsProvider::create);
        S3TablesClientBuilder builder = S3TablesClient.builder()
                .region(Region.of(region))
                .credentialsProvider(credentialsProvider);
        String endpoint = properties.get(S3TablesProperties.S3TABLES_ENDPOINT);
        if (StringUtils.isNotBlank(endpoint)) {
            builder.endpointOverride(URI.create(endpoint));
        }
        new HttpClientProperties(properties).applyHttpClientConfigurations(builder);
        return builder.build();
    }

    /**
     * Derives the AWS SDK v2 credentials provider for the s3tables control-plane client from the typed
     * fe-filesystem storage, mirroring legacy
     * {@code IcebergAwsClientCredentialsProperties.createAwsCredentialsProvider}: static AK/SK ->
     * {@link StaticCredentialsProvider} (with a session token when present); a role ARN ->
     * {@link StsAssumeRoleCredentialsProvider} (role session name {@code aws-sdk-java-v2-fe}, optional external
     * id); otherwise the SDK default chain ({@link DefaultCredentialsProvider}).
     *
     * <p>F14: the no-credential (PROVIDER_CHAIN) case resolves the non-DEFAULT provider the user selected via
     * {@code s3.credentials_provider_type} through {@link AwsCredentialsProviderModes} — a self-contained twin of
     * legacy {@code AwsCredentialsProviderFactory.createV2} (the connector cannot import fe-core). {@code DEFAULT}
     * (and blank / unknown) still yields {@link DefaultCredentialsProvider}. The STS base credentials for the
     * ASSUME_ROLE path stay on the default chain (matching the already-twinned assume-role case).
     */
    private static AwsCredentialsProvider buildAwsCredentialsProvider(
            S3CompatibleFileSystemProperties s3, Map<String, String> props) {
        if (s3.hasStaticCredentials()) {
            if (StringUtils.isBlank(s3.getSessionToken())) {
                return StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(s3.getAccessKey(), s3.getSecretKey()));
            }
            return StaticCredentialsProvider.create(
                    AwsSessionCredentials.create(s3.getAccessKey(), s3.getSecretKey(), s3.getSessionToken()));
        }
        if (s3.hasAssumeRole()) {
            StsClient stsClient = StsClient.builder()
                    .region(Region.of(s3.getRegion()))
                    .credentialsProvider(DefaultCredentialsProvider.create())
                    .build();
            return StsAssumeRoleCredentialsProvider.builder()
                    .stsClient(stsClient)
                    .refreshRequest(b -> {
                        b.roleArn(s3.getRoleArn()).roleSessionName("aws-sdk-java-v2-fe");
                        if (StringUtils.isNotBlank(s3.getExternalId())) {
                            b.externalId(s3.getExternalId());
                        }
                    })
                    .build();
        }
        // F14: PROVIDER_CHAIN — the non-DEFAULT provider the user selected (DEFAULT -> DefaultCredentialsProvider).
        return AwsCredentialsProviderModes.providerFor(props, AwsCredentialsProviderModes.S3_MODE_KEYS);
    }

    // HDFS scheme constants for the warehouse -> fs.defaultFS bridge (inlined; the connector must not import
    // fe-core's HdfsResource). Values match HdfsResource.HDFS_PREFIX / HDFS_FILE_PREFIX / HADOOP_FS_NAME.
    private static final String HDFS_SCHEME_PREFIX = "hdfs:";
    private static final String HDFS_URI_PREFIX = "hdfs://";
    private static final String FS_DEFAULT_FS_KEY = "fs.defaultFS";

    /**
     * Design S8: the iceberg connector owns the {@code warehouse -> fs.defaultFS} storage derivation that used
     * to live in fe-core's {@code IcebergFileSystemMetaStoreProperties.getDerivedStorageProperties}. Only the
     * hadoop (filesystem) catalog flavor bridges the warehouse; the other flavors (rest/hms/glue/jdbc/
     * s3tables) contribute no storage derivation (empty), matching the legacy override which only the hadoop
     * flavor carried. fe-core folds the result into its storage map as defaults, feeding both the fe-filesystem
     * bind and the BE storage map identically.
     */
    @Override
    public Map<String, String> deriveStorageProperties(Map<String, String> rawCatalogProps) {
        return deriveStorageDefaults(rawCatalogProps);
    }

    /**
     * Gate + derivation for {@link #deriveStorageProperties} (static so it is unit-testable without constructing
     * a connector): only the hadoop (filesystem) catalog flavor bridges the warehouse; every other flavor
     * (rest/hms/glue/jdbc/s3tables) contributes nothing.
     */
    static Map<String, String> deriveStorageDefaults(Map<String, String> rawCatalogProps) {
        if (!IcebergConnectorProperties.TYPE_HADOOP.equalsIgnoreCase(
                rawCatalogProps.get(IcebergConnectorProperties.ICEBERG_CATALOG_TYPE))) {
            return Collections.emptyMap();
        }
        return deriveHdfsDefaultFsFromWarehouse(rawCatalogProps.get(IcebergConnectorProperties.WAREHOUSE));
    }

    /**
     * Bridges a hadoop-flavor {@code warehouse=hdfs://<ns>/path} to {@code fs.defaultFS=hdfs://<ns>} so an
     * HA-nameservice catalog configured with only {@code warehouse} (relying on classpath {@code core-site.xml}/
     * {@code hdfs-site.xml} for the nameservice, no inline {@code uri}/{@code fs.defaultFS}) still binds HDFS
     * storage with the warehouse nameservice. Non-hdfs and blank warehouses derive nothing; a blank nameservice
     * fails loud. Verbatim port of the former {@code IcebergFileSystemMetaStoreProperties.getDerivedStorageProperties}.
     */
    static Map<String, String> deriveHdfsDefaultFsFromWarehouse(String warehouse) {
        if (StringUtils.isBlank(warehouse) || !StringUtils.startsWith(warehouse, HDFS_SCHEME_PREFIX)) {
            return Collections.emptyMap();
        }
        String nameService = StringUtils.substringBetween(warehouse, HDFS_URI_PREFIX, "/");
        if (StringUtils.isEmpty(nameService)) {
            throw new IllegalArgumentException("Unrecognized 'warehouse' location format"
                    + " because name service is required.");
        }
        return Collections.singletonMap(FS_DEFAULT_FS_KEY, HDFS_URI_PREFIX + nameService);
    }

    /**
     * Assembles the canonical storage Hadoop config from the FE-bound storage properties (P1-T03), mirroring
     * {@code PaimonConnector.buildStorageHadoopConfig}: object stores contribute their fs.s3a.* / fs.oss.* /
     * fs.cosn.* / fs.obs.* translation, and an HDFS-backed catalog contributes its hadoop.config.resources XML +
     * HA + auth keys (C2; the defaults-free fe-filesystem HDFS map). Empty for a catalog with no typed storage.
     */
    private Map<String, String> buildStorageHadoopConfig() {
        Map<String, String> merged = new HashMap<>();
        for (StorageProperties sp : context.getStorageProperties()) {
            sp.toHadoopProperties().ifPresent(h -> merged.putAll(h.toHadoopConfigurationMap()));
        }
        return merged;
    }

    /**
     * Lazily builds and memoizes the plugin-side Kerberos authenticator that {@link TcclPinningConnectorContext}
     * runs each op under, so remote HDFS access uses the PLUGIN's own {@code UserGroupInformation} copy (the one
     * the plugin's {@code FileSystem} reads). Returns {@code null} for a non-Kerberos catalog so the FE-injected
     * auth path is preserved unchanged. The Kerberos keys ride the {@code hadoop.*} passthrough in
     * {@link IcebergCatalogFactory#buildHadoopConfiguration}; {@link HadoopAuthenticator#getHadoopAuthenticator}
     * resolves the plugin (child-first) copy of fe-kerberos, so its {@code doAs} logs in / acts on the plugin
     * UGI. Construction is cheap — the keytab login is lazy in {@code getUGI()} on the first {@code doAs}.
     */
    private HadoopAuthenticator pluginAuthenticator() {
        if (!pluginAuthComputed) {
            synchronized (this) {
                if (!pluginAuthComputed) {
                    pluginAuth = buildPluginAuthenticator(properties, buildStorageHadoopConfig());
                    pluginAuthComputed = true;
                }
            }
        }
        return pluginAuth;
    }

    /**
     * Resolves the plugin-side Kerberos authenticator for the catalog, or {@code null} for a non-Kerberos
     * catalog. Two Kerberos sources are covered, in precedence order:
     * <ol>
     *   <li><b>Storage</b> Kerberos — the raw {@code hadoop.security.authentication=kerberos} passthrough
     *       (HDFS / data-lake login), built from the storage Hadoop configuration. Unchanged prior behavior;
     *       when storage is Kerberos this single login also carries the HMS metastore RPC (same UGI).</li>
     *   <li><b>HMS-metastore</b> Kerberos with non-Kerberos storage — a secured Hive Metastore whose data
     *       storage is simple (e.g. a Kerberized HMS over S3). Legacy fe-core served this from the fe-core
     *       {@code IcebergHMSMetaStoreProperties} HMS authenticator (delivered via {@code DefaultConnectorContext});
     *       once the fe-core iceberg property cluster is deleted the connector must own it. This mirrors
     *       {@code HMSBaseProperties.initHadoopAuthenticator}: the HMS client principal/keytab facts
     *       ({@link HmsMetaStoreProperties#kerberos()}) feed a {@link KerberosAuthenticationConfig}, so the
     *       {@code doAs} logs in the same client identity fe-core used. The HMS <em>service</em> principal /
     *       SASL settings ride the catalog's own HiveConf ({@code hms.toHiveConfOverrides}), not the login.</li>
     * </ol>
     * Package-visible + static for direct unit testing (mirrors the {@code metaFailureMessage} helpers).
     */
    static HadoopAuthenticator buildPluginAuthenticator(Map<String, String> properties,
            Map<String, String> storageHadoopConfig) {
        if ("kerberos".equalsIgnoreCase(properties.get(HADOOP_SECURITY_AUTHENTICATION))) {
            return HadoopAuthenticator.getHadoopAuthenticator(
                    IcebergCatalogFactory.buildHadoopConfiguration(properties, storageHadoopConfig));
        }
        if (IcebergConnectorProperties.TYPE_HMS.equals(IcebergCatalogFactory.resolveFlavor(properties))) {
            HmsMetaStoreProperties hms = (HmsMetaStoreProperties) MetaStoreProviders.bindForType(
                    IcebergConnectorProperties.TYPE_HMS, properties, storageHadoopConfig);
            Optional<KerberosAuthSpec> spec = hms.kerberos();
            if (spec.isPresent() && spec.get().hasCredentials()) {
                Configuration conf =
                        IcebergCatalogFactory.buildHadoopConfiguration(properties, storageHadoopConfig);
                conf.set("hadoop.security.authentication", "kerberos");
                conf.set("hive.metastore.sasl.enabled", "true");
                return HadoopAuthenticator.getHadoopAuthenticator(
                        new KerberosAuthenticationConfig(spec.get().getPrincipal(), spec.get().getKeytab(), conf));
            }
        }
        return null;
    }

    private Catalog buildCatalogAuthenticated(String flavor, Callable<Catalog> builder) {
        // Catalog creation needs the thread-context classloader pinned to the plugin loader (Hadoop's
        // FileSystem ServiceLoader + SecurityUtil static init, and iceberg-aws's reflective client build, all
        // resolve helper classes through the TCCL; without the pin they read the parent 'app' loader and
        // split-brain against the child-loaded classes). That pin is now applied once, for every
        // executeAuthenticated call, by TcclPinningConnectorContext (which wraps the injected context in the
        // constructor) — the single seam shared with the write/DDL/procedure commits — so it is not repeated
        // here. PaimonConnector.createCatalogFromContext still pins inline (no such wrapper).
        try {
            Catalog catalog = context.executeAuthenticated(builder);
            // iceberg's parallel data-manifest WRITE runs on its own shared worker pool, whose threads do NOT
            // inherit the per-commit TCCL pin TcclPinningConnectorContext applies on the engine thread; pin
            // those threads to the plugin loader once so that path resolves iceberg-aws on the plugin side too
            // (see pinIcebergWorkerPoolToPluginClassLoader).
            pinIcebergWorkerPoolToPluginClassLoader();
            return catalog;
        } catch (Exception e) {
            throw new DorisConnectorException(
                    "Failed to create Iceberg catalog (flavor=" + flavor + "): " + e.getMessage(), e);
        }
    }

    /**
     * Pins the thread-context classloader of iceberg's shared worker pool to this plugin's classloader, once
     * per JVM.
     *
     * <p>iceberg fans its parallel data-manifest WRITE ({@code SnapshotProducer.writeManifests}, reached on
     * every INSERT/UPDATE/DELETE/MERGE/REWRITE commit and the snapshot procedures) onto
     * {@code ThreadPools.getWorkerPool()}. Because the iceberg connector provider is loaded once and shared by
     * every iceberg catalog, that is a single JVM-wide daemon pool. {@link TcclPinningConnectorContext} pins
     * only the engine thread that drives a commit; the worker-pool threads do NOT inherit that pin, so the lazy
     * iceberg-aws S3-client build on a worker thread resolves {@code ApacheHttpClientConfigurations} via
     * {@code DynMethods} against the parent 'app' loader and {@link ClassCastException}s the child-loaded
     * plugin copy the rest of the iceberg-aws stack uses. Setting each worker thread's TCCL to the plugin
     * loader (the loader the iceberg-aws classes are child-first-loaded from) keeps every reflective load on
     * the plugin side — the worker-pool analogue of the scan ({@code PluginDrivenScanNode.onPluginClassLoader})
     * and commit-thread ({@link TcclPinningConnectorContext}) guards.
     *
     * <p>Set explicitly (not relying on thread-creation inheritance) so it also repins any worker thread an
     * earlier unpinned use already created; a {@code ThreadPoolExecutor} never resets a worker's TCCL between
     * tasks, so the pin persists. A short-lived barrier forces every thread of the fixed pool to run a primer.
     * Best-effort: a failure or timeout is logged and never fails catalog creation (the write path then behaves
     * as before the pin), and the guard is reset so a later catalog build retries.
     */
    private void pinIcebergWorkerPoolToPluginClassLoader() {
        if (!ICEBERG_WORKER_POOL_PINNED.compareAndSet(false, true)) {
            return;
        }
        int poolSize = ThreadPools.WORKER_THREAD_POOL_SIZE;
        if (poolSize <= 0) {
            return;
        }
        try {
            if (!pinPoolThreadsToClassLoader(
                    ThreadPools.getWorkerPool(), poolSize, getClass().getClassLoader(), 30)) {
                ICEBERG_WORKER_POOL_PINNED.set(false);
                LOG.warn("Timed out pinning iceberg worker pool ({} threads) to the plugin classloader; "
                        + "iceberg-aws writes may ClassCast until a later catalog build retries", poolSize);
            }
        } catch (InterruptedException e) {
            ICEBERG_WORKER_POOL_PINNED.set(false);
            Thread.currentThread().interrupt();
        } catch (RuntimeException e) {
            ICEBERG_WORKER_POOL_PINNED.set(false);
            LOG.warn("Failed to pin iceberg worker pool to the plugin classloader", e);
        }
    }

    /**
     * Sets the thread-context classloader of EVERY thread of a fixed-size {@code pool} to {@code target},
     * returning whether all {@code poolSize} threads were reached within {@code timeoutSeconds}. A barrier holds
     * each primer until all have started, forcing every distinct worker thread to run a primer and set its TCCL
     * (a single fast thread could otherwise serve every submitted task, leaving the rest unpinned).
     * Package-private for {@code IcebergConnectorWorkerPoolPinTest}.
     */
    static boolean pinPoolThreadsToClassLoader(ExecutorService pool, int poolSize, ClassLoader target,
            long timeoutSeconds) throws InterruptedException {
        CountDownLatch allStarted = new CountDownLatch(poolSize);
        CountDownLatch release = new CountDownLatch(1);
        try {
            for (int i = 0; i < poolSize; i++) {
                pool.execute(() -> {
                    Thread.currentThread().setContextClassLoader(target);
                    allStarted.countDown();
                    // Park so the next task is forced onto a DISTINCT worker thread, until every thread in the
                    // fixed pool has run a primer and set its TCCL.
                    try {
                        release.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            }
            return allStarted.await(timeoutSeconds, TimeUnit.SECONDS);
        } finally {
            release.countDown();
        }
    }

    /**
     * Enforces JDBC driver-url security at CREATE CATALOG (mirrors {@code PaimonConnector.preCreateValidation}):
     * for the jdbc flavor a configured {@code iceberg.jdbc.driver_url} is routed through the engine's
     * {@link ConnectorValidationContext#validateAndResolveDriverPath} hook (the FE format /
     * {@code jdbc_driver_url_white_list} / {@code jdbc_driver_secure_path} gates), so a rejected url fails
     * CREATE CATALOG before the jar is ever loaded by {@link #maybeRegisterJdbcDriver}. Non-jdbc flavors are
     * a no-op.
     */
    @Override
    public void preCreateValidation(ConnectorValidationContext validationContext) throws Exception {
        if (!IcebergConnectorProperties.TYPE_JDBC.equals(IcebergCatalogFactory.resolveFlavor(properties))) {
            return;
        }
        String driverUrl = IcebergCatalogFactory.firstNonBlank(properties, IcebergConnectorProperties.JDBC_DRIVER_URL);
        if (StringUtils.isNotBlank(driverUrl)) {
            validationContext.validateAndResolveDriverPath(driverUrl);
        }
    }

    /**
     * If an {@code iceberg.jdbc.driver_url} is configured, dynamically load + register the driver before
     * creating the catalog. {@link java.sql.DriverManager#getConnection} does not consult the thread context
     * class loader, so the driver must be registered globally. Ported from the legacy
     * {@code IcebergJdbcMetaStoreProperties.registerJdbcDriver}, with the fe-core
     * {@code JdbcResource.getFullDriverUrl} dependency replaced by the shared
     * {@link JdbcDriverSupport#resolveDriverUrl} against {@code ConnectorContext.getEnvironment()}.
     */
    private void maybeRegisterJdbcDriver() {
        String driverUrl = IcebergCatalogFactory.firstNonBlank(properties, IcebergConnectorProperties.JDBC_DRIVER_URL);
        if (StringUtils.isBlank(driverUrl)) {
            return;
        }
        String driverClass =
                IcebergCatalogFactory.firstNonBlank(properties, IcebergConnectorProperties.JDBC_DRIVER_CLASS);
        registerJdbcDriver(driverUrl, driverClass);
        LOG.info("Using dynamic JDBC driver for Iceberg JDBC catalog from: {}", driverUrl);
    }

    private void registerJdbcDriver(String driverUrl, String driverClassName) {
        try {
            if (StringUtils.isBlank(driverClassName)) {
                throw new IllegalArgumentException("driver_class is required when driver_url is specified");
            }
            Map<String, String> env = context != null ? context.getEnvironment() : Collections.emptyMap();
            String fullDriverUrl = JdbcDriverSupport.resolveDriverUrl(driverUrl, env);
            URL url = new URL(fullDriverUrl);
            String driverKey = fullDriverUrl + "#" + driverClassName;
            if (!REGISTERED_DRIVER_KEYS.add(driverKey)) {
                LOG.info("JDBC driver already registered for Iceberg catalog: {} from {}",
                        driverClassName, fullDriverUrl);
                return;
            }
            try {
                ClassLoader classLoader = DRIVER_CLASS_LOADER_CACHE.computeIfAbsent(url,
                        u -> URLClassLoader.newInstance(new URL[] {u}, getClass().getClassLoader()));
                Class<?> loadedDriverClass = Class.forName(driverClassName, true, classLoader);
                java.sql.Driver driver = (java.sql.Driver) loadedDriverClass.getDeclaredConstructor().newInstance();
                java.sql.DriverManager.registerDriver(new DriverShim(driver));
                LOG.info("Successfully registered JDBC driver for Iceberg catalog: {} from {}",
                        driverClassName, fullDriverUrl);
            } catch (ClassNotFoundException e) {
                REGISTERED_DRIVER_KEYS.remove(driverKey);
                throw new IllegalArgumentException("Failed to load JDBC driver class: " + driverClassName, e);
            } catch (Exception e) {
                REGISTERED_DRIVER_KEYS.remove(driverKey);
                throw new RuntimeException("Failed to register JDBC driver: " + driverClassName, e);
            }
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException("Invalid driver URL: " + driverUrl, e);
        }
    }

    /**
     * A shim driver that wraps a driver loaded from a custom ClassLoader, because {@code DriverManager}
     * refuses to use a driver not loaded by the system classloader. Ported verbatim from the legacy
     * {@code IcebergJdbcMetaStoreProperties.DriverShim}.
     */
    private static class DriverShim implements java.sql.Driver {
        private final java.sql.Driver delegate;

        DriverShim(java.sql.Driver delegate) {
            this.delegate = delegate;
        }

        @Override
        public java.sql.Connection connect(String url, java.util.Properties info) throws java.sql.SQLException {
            return delegate.connect(url, info);
        }

        @Override
        public boolean acceptsURL(String url) throws java.sql.SQLException {
            return delegate.acceptsURL(url);
        }

        @Override
        public java.sql.DriverPropertyInfo[] getPropertyInfo(String url, java.util.Properties info)
                throws java.sql.SQLException {
            return delegate.getPropertyInfo(url, info);
        }

        @Override
        public int getMajorVersion() {
            return delegate.getMajorVersion();
        }

        @Override
        public int getMinorVersion() {
            return delegate.getMinorVersion();
        }

        @Override
        public boolean jdbcCompliant() {
            return delegate.jdbcCompliant();
        }

        @Override
        public java.util.logging.Logger getParentLogger() throws java.sql.SQLFeatureNotSupportedException {
            return delegate.getParentLogger();
        }
    }

    @Override
    public void close() throws IOException {
        Catalog c = icebergCatalog;
        if (c != null) {
            if (c instanceof java.io.Closeable) {
                ((java.io.Closeable) c).close();
            }
            icebergCatalog = null;
        }
        // The default catalog (asCatalog(empty)) is a lightweight view and NOT Closeable, so close the shared
        // underlying REST session catalog (its REST client + OAuth2 auth resources) explicitly here. It is the
        // ReauthenticatingRestSessionCatalog wrapper, a Closeable that closes its current delegate.
        BaseViewSessionCatalog sc = restSessionCatalog;
        if (sc != null) {
            if (sc instanceof java.io.Closeable) {
                ((java.io.Closeable) sc).close();
            }
            restSessionCatalog = null;
            sessionCatalogAdapter = null;
        }
    }
}
