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

package org.apache.doris.datasource.storage;

import org.apache.doris.common.Config;
import org.apache.doris.datasource.property.common.AwsCredentialsProviderFactory;
import org.apache.doris.datasource.property.common.AwsCredentialsProviderMode;
import org.apache.doris.filesystem.properties.FileSystemProperties;
import org.apache.doris.filesystem.properties.HadoopStorageProperties;
import org.apache.doris.filesystem.properties.S3CompatibleFileSystemProperties;
import org.apache.doris.filesystem.spi.FileSystemProvider;
import org.apache.doris.foundation.property.StoragePropertiesException;
import org.apache.doris.foundation.security.ExecutionAuthenticator;
import org.apache.doris.fs.FileSystemPluginManager;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * fe-core facade over the fe-filesystem SPI typed properties (Phase B3 of the
 * StorageProperties removal plan).
 *
 * <p>One adapter wraps one {@link FileSystemProperties} binding produced by
 * {@link FileSystemPluginManager#bindPrimary}/{@code bindAll}. Its public surface mirrors the
 * legacy typed storage-properties contract exactly — backend map,
 * Hadoop configuration, storage name, schemas, type — so consumers can migrate mechanically.
 * Every known SPI-vs-fe-core drift from the master plan's §2.4 parity ledger is reconciled here
 * (or in the SPI implementation, where noted); each reconciliation carries a
 * "align fe-core" comment referencing the ledger item.</p>
 */
public final class StorageAdapter {

    private static final Logger LOG = LogManager.getLogger(StorageAdapter.class);

    /**
     * Plugin manager injected at FE startup (see {@code FileSystemFactory.initPluginManager},
     * which forwards here). Held locally so the facade does not reach back into the {@code fs}
     * runtime layer — the dependency direction is fs → datasource.storage only.
     */
    private static volatile FileSystemPluginManager pluginManager;

    /**
     * Lazy fallback registry for environments where FE startup did not run
     * {@link #initPluginManager} (unit tests, migration phase).
     */
    private static volatile FileSystemPluginManager fallbackManager;

    /** Called once at FE startup, before any {@code of}/{@code ofAll} binding. */
    public static void initPluginManager(FileSystemPluginManager manager) {
        pluginManager = manager;
    }

    private static final String SIMPLE_AWS_CREDENTIALS_PROVIDER =
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider";
    private static final String ASSUMED_ROLE_CREDENTIAL_PROVIDER =
            "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider";

    /**
     * Hadoop keys the facade re-derives for the S3 provider instead of taking the SPI values:
     * fe-core gates them on accessKey-blank and on Config.aws_credentials_provider_version,
     * which the SPI layer cannot see (align fe-core, ledger 2.4-3).
     */
    private static final Set<String> S3_CREDENTIAL_KEYS = ImmutableSet.of(
            "fs.s3a.aws.credentials.provider",
            "fs.s3a.assumed.role.arn",
            "fs.s3a.assumed.role.credentials.provider",
            "fs.s3a.assumed.role.external.id");

    /**
     * Legacy per-dialect alias prefix for {@code force_parsing_by_standard_uri} (fe-core
     * {@code AbstractS3CompatibleProperties} subclasses declared
     * {@code <prefix>.force_parsing_by_standard_uri} plus the generic key; plain S3 had the
     * generic key only). Read from the raw props because the SPI api interface does not expose
     * this flag.
     */
    private static final Map<String, String> FORCE_PARSING_PREFIXES = ImmutableMap.<String, String>builder()
            .put("OSS", "oss")
            .put("COS", "cos")
            .put("OBS", "obs")
            .put("GCS", "gs")
            .put("MINIO", "minio")
            .put("OZONE", "ozone")
            .build();

    private final FileSystemProperties spi;
    private final Map<String, String> origProps;
    private final String providerKey;
    private final StorageTypeId type;
    /** Explicit broker name for adapters built via {@link #ofBroker}; mirrors the legacy
     *  {@code BrokerProperties.of(name, props)} where the name is NOT part of the raw props. */
    private final String brokerNameOverride;
    /** fe-core S3Properties credentials-provider mode; only resolved for the S3 provider. */
    private final AwsCredentialsProviderMode s3CredentialsMode;
    /** Resolved once at construction (legacy bound it at init); read per file in listing loops. */
    private final String forceParsingByStandardUriValue;
    /**
     * Lazily built (legacy {@code X.of()} factories never built one; {@code new Configuration()}
     * re-parses the Hadoop XML defaults and is too expensive for per-statement bindings that
     * never read it, e.g. cloud COPY INTO). Volatile double-checked; construction is a pure
     * function of final fields.
     */
    private volatile Configuration hadoopStorageConfig;
    private volatile boolean hadoopStorageConfigBuilt;
    /**
     * Adapters are shared across threads (catalog adapter map, FS caches), so the lazily cached
     * map must be safely published — legacy classes were immune (eager init or fresh map per
     * call); the cache is a facade addition.
     */
    private volatile Map<String, String> backendConfigProperties;

    private StorageAdapter(FileSystemProperties spi, Map<String, String> origProps) {
        this(spi, origProps, null);
    }

    private StorageAdapter(FileSystemProperties spi, Map<String, String> origProps, String brokerNameOverride) {
        this.spi = spi;
        this.origProps = origProps;
        this.brokerNameOverride = brokerNameOverride;
        this.providerKey = spi.providerName().toUpperCase(Locale.ROOT);
        // Providers outside the StorageRegistry.Provider table (out-of-tree plugins): an
        // S3-compatible dialect joins the S3 family id so every TypeId-dispatched consumer
        // (TVF dispatch, connectivity, credentials) treats it like the built-in dialects —
        // no new enum value or case additions needed. Note the type-keyed catalog map can
        // hold one S3-family binding at a time, so one catalog cannot combine such a plugin
        // with another S3-family storage. Non-S3 protocols stay UNKNOWN (they need the
        // full scenario-B integration incl. BE support).
        this.type = StorageRegistry.Provider.byName(providerKey)
                .map(StorageRegistry.Provider::typeId)
                .orElse(spi instanceof S3CompatibleFileSystemProperties
                        ? StorageTypeId.S3 : StorageTypeId.UNKNOWN);
        // Align fe-core S3Properties.initNormalizeAndCheckProps: the mode string is parsed with
        // the fe-core enum (which rejects spellings the SPI tolerates) at construction time.
        this.s3CredentialsMode = "S3".equals(providerKey)
                ? AwsCredentialsProviderMode.fromString(feCoreS3CredentialsProviderType())
                : null;
        this.forceParsingByStandardUriValue = resolveForceParsingByStandardUri();
        checkAzureOauth2OnlyForIcebergRest();
    }

    /**
     * Binds the primary provider for the raw user properties, mirroring the legacy
     * {@code StorageProperties.createPrimary}.
     */
    public static StorageAdapter of(Map<String, String> origProps) {
        return new StorageAdapter(manager().bindPrimary(withHadoopConfigDir(origProps)), origProps);
    }

    /**
     * Binds every matching provider, mirroring the legacy {@code StorageProperties.createAll}
     * (multi-hit, OSS-HDFS/OSS exclusivity, default HDFS fallback at index 0).
     */
    public static List<StorageAdapter> ofAll(Map<String, String> origProps) {
        List<FileSystemProperties> bindings = manager().bindAll(withHadoopConfigDir(origProps));
        List<StorageAdapter> result = new ArrayList<>(bindings.size());
        for (FileSystemProperties binding : bindings) {
            result.add(new StorageAdapter(binding, origProps));
        }
        return result;
    }

    /**
     * Binds one specific provider by name, bypassing routing entirely — the facade twin of the
     * legacy per-dialect factories ({@code S3Properties.of}, {@code OSSProperties.of},
     * {@code AzureProperties.of}, ...) that constructed and validated a fixed typed class no
     * matter what routing would have chosen for the same map.
     */
    public static StorageAdapter ofProvider(String providerName, Map<String, String> origProps) {
        Map<String, String> props = origProps == null ? new HashMap<>() : origProps;
        for (FileSystemProvider<?> provider : manager().getProviders()) {
            if (provider.name().equalsIgnoreCase(providerName)) {
                return new StorageAdapter(provider.bind(withHadoopConfigDir(props)), props);
            }
        }
        throw new StoragePropertiesException(
                "Filesystem provider '" + providerName + "' is not available");
    }

    /**
     * Returns whether the named provider's guess heuristics claim the raw properties — the
     * facade twin of the legacy per-dialect {@code guessIsMe} statics (guess only, explicit
     * {@code fs.<x>.support} flags deliberately NOT consulted, matching the legacy call sites).
     */
    public static boolean matchesProviderGuess(String providerName, Map<String, String> origProps) {
        Map<String, String> props = origProps == null ? new HashMap<>() : origProps;
        // Same probe view the bind registry uses: carries Config-derived guess context
        // (e.g. the admin-extensible Azure host suffixes) into the provider.
        Map<String, String> probeView = FileSystemPluginManager.withProbeContext(props);
        for (FileSystemProvider<?> provider : manager().getProviders()) {
            if (provider.name().equalsIgnoreCase(providerName)) {
                return provider.supportsGuess(probeView);
            }
        }
        return false;
    }

    /**
     * Quirk preservation (converter parity): fe-filesystem resolves relative
     * {@code hadoop.config.resources} entries against the injected {@code _HADOOP_CONFIG_DIR_}
     * context key because it cannot see fe-core {@code Config.hadoop_config_dir}; the legacy
     * conversion layer injected that key for every HDFS-family conversion. The marker is added
     * only to the copy handed to {@code bind()} — {@link #getOrigProps()} and the
     * Broker/Local/Http verbatim backend maps stay clean — and only when a
     * {@code *config.resources} key is present (the only consumer of the marker).
     */
    private static Map<String, String> withHadoopConfigDir(Map<String, String> props) {
        if (props == null || props.containsKey("_HADOOP_CONFIG_DIR_")
                || StringUtils.isBlank(Config.hadoop_config_dir)) {
            return props;
        }
        boolean hasConfigResources = false;
        for (Map.Entry<String, String> entry : props.entrySet()) {
            if (entry.getKey() != null && entry.getKey().endsWith("config.resources")
                    && StringUtils.isNotBlank(entry.getValue())) {
                hasConfigResources = true;
                break;
            }
        }
        if (!hasConfigResources) {
            return props;
        }
        Map<String, String> copy = new HashMap<>(props);
        copy.put("_HADOOP_CONFIG_DIR_", Config.hadoop_config_dir);
        return copy;
    }

    /**
     * Builds a BROKER adapter with an explicitly supplied broker name, mirroring the legacy
     * {@code BrokerProperties.of(name, props)}: routing is bypassed (the props of a
     * {@code WITH BROKER "name"} clause carry no {@code broker.name} key), the raw props are
     * NOT mutated, and {@link #getBrokerName()} returns the supplied name.
     */
    public static StorageAdapter ofBroker(String brokerName, Map<String, String> origProps) {
        Map<String, String> props = origProps == null ? new HashMap<>() : origProps;
        for (FileSystemProvider<?> provider : manager().getProviders()) {
            if ("BROKER".equalsIgnoreCase(provider.name())) {
                return new StorageAdapter(provider.bind(props), props, brokerName);
            }
        }
        throw new StoragePropertiesException("Broker filesystem provider is not available");
    }

    private static FileSystemPluginManager manager() {
        FileSystemPluginManager managerFromStartup = pluginManager;
        if (managerFromStartup != null) {
            return managerFromStartup;
        }
        FileSystemPluginManager local = fallbackManager;
        if (local == null) {
            synchronized (StorageAdapter.class) {
                local = fallbackManager;
                if (local == null) {
                    LOG.warn("StorageAdapter used before initPluginManager; falling back to"
                            + " built-in providers loaded from the classpath. Expected in unit"
                            + " tests only — in production this means the FE startup sequence"
                            + " skipped filesystem plugin initialization and bindings may not"
                            + " match the configured plugin directory.");
                    local = new FileSystemPluginManager();
                    local.loadBuiltins();
                    fallbackManager = local;
                }
            }
        }
        return local;
    }

    /** The underlying SPI typed properties (for callers migrating deeper integrations). */
    public FileSystemProperties getSpiProperties() {
        return spi;
    }

    /** The raw user properties this adapter was bound from. */
    public Map<String, String> getOrigProps() {
        return origProps;
    }

    public StorageTypeId getType() {
        return type;
    }

    /** Legacy getStorageName() (2.4-1 mapping: every S3-compatible dialect reports "S3"). */
    public String getStorageName() {
        // provider-declared family: the 2.4-1 "every S3 dialect reports S3" mapping lives in the SPI
        return spi.storageFamilyName();
    }

    /** Legacy schemas() (drives ensureDisableCache): provider-declared legacy scheme set. */
    public Set<String> schemas() {
        // provider-declared legacy scheme set (ledger 2.4-6 note lives on legacyCacheSchemes)
        return spi.legacyCacheSchemes();
    }

    /** Hadoop configuration equivalent of legacy getHadoopStorageConfig(); null for BROKER/HTTP. */
    public Configuration getHadoopStorageConfig() {
        if (!hadoopStorageConfigBuilt) {
            synchronized (this) {
                if (!hadoopStorageConfigBuilt) {
                    hadoopStorageConfig = buildHadoopStorageConfig();
                    hadoopStorageConfigBuilt = true;
                }
            }
        }
        return hadoopStorageConfig;
    }

    /** Legacy isKerberos() — meaningful for the HDFS family only. */
    public boolean isKerberos() {
        return spi.toHadoopProperties().map(HadoopStorageProperties::isKerberos).orElse(false);
    }

    /** Authenticator for HDFS-family doAs execution; DIRECT passthrough for everything else. */
    public ExecutionAuthenticator getExecutionAuthenticator() {
        return spi.toHadoopProperties()
                .map(HadoopStorageProperties::getExecutionAuthenticator)
                .orElse(ExecutionAuthenticator.DIRECT);
    }

    /**
     * Legacy {@code HdfsProperties.isExplicitlyConfigured()}: {@code false} only for the
     * default-HDFS fallback binding that {@code bindAll}/legacy {@code createAll} auto-prepends
     * when nothing HDFS-like matched. A binding whose own provider matches the raw props
     * (explicit {@code fs.x.support} flag or guess heuristics) is explicit; non-HDFS types were
     * always explicit in fe-core.
     */
    public boolean isExplicitlyConfigured() {
        if (type != StorageTypeId.HDFS) {
            return true;
        }
        for (FileSystemProvider provider : manager().getProviders()) {
            if (providerKey.equalsIgnoreCase(provider.name())) {
                return provider.supportsExplicit(origProps) || provider.supportsGuess(origProps);
            }
        }
        return true;
    }

    /**
     * fe-core S3Properties credentials-provider mode ({@code s3.credentials_provider_type}
     * alias family, default DEFAULT). Non-null only for the S3 provider, mirroring the legacy
     * class layout where only the generic S3 typed class carried the mode.
     */
    public AwsCredentialsProviderMode getAwsCredentialsProviderMode() {
        return s3CredentialsMode;
    }

    /**
     * fe-core-only AWS SDK credentials accessor, mirroring the legacy typed classes'
     * {@code getAwsCredentialsProvider()} overrides exactly: static (optionally session)
     * credentials first for every S3-compatible dialect; the S3 provider adds assume-role and
     * the v1/v2 chain selection per {@code Config.aws_credentials_provider_version};
     * OSS/GCS/COS/OBS fall back to anonymous credentials when both AK and SK are blank;
     * everything else (Minio/Ozone/Azure and non-S3 types) returns {@code null}.
     */
    public AwsCredentialsProvider getAwsCredentialsProvider() {
        if (!(spi instanceof S3CompatibleFileSystemProperties)) {
            return null;
        }
        S3CompatibleFileSystemProperties s3 = (S3CompatibleFileSystemProperties) spi;
        AwsCredentialsProvider staticProvider = staticAwsCredentialsProvider(s3);
        if ("S3".equals(providerKey)) {
            return s3AwsCredentialsProvider(s3, staticProvider);
        }
        if (staticProvider != null) {
            return staticProvider;
        }
        switch (providerKey) {
            case "OSS":
            case "GCS":
            case "COS":
            case "OBS":
                // Align fe-core OSS/GCS/COS/OBS Properties: anonymous access when unauthenticated.
                if (StringUtils.isBlank(s3.getAccessKey()) && StringUtils.isBlank(s3.getSecretKey())) {
                    return AnonymousCredentialsProvider.create();
                }
                return null;
            default:
                return null;
        }
    }

    /** Align fe-core AbstractS3CompatibleProperties.getAwsCredentialsProvider (static creds only). */
    private static AwsCredentialsProvider staticAwsCredentialsProvider(S3CompatibleFileSystemProperties s3) {
        if (StringUtils.isNotBlank(s3.getAccessKey()) && StringUtils.isNotBlank(s3.getSecretKey())) {
            if (StringUtils.isEmpty(s3.getSessionToken())) {
                return StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(s3.getAccessKey(), s3.getSecretKey()));
            }
            return StaticCredentialsProvider.create(AwsSessionCredentials.create(
                    s3.getAccessKey(), s3.getSecretKey(), s3.getSessionToken()));
        }
        return null;
    }

    /** Align fe-core S3Properties.getAwsCredentialsProviderV1/V2 (assume-role + chain selection). */
    private AwsCredentialsProvider s3AwsCredentialsProvider(S3CompatibleFileSystemProperties s3,
            AwsCredentialsProvider staticProvider) {
        if (staticProvider != null) {
            return staticProvider;
        }
        boolean v2 = Config.aws_credentials_provider_version.equalsIgnoreCase("v2");
        if (StringUtils.isNotBlank(s3.getRoleArn())) {
            StsClient stsClient = StsClient.builder()
                    .region(Region.of(s3.getRegion()))
                    .credentialsProvider(v2
                            ? AwsCredentialsProviderFactory.createV2(s3CredentialsMode, false)
                            : InstanceProfileCredentialsProvider.create())
                    .build();
            return StsAssumeRoleCredentialsProvider.builder()
                    .stsClient(stsClient)
                    .refreshRequest(builder -> {
                        builder.roleArn(s3.getRoleArn()).roleSessionName("aws-sdk-java-v2-fe");
                        if (StringUtils.isNotBlank(s3.getExternalId())) {
                            builder.externalId(s3.getExternalId());
                        }
                    }).build();
        }
        // For anonymous access (no credentials required) v1 uses the anonymous provider; v2
        // delegates to the factory's default chain (which may include anonymous).
        return v2 ? AwsCredentialsProviderFactory.createV2(s3CredentialsMode, true)
                : AnonymousCredentialsProvider.create();
    }

    public String validateAndNormalizeUri(String uri) {
        // Align fe-core AbstractS3CompatibleProperties/AzureProperties: the SPI S3/Azure typed
        // props do not normalize URIs (compat schemes like cos:// must become s3:// before the
        // path reaches BE or a concrete filesystem), so the facade owns the legacy logic.
        if (spi instanceof S3CompatibleFileSystemProperties) {
            return StorageUriUtils.validateAndNormalizeS3Uri(uri,
                    ((S3CompatibleFileSystemProperties) spi).getUsePathStyle(),
                    forceParsingByStandardUriValue);
        }
        if ("AZURE".equals(providerKey)) {
            return StorageUriUtils.validateAndNormalizeAzureUri(uri);
        }
        if (type == StorageTypeId.HDFS && StorageUriUtils.isJfsLocation(uri)) {
            // Align fe-core: the legacy HDFS typed class accepted {hdfs, viewfs, jfs} while the
            // HDFS plugin's scheme identity excludes jfs (it has its own plugin), so the facade
            // owns the jfs leg of an HDFS binding.
            return StorageUriUtils.validateAndNormalizeJfsUri(uri);
        }
        return spi.validateAndNormalizeUri(uri);
    }

    public String validateAndGetUri(Map<String, String> loadProps) {
        // Align fe-core: the S3/Azure legacy classes return the RAW uri value (no normalization)
        // and throw when the map is empty or has no uri key; the SPI default would silently
        // return null instead.
        if (spi instanceof S3CompatibleFileSystemProperties) {
            return StorageUriUtils.validateAndGetS3Uri(loadProps);
        }
        if ("AZURE".equals(providerKey)) {
            return StorageUriUtils.validateAndGetAzureUri(loadProps);
        }
        if (type == StorageTypeId.HDFS && loadProps != null) {
            // jfs leg of the HDFS binding (see validateAndNormalizeUri above): a single
            // case-insensitive `uri` entry with a jfs scheme is normalized by the facade.
            String uriValue = loadProps.entrySet().stream()
                    .filter(e -> "uri".equalsIgnoreCase(e.getKey()))
                    .map(Map.Entry::getValue)
                    .filter(StringUtils::isNotBlank)
                    .findFirst()
                    .orElse(null);
            if (uriValue != null && uriValue.split(",").length == 1
                    && StorageUriUtils.isJfsLocation(uriValue)) {
                return StorageUriUtils.validateAndNormalizeJfsUri(uriValue);
            }
        }
        return spi.validateAndGetUri(loadProps);
    }

    /** fe-core S3 family alias resolution for force_parsing_by_standard_uri (see map above). */
    private String resolveForceParsingByStandardUri() {
        String prefix = FORCE_PARSING_PREFIXES.get(providerKey);
        String value = prefix == null ? null : getOrigPropIgnoreCase(prefix + ".force_parsing_by_standard_uri");
        if (StringUtils.isBlank(value)) {
            value = getOrigPropIgnoreCase("force_parsing_by_standard_uri");
        }
        return StringUtils.isNotBlank(value) ? value : "false";
    }

    private String getOrigPropIgnoreCase(String key) {
        for (Map.Entry<String, String> entry : origProps.entrySet()) {
            if (key.equalsIgnoreCase(entry.getKey())) {
                return entry.getValue();
            }
        }
        return null;
    }

    /** Legacy BrokerProperties.getBrokerName(): the explicit name from {@link #ofBroker} wins,
     *  otherwise a case-insensitive {@code broker.name} lookup on the raw props. */
    public String getBrokerName() {
        if (brokerNameOverride != null) {
            return brokerNameOverride;
        }
        return getOrigPropIgnoreCase("broker.name");
    }

    /** Legacy BrokerProperties.getBrokerParams(): {@code broker.}-prefixed keys, prefix stripped. */
    public Map<String, String> getBrokerParams() {
        Map<String, String> params = new HashMap<>();
        for (Map.Entry<String, String> entry : origProps.entrySet()) {
            if (entry.getKey().startsWith("broker.")) {
                params.put(entry.getKey().substring("broker.".length()), entry.getValue());
            }
        }
        return params;
    }

    /** Backend (BE) configuration map, key-for-key equal to the legacy typed classes. */
    public Map<String, String> getBackendConfigProperties() {
        if (backendConfigProperties == null) {
            backendConfigProperties = computeBackendConfigProperties();
        }
        return backendConfigProperties;
    }

    private Map<String, String> computeBackendConfigProperties() {
        switch (type) {
            case BROKER:
            case LOCAL:
            case HTTP:
                // Align fe-core: Broker/Local/Http return the raw user properties verbatim.
                return origProps;
            case AZURE:
                return azureBackendConfigProperties();
            default:
                break;
        }
        Map<String, String> base = spi.toBackendProperties()
                .orElseThrow(() -> new IllegalStateException(
                        "Provider " + providerKey + " exposes no backend properties"))
                .toMap();
        if (spi instanceof S3CompatibleFileSystemProperties) {
            return alignS3FamilyBackendMap((S3CompatibleFileSystemProperties) spi, base);
        }
        if ("JFS".equals(providerKey)) {
            return alignJfsBackendMap(base);
        }
        return base;
    }

    /**
     * Align fe-core, ledger 2.4-7: with OAuth2 the legacy AzureProperties dumps the ENTIRE
     * Hadoop Configuration (hadoop defaults + fs.azure.* + user fs.* passthrough +
     * disable-cache keys) as the backend map; shared-key uses the SPI's exact 7-key map.
     */
    private Map<String, String> azureBackendConfigProperties() {
        if (!isAzureOauth2()) {
            return spi.toBackendProperties().orElseThrow().toMap();
        }
        Map<String, String> dump = new HashMap<>();
        getHadoopStorageConfig().forEach(entry -> dump.put(entry.getKey(), entry.getValue()));
        return dump;
    }

    /**
     * Backend-map reconciliation for every S3-compatible provider (S3/OSS/OBS/COS/GCS/MinIO/Ozone).
     */
    private Map<String, String> alignS3FamilyBackendMap(S3CompatibleFileSystemProperties s3,
            Map<String, String> base) {
        Map<String, String> aligned = new HashMap<>(base);
        // Align fe-core, ledger 2.4-5: fe-core never emits AWS_BUCKET / AWS_ROOT_PATH to BE.
        aligned.remove("AWS_BUCKET");
        aligned.remove("AWS_ROOT_PATH");
        // Align fe-core, ledger 2.4-5: endpoint/region/AK/SK are emitted unconditionally —
        // anonymous access sends empty-string AWS_ACCESS_KEY/AWS_SECRET_KEY, not absent keys.
        aligned.put("AWS_ENDPOINT", s3.getEndpoint());
        aligned.put("AWS_REGION", s3.getRegion());
        aligned.put("AWS_ACCESS_KEY", s3.getAccessKey());
        aligned.put("AWS_SECRET_KEY", s3.getSecretKey());
        if ("S3".equals(providerKey)) {
            // Align fe-core, ledger 2.4-3/5: S3Properties always emits the mode name (default
            // DEFAULT, even with static AK/SK), parsed by the fe-core enum. AWS_ROLE_ARN /
            // AWS_EXTERNAL_ID pass through unconditionally when set (the SPI already emits them).
            aligned.put("AWS_CREDENTIALS_PROVIDER_TYPE", s3CredentialsMode.getMode());
        } else {
            // Align fe-core, ledger 2.4-5: non-S3 dialects never emit role keys, and emit
            // AWS_CREDENTIALS_PROVIDER_TYPE=ANONYMOUS exactly when both AK and SK are blank.
            aligned.remove("AWS_ROLE_ARN");
            aligned.remove("AWS_EXTERNAL_ID");
            if (s3.hasStaticCredentials()) {
                aligned.remove("AWS_CREDENTIALS_PROVIDER_TYPE");
            } else {
                aligned.put("AWS_CREDENTIALS_PROVIDER_TYPE", AwsCredentialsProviderMode.ANONYMOUS.name());
            }
        }
        return aligned;
    }

    /**
     * Align fe-core (Phase A ledger item ②): fe-core rode jfs:// on HdfsProperties, whose
     * backend map always carries the ipc fallback flag and the hdfs/hadoop authentication keys;
     * the JFS SPI plugin intentionally emits neither. Re-derive them here from the raw props
     * exactly the way HdfsProperties.initBackendConfigProperties did.
     */
    private Map<String, String> alignJfsBackendMap(Map<String, String> base) {
        Map<String, String> aligned = new HashMap<>(base);
        String ipcFallback = origProps.get("ipc.client.fallback-to-simple-auth-allowed");
        aligned.put("ipc.client.fallback-to-simple-auth-allowed",
                StringUtils.isNotBlank(ipcFallback) ? ipcFallback : "true");
        String authType = firstNonBlank(origProps.get("hdfs.authentication.type"),
                origProps.get("hadoop.security.authentication"), "simple");
        aligned.put("hdfs.security.authentication", authType);
        if ("kerberos".equalsIgnoreCase(authType)) {
            aligned.put("hadoop.security.authentication", "kerberos");
            aligned.put("hadoop.kerberos.principal",
                    firstNonBlank(origProps.get("hdfs.authentication.kerberos.principal"),
                            origProps.get("hadoop.kerberos.principal"), ""));
            aligned.put("hadoop.kerberos.keytab",
                    firstNonBlank(origProps.get("hdfs.authentication.kerberos.keytab"),
                            origProps.get("hadoop.kerberos.keytab"), ""));
        }
        return aligned;
    }

    private Configuration buildHadoopStorageConfig() {
        switch (type) {
            case BROKER:
            case HTTP:
                // Align fe-core: BrokerProperties/HttpProperties leave hadoopStorageConfig null.
                return null;
            case LOCAL:
                return buildLocalHadoopConfig();
            default:
                break;
        }
        // Align fe-core, ledger 2.4-2: `new Configuration()` loads core-default/core-site,
        // where the SPI map is a bare key-value view.
        Configuration conf = new Configuration();
        Map<String, String> spiMap = spi.toHadoopProperties()
                .orElseThrow(() -> new IllegalStateException(
                        "Provider " + providerKey + " exposes no hadoop properties"))
                .toHadoopConfigurationMap();
        if ("JFS".equals(providerKey)) {
            // fe-core builds the HDFS-family Configuration FROM the backend map, so the JFS
            // auth-key alignment (see alignJfsBackendMap) must be materialized here too.
            spiMap = alignJfsBackendMap(spiMap);
        }
        boolean isS3 = "S3".equals(providerKey);
        boolean skipGcsAnonProvider = isGcsAnonymous();
        for (Map.Entry<String, String> entry : spiMap.entrySet()) {
            if (isS3 && S3_CREDENTIAL_KEYS.contains(entry.getKey())) {
                // Re-derived below with fe-core Config gating (align fe-core, ledger 2.4-3).
                continue;
            }
            if (skipGcsAnonProvider && "fs.s3a.aws.credentials.provider".equals(entry.getKey())) {
                // Align fe-core: GCSProperties never sets an anonymous s3a credentials provider;
                // the SPI's AnonymousAWSCredentialsProvider extra is dropped here.
                continue;
            }
            conf.set(entry.getKey(), entry.getValue());
        }
        if (isS3) {
            applyS3CredentialProviders(conf, (S3CompatibleFileSystemProperties) spi);
        }
        if ("AZURE".equals(providerKey) && !isAzureOauth2()) {
            applyAzureAccountKeysFromConfig(conf);
        }
        appendUserFsConfig(conf);
        ensureDisableCache(conf);
        return conf;
    }

    private Configuration buildLocalHadoopConfig() {
        // Align fe-core LocalProperties.initializeHadoopStorageConfig (Local has no SPI
        // hadoop view; the two impl keys are fe-core knowledge).
        Configuration conf = new Configuration();
        conf.set("fs.local.impl", "org.apache.hadoop.fs.LocalFileSystem");
        conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
        appendUserFsConfig(conf);
        ensureDisableCache(conf);
        return conf;
    }

    /**
     * Align fe-core, ledger 2.4-3: S3Properties only emits assumed-role keys when accessKey is
     * blank, and selects the provider chain per Config.aws_credentials_provider_version — the
     * SPI cannot see fe-core Config, so these four keys are owned by the facade.
     */
    private void applyS3CredentialProviders(Configuration conf, S3CompatibleFileSystemProperties s3) {
        boolean v2 = Config.aws_credentials_provider_version.equalsIgnoreCase("v2");
        if (StringUtils.isNotBlank(s3.getAccessKey())) {
            // Static credentials win; access/secret/session keys came from the SPI map.
            conf.set("fs.s3a.aws.credentials.provider", SIMPLE_AWS_CREDENTIALS_PROVIDER);
            return;
        }
        if (StringUtils.isNotBlank(s3.getRoleArn())) {
            conf.set("fs.s3a.assumed.role.arn", s3.getRoleArn());
            conf.set("fs.s3a.aws.credentials.provider", ASSUMED_ROLE_CREDENTIAL_PROVIDER);
            conf.set("fs.s3a.assumed.role.credentials.provider",
                    v2 ? AwsCredentialsProviderFactory.getV2ClassName(s3CredentialsMode, false)
                            : InstanceProfileCredentialsProvider.class.getName());
            if (StringUtils.isNotBlank(s3.getExternalId())) {
                conf.set("fs.s3a.assumed.role.external.id", s3.getExternalId());
            }
            return;
        }
        if (v2) {
            conf.set("fs.s3a.aws.credentials.provider",
                    AwsCredentialsProviderFactory.getV2ClassName(s3CredentialsMode, true));
        }
        // v1 + anonymous: fe-core sets nothing and leaves the hadoop default untouched.
    }

    /**
     * Align fe-core AzureProperties.setHDFSAzureAccountKeys: shared-key account keys are derived
     * from Config.azure_blob_host_suffixes (blob + dfs endpoints, admin-extensible), not from the
     * SPI's static suffix list. Runs before appendUserFsConfig so explicit user values still win.
     */
    private void applyAzureAccountKeysFromConfig(Configuration conf) {
        Map<String, String> backend = spi.toBackendProperties().orElseThrow().toMap();
        String accountName = backend.get("AWS_ACCESS_KEY");
        String accountKey = backend.get("AWS_SECRET_KEY");
        Set<String> suffixes = new LinkedHashSet<>();
        if (Config.azure_blob_host_suffixes != null) {
            for (String suffix : Config.azure_blob_host_suffixes) {
                if (StringUtils.isBlank(suffix)) {
                    continue;
                }
                String normalized = suffix.trim().toLowerCase(Locale.ROOT);
                if (normalized.startsWith(".")) {
                    normalized = normalized.substring(1);
                }
                if (!normalized.isEmpty()) {
                    suffixes.add(normalized);
                }
            }
        }
        for (String suffix : suffixes) {
            conf.set(String.format("fs.azure.account.key.%s.%s", accountName, suffix), accountKey);
        }
        conf.set("fs.azure.account.key", accountKey);
    }

    /** Ledger 2.4-8: user fs.* keys with non-blank values pass through into the configuration. */
    private void appendUserFsConfig(Configuration conf) {
        origProps.forEach((key, value) -> {
            if (key.startsWith("fs.") && StringUtils.isNotBlank(value)) {
                conf.set(key, value);
            }
        });
    }

    /**
     * Ledger 2.4-8: per-schema FileSystem cache disabling over the LEGACY schemas() set, with an
     * explicit user value taking precedence — copied from StorageProperties.ensureDisableCache.
     */
    private void ensureDisableCache(Configuration conf) {
        for (String schema : schemas()) {
            String key = "fs." + schema + ".impl.disable.cache";
            String userValue = origProps.get(key);
            if (StringUtils.isNotBlank(userValue)) {
                conf.setBoolean(key, BooleanUtils.toBoolean(userValue));
            } else {
                conf.setBoolean(key, true);
            }
        }
    }

    /** fe-core S3Properties alias list for s3.credentials_provider_type (no AWS_* alias). */
    private String feCoreS3CredentialsProviderType() {
        return firstNonBlank(origProps.get("s3.credentials_provider_type"),
                origProps.get("glue.credentials_provider_type"),
                origProps.get("iceberg.rest.credentials_provider_type"));
    }

    private boolean isAzureOauth2() {
        return "OAuth2".equalsIgnoreCase(origProps.getOrDefault("azure.auth_type", "SharedKey"));
    }

    private boolean isGcsAnonymous() {
        if (!"GCS".equals(providerKey) || !(spi instanceof S3CompatibleFileSystemProperties)) {
            return false;
        }
        return !((S3CompatibleFileSystemProperties) spi).hasStaticCredentials();
    }

    /**
     * Align fe-core AzureProperties.initNormalizeAndCheckProps: the temporary fe-core-only
     * restriction that OAuth2 is supported only for the Iceberg REST catalog. This check reads
     * catalog-level keys the SPI never sees, so it stays in the facade.
     */
    private void checkAzureOauth2OnlyForIcebergRest() {
        // Align fe-core AzureProperties exactly: the REST-only gate compared CASE-SENSITIVELY
        // ("OAuth2".equals(...)) while the rest of the class used equalsIgnoreCase — so a
        // lowercase "oauth2" slipped past this gate in legacy (and old images may carry it).
        // Replicate the asymmetry; tightening it would break replay of such images.
        if (!"AZURE".equals(providerKey)
                || !"OAuth2".equals(origProps.getOrDefault("azure.auth_type", "SharedKey"))) {
            return;
        }
        boolean hasIcebergType = origProps.entrySet().stream()
                .anyMatch(entry -> "type".equalsIgnoreCase(entry.getKey())
                        && "iceberg".equalsIgnoreCase(entry.getValue()));
        boolean hasTypeKey = origProps.keySet().stream().anyMatch("type"::equalsIgnoreCase);
        boolean isIcebergRest = (hasIcebergType || !hasTypeKey)
                && origProps.entrySet().stream()
                        .anyMatch(entry -> "iceberg.catalog.type".equalsIgnoreCase(entry.getKey())
                                && "rest".equalsIgnoreCase(entry.getValue()));
        if (!isIcebergRest) {
            throw new UnsupportedOperationException(
                    "OAuth2 auth type is only supported for iceberg rest catalog");
        }
    }

    private static String firstNonBlank(String... values) {
        for (String value : values) {
            if (StringUtils.isNotBlank(value)) {
                return value;
            }
        }
        return null;
    }

    /**
     * Value equality over (provider, raw properties, broker-name override), mirroring the legacy
     * {@code ConnectionProperties.equals}: logically identical configurations must share one
     * {@code FileSystemCache} key so equal-config rebinds (e.g. catalog property rollback)
     * re-hit cached filesystems instead of duplicating them.
     */
    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof StorageAdapter)) {
            return false;
        }
        StorageAdapter that = (StorageAdapter) other;
        return providerKey.equals(that.providerKey)
                && origProps.equals(that.origProps)
                && Objects.equals(brokerNameOverride, that.brokerNameOverride);
    }

    @Override
    public int hashCode() {
        return Objects.hash(providerKey, origProps, brokerNameOverride);
    }
}
