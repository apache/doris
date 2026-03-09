/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package software.amazon.awssdk.core.client.builder;

import org.apache.doris.common.Config;
import org.apache.doris.common.util.UncloseableScheduledExecutorService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.annotations.SdkPreviewApi;
import software.amazon.awssdk.annotations.SdkProtectedApi;
import software.amazon.awssdk.annotations.SdkTestInternalApi;
import software.amazon.awssdk.core.ClientEndpointProvider;
import software.amazon.awssdk.core.ClientType;
import software.amazon.awssdk.core.CompressionConfiguration;
import software.amazon.awssdk.core.SdkPlugin;
import software.amazon.awssdk.core.SdkSystemSetting;
import software.amazon.awssdk.core.client.config.ClientAsyncConfiguration;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkClientConfiguration;
import software.amazon.awssdk.core.client.config.SdkClientOption;
import software.amazon.awssdk.core.interceptor.ClasspathInterceptorChainFactory;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.core.internal.http.loader.DefaultSdkAsyncHttpClientBuilder;
import software.amazon.awssdk.core.internal.http.loader.DefaultSdkHttpClientBuilder;
import software.amazon.awssdk.core.internal.http.pipeline.stages.CompressRequestStage;
import software.amazon.awssdk.core.internal.interceptor.HttpChecksumValidationInterceptor;
import software.amazon.awssdk.core.internal.retry.SdkDefaultRetryStrategy;
import software.amazon.awssdk.core.internal.useragent.AppIdResolver;
import software.amazon.awssdk.core.internal.useragent.SdkClientUserAgentProperties;
import software.amazon.awssdk.core.internal.useragent.SdkUserAgentBuilder;
import software.amazon.awssdk.core.retry.RetryMode;
import software.amazon.awssdk.core.util.SystemUserAgent;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.async.AsyncExecuteRequest;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.identity.spi.IdentityProviders;
import software.amazon.awssdk.metrics.MetricPublisher;
import software.amazon.awssdk.profiles.ProfileFile;
import software.amazon.awssdk.profiles.ProfileFileSystemSetting;
import software.amazon.awssdk.profiles.ProfileProperty;
import software.amazon.awssdk.retries.api.RetryStrategy;
import software.amazon.awssdk.utils.AttributeMap;
import software.amazon.awssdk.utils.AttributeMap.LazyValueSource;
import software.amazon.awssdk.utils.Either;
import software.amazon.awssdk.utils.Lazy;
import software.amazon.awssdk.utils.OptionalUtils;
import software.amazon.awssdk.utils.StringUtils;
import software.amazon.awssdk.utils.ThreadFactoryBuilder;
import software.amazon.awssdk.utils.Validate;
import software.amazon.awssdk.utils.http.SdkHttpUtils;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import static software.amazon.awssdk.core.ClientType.ASYNC;
import static software.amazon.awssdk.core.ClientType.SYNC;
import static software.amazon.awssdk.core.client.config.SdkAdvancedAsyncClientOption.FUTURE_COMPLETION_EXECUTOR;
import static software.amazon.awssdk.core.client.config.SdkAdvancedClientOption.USER_AGENT_PREFIX;
import static software.amazon.awssdk.core.client.config.SdkAdvancedClientOption.USER_AGENT_SUFFIX;
import static software.amazon.awssdk.core.client.config.SdkClientOption.ADDITIONAL_HTTP_HEADERS;
import static software.amazon.awssdk.core.client.config.SdkClientOption.ASYNC_HTTP_CLIENT;
import static software.amazon.awssdk.core.client.config.SdkClientOption.CLIENT_TYPE;
import static software.amazon.awssdk.core.client.config.SdkClientOption.CLIENT_USER_AGENT;
import static software.amazon.awssdk.core.client.config.SdkClientOption.COMPRESSION_CONFIGURATION;
import static software.amazon.awssdk.core.client.config.SdkClientOption.CONFIGURED_ASYNC_HTTP_CLIENT;
import static software.amazon.awssdk.core.client.config.SdkClientOption.CONFIGURED_ASYNC_HTTP_CLIENT_BUILDER;
import static software.amazon.awssdk.core.client.config.SdkClientOption.CONFIGURED_COMPRESSION_CONFIGURATION;
import static software.amazon.awssdk.core.client.config.SdkClientOption.CONFIGURED_RETRY_CONFIGURATOR;
import static software.amazon.awssdk.core.client.config.SdkClientOption.CONFIGURED_RETRY_MODE;
import static software.amazon.awssdk.core.client.config.SdkClientOption.CONFIGURED_RETRY_STRATEGY;
import static software.amazon.awssdk.core.client.config.SdkClientOption.CONFIGURED_SCHEDULED_EXECUTOR_SERVICE;
import static software.amazon.awssdk.core.client.config.SdkClientOption.CONFIGURED_SYNC_HTTP_CLIENT;
import static software.amazon.awssdk.core.client.config.SdkClientOption.CONFIGURED_SYNC_HTTP_CLIENT_BUILDER;
import static software.amazon.awssdk.core.client.config.SdkClientOption.CRC32_FROM_COMPRESSED_DATA_ENABLED;
import static software.amazon.awssdk.core.client.config.SdkClientOption.DEFAULT_RETRY_MODE;
import static software.amazon.awssdk.core.client.config.SdkClientOption.EXECUTION_INTERCEPTORS;
import static software.amazon.awssdk.core.client.config.SdkClientOption.HTTP_CLIENT_CONFIG;
import static software.amazon.awssdk.core.client.config.SdkClientOption.IDENTITY_PROVIDERS;
import static software.amazon.awssdk.core.client.config.SdkClientOption.INTERNAL_USER_AGENT;
import static software.amazon.awssdk.core.client.config.SdkClientOption.METRIC_PUBLISHERS;
import static software.amazon.awssdk.core.client.config.SdkClientOption.PROFILE_FILE;
import static software.amazon.awssdk.core.client.config.SdkClientOption.PROFILE_FILE_SUPPLIER;
import static software.amazon.awssdk.core.client.config.SdkClientOption.PROFILE_NAME;
import static software.amazon.awssdk.core.client.config.SdkClientOption.RETRY_STRATEGY;
import static software.amazon.awssdk.core.client.config.SdkClientOption.SCHEDULED_EXECUTOR_SERVICE;
import static software.amazon.awssdk.core.client.config.SdkClientOption.SYNC_HTTP_CLIENT;
import static software.amazon.awssdk.core.client.config.SdkClientOption.USER_AGENT_APP_ID;
import static software.amazon.awssdk.core.internal.useragent.UserAgentConstant.APP_ID;
import static software.amazon.awssdk.core.internal.useragent.UserAgentConstant.HTTP;
import static software.amazon.awssdk.core.internal.useragent.UserAgentConstant.INTERNAL_METADATA_MARKER;
import static software.amazon.awssdk.core.internal.useragent.UserAgentConstant.IO;
import static software.amazon.awssdk.utils.CollectionUtils.mergeLists;
import static software.amazon.awssdk.utils.Validate.paramNotNull;

/**
 * An SDK-internal implementation of the methods in {@link SdkClientBuilder}, {@link SdkAsyncClientBuilder} and
 * {@link SdkSyncClientBuilder}. This implements all methods required by those interfaces, allowing service-specific builders to
 * just implement the configuration they wish to add.
 *
 * <p>By implementing both the sync and async interface's methods, service-specific builders can share code between their sync
 * and
 * async variants without needing one to extend the other. Note: This only defines the methods in the sync and async builder
 * interfaces. It does not implement the interfaces themselves. This is because the sync and async client builder interfaces both
 * require a type-constrained parameter for use in fluent chaining, and a generic type parameter conflict is introduced into the
 * class hierarchy by this interface extending the builder interfaces themselves.</p>
 *
 * <p>Like all {@link SdkClientBuilder}s, this class is not thread safe.</p>
 *
 * @param <B> The type of builder, for chaining.
 * @param <C> The type of client generated by this builder.
 */
/**
 * This class(software.amazon.awssdk.core.client.builder.SdkDefaultClientBuilder) is copied from AWS SDK v2 (sdk-core 2.29.52),
 * with minor modifications.
 */
@SdkProtectedApi
public abstract class SdkDefaultClientBuilder<B extends SdkClientBuilder<B, C>, C> implements SdkClientBuilder<B, C> {
    private static final Logger LOG = LogManager.getLogger(SdkDefaultClientBuilder.class);

    private static final SdkHttpClient.Builder DEFAULT_HTTP_CLIENT_BUILDER = new DefaultSdkHttpClientBuilder();
    private static final SdkAsyncHttpClient.Builder DEFAULT_ASYNC_HTTP_CLIENT_BUILDER = new DefaultSdkAsyncHttpClientBuilder();

    protected final SdkClientConfiguration.Builder clientConfiguration = SdkClientConfiguration.builder();

    protected final AttributeMap.Builder clientContextParams = AttributeMap.builder();
    protected ClientOverrideConfiguration overrideConfig;
    private final SdkHttpClient.Builder defaultHttpClientBuilder;
    private final SdkAsyncHttpClient.Builder defaultAsyncHttpClientBuilder;
    private final List<SdkPlugin> plugins = new ArrayList<>();
    private static final ScheduledExecutorService awsSdkScheduler;

    static {
        ScheduledExecutorService realScheduler =
                Executors.newScheduledThreadPool(
                        Config.aws_sdk_async_scheduler_thread_pool_size,
                        r -> {
                            Thread t = new Thread(r, "aws-sdk-scheduler");
                            t.setDaemon(true);
                            return t;
                        }
                );

        awsSdkScheduler = new UncloseableScheduledExecutorService(realScheduler);
    }


    protected SdkDefaultClientBuilder() {
        this(DEFAULT_HTTP_CLIENT_BUILDER, DEFAULT_ASYNC_HTTP_CLIENT_BUILDER);
    }

    @SdkTestInternalApi
    protected SdkDefaultClientBuilder(SdkHttpClient.Builder defaultHttpClientBuilder,
                                      SdkAsyncHttpClient.Builder defaultAsyncHttpClientBuilder) {
        this.defaultHttpClientBuilder = defaultHttpClientBuilder;
        this.defaultAsyncHttpClientBuilder = defaultAsyncHttpClientBuilder;
    }

    /**
     * Build a client using the current state of this builder. This is marked final in order to allow this class to add standard
     * "build" logic between all service clients. Service clients are expected to implement the {@link #buildClient} method, that
     * accepts the immutable client configuration generated by this build method.
     */
    @Override
    public final C build() {
        return buildClient();
    }

    /**
     * Implemented by child classes to create a client using the provided immutable configuration objects. The async and sync
     * configurations are not yet immutable. Child classes will need to make them immutable in order to validate them and pass
     * them to the client's constructor.
     *
     * @return A client based on the provided configuration.
     */
    protected abstract C buildClient();

    /**
     * Return a client configuration object, populated with the following chain of priorities.
     * <ol>
     * <li>Client Configuration Overrides</li>
     * <li>Customer Configuration</li>
     * <li>Service-Specific Defaults</li>
     * <li>Global Defaults</li>
     * </ol>
     */
    protected final SdkClientConfiguration syncClientConfiguration() {
        clientConfiguration.option(SdkClientOption.CLIENT_CONTEXT_PARAMS, clientContextParams.build());
        SdkClientConfiguration configuration = clientConfiguration.build();

        // Apply overrides
        configuration = setOverrides(configuration);

        // Apply defaults
        configuration = mergeChildDefaults(configuration);
        configuration = mergeGlobalDefaults(configuration);

        // Create additional configuration from the default-applied configuration
        configuration = finalizeChildConfiguration(configuration);
        configuration = finalizeSyncConfiguration(configuration);
        configuration = finalizeConfiguration(configuration);

        // Invoke the plugins
        configuration = invokePlugins(configuration);

        return configuration;
    }

    /**
     * Return a client configuration object, populated with the following chain of priorities.
     * <ol>
     * <li>Client Configuration Overrides</li>
     * <li>Customer Configuration</li>
     * <li>Implementation/Service-Specific Configuration</li>
     * <li>Global Default Configuration</li>
     * </ol>
     */
    protected final SdkClientConfiguration asyncClientConfiguration() {
        clientConfiguration.option(SdkClientOption.CLIENT_CONTEXT_PARAMS, clientContextParams.build());
        SdkClientConfiguration configuration = clientConfiguration.build();

        // Apply overrides
        configuration = setOverrides(configuration);

        // Apply defaults
        configuration = mergeChildDefaults(configuration);
        configuration = mergeGlobalDefaults(configuration);

        // Create additional configuration from the default-applied configuration
        configuration = finalizeChildConfiguration(configuration);
        configuration = finalizeAsyncConfiguration(configuration);
        configuration = finalizeConfiguration(configuration);

        // Invoke the plugins
        configuration = invokePlugins(configuration);

        return configuration;
    }

    /**
     * Apply the client override configuration to the provided configuration. This generally does not need to be overridden by
     * child classes, but some previous client versions override it.
     */
    protected SdkClientConfiguration setOverrides(SdkClientConfiguration configuration) {
        if (overrideConfig == null) {
            return configuration;
        }
        SdkClientConfiguration.Builder builder = configuration.toBuilder();
        overrideConfig.retryStrategy().ifPresent(retryStrategy -> builder.option(RETRY_STRATEGY, retryStrategy));
        overrideConfig.retryMode().ifPresent(retryMode -> builder.option(RETRY_STRATEGY,
                SdkDefaultRetryStrategy.forRetryMode(retryMode)));
        overrideConfig.retryStrategyConfigurator().ifPresent(configurator -> {
            RetryStrategy.Builder<?, ?> defaultBuilder = SdkDefaultRetryStrategy.defaultRetryStrategy().toBuilder();
            configurator.accept(defaultBuilder);
            builder.option(RETRY_STRATEGY, defaultBuilder.build());
        });
        builder.putAll(overrideConfig);
        // Forget anything we configured in the override configuration else it might be re-applied.
        builder.option(CONFIGURED_RETRY_MODE, null);
        builder.option(CONFIGURED_RETRY_STRATEGY, null);
        builder.option(CONFIGURED_RETRY_CONFIGURATOR, null);
        return builder.build();
    }


    /**
     * Optionally overridden by child implementations to apply implementation-specific default configuration.
     * (eg. AWS's default credentials providers)
     */
    protected SdkClientConfiguration mergeChildDefaults(SdkClientConfiguration configuration) {
        return configuration;
    }

    /**
     * Apply global default configuration
     */
    private SdkClientConfiguration mergeGlobalDefaults(SdkClientConfiguration configuration) {
        Supplier<ProfileFile> defaultProfileFileSupplier = new Lazy<>(ProfileFile::defaultProfileFile)::getValue;

        configuration = configuration.merge(c -> c.option(EXECUTION_INTERCEPTORS, new ArrayList<>())
                .option(METRIC_PUBLISHERS, new ArrayList<>())
                .option(ADDITIONAL_HTTP_HEADERS, new LinkedHashMap<>())
                .option(PROFILE_FILE_SUPPLIER, defaultProfileFileSupplier)
                .lazyOption(PROFILE_FILE, conf -> conf.get(PROFILE_FILE_SUPPLIER).get())
                .option(PROFILE_NAME,
                        ProfileFileSystemSetting.AWS_PROFILE.getStringValueOrThrow())
                .option(USER_AGENT_PREFIX, "")
                .option(USER_AGENT_SUFFIX, "")
                .option(CRC32_FROM_COMPRESSED_DATA_ENABLED, false)
                .option(CONFIGURED_COMPRESSION_CONFIGURATION,
                        CompressionConfiguration.builder().build()));
        return configuration;
    }

    /**
     * Optionally overridden by child implementations to derive implementation-specific configuration from the
     * default-applied configuration. (eg. AWS's endpoint, derived from the region).
     */
    protected SdkClientConfiguration finalizeChildConfiguration(SdkClientConfiguration configuration) {
        return configuration;
    }

    /**
     * Finalize sync-specific configuration from the default-applied configuration.
     */
    private SdkClientConfiguration finalizeSyncConfiguration(SdkClientConfiguration config) {
        return config.toBuilder()
                .lazyOption(SdkClientOption.SYNC_HTTP_CLIENT, c -> resolveSyncHttpClient(c, config))
                .option(SdkClientOption.CLIENT_TYPE, SYNC)
                .build();
    }

    /**
     * Finalize async-specific configuration from the default-applied configuration.
     */
    private SdkClientConfiguration finalizeAsyncConfiguration(SdkClientConfiguration config) {
        return config.toBuilder()
                .lazyOptionIfAbsent(FUTURE_COMPLETION_EXECUTOR, this::resolveAsyncFutureCompletionExecutor)
                .lazyOption(ASYNC_HTTP_CLIENT, c -> resolveAsyncHttpClient(c, config))
                .option(SdkClientOption.CLIENT_TYPE, ASYNC)
                .build();
    }

    /**
     * Finalize global configuration from the default-applied configuration.
     */
    private SdkClientConfiguration finalizeConfiguration(SdkClientConfiguration config) {
        return config.toBuilder()
                .lazyOption(SCHEDULED_EXECUTOR_SERVICE, this::resolveScheduledExecutorService)
                .lazyOptionIfAbsent(RETRY_STRATEGY, this::resolveRetryStrategy)
                .option(EXECUTION_INTERCEPTORS, resolveExecutionInterceptors(config))
                .lazyOption(CLIENT_USER_AGENT, this::resolveClientUserAgent)
                .lazyOption(COMPRESSION_CONFIGURATION, this::resolveCompressionConfiguration)
                .lazyOptionIfAbsent(IDENTITY_PROVIDERS, c -> IdentityProviders.builder().build())
                .build();
    }

    private CompressionConfiguration resolveCompressionConfiguration(LazyValueSource config) {
        CompressionConfiguration compressionConfig = config.get(CONFIGURED_COMPRESSION_CONFIGURATION);
        return compressionConfig.toBuilder()
                .requestCompressionEnabled(resolveCompressionEnabled(config, compressionConfig))
                .minimumCompressionThresholdInBytes(resolveMinCompressionThreshold(config, compressionConfig))
                .build();
    }

    private Boolean resolveCompressionEnabled(LazyValueSource config, CompressionConfiguration compressionConfig) {
        Supplier<Optional<Boolean>> systemSettingConfiguration =
                () -> SdkSystemSetting.AWS_DISABLE_REQUEST_COMPRESSION.getBooleanValue()
                        .map(v -> !v);

        Supplier<Optional<Boolean>> profileFileConfiguration =
                () -> config.get(PROFILE_FILE_SUPPLIER).get()
                        .profile(config.get(PROFILE_NAME))
                        .flatMap(p -> p.booleanProperty(ProfileProperty.DISABLE_REQUEST_COMPRESSION))
                        .map(v -> !v);

        return OptionalUtils.firstPresent(Optional.ofNullable(compressionConfig.requestCompressionEnabled()),
                        systemSettingConfiguration,
                        profileFileConfiguration)
                .orElse(true);
    }

    private Integer resolveMinCompressionThreshold(LazyValueSource config, CompressionConfiguration compressionConfig) {
        Supplier<Optional<Integer>> systemSettingConfiguration =
                SdkSystemSetting.AWS_REQUEST_MIN_COMPRESSION_SIZE_BYTES::getIntegerValue;

        Supplier<Optional<Integer>> profileFileConfiguration =
                () -> config.get(PROFILE_FILE_SUPPLIER).get()
                        .profile(config.get(PROFILE_NAME))
                        .flatMap(p -> p.property(ProfileProperty.REQUEST_MIN_COMPRESSION_SIZE_BYTES))
                        .map(Integer::parseInt);

        return OptionalUtils.firstPresent(Optional.ofNullable(compressionConfig.minimumCompressionThresholdInBytes()),
                        systemSettingConfiguration,
                        profileFileConfiguration)
                .orElse(CompressRequestStage.DEFAULT_MIN_COMPRESSION_SIZE);
    }

    /**
     * By default, returns the configuration as-is. Classes extending this method will take care of running the plugins and
     * return the updated configuration if plugins are supported.
     */
    @SdkPreviewApi
    protected SdkClientConfiguration invokePlugins(SdkClientConfiguration config) {
        return config;
    }

    private String resolveClientUserAgent(LazyValueSource config) {
        SdkClientUserAgentProperties clientProperties = new SdkClientUserAgentProperties();

        ClientType clientType = config.get(CLIENT_TYPE);
        ClientType resolvedClientType = clientType == null ? ClientType.UNKNOWN : clientType;

        clientProperties.putProperty(INTERNAL_METADATA_MARKER, StringUtils.trimToEmpty(config.get(INTERNAL_USER_AGENT)));
        clientProperties.putProperty(IO, StringUtils.lowerCase(resolvedClientType.name()));
        clientProperties.putProperty(HTTP, SdkHttpUtils.urlEncode(clientName(resolvedClientType,
                config.get(SYNC_HTTP_CLIENT),
                config.get(ASYNC_HTTP_CLIENT))));
        String appId = config.get(USER_AGENT_APP_ID);
        String resolvedAppId = appId == null ? resolveAppId(config) : appId;
        clientProperties.putProperty(APP_ID, resolvedAppId);
        return SdkUserAgentBuilder.buildClientUserAgentString(SystemUserAgent.getOrCreate(), clientProperties);
    }

    private String resolveAppId(LazyValueSource config) {
        Optional<String> appIdFromConfig = AppIdResolver.create()
                .profileFile(config.get(PROFILE_FILE_SUPPLIER))
                .profileName(config.get(PROFILE_NAME))
                .resolve();
        return appIdFromConfig.orElse(null);
    }

    private static String clientName(ClientType clientType, SdkHttpClient syncHttpClient, SdkAsyncHttpClient asyncHttpClient) {
        if (clientType == SYNC) {
            return syncHttpClient == null ? "null" : syncHttpClient.clientName();
        }

        if (clientType == ASYNC) {
            return asyncHttpClient == null ? "null" : asyncHttpClient.clientName();
        }

        return ClientType.UNKNOWN.name();
    }

    private RetryStrategy resolveRetryStrategy(LazyValueSource config) {
        RetryMode retryMode = RetryMode.resolver()
                .profileFile(config.get(PROFILE_FILE_SUPPLIER))
                .profileName(config.get(PROFILE_NAME))
                .defaultRetryMode(config.get(DEFAULT_RETRY_MODE))
                .resolve();
        return SdkDefaultRetryStrategy.forRetryMode(retryMode);
    }

    /**
     * Finalize which sync HTTP client will be used for the created client.
     */
    private SdkHttpClient resolveSyncHttpClient(LazyValueSource config,
                                                SdkClientConfiguration deprecatedConfigDoNotUseThis) {
        SdkHttpClient httpClient = config.get(CONFIGURED_SYNC_HTTP_CLIENT);
        SdkHttpClient.Builder<?> httpClientBuilder = config.get(CONFIGURED_SYNC_HTTP_CLIENT_BUILDER);
        Validate.isTrue(httpClient == null ||
                        httpClientBuilder == null,
                "The httpClient and the httpClientBuilder can't both be configured.");

        AttributeMap httpClientConfig = getHttpClientConfig(config, deprecatedConfigDoNotUseThis);

        return Either.fromNullable(httpClient, httpClientBuilder)
                .map(e -> e.map(Function.identity(), b -> b.buildWithDefaults(httpClientConfig)))
                .orElseGet(() -> defaultHttpClientBuilder.buildWithDefaults(httpClientConfig));
    }

    /**
     * Finalize which async HTTP client will be used for the created client.
     */
    private SdkAsyncHttpClient resolveAsyncHttpClient(LazyValueSource config,
                                                      SdkClientConfiguration deprecatedConfigDoNotUseThis) {
        Validate.isTrue(config.get(CONFIGURED_ASYNC_HTTP_CLIENT) == null ||
                        config.get(CONFIGURED_ASYNC_HTTP_CLIENT_BUILDER) == null,
                "The asyncHttpClient and the asyncHttpClientBuilder can't both be configured.");

        AttributeMap httpClientConfig = getHttpClientConfig(config, deprecatedConfigDoNotUseThis);

        return Either.fromNullable(config.get(CONFIGURED_ASYNC_HTTP_CLIENT), config.get(CONFIGURED_ASYNC_HTTP_CLIENT_BUILDER))
                .map(e -> e.map(Function.identity(), b -> b.buildWithDefaults(httpClientConfig)))
                .orElseGet(() -> defaultAsyncHttpClientBuilder.buildWithDefaults(httpClientConfig));
    }

    private AttributeMap getHttpClientConfig(LazyValueSource config, SdkClientConfiguration deprecatedConfigDoNotUseThis) {
        AttributeMap httpClientConfig = config.get(HTTP_CLIENT_CONFIG);
        if (httpClientConfig == null) {
            // We must be using an old client, use the deprecated way of loading HTTP_CLIENT_CONFIG, instead. This won't take
            // into account any configuration changes (e.g. defaults mode) from plugins, but this is the best we can do without
            // breaking protected APIs. TODO: if we ever break protected APIs, remove these "childHttpConfig" hooks.
            httpClientConfig = childHttpConfig(deprecatedConfigDoNotUseThis);
        }
        return httpClientConfig;
    }

    /**
     * @deprecated Configure {@link SdkClientOption#HTTP_CLIENT_CONFIG} from {@link #finalizeChildConfiguration} instead.
     */
    @Deprecated
    protected AttributeMap childHttpConfig(SdkClientConfiguration configuration) {
        return childHttpConfig();
    }

    /**
     * @deprecated Configure {@link SdkClientOption#HTTP_CLIENT_CONFIG} from {@link #finalizeChildConfiguration} instead.
     */
    @Deprecated
    protected AttributeMap childHttpConfig() {
        return AttributeMap.empty();
    }

    /**
     * Finalize which async executor service will be used for the created client. The default async executor
     * service has at least 8 core threads and can scale up to at least 64 threads when needed depending
     * on the number of processors available.
     */
    private Executor resolveAsyncFutureCompletionExecutor(LazyValueSource config) {
        int processors = Runtime.getRuntime().availableProcessors();
        int corePoolSize = Math.max(8, processors);
        int maxPoolSize = Math.max(64, processors * 2);
        ThreadPoolExecutor executor = new ThreadPoolExecutor(corePoolSize, maxPoolSize,
                10, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(1_000),
                new ThreadFactoryBuilder()
                        .threadNamePrefix("sdk-async-response").build());
        // Allow idle core threads to time out
        executor.allowCoreThreadTimeOut(true);
        return executor;
    }

    /**
     * Finalize the internal SDK scheduled executor service that is used for scheduling tasks such as async retry attempts and
     * timeout task.
     */
    private ScheduledExecutorService resolveScheduledExecutorService(LazyValueSource c) {
        ScheduledExecutorService executor = c.get(CONFIGURED_SCHEDULED_EXECUTOR_SERVICE);
        if (executor != null) {
            return executor;
        }

        return awsSdkScheduler;
    }

    /**
     * Finalize which execution interceptors will be used for the created client.
     */
    private List<ExecutionInterceptor> resolveExecutionInterceptors(SdkClientConfiguration config) {
        List<ExecutionInterceptor> globalInterceptors = new ArrayList<>();
        globalInterceptors.addAll(sdkInterceptors());
        globalInterceptors.addAll(new ClasspathInterceptorChainFactory().getGlobalInterceptors());
        return mergeLists(globalInterceptors, config.option(EXECUTION_INTERCEPTORS));
    }


    /**
     * The set of interceptors that should be included with all services.
     */
    private List<ExecutionInterceptor> sdkInterceptors() {
        return Collections.unmodifiableList(Arrays.asList(
                new HttpChecksumValidationInterceptor()
        ));
    }

    @Override
    public final B endpointOverride(URI endpointOverride) {
        if (endpointOverride == null) {
            clientConfiguration.option(SdkClientOption.CLIENT_ENDPOINT_PROVIDER, null);
        } else {
            clientConfiguration.option(SdkClientOption.CLIENT_ENDPOINT_PROVIDER,
                    ClientEndpointProvider.forEndpointOverride(endpointOverride));
        }
        return thisBuilder();
    }

    public final void setEndpointOverride(URI endpointOverride) {
        endpointOverride(endpointOverride);
    }

    public final B asyncConfiguration(ClientAsyncConfiguration asyncConfiguration) {
        clientConfiguration.option(FUTURE_COMPLETION_EXECUTOR, asyncConfiguration.advancedOption(FUTURE_COMPLETION_EXECUTOR));
        return thisBuilder();
    }

    public final void setAsyncConfiguration(ClientAsyncConfiguration asyncConfiguration) {
        asyncConfiguration(asyncConfiguration);
    }

    @Override
    public final B overrideConfiguration(ClientOverrideConfiguration overrideConfig) {
        this.overrideConfig = overrideConfig;
        return thisBuilder();
    }

    public final void setOverrideConfiguration(ClientOverrideConfiguration overrideConfiguration) {
        overrideConfiguration(overrideConfiguration);
    }

    @Override
    public final ClientOverrideConfiguration overrideConfiguration() {
        if (overrideConfig == null) {
            return ClientOverrideConfiguration.builder().build();
        }
        return overrideConfig;
    }

    public final B httpClient(SdkHttpClient httpClient) {
        if (httpClient != null) {
            httpClient = new NonManagedSdkHttpClient(httpClient);
        }
        clientConfiguration.option(CONFIGURED_SYNC_HTTP_CLIENT, httpClient);
        return thisBuilder();
    }

    public final B httpClientBuilder(SdkHttpClient.Builder httpClientBuilder) {
        clientConfiguration.option(CONFIGURED_SYNC_HTTP_CLIENT_BUILDER, httpClientBuilder);
        return thisBuilder();
    }

    public final B httpClient(SdkAsyncHttpClient httpClient) {
        if (httpClient != null) {
            httpClient = new NonManagedSdkAsyncHttpClient(httpClient);
        }
        clientConfiguration.option(CONFIGURED_ASYNC_HTTP_CLIENT, httpClient);
        return thisBuilder();
    }

    public final B httpClientBuilder(SdkAsyncHttpClient.Builder httpClientBuilder) {
        clientConfiguration.option(CONFIGURED_ASYNC_HTTP_CLIENT_BUILDER, httpClientBuilder);
        return thisBuilder();
    }

    public final B metricPublishers(List<MetricPublisher> metricPublishers) {
        clientConfiguration.option(METRIC_PUBLISHERS, metricPublishers);
        return thisBuilder();
    }

    @Override
    public final B addPlugin(SdkPlugin plugin) {
        plugins.add(paramNotNull(plugin, "plugin"));
        return thisBuilder();
    }

    @Override
    public final List<SdkPlugin> plugins() {
        return Collections.unmodifiableList(plugins);
    }

    /**
     * Return "this" for method chaining.
     */
    @SuppressWarnings("unchecked")
    protected B thisBuilder() {
        return (B) this;
    }

    /**
     * Wrapper around {@link SdkHttpClient} to prevent it from being closed. Used when the customer provides
     * an already built client in which case they are responsible for the lifecycle of it.
     */
    @SdkTestInternalApi
    public static final class NonManagedSdkHttpClient implements SdkHttpClient {

        private final SdkHttpClient delegate;

        private NonManagedSdkHttpClient(SdkHttpClient delegate) {
            this.delegate = paramNotNull(delegate, "SdkHttpClient");
        }

        @Override
        public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
            return delegate.prepareRequest(request);
        }

        @Override
        public void close() {
            // Do nothing, this client is managed by the customer.
        }

        @Override
        public String clientName() {
            return delegate.clientName();
        }
    }

    /**
     * Wrapper around {@link SdkAsyncHttpClient} to prevent it from being closed. Used when the customer provides
     * an already built client in which case they are responsible for the lifecycle of it.
     */
    @SdkTestInternalApi
    public static final class NonManagedSdkAsyncHttpClient implements SdkAsyncHttpClient {

        private final SdkAsyncHttpClient delegate;

        NonManagedSdkAsyncHttpClient(SdkAsyncHttpClient delegate) {
            this.delegate = paramNotNull(delegate, "SdkAsyncHttpClient");
        }

        @Override
        public CompletableFuture<Void> execute(AsyncExecuteRequest request) {
            return delegate.execute(request);
        }

        @Override
        public String clientName() {
            return delegate.clientName();
        }

        @Override
        public void close() {
            // Do nothing, this client is managed by the customer.
        }
    }
}
