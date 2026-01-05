/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.s3a;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.s3a.impl.AWSClientConfig;
import org.apache.hadoop.fs.s3a.statistics.impl.AwsStatisticsCollector;
import org.apache.hadoop.fs.store.LogExactlyOnce;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.util.AwsHostNameUtils;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.auth.spi.scheme.AuthScheme;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;
import software.amazon.awssdk.metrics.LoggingMetricPublisher;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.S3BaseClientBuilder;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.multipart.MultipartConfiguration;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.utils.ThreadFactoryBuilder;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.hadoop.fs.s3a.Constants.AWS_REGION;
import static org.apache.hadoop.fs.s3a.Constants.AWS_S3_CROSS_REGION_ACCESS_ENABLED;
import static org.apache.hadoop.fs.s3a.Constants.AWS_S3_CROSS_REGION_ACCESS_ENABLED_DEFAULT;
import static org.apache.hadoop.fs.s3a.Constants.AWS_S3_DEFAULT_REGION;
import static org.apache.hadoop.fs.s3a.Constants.AWS_SERVICE_IDENTIFIER_S3;
import static org.apache.hadoop.fs.s3a.Constants.CENTRAL_ENDPOINT;
import static org.apache.hadoop.fs.s3a.Constants.DEFAULT_SECURE_CONNECTIONS;
import static org.apache.hadoop.fs.s3a.Constants.FIPS_ENDPOINT;
import static org.apache.hadoop.fs.s3a.Constants.HTTP_SIGNER_CLASS_NAME;
import static org.apache.hadoop.fs.s3a.Constants.HTTP_SIGNER_ENABLED;
import static org.apache.hadoop.fs.s3a.Constants.HTTP_SIGNER_ENABLED_DEFAULT;
import static org.apache.hadoop.fs.s3a.Constants.SECURE_CONNECTIONS;
import static org.apache.hadoop.fs.s3a.auth.SignerFactory.createHttpSigner;
import static org.apache.hadoop.fs.s3a.impl.AWSHeaders.REQUESTER_PAYS_HEADER;
import static org.apache.hadoop.fs.s3a.impl.InternalConstants.AUTH_SCHEME_AWS_SIGV_4;
import static org.apache.hadoop.util.Preconditions.checkArgument;


/**
 * The default {@link S3ClientFactory} implementation.
 * This calls the AWS SDK to configure and create an
 * {@code AmazonS3Client} that communicates with the S3 service.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class DefaultS3ClientFactory extends Configured
        implements S3ClientFactory {

    private static final String REQUESTER_PAYS_HEADER_VALUE = "requester";

    private static final String S3_SERVICE_NAME = "s3";

    private static final Pattern VPC_ENDPOINT_PATTERN =
            Pattern.compile("^(?:.+\\.)?([a-z0-9-]+)\\.vpce\\.amazonaws\\.(?:com|com\\.cn)$");

    /**
     * Subclasses refer to this.
     */
    protected static final Logger LOG =
            LoggerFactory.getLogger(DefaultS3ClientFactory.class);

    /**
     * A one-off warning of default region chains in use.
     */
    private static final LogExactlyOnce WARN_OF_DEFAULT_REGION_CHAIN =
            new LogExactlyOnce(LOG);

    /**
     * Warning message printed when the SDK Region chain is in use.
     */
    private static final String SDK_REGION_CHAIN_IN_USE =
            "S3A filesystem client is using"
                    + " the SDK region resolution chain.";


    /** Exactly once log to inform about ignoring the AWS-SDK Warnings for CSE. */
    private static final LogExactlyOnce IGNORE_CSE_WARN = new LogExactlyOnce(LOG);

    /**
     * Error message when an endpoint is set with FIPS enabled: {@value}.
     */
    @VisibleForTesting
    public static final String ERROR_ENDPOINT_WITH_FIPS =
            "Non central endpoint cannot be set when " + FIPS_ENDPOINT + " is true";

    @Override
    public S3Client createS3Client(
            final URI uri,
            final S3ClientCreationParameters parameters) throws IOException {

        Configuration conf = getConf();
        String bucket = uri.getHost();

        ApacheHttpClient.Builder httpClientBuilder = AWSClientConfig
                .createHttpClientBuilder(conf)
                .proxyConfiguration(AWSClientConfig.createProxyConfiguration(conf, bucket));
        return configureClientBuilder(S3Client.builder(), parameters, conf, bucket)
                .httpClientBuilder(httpClientBuilder)
                .build();
    }

    @Override
    public S3AsyncClient createS3AsyncClient(
            final URI uri,
            final S3ClientCreationParameters parameters) throws IOException {

        Configuration conf = getConf();
        String bucket = uri.getHost();

        NettyNioAsyncHttpClient.Builder httpClientBuilder = AWSClientConfig
                .createAsyncHttpClientBuilder(conf)
                .proxyConfiguration(AWSClientConfig.createAsyncProxyConfiguration(conf, bucket));

        MultipartConfiguration multipartConfiguration = MultipartConfiguration.builder()
                .minimumPartSizeInBytes(parameters.getMinimumPartSize())
                .thresholdInBytes(parameters.getMultiPartThreshold())
                .build();

        S3AsyncClientBuilder s3AsyncClientBuilder =
                configureClientBuilder(S3AsyncClient.builder(), parameters, conf, bucket)
                        .httpClientBuilder(httpClientBuilder);

        // multipart upload pending with HADOOP-19326.
        if (!parameters.isClientSideEncryptionEnabled() &&
                !parameters.isAnalyticsAcceleratorEnabled()) {
            s3AsyncClientBuilder.multipartConfiguration(multipartConfiguration)
                    .multipartEnabled(parameters.isMultipartCopy());
        }

        return s3AsyncClientBuilder.build();
    }

    @Override
    public S3TransferManager createS3TransferManager(final S3AsyncClient s3AsyncClient) {
        return S3TransferManager.builder()
                .s3Client(s3AsyncClient)
                .build();
    }

    /**
     * Configure a sync or async S3 client builder.
     * This method handles all shared configuration, including
     * path style access, credentials and whether or not to use S3Express
     * CreateSession.
     * @param builder S3 client builder
     * @param parameters parameter object
     * @param conf configuration object
     * @param bucket bucket name
     * @return the builder object
     * @param <BuilderT> S3 client builder type
     * @param <ClientT> S3 client type
     */
    private <BuilderT extends S3BaseClientBuilder<BuilderT, ClientT>, ClientT> BuilderT configureClientBuilder(
            BuilderT builder, S3ClientCreationParameters parameters, Configuration conf, String bucket)
            throws IOException {

        configureEndpointAndRegion(builder, parameters, conf);

        S3Configuration serviceConfiguration = S3Configuration.builder()
                .pathStyleAccessEnabled(parameters.isPathStyleAccess())
                .checksumValidationEnabled(parameters.isChecksumValidationEnabled())
                .build();

        final ClientOverrideConfiguration.Builder override =
                createClientOverrideConfiguration(parameters, conf);
        ScheduledExecutorService awsSdkScheduler=Executors.newScheduledThreadPool(20, new ThreadFactoryBuilder().threadNamePrefix("aws-sdk-scheduler-%d").build());
        S3BaseClientBuilder<BuilderT, ClientT> s3BaseClientBuilder = builder
                .overrideConfiguration(override.build())
                .credentialsProvider(parameters.getCredentialSet())
                .disableS3ExpressSessionAuth(!parameters.isExpressCreateSession())
                .serviceConfiguration(serviceConfiguration);
         s3BaseClientBuilder.overrideConfiguration(ClientOverrideConfiguration.builder().scheduledExecutorService(awsSdkScheduler).build());
        if (LOG.isTraceEnabled()) {
            // if this log is set to "trace" then we turn on logging of SDK metrics.
            // The metrics itself will log at info; it is just that reflection work
            // would be needed to change that setting safely for shaded and unshaded aws artifacts.
            s3BaseClientBuilder.overrideConfiguration(o ->
                    o.addMetricPublisher(LoggingMetricPublisher.create()));
        }

        if (conf.getBoolean(HTTP_SIGNER_ENABLED, HTTP_SIGNER_ENABLED_DEFAULT)) {
            // use an http signer through an AuthScheme
            final AuthScheme<AwsCredentialsIdentity> signer =
                    createHttpSigner(conf, AUTH_SCHEME_AWS_SIGV_4, HTTP_SIGNER_CLASS_NAME);
            builder.putAuthScheme(signer);
        }
        return (BuilderT) s3BaseClientBuilder;
    }

    /**
     * Create an override configuration for an S3 client.
     * @param parameters parameter object
     * @param conf configuration object
     * @throws IOException any IOE raised, or translated exception
     * @throws RuntimeException some failures creating an http signer
     * @return the override configuration
     * @throws IOException any IOE raised, or translated exception
     */
    protected ClientOverrideConfiguration.Builder createClientOverrideConfiguration(
            S3ClientCreationParameters parameters, Configuration conf) throws IOException {
        final ClientOverrideConfiguration.Builder clientOverrideConfigBuilder =
                AWSClientConfig.createClientConfigBuilder(conf, AWS_SERVICE_IDENTIFIER_S3);

        // add any headers
        parameters.getHeaders().forEach((h, v) -> clientOverrideConfigBuilder.putHeader(h, v));

        if (parameters.isRequesterPays()) {
            // All calls must acknowledge requester will pay via header.
            clientOverrideConfigBuilder.putHeader(REQUESTER_PAYS_HEADER, REQUESTER_PAYS_HEADER_VALUE);
        }

        if (!StringUtils.isEmpty(parameters.getUserAgentSuffix())) {
            clientOverrideConfigBuilder.putAdvancedOption(SdkAdvancedClientOption.USER_AGENT_SUFFIX,
                    parameters.getUserAgentSuffix());
        }

        if (parameters.getExecutionInterceptors() != null) {
            for (ExecutionInterceptor interceptor : parameters.getExecutionInterceptors()) {
                clientOverrideConfigBuilder.addExecutionInterceptor(interceptor);
            }
        }

        if (parameters.getMetrics() != null) {
            clientOverrideConfigBuilder.addMetricPublisher(
                    new AwsStatisticsCollector(parameters.getMetrics()));
        }

        final RetryPolicy.Builder retryPolicyBuilder = AWSClientConfig.createRetryPolicyBuilder(conf);
        clientOverrideConfigBuilder.retryPolicy(retryPolicyBuilder.build());

        return clientOverrideConfigBuilder;
    }

    /**
     * This method configures the endpoint and region for a S3 client.
     * The order of configuration is:
     *
     * <ol>
     * <li>If region is configured via fs.s3a.endpoint.region, use it.</li>
     * <li>If endpoint is configured via via fs.s3a.endpoint, set it.
     *     If no region is configured, try to parse region from endpoint. </li>
     * <li> If no region is configured, and it could not be parsed from the endpoint,
     *     set the default region as US_EAST_2</li>
     * <li> If configured region is empty, fallback to SDK resolution chain. </li>
     * <li> S3 cross region is enabled by default irrespective of region or endpoint
     *      is set or not.</li>
     * </ol>
     *
     * @param builder S3 client builder.
     * @param parameters parameter object
     * @param conf  conf configuration object
     * @param <BuilderT> S3 client builder type
     * @param <ClientT> S3 client type
     * @throws IllegalArgumentException if endpoint is set when FIPS is enabled.
     */
    private <BuilderT extends S3BaseClientBuilder<BuilderT, ClientT>, ClientT> void configureEndpointAndRegion(
            BuilderT builder, S3ClientCreationParameters parameters, Configuration conf) {
        final String endpointStr = parameters.getEndpoint();
        final URI endpoint = getS3Endpoint(endpointStr, conf);

        final String configuredRegion = parameters.getRegion();
        Region region = null;
        String origin = "";

        // If the region was configured, set it.
        if (configuredRegion != null && !configuredRegion.isEmpty()) {
            origin = AWS_REGION;
            region = Region.of(configuredRegion);
        }

        // FIPs? Log it, then reject any attempt to set an endpoint
        final boolean fipsEnabled = parameters.isFipsEnabled();
        if (fipsEnabled) {
            LOG.debug("Enabling FIPS mode");
        }
        // always setting it guarantees the value is non-null,
        // which tests expect.
        builder.fipsEnabled(fipsEnabled);

        if (endpoint != null) {
            boolean endpointEndsWithCentral =
                    endpointStr.endsWith(CENTRAL_ENDPOINT);
            checkArgument(!fipsEnabled || endpointEndsWithCentral, "%s : %s",
                    ERROR_ENDPOINT_WITH_FIPS,
                    endpoint);

            // No region was configured,
            // determine the region from the endpoint.
            if (region == null) {
                region = getS3RegionFromEndpoint(endpointStr,
                        endpointEndsWithCentral);
                if (region != null) {
                    origin = "endpoint";
                }
            }

            // No need to override endpoint with "s3.amazonaws.com".
            // Let the client take care of endpoint resolution. Overriding
            // the endpoint with "s3.amazonaws.com" causes 400 Bad Request
            // errors for non-existent buckets and objects.
            // ref: https://github.com/aws/aws-sdk-java-v2/issues/4846
            if (!endpointEndsWithCentral) {
                builder.endpointOverride(endpoint);
                LOG.debug("Setting endpoint to {}", endpoint);
            } else {
                origin = "central endpoint with cross region access";
                LOG.debug("Enabling cross region access for endpoint {}",
                        endpointStr);
            }
        }

        if (region != null) {
            builder.region(region);
        } else if (configuredRegion == null) {
            // no region is configured, and none could be determined from the endpoint.
            // Use US_EAST_2 as default.
            region = Region.of(AWS_S3_DEFAULT_REGION);
            builder.region(region);
            origin = "cross region access fallback";
        } else if (configuredRegion.isEmpty()) {
            // region configuration was set to empty string.
            // allow this if people really want it; it is OK to rely on this
            // when deployed in EC2.
            WARN_OF_DEFAULT_REGION_CHAIN.warn(SDK_REGION_CHAIN_IN_USE);
            LOG.debug(SDK_REGION_CHAIN_IN_USE);
            origin = "SDK region chain";
        }
        boolean isCrossRegionAccessEnabled = conf.getBoolean(AWS_S3_CROSS_REGION_ACCESS_ENABLED,
                AWS_S3_CROSS_REGION_ACCESS_ENABLED_DEFAULT);
        // s3 cross region access
        if (isCrossRegionAccessEnabled) {
            builder.crossRegionAccessEnabled(true);
        }
        LOG.debug("Setting region to {} from {} with cross region access {}",
                region, origin, isCrossRegionAccessEnabled);
    }

    /**
     * Given a endpoint string, create the endpoint URI.
     *
     * @param endpoint possibly null endpoint.
     * @param conf config to build the URI from.
     * @return an endpoint uri
     */
    protected static URI getS3Endpoint(String endpoint, final Configuration conf) {

        boolean secureConnections = conf.getBoolean(SECURE_CONNECTIONS, DEFAULT_SECURE_CONNECTIONS);

        String protocol = secureConnections ? "https" : "http";

        if (endpoint == null || endpoint.isEmpty()) {
            // don't set an endpoint if none is configured, instead let the SDK figure it out.
            return null;
        }

        if (!endpoint.contains("://")) {
            endpoint = String.format("%s://%s", protocol, endpoint);
        }

        try {
            return new URI(endpoint);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * Parses the endpoint to get the region.
     * If endpoint is the central one, use US_EAST_2.
     *
     * @param endpoint the configure endpoint.
     * @param endpointEndsWithCentral true if the endpoint is configured as central.
     * @return the S3 region, null if unable to resolve from endpoint.
     */
    @VisibleForTesting
    static Region getS3RegionFromEndpoint(final String endpoint,
                                          final boolean endpointEndsWithCentral) {

        if (!endpointEndsWithCentral) {
            // S3 VPC endpoint parsing
            Matcher matcher = VPC_ENDPOINT_PATTERN.matcher(endpoint);
            if (matcher.find()) {
                LOG.debug("Mapping to VPCE");
                LOG.debug("Endpoint {} is vpc endpoint; parsing region as {}", endpoint, matcher.group(1));
                return Region.of(matcher.group(1));
            }

            LOG.debug("Endpoint {} is not the default; parsing", endpoint);
            return AwsHostNameUtils.parseSigningRegion(endpoint, S3_SERVICE_NAME).orElse(null);
        }

        // Select default region here to enable cross-region access.
        // If both "fs.s3a.endpoint" and "fs.s3a.endpoint.region" are empty,
        // Spark sets "fs.s3a.endpoint" to "s3.amazonaws.com".
        // This applies to Spark versions with the changes of SPARK-35878.
        // ref:
        // https://github.com/apache/spark/blob/v3.5.0/core/
        // src/main/scala/org/apache/spark/deploy/SparkHadoopUtil.scala#L528
        // If we do not allow cross region access, Spark would not be able to
        // access any bucket that is not present in the given region.
        // Hence, we should use default region us-east-2 to allow cross-region
        // access.
        return Region.of(AWS_S3_DEFAULT_REGION);
    }

}
