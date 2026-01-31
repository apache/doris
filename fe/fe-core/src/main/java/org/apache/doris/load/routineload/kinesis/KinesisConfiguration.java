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

package org.apache.doris.load.routineload.kinesis;

import com.google.common.base.Splitter;

import java.util.Arrays;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Configuration enum for AWS Kinesis data source properties.
 * 
 * AWS Kinesis is a managed real-time data streaming service from AWS.
 * The main concepts in Kinesis are:
 * - Stream: Similar to Kafka topic, a named stream for data records
 * - Shard: Similar to Kafka partition, the base throughput unit of a stream
 * - Sequence Number: Similar to Kafka offset, a unique identifier for each record within a shard
 * - Consumer: Application that reads from a stream
 */
public enum KinesisConfiguration {

    /**
     * AWS region where the Kinesis stream is located.
     * Required property.
     * Example: us-east-1, ap-southeast-1, cn-north-1
     */
    KINESIS_REGION("kinesis_region", null, value -> value.trim()),

    /**
     * Name of the Kinesis stream to consume from.
     * Required property.
     */
    KINESIS_STREAM("kinesis_stream", null, value -> value.trim()),

    /**
     * Endpoint URL for Kinesis service (optional).
     * Used for custom endpoints like LocalStack for testing, or VPC endpoints.
     * If not specified, the default AWS endpoint for the region will be used.
     */
    KINESIS_ENDPOINT("kinesis_endpoint", null, value -> value.trim()),

    /**
     * Comma-separated list of shard IDs to consume from.
     * If not specified, all shards will be consumed.
     * Example: shardId-000000000000,shardId-000000000001
     */
    KINESIS_SHARDS("kinesis_shards", null, shardsString ->
            Arrays.stream(shardsString.replace(" ", "").split(","))
                    .collect(Collectors.toList())),

    /**
     * Shard iterator positions (sequence numbers) for each shard.
     * Format: position1,position2,... corresponding to shards order.
     * Special values:
     * - TRIM_HORIZON: Start from the oldest record
     * - LATEST: Start from the newest record
     * - AT_TIMESTAMP: Start from a specific timestamp
     * - Specific sequence number
     */
    KINESIS_POSITIONS("kinesis_positions", null, 
            positionsString -> Splitter.on(",").trimResults().splitToList(positionsString)),

    /**
     * Default starting position for new shards.
     * Valid values: TRIM_HORIZON, LATEST, AT_TIMESTAMP
     * Default: LATEST
     */
    KINESIS_DEFAULT_POSITION("kinesis_default_position", "LATEST", position -> position.trim()),

    /**
     * AWS Access Key ID for authentication.
     * Can be omitted if using IAM role, EC2 instance profile, or environment variables.
     */
    KINESIS_ACCESS_KEY("kinesis_access_key", null, value -> value),

    /**
     * AWS Secret Access Key for authentication.
     * Can be omitted if using IAM role, EC2 instance profile, or environment variables.
     */
    KINESIS_SECRET_KEY("kinesis_secret_key", null, value -> value),

    /**
     * AWS Session Token for temporary credentials.
     * Used with STS assume role.
     */
    KINESIS_SESSION_TOKEN("kinesis_session_token", null, value -> value),

    /**
     * IAM Role ARN to assume for accessing Kinesis.
     * Useful for cross-account access.
     */
    KINESIS_ROLE_ARN("kinesis_role_arn", null, value -> value.trim()),

    /**
     * External ID for IAM role assumption.
     * Additional security measure for cross-account access.
     */
    KINESIS_EXTERNAL_ID("kinesis_external_id", null, value -> value.trim()),

    /**
     * AWS Profile name to use from credentials file.
     * If not specified, uses default profile or environment credentials.
     */
    KINESIS_PROFILE_NAME("kinesis_profile_name", null, value -> value.trim()),

    /**
     * Consumer name for enhanced fan-out (EFO).
     * When specified, uses dedicated throughput via SubscribeToShard API.
     * Provides ~2MB/sec per shard with lower latency (~70ms).
     */
    KINESIS_CONSUMER_NAME("kinesis_consumer_name", null, value -> value.trim()),

    /**
     * Maximum records per GetRecords call.
     * Default: 10000 (Kinesis limit)
     * Higher values improve throughput but increase memory usage.
     */
    KINESIS_MAX_RECORDS_PER_FETCH("kinesis_max_records_per_fetch", 10000, Integer::parseInt),

    /**
     * Interval between GetRecords calls in milliseconds.
     * Default: 200ms (to stay within Kinesis 5 calls/sec limit per shard)
     */
    KINESIS_FETCH_INTERVAL_MS("kinesis_fetch_interval_ms", 200, Integer::parseInt),

    /**
     * Connection timeout in milliseconds.
     * Default: 10000 (10 seconds)
     */
    KINESIS_CONNECTION_TIMEOUT_MS("kinesis_connection_timeout_ms", 10000, Integer::parseInt),

    /**
     * Request timeout in milliseconds.
     * Default: 10000 (10 seconds)
     */
    KINESIS_REQUEST_TIMEOUT_MS("kinesis_request_timeout_ms", 10000, Integer::parseInt),

    /**
     * Max number of retry attempts for Kinesis API calls.
     * Default: 3
     */
    KINESIS_MAX_RETRIES("kinesis_max_retries", 3, Integer::parseInt),

    /**
     * Use HTTPS for Kinesis endpoint.
     * Default: true
     * Set to false only for local testing (e.g., LocalStack).
     */
    KINESIS_USE_HTTPS("kinesis_use_https", true, Boolean::parseBoolean);

    private final String name;
    private final Object defaultValue;
    private final Function<String, Object> converter;

    <T> KinesisConfiguration(String name, T defaultValue, Function<String, T> converter) {
        this.name = name;
        this.defaultValue = defaultValue;
        this.converter = (Function<String, Object>) converter;
    }

    public String getName() {
        return name;
    }

    public Object getDefaultValue() {
        return defaultValue;
    }

    public static KinesisConfiguration getByName(String name) {
        return Arrays.stream(KinesisConfiguration.values())
                .filter(config -> config.getName().equals(name))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown Kinesis configuration: " + name));
    }

    @SuppressWarnings("unchecked")
    public <T> T getParameterValue(String param) {
        Object value = param != null ? converter.apply(param) : defaultValue;
        return (T) value;
    }
}
