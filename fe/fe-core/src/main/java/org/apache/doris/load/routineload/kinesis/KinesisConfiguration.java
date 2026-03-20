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
 * Parameters are divided into two categories:
 * 1. AWS Client parameters (aws.*): region, endpoint, credentials, timeouts
 * 2. Kinesis-specific parameters (aws.kinesis.*): stream, shards, positions, API settings
 */
public enum KinesisConfiguration {

    /**
     * AWS region where the Kinesis stream is located.
     * Required property.
     */
    KINESIS_REGION("aws.region", null, value -> value.trim()),

    /**
     * Name of the Kinesis stream to consume from.
     * Required property.
     */
    KINESIS_STREAM("kinesis_stream", null, value -> value.trim()),

    /**
     * Comma-separated list of shard IDs to consume from.
     * If not specified, all shards will be consumed.
     */
    KINESIS_SHARDS("kinesis_shards", null, shardsString ->
            Arrays.stream(shardsString.replace(" ", "").split(","))
                    .collect(Collectors.toList())),

    /**
     * Shard iterator positions (sequence numbers) for each shard.
     * Format: position1,position2,... corresponding to shards order.
     */
    KINESIS_POSITIONS("kinesis_shards_pos", null,
            positionsString -> Splitter.on(",").trimResults().splitToList(positionsString)),

    /**
     * Default starting position for new shards.
     * Valid values: TRIM_HORIZON, LATEST, AT_TIMESTAMP
     */
    KINESIS_DEFAULT_POSITION("property.kinesis_default_pos", "LATEST", position -> position.trim()),

    /**
     * AWS Access Key ID for authentication.
     */
    KINESIS_ACCESS_KEY("aws.access_key", null, value -> value),

    /**
     * AWS Secret Access Key for authentication.
     */
    KINESIS_SECRET_KEY("aws.secret_key", null, value -> value),

    /**
     * AWS Session Token for temporary credentials.
     */
    KINESIS_SESSION_TOKEN("aws.session_key", null, value -> value),

    /**
     * IAM Role ARN to assume for accessing Kinesis.
     */
    KINESIS_ROLE_ARN("aws.role_arn", null, value -> value.trim()),

    /**
     * External ID for IAM role assumption.
     */
    KINESIS_EXTERNAL_ID("aws.external.id", null, value -> value.trim()),

    /**
     * AWS Profile name to use from credentials file.
     */
    KINESIS_PROFILE_NAME("aws.profile.name", null, value -> value.trim()),

    /**
     * Consumer name for enhanced fan-out (EFO).
     */
    KINESIS_CONSUMER_NAME("aws.kinesis.consumer.name", null, value -> value.trim()),

    /**
     * Maximum records per GetRecords call.
     */
    KINESIS_MAX_RECORDS_PER_FETCH("aws.kinesis.max_records", 10000, Integer::parseInt),

    /**
     * Interval between GetRecords calls in milliseconds.
     */
    KINESIS_FETCH_INTERVAL_MS("aws.kinesis.fetch_interval_ms", 200, Integer::parseInt),

    /**
     * Connection timeout in milliseconds.
     */
    KINESIS_CONNECTION_TIMEOUT_MS("aws.connection.timeout.ms", 10000, Integer::parseInt),

    /**
     * Request timeout in milliseconds.
     */
    KINESIS_REQUEST_TIMEOUT_MS("aws.request.timeout.ms", 10000, Integer::parseInt),

    /**
     * Max number of retry attempts for Kinesis API calls.
     */
    KINESIS_MAX_RETRIES("aws.kinesis.max_retries", 3, Integer::parseInt),

    /**
     * Use HTTPS for Kinesis endpoint.
     */
    KINESIS_USE_HTTPS("aws.kinesis.use_https", true, Boolean::parseBoolean);

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
