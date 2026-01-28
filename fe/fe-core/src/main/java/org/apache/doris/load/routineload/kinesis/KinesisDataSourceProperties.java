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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.load.routineload.AbstractDataSourceProperties;
import org.apache.doris.load.routineload.LoadDataSourceType;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;

/**
 * AWS Kinesis data source properties for Routine Load.
 * 
 * Kinesis is AWS's managed streaming data service, similar to Apache Kafka.
 * Key differences from Kafka:
 * - Uses shards instead of partitions
 * - Uses sequence numbers instead of offsets
 * - Requires AWS authentication (IAM, access keys, etc.)
 * - Region-specific endpoints
 * 
 * Example usage in SQL:
 * CREATE ROUTINE LOAD my_job ON my_table
 * FROM KINESIS (
 *     "kinesis_region" = "us-east-1",
 *     "kinesis_stream" = "my-stream",
 *     "kinesis_access_key" = "AKIAIOSFODNN7EXAMPLE",
 *     "kinesis_secret_key" = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
 * );
 */
public class KinesisDataSourceProperties extends AbstractDataSourceProperties {

    private static final String CUSTOM_KINESIS_PROPERTY_PREFIX = "property.";

    /**
     * List of shard IDs with their starting sequence numbers.
     * Pair<ShardId, SequenceNumber>
     * SequenceNumber can be:
     * - Actual sequence number string
     * - TRIM_HORIZON_VAL (-2) for oldest record
     * - LATEST_VAL (-1) for newest record  
     * - Timestamp value for AT_TIMESTAMP
     */
    @Getter
    @Setter
    @SerializedName(value = "kinesisShardPositions")
    private List<Pair<String, String>> kinesisShardPositions = Lists.newArrayList();

    /**
     * Custom Kinesis properties for advanced configuration.
     * Includes AWS credentials and client configuration.
     */
    @Getter
    @SerializedName(value = "customKinesisProperties")
    private Map<String, String> customKinesisProperties;

    /**
     * Whether positions are specified as timestamps.
     */
    @Getter
    @SerializedName(value = "isPositionsForTimes")
    private boolean isPositionsForTimes = false;

    /**
     * AWS region for the Kinesis stream.
     */
    @Getter
    @SerializedName(value = "region")
    private String region;

    /**
     * Name of the Kinesis stream.
     */
    @Getter
    @SerializedName(value = "stream")
    private String stream;

    /**
     * Optional endpoint URL for custom endpoints.
     */
    @Getter
    @SerializedName(value = "endpoint")
    private String endpoint;

    // Standard position constants (similar to Kafka's OFFSET_BEGINNING/OFFSET_END)
    public static final String POSITION_TRIM_HORIZON = "TRIM_HORIZON";
    public static final String POSITION_LATEST = "LATEST";
    public static final String POSITION_AT_TIMESTAMP = "AT_TIMESTAMP";

    // Configurable data source properties that can be set by user
    private static final ImmutableSet<String> CONFIGURABLE_DATA_SOURCE_PROPERTIES_SET =
            new ImmutableSet.Builder<String>()
                    .add(KinesisConfiguration.KINESIS_REGION.getName())
                    .add(KinesisConfiguration.KINESIS_STREAM.getName())
                    .add(KinesisConfiguration.KINESIS_ENDPOINT.getName())
                    .add(KinesisConfiguration.KINESIS_SHARDS.getName())
                    .add(KinesisConfiguration.KINESIS_POSITIONS.getName())
                    .add(KinesisConfiguration.KINESIS_DEFAULT_POSITION.getName())
                    .add(KinesisConfiguration.KINESIS_ACCESS_KEY.getName())
                    .add(KinesisConfiguration.KINESIS_SECRET_KEY.getName())
                    .add(KinesisConfiguration.KINESIS_SESSION_TOKEN.getName())
                    .add(KinesisConfiguration.KINESIS_ROLE_ARN.getName())
                    .add(KinesisConfiguration.KINESIS_EXTERNAL_ID.getName())
                    .add(KinesisConfiguration.KINESIS_PROFILE_NAME.getName())
                    .add(KinesisConfiguration.KINESIS_CONSUMER_NAME.getName())
                    .add(KinesisConfiguration.KINESIS_MAX_RECORDS_PER_FETCH.getName())
                    .add(KinesisConfiguration.KINESIS_FETCH_INTERVAL_MS.getName())
                    .add(KinesisConfiguration.KINESIS_CONNECTION_TIMEOUT_MS.getName())
                    .add(KinesisConfiguration.KINESIS_REQUEST_TIMEOUT_MS.getName())
                    .add(KinesisConfiguration.KINESIS_MAX_RETRIES.getName())
                    .add(KinesisConfiguration.KINESIS_USE_HTTPS.getName())
                    .build();

    public KinesisDataSourceProperties(Map<String, String> dataSourceProperties, boolean multiLoad) {
        super(dataSourceProperties, multiLoad);
    }

    public KinesisDataSourceProperties(Map<String, String> originalDataSourceProperties) {
        super(originalDataSourceProperties);
    }

    @Override
    protected String getDataSourceType() {
        return LoadDataSourceType.KINESIS.name();
    }

    @Override
    protected List<String> getRequiredProperties() {
        return Arrays.asList(
                KinesisConfiguration.KINESIS_REGION.getName(),
                KinesisConfiguration.KINESIS_STREAM.getName()
        );
    }

    @Override
    public void convertAndCheckDataSourceProperties() throws UserException {
        // Check for invalid properties
        Optional<String> invalidProperty = originalDataSourceProperties.keySet().stream()
                .filter(key -> !CONFIGURABLE_DATA_SOURCE_PROPERTIES_SET.contains(key))
                .filter(key -> !key.startsWith(CUSTOM_KINESIS_PROPERTY_PREFIX))
                .findFirst();
        if (invalidProperty.isPresent()) {
            throw new AnalysisException(invalidProperty.get() + " is invalid Kinesis property or cannot be set");
        }

        // Parse region (required)
        this.region = KinesisConfiguration.KINESIS_REGION.getParameterValue(
                originalDataSourceProperties.get(KinesisConfiguration.KINESIS_REGION.getName()));
        if (!isAlter() && StringUtils.isBlank(region)) {
            throw new AnalysisException(KinesisConfiguration.KINESIS_REGION.getName() + " is a required property");
        }
        if (StringUtils.isNotBlank(region)) {
            validateRegion(region);
        }

        // Parse stream name (required)
        this.stream = KinesisConfiguration.KINESIS_STREAM.getParameterValue(
                originalDataSourceProperties.get(KinesisConfiguration.KINESIS_STREAM.getName()));
        if (!isAlter() && StringUtils.isBlank(stream)) {
            throw new AnalysisException(KinesisConfiguration.KINESIS_STREAM.getName() + " is a required property");
        }

        // Parse optional endpoint
        this.endpoint = KinesisConfiguration.KINESIS_ENDPOINT.getParameterValue(
                originalDataSourceProperties.get(KinesisConfiguration.KINESIS_ENDPOINT.getName()));

        // Parse custom properties (AWS credentials, etc.)
        analyzeCustomProperties();

        // Validate AWS authentication configuration
        validateAwsAuthConfig();

        // Parse shards
        List<String> shards = KinesisConfiguration.KINESIS_SHARDS.getParameterValue(
                originalDataSourceProperties.get(KinesisConfiguration.KINESIS_SHARDS.getName()));
        if (CollectionUtils.isNotEmpty(shards)) {
            analyzeKinesisShardProperty(shards);
        }

        // Parse positions
        List<String> positions = KinesisConfiguration.KINESIS_POSITIONS.getParameterValue(
                originalDataSourceProperties.get(KinesisConfiguration.KINESIS_POSITIONS.getName()));
        String defaultPositionString = originalDataSourceProperties.get(
                KinesisConfiguration.KINESIS_DEFAULT_POSITION.getName());

        // Validate that positions and default_position are not both set
        if (CollectionUtils.isNotEmpty(positions) && StringUtils.isNotBlank(defaultPositionString)) {
            throw new AnalysisException("Only one of " + KinesisConfiguration.KINESIS_POSITIONS.getName() 
                    + " and " + KinesisConfiguration.KINESIS_DEFAULT_POSITION.getName() + " can be set.");
        }

        // For alter operation, shards and positions must be set together
        if (isAlter() && CollectionUtils.isNotEmpty(shards) && CollectionUtils.isEmpty(positions)
                && StringUtils.isBlank(defaultPositionString)) {
            throw new AnalysisException("Must set position or default position with shard property");
        }

        // Process positions
        if (CollectionUtils.isNotEmpty(positions)) {
            this.isPositionsForTimes = analyzeKinesisPositionProperty(positions);
            return;
        }
        this.isPositionsForTimes = analyzeKinesisDefaultPositionProperty();
        if (CollectionUtils.isNotEmpty(kinesisShardPositions)) {
            setDefaultPositionForShards(this.kinesisShardPositions, defaultPositionString, this.isPositionsForTimes);
        }
    }

    /**
     * Validate AWS region format.
     */
    private void validateRegion(String region) throws AnalysisException {
        // AWS regions follow patterns like: us-east-1, eu-west-2, ap-southeast-1, cn-north-1
        if (!region.matches("^[a-z]{2}(-[a-z]+)?-[a-z]+-\\d$")) {
            throw new AnalysisException("Invalid AWS region format: " + region 
                    + ". Expected format like: us-east-1, eu-west-2, cn-north-1");
        }
    }

    /**
     * Parse and store custom Kinesis properties.
     */
    private void analyzeCustomProperties() throws AnalysisException {
        this.customKinesisProperties = new HashMap<>();
        for (Map.Entry<String, String> entry : originalDataSourceProperties.entrySet()) {
            if (entry.getKey().startsWith(CUSTOM_KINESIS_PROPERTY_PREFIX)) {
                String propertyKey = entry.getKey();
                String propertyValue = entry.getValue();
                String[] propertyKeyParts = propertyKey.split("\\.", 2);
                if (propertyKeyParts.length < 2 || propertyKeyParts[1].isEmpty()) {
                    throw new AnalysisException("Kinesis property key format is invalid: " + propertyKey);
                }
                this.customKinesisProperties.put(propertyKeyParts[1], propertyValue);
            }
        }

        // Store AWS credentials in custom properties for later use
        storeCredentialInCustomProperties(KinesisConfiguration.KINESIS_ACCESS_KEY.getName());
        storeCredentialInCustomProperties(KinesisConfiguration.KINESIS_SECRET_KEY.getName());
        storeCredentialInCustomProperties(KinesisConfiguration.KINESIS_SESSION_TOKEN.getName());
        storeCredentialInCustomProperties(KinesisConfiguration.KINESIS_ROLE_ARN.getName());
        storeCredentialInCustomProperties(KinesisConfiguration.KINESIS_EXTERNAL_ID.getName());
        storeCredentialInCustomProperties(KinesisConfiguration.KINESIS_PROFILE_NAME.getName());
        storeCredentialInCustomProperties(KinesisConfiguration.KINESIS_CONSUMER_NAME.getName());

        // Store connection settings
        storeCredentialInCustomProperties(KinesisConfiguration.KINESIS_MAX_RECORDS_PER_FETCH.getName());
        storeCredentialInCustomProperties(KinesisConfiguration.KINESIS_FETCH_INTERVAL_MS.getName());
        storeCredentialInCustomProperties(KinesisConfiguration.KINESIS_CONNECTION_TIMEOUT_MS.getName());
        storeCredentialInCustomProperties(KinesisConfiguration.KINESIS_REQUEST_TIMEOUT_MS.getName());
        storeCredentialInCustomProperties(KinesisConfiguration.KINESIS_MAX_RETRIES.getName());
        storeCredentialInCustomProperties(KinesisConfiguration.KINESIS_USE_HTTPS.getName());

        // Store default position for later use
        String defaultPosition = originalDataSourceProperties.get(
                KinesisConfiguration.KINESIS_DEFAULT_POSITION.getName());
        if (StringUtils.isNotBlank(defaultPosition)) {
            customKinesisProperties.put(KinesisConfiguration.KINESIS_DEFAULT_POSITION.getName(), defaultPosition);
        }
    }

    private void storeCredentialInCustomProperties(String key) {
        String value = originalDataSourceProperties.get(key);
        if (StringUtils.isNotBlank(value)) {
            customKinesisProperties.put(key, value);
        }
    }

    /**
     * Validate AWS authentication configuration.
     * At least one authentication method must be provided:
     * 1. Access key + Secret key
     * 2. IAM Role ARN
     * 3. AWS Profile name
     * 4. Default credential chain (EC2 instance profile, environment variables, etc.)
     */
    private void validateAwsAuthConfig() throws AnalysisException {
        String accessKey = customKinesisProperties.get(KinesisConfiguration.KINESIS_ACCESS_KEY.getName());
        String secretKey = customKinesisProperties.get(KinesisConfiguration.KINESIS_SECRET_KEY.getName());
        String roleArn = customKinesisProperties.get(KinesisConfiguration.KINESIS_ROLE_ARN.getName());
        String profileName = customKinesisProperties.get(KinesisConfiguration.KINESIS_PROFILE_NAME.getName());

        // If access key is provided, secret key must also be provided
        if (StringUtils.isNotBlank(accessKey) && StringUtils.isBlank(secretKey)) {
            throw new AnalysisException("When " + KinesisConfiguration.KINESIS_ACCESS_KEY.getName() 
                    + " is set, " + KinesisConfiguration.KINESIS_SECRET_KEY.getName() + " must also be set");
        }
        if (StringUtils.isNotBlank(secretKey) && StringUtils.isBlank(accessKey)) {
            throw new AnalysisException("When " + KinesisConfiguration.KINESIS_SECRET_KEY.getName() 
                    + " is set, " + KinesisConfiguration.KINESIS_ACCESS_KEY.getName() + " must also be set");
        }

        // If external ID is provided, role ARN must be provided
        String externalId = customKinesisProperties.get(KinesisConfiguration.KINESIS_EXTERNAL_ID.getName());
        if (StringUtils.isNotBlank(externalId) && StringUtils.isBlank(roleArn)) {
            throw new AnalysisException("When " + KinesisConfiguration.KINESIS_EXTERNAL_ID.getName() 
                    + " is set, " + KinesisConfiguration.KINESIS_ROLE_ARN.getName() + " must also be set");
        }

        // Note: We don't require any authentication config because the default credential chain
        // can be used in EC2/EKS environments with instance profiles or service accounts
    }

    /**
     * Initialize shard positions with default values.
     */
    private void analyzeKinesisShardProperty(List<String> shards) {
        shards.forEach(shardId -> this.kinesisShardPositions.add(Pair.of(shardId, POSITION_LATEST)));
    }

    /**
     * Parse position property and set positions for each shard.
     * Returns true if positions are timestamps.
     */
    private boolean analyzeKinesisPositionProperty(List<String> positions) throws UserException {
        if (positions.size() != kinesisShardPositions.size()) {
            throw new AnalysisException("Number of shards must equal number of positions");
        }

        // Check if positions are timestamps
        boolean foundTime = false;
        boolean foundPosition = false;
        for (String position : positions) {
            if (TimeUtils.timeStringToLong(position) != -1) {
                foundTime = true;
            } else {
                foundPosition = true;
            }
        }
        if (foundTime && foundPosition) {
            throw new AnalysisException("Cannot mix timestamp and position values in " 
                    + KinesisConfiguration.KINESIS_POSITIONS.getName());
        }

        if (foundTime) {
            TimeZone timeZone = TimeUtils.getOrSystemTimeZone(getTimezone());
            for (int i = 0; i < positions.size(); i++) {
                long timestamp = TimeUtils.timeStringToLong(positions.get(i), timeZone);
                kinesisShardPositions.get(i).second = String.valueOf(timestamp);
            }
        } else {
            for (int i = 0; i < positions.size(); i++) {
                String position = positions.get(i);
                validatePosition(position);
                kinesisShardPositions.get(i).second = position;
            }
        }
        return foundTime;
    }

    /**
     * Validate position value.
     */
    private void validatePosition(String position) throws AnalysisException {
        if (!position.equalsIgnoreCase(POSITION_TRIM_HORIZON) 
                && !position.equalsIgnoreCase(POSITION_LATEST)
                && !position.equalsIgnoreCase(POSITION_AT_TIMESTAMP)
                && !isValidSequenceNumber(position)) {
            throw new AnalysisException(KinesisConfiguration.KINESIS_POSITIONS.getName() 
                    + " must be TRIM_HORIZON, LATEST, AT_TIMESTAMP, or a valid sequence number. Got: " + position);
        }
    }

    /**
     * Check if the string is a valid Kinesis sequence number.
     * Kinesis sequence numbers are numeric strings.
     */
    private boolean isValidSequenceNumber(String position) {
        try {
            // Kinesis sequence numbers are large numeric strings
            new java.math.BigInteger(position);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    /**
     * Analyze default position property.
     * Returns true if position is a timestamp.
     */
    private boolean analyzeKinesisDefaultPositionProperty() throws AnalysisException {
        customKinesisProperties.putIfAbsent(KinesisConfiguration.KINESIS_DEFAULT_POSITION.getName(), POSITION_LATEST);
        String defaultPosition = customKinesisProperties.get(KinesisConfiguration.KINESIS_DEFAULT_POSITION.getName());
        
        TimeZone timeZone = TimeUtils.getOrSystemTimeZone(this.getTimezone());
        long timestamp = TimeUtils.timeStringToLong(defaultPosition, timeZone);
        if (timestamp != -1) {
            // This is a datetime format, convert to timestamp
            customKinesisProperties.put(KinesisConfiguration.KINESIS_DEFAULT_POSITION.getName(),
                    String.valueOf(timestamp));
            return true;
        } else {
            if (!defaultPosition.equalsIgnoreCase(POSITION_TRIM_HORIZON) 
                    && !defaultPosition.equalsIgnoreCase(POSITION_LATEST)) {
                throw new AnalysisException(KinesisConfiguration.KINESIS_DEFAULT_POSITION.getName() 
                        + " can only be set to TRIM_HORIZON, LATEST, or a datetime string. Got: " + defaultPosition);
            }
            return false;
        }
    }

    /**
     * Set default position for all shards.
     */
    private static void setDefaultPositionForShards(List<Pair<String, String>> shardPositions,
                                                    String defaultPosition, boolean isForTimes) {
        if (isForTimes) {
            for (Pair<String, String> pair : shardPositions) {
                pair.second = defaultPosition;
            }
        } else {
            for (Pair<String, String> pair : shardPositions) {
                pair.second = defaultPosition != null ? defaultPosition : POSITION_LATEST;
            }
        }
    }
}
