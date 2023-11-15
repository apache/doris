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

package org.apache.doris.load.routineload.pulsar;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.load.routineload.AbstractDataSourceProperties;
import org.apache.doris.load.routineload.LoadDataSourceType;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import lombok.Getter;
import lombok.Setter;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * Pulsar data source properties
 */
public class PulsarDataSourceProperties extends AbstractDataSourceProperties {

    private static final String ENDPOINT_REGEX = "[-A-Za-z0-9+&@#/%?=~_|!:,.;]+[-A-Za-z0-9+&@#/%=~_|]";

    private static final String CUSTOM_KAFKA_PROPERTY_PREFIX = "property.";

    public static final String POSITION_EARLIEST = "POSITION_EARLIEST"; // 1
    public static final String POSITION_LATEST = "POSITION_LATEST"; // 0
    public static final long POSITION_LATEST_VAL = 0;
    public static final long POSITION_EARLIEST_VAL = 1;

    @Getter
    @SerializedName(value = "pulsarServiceUrl")
    private String pulsarServiceUrl;

    @Getter
    @SerializedName(value = "pulsarTopic")
    private String pulsarTopic;

    @Getter
    @SerializedName(value = "pulsarSubscription")
    private String pulsarSubscription;

    @Getter
    @SerializedName(value = "pulsarPartitions")
    private List<String> pulsarPartitions = Lists.newArrayList();


    @Getter
    @Setter
    @SerializedName(value = "pulsarPartitionInitialPositions")
    private List<Pair<String, Long>> pulsarPartitionInitialPositions = Lists.newArrayList();


    @Getter
    @SerializedName(value = "customPulsarProperties")
    private Map<String, String> customPulsarProperties = Maps.newHashMap();

    private static final ImmutableSet<String> CONFIGURABLE_DATA_SOURCE_PROPERTIES_SET =
            new ImmutableSet.Builder<String>().add(PulsarConfiguration.PULSAR_SERVICE_URL_PROPERTY.getName())
            .add(PulsarConfiguration.PULSAR_TOPIC_PROPERTY.getName())
            .add(PulsarConfiguration.PULSAR_SUBSCRIPTION_PROPERTY.getName())
            .add(PulsarConfiguration.PULSAR_PARTITIONS_PROPERTY.getName())
            .add(PulsarConfiguration.PULSAR_INITIAL_POSITIONS_PROPERTY.getName())
            .build();

    public PulsarDataSourceProperties(Map<String, String> dataSourceProperties, boolean multiLoad) {
        super(dataSourceProperties, multiLoad);
    }

    public PulsarDataSourceProperties(Map<String, String> originalDataSourceProperties) {
        super(originalDataSourceProperties);
    }

    @Override
    protected String getDataSourceType() {
        return LoadDataSourceType.PULSAR.name();
    }

    @Override
    protected List<String> getRequiredProperties() throws UserException {
        return Arrays.asList(PulsarConfiguration.PULSAR_SERVICE_URL_PROPERTY.getName(),
            PulsarConfiguration.PULSAR_TOPIC_PROPERTY.getName());
    }

    @Override
    public void convertAndCheckDataSourceProperties() throws UserException {
        Optional<String> optional = originalDataSourceProperties.keySet().stream()
                .filter(entity -> !CONFIGURABLE_DATA_SOURCE_PROPERTIES_SET.contains(entity))
                .filter(entity -> !entity.startsWith("property.")).findFirst();
        if (optional.isPresent()) {
            throw new AnalysisException(optional.get() + " is invalid pulsar custom property");
        }

        // check service url
        pulsarServiceUrl = Strings.nullToEmpty(originalDataSourceProperties.get(
            PulsarConfiguration.PULSAR_SERVICE_URL_PROPERTY.getName())).replaceAll(" ", "");
        if (Strings.isNullOrEmpty(pulsarServiceUrl)) {
            throw new AnalysisException(PulsarConfiguration.PULSAR_SERVICE_URL_PROPERTY.getName()
                + " is a required property");
        }

        String[] pulsarServiceUrlList = this.pulsarServiceUrl.split(",");
        for (String serviceUrl : pulsarServiceUrlList) {
            if (!Pattern.matches(ENDPOINT_REGEX, serviceUrl)) {
                throw new AnalysisException(PulsarConfiguration.PULSAR_SERVICE_URL_PROPERTY.getName() + ":" + serviceUrl
                    + " not match pattern " + ENDPOINT_REGEX);
            }
        }

        // check topic
        pulsarTopic = Strings.nullToEmpty(originalDataSourceProperties.get(
            PulsarConfiguration.PULSAR_TOPIC_PROPERTY.getName())).replaceAll(" ", "");
        if (Strings.isNullOrEmpty(pulsarTopic)) {
            throw new AnalysisException(PulsarConfiguration.PULSAR_TOPIC_PROPERTY + " is a required property");
        }

        // check subscription
        pulsarSubscription = Strings.nullToEmpty(originalDataSourceProperties.get(
            PulsarConfiguration.PULSAR_SUBSCRIPTION_PROPERTY.getName())).replaceAll(" ", "");
        if (Strings.isNullOrEmpty(pulsarSubscription)) {
            throw new AnalysisException(PulsarConfiguration.PULSAR_SUBSCRIPTION_PROPERTY + " is a required property");
        }

        // check custom pulsar property before check partitions,
        // because partitions can use pulsar_default_position property
        analyzePulsarCustomProperties(originalDataSourceProperties, customPulsarProperties);

        // check partitions
        String pulsarPartitionsString = originalDataSourceProperties.get(
                PulsarConfiguration.PULSAR_PARTITIONS_PROPERTY.getName());
        if (pulsarPartitionsString != null) {
            analyzePulsarPartitionProperty(pulsarPartitionsString, customPulsarProperties, pulsarPartitions,
                    pulsarPartitionInitialPositions);
        }

        // check positions
        String pulsarPositionString = originalDataSourceProperties.get(
                PulsarConfiguration.PULSAR_INITIAL_POSITIONS_PROPERTY.getName());
        if (pulsarPositionString != null) {
            analyzePulsarPositionProperty(pulsarPositionString, pulsarPartitions, pulsarPartitionInitialPositions);
        }
    }

    public static void analyzePulsarPartitionProperty(String pulsarPartitionsString,
                                                      Map<String, String> customPulsarProperties,
                                                      List<String> pulsarPartitions,
                                                      List<Pair<String, Long>> pulsarPartitionInitialPositions)
            throws AnalysisException {
        pulsarPartitionsString = pulsarPartitionsString.replaceAll(" ", "");
        if (pulsarPartitionsString.isEmpty()) {
            throw new AnalysisException(PulsarConfiguration.PULSAR_PARTITIONS_PROPERTY
                + " could not be a empty string");
        }

        String[] pulsarPartitionsStringList = pulsarPartitionsString.split(",");
        for (String s : pulsarPartitionsStringList) {
            pulsarPartitions.add(s);
        }

        // get default initial positions if set
        if (customPulsarProperties.containsKey(PulsarConfiguration.PULSAR_DEFAULT_INITIAL_POSITION.getName())) {
            Long pulsarDefaultInitialPosition = getPulsarPosition(customPulsarProperties.get(
                    PulsarConfiguration.PULSAR_DEFAULT_INITIAL_POSITION.getName()));
            pulsarPartitions.stream().forEach(
                    entry -> pulsarPartitionInitialPositions.add(Pair.of(entry, pulsarDefaultInitialPosition)));
        }
    }

    public static void analyzePulsarPositionProperty(String pulsarPositionsString,
                                                     List<String> pulsarPartitions,
                                                     List<Pair<String, Long>> pulsarPartitionInitialPositions)
            throws AnalysisException {
        pulsarPositionsString = pulsarPositionsString.replaceAll(" ", "");
        if (pulsarPositionsString.isEmpty()) {
            throw new AnalysisException(PulsarConfiguration.PULSAR_INITIAL_POSITIONS_PROPERTY.getName()
                + " could not be a empty string");
        }
        String[] pulsarPositionsStringList = pulsarPositionsString.split(",");
        if (pulsarPositionsStringList.length != pulsarPartitions.size()) {
            throw new AnalysisException("Partitions number should be equals to positions number");
        }

        if (!pulsarPartitionInitialPositions.isEmpty()) {
            for (int i = 0; i < pulsarPositionsStringList.length; i++) {
                pulsarPartitionInitialPositions.get(i).second = getPulsarPosition(pulsarPositionsStringList[i]);
            }
        } else {
            for (int i = 0; i < pulsarPositionsStringList.length; i++) {
                pulsarPartitionInitialPositions.add(Pair.of(pulsarPartitions.get(i),
                        getPulsarPosition(pulsarPositionsStringList[i])));
            }
        }
    }

    // Get pulsar position from string
    // defined in pulsar-client-cpp/InitialPosition.h
    // InitialPositionLatest: 0
    // InitialPositionEarliest: 1
    public static long getPulsarPosition(String positionStr) throws AnalysisException {
        long position;
        if (positionStr.equalsIgnoreCase(POSITION_EARLIEST)) {
            position = POSITION_EARLIEST_VAL;
        } else if (positionStr.equalsIgnoreCase(POSITION_LATEST)) {
            position = POSITION_LATEST_VAL;
        } else {
            throw new AnalysisException("Only POSITION_EARLIEST or POSITION_LATEST can be specified");
        }
        return position;
    }

    public static void analyzePulsarCustomProperties(Map<String, String> dataSourceProperties,
                                                     Map<String, String> customPulsarProperties)
            throws AnalysisException {
        for (Map.Entry<String, String> dataSourceProperty : dataSourceProperties.entrySet()) {
            if (dataSourceProperty.getKey().startsWith("property.")) {
                String propertyKey = dataSourceProperty.getKey();
                String propertyValue = dataSourceProperty.getValue();
                String[] propertyValueArr = propertyKey.split("\\.");
                if (propertyValueArr.length < 2) {
                    throw new AnalysisException("pulsar property value could not be a empty string");
                }
                customPulsarProperties.put(propertyKey.substring(propertyKey.indexOf(".") + 1), propertyValue);
            }
            // can be extended in the future which other prefix
        }
    }
}
