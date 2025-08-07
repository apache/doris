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

package org.apache.doris.datasource.property.storage;

import org.apache.doris.common.UserException;
import org.apache.doris.datasource.property.ConnectorProperty;
import org.apache.doris.datasource.property.PropertyConverter;

import lombok.Getter;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;

public class BrokerProperties extends StorageProperties {

    @Setter
    @Getter
    @ConnectorProperty(names = {"broker.name"},
            required = false,
            description = "The name of the broker. "
                    + "This is used to identify the broker in the system.")
    private String brokerName = "";

    @Getter
    private Map<String, String> brokerParams;

    public BrokerProperties(Map<String, String> origProps) {
        super(Type.BROKER, origProps);
    }

    public static BrokerProperties of(String brokerName, Map<String, String> origProps) {
        BrokerProperties properties = new BrokerProperties(origProps);
        properties.setBrokerName(brokerName);
        properties.initNormalizeAndCheckProps();
        return properties;
    }

    private static final String BIND_BROKER_NAME_KEY = "broker.name";

    public static boolean guessIsMe(Map<String, String> props) {
        if (props == null || props.isEmpty()) {
            return false;
        }
        return props.keySet().stream()
                .anyMatch(key -> key.equalsIgnoreCase(BIND_BROKER_NAME_KEY));
    }

    @Override
    public void initNormalizeAndCheckProps() {
        super.initNormalizeAndCheckProps();
        this.brokerParams = new HashMap<>(origProps);
        //why need this convert
        this.brokerParams.putAll(PropertyConverter.convertToHadoopFSProperties(origProps));
    }

    @Override
    public Map<String, String> getBackendConfigProperties() {
        return origProps;
    }

    @Override
    public String validateAndNormalizeUri(String url) throws UserException {
        return url;
    }

    @Override
    public String validateAndGetUri(Map<String, String> loadProps) throws UserException {
        return loadProps.get("uri");
    }

    @Override
    public String getStorageName() {
        return "BROKER";
    }

    @Override
    public void initializeHadoopStorageConfig() {
        // do nothing
    }
}
