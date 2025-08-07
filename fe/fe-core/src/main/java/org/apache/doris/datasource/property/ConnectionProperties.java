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

package org.apache.doris.datasource.property;

import org.apache.doris.common.CatalogConfigFileUtils;
import org.apache.doris.datasource.property.storage.exception.StoragePropertiesException;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.conf.Configuration;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class ConnectionProperties {
    /**
     * The original user-provided properties.
     * <p>
     * This map may contain various configuration entries, not all of which are relevant
     * to the specific Connector implementation. It serves as the raw input from the user.
     */
    @Getter
    @Setter
    protected Map<String, String> origProps;

    /**
     * The filtered properties that are actually used by the Connector.
     * <p>
     * This map only contains key-value pairs that are recognized and matched by
     * the specific Connector implementation. It's a subset of {@code origProps}.
     */
    @Getter
    protected Map<String, String> matchedProperties = new HashMap<>();

    protected ConnectionProperties(Map<String, String> origProps) {
        this.origProps = origProps;
    }

    public void initNormalizeAndCheckProps() {
        ConnectorPropertiesUtils.bindConnectorProperties(this, origProps);
        for (Field field : ConnectorPropertiesUtils.getConnectorProperties(this.getClass())) {
            ConnectorProperty annotation = field.getAnnotation(ConnectorProperty.class);
            for (String name : annotation.names()) {
                if (origProps.containsKey(name)) {
                    matchedProperties.put(name, origProps.get(name));
                    break;
                }
            }
        }
        // 3. check properties
        checkRequiredProperties();
    }

    // Some properties may be loaded from file
    // Subclass can override this method to load properties from file.
    // The return value is the properties loaded from file, not include original properties
    protected Map<String, String> loadConfigFromFile(String resourceConfig) {
        if (Strings.isNullOrEmpty(resourceConfig)) {
            return new HashMap<>();
        }
        Configuration conf = CatalogConfigFileUtils.loadConfigurationFromHadoopConfDir(resourceConfig);
        Map<String, String> confMap = Maps.newHashMap();
        for (Map.Entry<String, String> entry : conf) {
            confMap.put(entry.getKey(), entry.getValue());
        }
        return confMap;
    }

    // Subclass can override this method to return the property name of resource config.
    protected String getResourceConfigPropName() {
        return null;
    }

    // This method will check if all required properties are set.
    // Subclass can implement this method for additional check.
    protected void checkRequiredProperties() {
        List<Field> supportedProps = ConnectorPropertiesUtils.getConnectorProperties(this.getClass());
        for (Field field : supportedProps) {
            field.setAccessible(true);
            ConnectorProperty anno = field.getAnnotation(ConnectorProperty.class);
            String[] names = anno.names();
            if (anno.required() && field.getType().equals(String.class)) {
                try {
                    String value = (String) field.get(this);
                    if (Strings.isNullOrEmpty(value)) {
                        throw new IllegalArgumentException("Property " + names[0] + " is required.");
                    }
                } catch (IllegalAccessException e) {
                    throw new StoragePropertiesException("Failed to get property " + names[0]
                            + ", " + e.getMessage(), e);
                }
            }
        }
    }
}
