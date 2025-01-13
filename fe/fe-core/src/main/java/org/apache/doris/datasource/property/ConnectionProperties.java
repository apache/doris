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

import org.apache.doris.common.ConfigurationUtils;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

public class ConnectionProperties {
    protected Map<String, String> origProps;

    protected ConnectionProperties(Map<String, String> origProps) {
        this.origProps = origProps;
    }

    protected void normalizedAndCheckProps() {
        // 1. prepare phase
        Map<String, String> allProps = loadConfigFromFile(getResourceConfigPropName());
        // 2. overwrite result properties with original properties
        allProps.putAll(origProps);
        // 3. set fields from resultProps
        List<Field> supportedProps = PropertyUtils.getConnectorProperties(this.getClass());
        for (Field field : supportedProps) {
            field.setAccessible(true);
            ConnectorProperty anno = field.getAnnotation(ConnectorProperty.class);
            String[] names = anno.names();
            for (String name : names) {
                if (allProps.containsKey(name)) {
                    try {
                        field.set(this, allProps.get(name));
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException("Failed to set property " + name + ", " + e.getMessage(), e);
                    }
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
        if (Strings.isNullOrEmpty(origProps.get(resourceConfig))) {
            return Maps.newHashMap();
        }
        Configuration conf = ConfigurationUtils.loadConfigurationFromHadoopConfDir(resourceConfig);
        Map<String, String> confMap = Maps.newHashMap();
        for (Map.Entry<String, String> entry : conf) {
            confMap.put(entry.getKey(), entry.getValue());
        }
        return confMap;
    }

    // Subclass can override this method to return the property name of resource config.
    protected String getResourceConfigPropName() {
        return "";
    }

    // This method will check if all required properties are set.
    // Subclass can implement this method for additional check.
    protected void checkRequiredProperties() {
        List<Field> supportedProps = PropertyUtils.getConnectorProperties(this.getClass());
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
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
