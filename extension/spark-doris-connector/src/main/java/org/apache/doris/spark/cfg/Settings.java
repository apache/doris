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

package org.apache.doris.spark.cfg;

import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.doris.spark.exception.IllegalArgumentException;
import org.apache.doris.spark.util.ErrorMessages;
import org.apache.doris.spark.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Settings {
    private final static Logger logger = LoggerFactory.getLogger(Settings.class);

    public abstract String getProperty(String name);

    public abstract void setProperty(String name, String value);

    public abstract Properties asProperties();

    public abstract Settings copy();

    public String getProperty(String name, String defaultValue) {
        String value = getProperty(name);
        if (StringUtils.isEmpty(value)) {
            return defaultValue;
        }
        return value;
    }

    public int getIntegerProperty(String name, int defaultValue) {
        try {
            if (getProperty(name) != null) {
                 return Integer.parseInt(getProperty(name));
            }
        } catch (NumberFormatException e) {
            logger.warn(ErrorMessages.PARSE_NUMBER_FAILED_MESSAGE, name, getProperty(name));
        }
        return defaultValue;
    }

    public Settings merge(Properties properties) {
        if (properties == null || properties.isEmpty()) {
            return this;
        }

        Enumeration<?> propertyNames = properties.propertyNames();

        for (; propertyNames.hasMoreElements();) {
            Object prop = propertyNames.nextElement();
            if (prop instanceof String) {
                Object value = properties.get(prop);
                setProperty((String) prop, value.toString());
            }
        }

        return this;
    }

    public Settings merge(Map<String, String> map) {
        if (map == null || map.isEmpty()) {
            return this;
        }

        for (Map.Entry<String, String> entry : map.entrySet()) {
            setProperty(entry.getKey(), entry.getValue());
        }

        return this;
    }

    public Settings load(String source) throws IllegalArgumentException {
        Properties copy = IOUtils.propsFromString(source);
        merge(copy);
        return this;
    }

    public String save() throws IllegalArgumentException {
        Properties copy = asProperties();
        return IOUtils.propsToString(copy);
    }

    @Override
    public int hashCode() {
        return asProperties().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        return asProperties().equals(((Settings) obj).asProperties());
    }
}
