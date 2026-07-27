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

package org.apache.doris.common;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public final class MetaServiceRpcRateLimitConfigValidator {
    private MetaServiceRpcRateLimitConfigValidator() {
    }

    public static Map<String, Integer> parseQpsPerCoreConfig(String config) throws ConfigException {
        if (config == null || config.trim().isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, Integer> qpsPerCore = new HashMap<>();
        Set<String> methods = new HashSet<>();
        for (String item : config.split(";")) {
            String trimmedItem = item.trim();
            if (trimmedItem.isEmpty()) {
                continue;
            }
            int separatorIndex = trimmedItem.indexOf(':');
            if (separatorIndex <= 0 || separatorIndex != trimmedItem.lastIndexOf(':')
                    || separatorIndex == trimmedItem.length() - 1) {
                throw new ConfigException("Invalid format, expected method1:qps1;method2:qps2");
            }

            String methodName = trimmedItem.substring(0, separatorIndex).trim();
            String qpsText = trimmedItem.substring(separatorIndex + 1).trim();
            if (methodName.isEmpty() || qpsText.isEmpty()) {
                throw new ConfigException("Invalid format, expected method1:qps1;method2:qps2");
            }
            if (!methods.add(methodName)) {
                throw new ConfigException("Duplicate method: " + methodName);
            }
            try {
                qpsPerCore.put(methodName, Integer.parseInt(qpsText));
            } catch (NumberFormatException e) {
                throw new ConfigException("Invalid qps for method: " + methodName, e);
            }
        }
        return qpsPerCore;
    }

    public static void validatePositive(String fieldName, int value) throws ConfigException {
        if (value <= 0) {
            throw new ConfigException(fieldName + " must be positive");
        }
    }

    public static void validateNonNegative(String fieldName, long value) throws ConfigException {
        if (value < 0) {
            throw new ConfigException(fieldName + " must be non-negative");
        }
    }

    public static class QpsConfigHandler extends ConfigBase.DefaultConfHandler {
        @Override
        public void handle(Field field, String confVal) throws Exception {
            parseQpsPerCoreConfig(confVal);
            super.handle(field, confVal);
        }
    }

    public static class PositiveIntConfigHandler extends ConfigBase.DefaultConfHandler {
        @Override
        public void handle(Field field, String confVal) throws Exception {
            String trimmedVal = confVal.trim();
            validatePositive(field.getName(), Integer.parseInt(trimmedVal));
            super.handle(field, trimmedVal);
        }
    }

    public static class NonNegativeLongConfigHandler extends ConfigBase.DefaultConfHandler {
        @Override
        public void handle(Field field, String confVal) throws Exception {
            String trimmedVal = confVal.trim();
            validateNonNegative(field.getName(), Long.parseLong(trimmedVal));
            super.handle(field, trimmedVal);
        }
    }
}
