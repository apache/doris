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

package org.apache.doris.load.routineload.kafka;

import org.apache.doris.common.util.DatasourcePrintableMap;

import org.apache.commons.collections4.MapUtils;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/** Shared masking for Kafka configuration displays, modification logs and audit SQL. */
public final class KafkaPropertiesUtil {
    public static final String SENSITIVE_PROPERTY_MASK = "******";

    private KafkaPropertiesUtil() {
    }

    public static Map<String, String> maskProperties(Map<String, String> properties) {
        Map<String, String> maskedProperties = new HashMap<>();
        MapUtils.emptyIfNull(properties).forEach((key, value) -> maskedProperties.put(key,
                isSensitiveKey(key) ? SENSITIVE_PROPERTY_MASK : value));
        return maskedProperties;
    }

    public static boolean isSensitiveKey(String key) {
        // librdkafka passes property names to its C API, which stops at the first NUL.
        int nulIndex = key.indexOf('\0');
        if (nulIndex >= 0) {
            key = key.substring(0, nulIndex);
        }
        String lowerKey = key.toLowerCase(Locale.ROOT);
        if (lowerKey.startsWith("property.")) {
            lowerKey = lowerKey.substring("property.".length());
        }
        return KafkaConfiguration.SASL_JAAS_CONFIG.equals(lowerKey)
                || KafkaConfiguration.AWS_ACCESS_KEY.equals(lowerKey)
                || "sasl.oauthbearer.config".equals(lowerKey)
                || "token".equals(lowerKey)
                || "credential".equals(lowerKey)
                || "credentials".equals(lowerKey)
                || DatasourcePrintableMap.isSensitiveKey(lowerKey)
                || lowerKey.endsWith(".password")
                || lowerKey.endsWith(".secret")
                || lowerKey.endsWith(".secret_key")
                || lowerKey.endsWith(".secret.key")
                || lowerKey.endsWith(".session_key")
                || lowerKey.endsWith(".token")
                || lowerKey.endsWith("_token")
                || lowerKey.endsWith(".credential")
                || lowerKey.endsWith(".credentials")
                || lowerKey.contains(".private.key.")
                || lowerKey.endsWith(".private.key")
                || lowerKey.endsWith(".private_key")
                || lowerKey.endsWith(".passphrase")
                || "ssl.keystore.key".equals(lowerKey)
                || "ssl.key.pem".equals(lowerKey);
    }
}
