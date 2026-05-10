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

package org.apache.doris.connector.maxcompute;

import java.time.ZoneId;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Resolves the MaxCompute service endpoint from catalog properties.
 * Handles backward compatibility with legacy property keys
 * (mc.region, mc.tunnel_endpoint, mc.odps_endpoint).
 */
public final class MCConnectorEndpoint {
    private static final String ENDPOINT_TEMPLATE =
            "http://service.{}.maxcompute.aliyun-inc.com/api";

    private static final Map<String, ZoneId> REGION_ZONE_MAP;

    static {
        Map<String, ZoneId> map = new HashMap<>();
        map.put("cn-hangzhou", ZoneId.of("Asia/Shanghai"));
        map.put("cn-shanghai", ZoneId.of("Asia/Shanghai"));
        map.put("cn-shanghai-finance-1", ZoneId.of("Asia/Shanghai"));
        map.put("cn-beijing", ZoneId.of("Asia/Shanghai"));
        map.put("cn-north-2-gov-1", ZoneId.of("Asia/Shanghai"));
        map.put("cn-zhangjiakou", ZoneId.of("Asia/Shanghai"));
        map.put("cn-wulanchabu", ZoneId.of("Asia/Shanghai"));
        map.put("cn-shenzhen", ZoneId.of("Asia/Shanghai"));
        map.put("cn-shenzhen-finance-1", ZoneId.of("Asia/Shanghai"));
        map.put("cn-chengdu", ZoneId.of("Asia/Shanghai"));
        map.put("cn-hongkong", ZoneId.of("Asia/Shanghai"));
        map.put("ap-southeast-1", ZoneId.of("Asia/Singapore"));
        map.put("ap-southeast-2", ZoneId.of("Australia/Sydney"));
        map.put("ap-southeast-3", ZoneId.of("Asia/Kuala_Lumpur"));
        map.put("ap-southeast-5", ZoneId.of("Asia/Jakarta"));
        map.put("ap-northeast-1", ZoneId.of("Asia/Tokyo"));
        map.put("eu-central-1", ZoneId.of("Europe/Berlin"));
        map.put("eu-west-1", ZoneId.of("Europe/London"));
        map.put("us-west-1", ZoneId.of("America/Los_Angeles"));
        map.put("us-east-1", ZoneId.of("America/New_York"));
        map.put("me-east-1", ZoneId.of("Asia/Dubai"));
        REGION_ZONE_MAP = Collections.unmodifiableMap(map);
    }

    private MCConnectorEndpoint() {
    }

    /**
     * Resolves the MaxCompute service endpoint from the given properties.
     * Priority order:
     * 1. mc.endpoint (new property)
     * 2. mc.tunnel_endpoint (legacy, converted)
     * 3. mc.odps_endpoint (legacy, used as-is)
     * 4. mc.region (legacy, template-based)
     */
    public static String resolveEndpoint(Map<String, String> properties) {
        if (properties.containsKey(MCConnectorProperties.ENDPOINT)) {
            return properties.get(MCConnectorProperties.ENDPOINT);
        }
        if (properties.containsKey(
                MCConnectorProperties.TUNNEL_SDK_ENDPOINT)) {
            String tunnelEndpoint = properties.get(
                    MCConnectorProperties.TUNNEL_SDK_ENDPOINT);
            return tunnelEndpoint.replace("//dt", "//service") + "/api";
        }
        if (properties.containsKey(
                MCConnectorProperties.ODPS_ENDPOINT)) {
            return properties.get(MCConnectorProperties.ODPS_ENDPOINT);
        }
        if (properties.containsKey(MCConnectorProperties.REGION)) {
            String region = properties.get(MCConnectorProperties.REGION);
            if (region.startsWith("oss-")) {
                region = region.replace("oss-", "");
            }
            boolean enablePublicAccess = Boolean.parseBoolean(
                    properties.getOrDefault(
                            MCConnectorProperties.PUBLIC_ACCESS,
                            MCConnectorProperties.DEFAULT_PUBLIC_ACCESS));
            String endpoint =
                    ENDPOINT_TEMPLATE.replace("{}", region);
            if (enablePublicAccess) {
                endpoint = endpoint.replace("-inc", "");
            }
            return endpoint;
        }
        return null;
    }

    /**
     * Derives the project timezone from the service endpoint URL.
     * Parses the region from the endpoint and maps it to a timezone.
     * Falls back to system default if region is unknown.
     */
    public static ZoneId resolveProjectTimeZone(String endpoint) {
        if (endpoint == null) {
            return ZoneId.systemDefault();
        }
        String[] parts = endpoint.split("\\.");
        if (parts.length >= 2) {
            String regionAndSuffix = parts[1];
            String region = regionAndSuffix.replace("-vpc", "")
                    .replace("-intranet", "");
            if (REGION_ZONE_MAP.containsKey(region)) {
                return REGION_ZONE_MAP.get(region);
            }
        }
        return ZoneId.systemDefault();
    }
}
