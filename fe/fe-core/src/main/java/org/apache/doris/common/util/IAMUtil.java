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

package org.apache.doris.common.util;

import org.apache.doris.common.Config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.util.DigestUtils;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;

public class IAMUtil {
    private static final Logger LOG = LogManager.getLogger(IAMUtil.class);
    private static Set<String> sourceWhitelistSet = Sets.newHashSet();
    private static final CloseableHttpClient httpClient = HttpClients.createDefault();
    private static final ThreadLocal<ObjectMapper> threadLocalObjectMapper = ThreadLocal.withInitial(
            () -> new ObjectMapper());
    private static String IAMEndpoint = "";
    private static String IAMToken = "";
    private static Map<String, String> userToTokenMap = Maps.newConcurrentMap();

    public static void init() {
        for (String source : Config.iam_source_whitelist) {
            sourceWhitelistSet.add(source);
        }
        IAMEndpoint = System.getenv("IAM_URL");
        IAMToken = System.getenv("IAM_TOKEN");
    }

    public static ObjectMapper getObjectMapperInstance() {
        return threadLocalObjectMapper.get();
    }

    public static boolean isSourceInWhitelist(String source) {
        return sourceWhitelistSet.contains(source);
    }

    public static String getUserTokenByHadoopUserName(String erp, String hadoopUserName) {
        if (userToTokenMap.containsKey(hadoopUserName)) {
            return userToTokenMap.get(hadoopUserName);
        } else {
            String userToken = getUserTokenFromIAMEndpoint(erp, hadoopUserName);
            if (userToken != null) {
                userToTokenMap.put(hadoopUserName, userToken);
            }
            return userToken;
        }
    }

    public static String getUserTokenFromIAMEndpoint(String erp, String hadoopUserName) {
        try {
            HttpPost post = new HttpPost(IAMEndpoint);
            post.setHeader("Content-Type", "application/json");
            post.setHeader("appId", Config.default_source);
            String time = String.valueOf(System.currentTimeMillis());
            String sign = DigestUtils.md5DigestAsHex((Config.default_source + IAMToken + time).getBytes(
                    StandardCharsets.UTF_8));
            post.setHeader("sign", sign);
            post.setHeader("time", time);
            ObjectNode json = getObjectMapperInstance().createObjectNode();
            json.put("proposer", erp);
            json.put("accountCode", hadoopUserName);
            json.put("appCode", Config.default_source);
            json.put("appName", Config.default_source + "平台");
            StringEntity entity = new StringEntity(json.toString());
            post.setEntity(entity);
            try (CloseableHttpResponse response = httpClient.execute(post)) {
                int statusCode = response.getStatusLine().getStatusCode();
                if (statusCode == 200) {
                    String result = EntityUtils.toString(response.getEntity());
                    JsonNode jsonNode = getObjectMapperInstance().readTree(result);
                    return jsonNode.get("data").get("entity").asText();
                }
            }
        } catch (Exception e) {
            LOG.warn("failed to get user token from iam endpoint", e);
        }
        return null;
    }
}
