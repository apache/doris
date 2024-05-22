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

package org.apache.doris.cdcloader.mysql.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.doris.cdcloader.common.rest.ResponseBody;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import javax.servlet.http.HttpServletRequest;

/*
 * used to forward http requests from manager to be.
 */
public class HttpUtils {
    static final int REQUEST_SUCCESS_CODE = 0;
    static final int DEFAULT_TIME_OUT_MS = 5000;
    private static final ObjectMapper objectMapper = new ObjectMapper();
    public static String doGet(String url, Map<String, String> headers, int timeoutMs) throws IOException {
        HttpGet httpGet = new HttpGet(url);
        setRequestConfig(httpGet, headers, timeoutMs);
        return executeRequest(httpGet);
    }

    public static String doGet(String url, Map<String, String> headers) throws IOException {
        return doGet(url, headers, DEFAULT_TIME_OUT_MS);
    }

    public static String doPost(String url) throws IOException {
        return doPost(url, new HashMap<>(), null);
    }

    public static String doPost(String url, Map<String, String> headers, Object body) throws IOException {
        HttpPost httpPost = new HttpPost(url);
        if (Objects.nonNull(body)) {
            String jsonString = objectMapper.writeValueAsString(body);
            StringEntity stringEntity = new StringEntity(jsonString, "UTF-8");
            httpPost.setEntity(stringEntity);
        }

        setRequestConfig(httpPost, headers, DEFAULT_TIME_OUT_MS);
        return executeRequest(httpPost);
    }

    private static void setRequestConfig(HttpRequestBase request, Map<String, String> headers, int timeoutMs) {
        if (null != headers) {
            for (String key : headers.keySet()) {
                request.setHeader(key, headers.get(key));
            }
        }

        RequestConfig config = RequestConfig.custom()
                .setConnectTimeout(timeoutMs)
                .setConnectionRequestTimeout(timeoutMs)
                .setSocketTimeout(timeoutMs)
                .build();
        request.setConfig(config);
    }

    private static String executeRequest(HttpRequestBase request) throws IOException {
        try (CloseableHttpClient client = HttpClientBuilder.create().build()) {
            return client.execute(request, httpResponse -> EntityUtils.toString(httpResponse.getEntity()));
        }
    }

    public static String parseResponse(String response) throws IOException {
        ResponseBody responseEntity = objectMapper.readValue(response, ResponseBody.class);
        if (responseEntity.getCode() != REQUEST_SUCCESS_CODE) {
            throw new RuntimeException(responseEntity.getMsg());
        }
        return objectMapper.writeValueAsString(responseEntity.getData());
    }

    public static String getBody(HttpServletRequest request) throws IOException {
        return IOUtils.toString(request.getInputStream(), StandardCharsets.UTF_8);
    }
}
