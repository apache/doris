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

package org.apache.doris.httpv2.rest.manager;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.httpv2.entity.ResponseBody;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.system.Frontend;

import com.google.common.base.Strings;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.io.IOUtils;
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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;

/*
 * used to forward http requests from manager to be.
 */
public class HttpUtils {
    static final int REQUEST_SUCCESS_CODE = 0;
    static final int DEFAULT_TIME_OUT_MS = 2000;

    static List<Pair<String, Integer>> getFeList() {
        return Env.getCurrentEnv().getFrontends(null)
                .stream().filter(Frontend::isAlive).map(fe -> Pair.of(fe.getHost(), Config.http_port))
                .collect(Collectors.toList());
    }

    static String concatUrl(Pair<String, Integer> ipPort, String path, Map<String, String> arguments) {
        StringBuilder url = new StringBuilder("http://")
                .append(ipPort.first).append(":").append(ipPort.second).append(path);
        boolean isFirst = true;
        for (Map.Entry<String, String> entry : arguments.entrySet()) {
            if (!Strings.isNullOrEmpty(entry.getValue())) {
                if (isFirst) {
                    url.append("?");
                } else {
                    url.append("&");
                }
                isFirst = false;
                url.append(entry.getKey()).append("=").append(entry.getValue());
            }
        }
        return url.toString();
    }

    public static String doGet(String url, Map<String, String> headers, int timeoutMs) throws IOException {
        HttpGet httpGet = new HttpGet(url);
        setRequestConfig(httpGet, headers, timeoutMs);
        return executeRequest(httpGet);
    }

    public static String doGet(String url, Map<String, String> headers) throws IOException {
        return doGet(url, headers, DEFAULT_TIME_OUT_MS);
    }

    static String doPost(String url, Map<String, String> headers, Object body) throws IOException {
        HttpPost httpPost = new HttpPost(url);
        if (Objects.nonNull(body)) {
            String jsonString = GsonUtils.GSON.toJson(body);
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
        CloseableHttpClient client = HttpClientBuilder.create().build();
        return client.execute(request, httpResponse -> EntityUtils.toString(httpResponse.getEntity()));
    }

    static String parseResponse(String response) {
        ResponseBody responseEntity = GsonUtils.GSON.fromJson(response, new TypeToken<ResponseBody>() {}.getType());
        if (responseEntity.getCode() != REQUEST_SUCCESS_CODE) {
            throw new RuntimeException(responseEntity.getMsg());
        }
        return GsonUtils.GSON.toJson(responseEntity.getData());
    }

    public static String getBody(HttpServletRequest request) throws IOException {
        return IOUtils.toString(request.getInputStream(), StandardCharsets.UTF_8);
    }
}
