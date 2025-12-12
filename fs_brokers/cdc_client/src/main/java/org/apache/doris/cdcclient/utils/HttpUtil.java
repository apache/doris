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

package org.apache.doris.cdcclient.utils;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.NoConnectionReuseStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.protocol.HttpRequestExecutor;
import org.apache.http.protocol.RequestContent;

public class HttpUtil {
    private static int connectTimeout = 30 * 1000;
    private static int waitForContinueTimeout = 60 * 1000;
    private static int socketTimeout = 10 * 60 * 1000; // stream load timeout 10 min

    public static CloseableHttpClient getHttpClient() {
        return HttpClients.custom()
                // default timeout 3s, maybe report 307 error when fe busy
                .setRequestExecutor(new HttpRequestExecutor(waitForContinueTimeout))
                .setRedirectStrategy(
                        new DefaultRedirectStrategy() {
                            @Override
                            protected boolean isRedirectable(String method) {
                                return true;
                            }
                        })
                .setRetryHandler((exception, executionCount, context) -> false)
                .setConnectionReuseStrategy(NoConnectionReuseStrategy.INSTANCE)
                .setDefaultRequestConfig(
                        RequestConfig.custom()
                                .setConnectTimeout(connectTimeout)
                                .setConnectionRequestTimeout(connectTimeout)
                                .setSocketTimeout(socketTimeout)
                                .build())
                .addInterceptorLast(new RequestContent(true))
                .build();
    }
}
