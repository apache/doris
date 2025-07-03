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

package org.pentaho.di.trans.steps.dorisstreamloader.load;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;

/** util to build http client. */
public class HttpUtil {

    private RequestConfig requestConfig =
            RequestConfig.custom()
                    .setConnectTimeout(60 * 1000)
                    .setConnectionRequestTimeout(60 * 1000)
                    // default checkpoint timeout is 10min
                    .setSocketTimeout(9 * 60 * 1000)
                    .build();

    public HttpClientBuilder getHttpClientBuilderForBatch() {
        return HttpClients.custom()
                .setRedirectStrategy(
                        new DefaultRedirectStrategy() {
                            @Override
                            protected boolean isRedirectable(String method) {
                                return true;
                            }
                        })
                .setDefaultRequestConfig(requestConfig);
    }

}
