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

package org.apache.doris.httpv2.client;

import org.apache.http.impl.client.CloseableHttpClient;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.net.HttpURLConnection;

public interface InternalHttpClientProvider {
    enum Target {
        FE,
        BE
    }

    String normalizeInternalUrl(String url, Target target);

    HttpURLConnection openConnection(String url, Target target) throws IOException;

    CloseableHttpClient getHttpClient(Target target);

    RestTemplate getRestTemplate(Target target);
}
