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

import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.security.SecurityChecker;
import org.apache.doris.common.Config;
import org.apache.doris.httpv2.client.InternalHttpClientProvider;
import org.apache.doris.httpv2.client.InternalHttpClientProviderFactory;
import org.apache.doris.httpv2.meta.MetaBaseAction;
import org.apache.doris.system.SystemInfoService.HostInfo;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.Map;

public class HttpURLUtil {

    public static HttpURLConnection getConnectionWithNodeIdent(String request) throws IOException {
        try {
            SecurityChecker.getInstance().startSSRFChecking(request);
            HttpURLConnection conn = InternalHttpClientProviderFactory.getProvider()
                    .openConnection(request, InternalHttpClientProvider.Target.FE);

            // Must use Env.getServingEnv() instead of getCurrentEnv(),
            // because here we need to obtain selfNode through the official service catalog.
            HostInfo selfNode = Env.getServingEnv().getSelfNode();
            conn.setRequestProperty(Env.CLIENT_NODE_HOST_KEY, selfNode.getHost());
            conn.setRequestProperty(Env.CLIENT_NODE_PORT_KEY, selfNode.getPort() + "");
            String token = Config.fe_meta_auth_token;
            if (!Strings.isNullOrEmpty(token)) {
                conn.setRequestProperty(MetaBaseAction.TOKEN, token);
            }
            return conn;
        } catch (Exception e) {
            throw new IOException(e);
        } finally {
            SecurityChecker.getInstance().stopSSRFChecking();
        }
    }

    public static Map<String, String> getNodeIdentHeaders() throws IOException {
        Map<String, String> headers = Maps.newHashMap();
        // Must use Env.getServingEnv() instead of getCurrentEnv(),
        // because here we need to obtain selfNode through the official service catalog.
        HostInfo selfNode = Env.getServingEnv().getSelfNode();
        headers.put(Env.CLIENT_NODE_HOST_KEY, selfNode.getHost());
        headers.put(Env.CLIENT_NODE_PORT_KEY, selfNode.getPort() + "");
        String token = Config.fe_meta_auth_token;
        if (!Strings.isNullOrEmpty(token)) {
            headers.put(MetaBaseAction.TOKEN, token);
        }
        return headers;
    }

    public static int getHttpPort() {
        return Config.enable_https ? Config.https_port : Config.http_port;
    }

    public static String buildInternalFeUrl(String host, String path, String queryParams) {
        String url = "http://" + NetUtils.getHostPortInAccessibleFormat(host, Config.http_port) + path;
        if (queryParams != null && !queryParams.isEmpty()) {
            url += "?" + queryParams;
        }
        return InternalHttpClientProviderFactory.getProvider()
                .normalizeInternalUrl(url, InternalHttpClientProvider.Target.FE);
    }

}
