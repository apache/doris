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
import org.apache.doris.system.SystemInfoService.HostInfo;

import com.google.common.collect.Maps;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;

public class HttpURLUtil {

    public static HttpURLConnection getConnectionWithNodeIdent(String request) throws IOException {
        try {
            SecurityChecker.getInstance().startSSRFChecking(request);
            URL url = new URL(request);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            // Must use Env.getServingEnv() instead of getCurrentEnv(),
            // because here we need to obtain selfNode through the official service catalog.
            HostInfo selfNode = Env.getServingEnv().getSelfNode();
            conn.setRequestProperty(Env.CLIENT_NODE_HOST_KEY, selfNode.getHost());
            conn.setRequestProperty(Env.CLIENT_NODE_PORT_KEY, selfNode.getPort() + "");
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
        return headers;
    }

}
