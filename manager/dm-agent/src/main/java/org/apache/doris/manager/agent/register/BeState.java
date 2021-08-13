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
package org.apache.doris.manager.agent.register;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;

public class BeState {
    public static boolean isHealth() {
        CloseableHttpClient httpclient = HttpClients.createDefault();

        String requestUrl = "http://" + AgentContext.getAgentIp() + ":" + AgentContext.getHealthCheckPort() + "/api/health";

        HttpGet httpget = new HttpGet(requestUrl);

        RequestConfig requestConfig = RequestConfig.custom().
                setConnectTimeout(5000).
                setConnectionRequestTimeout(5000)
                .setSocketTimeout(5000).build();
        httpget.setConfig(requestConfig);

        CloseableHttpResponse response = null;
        String result = "";

        try {
            response = httpclient.execute(httpget);
            result = EntityUtils.toString(response.getEntity());
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

        JSONObject jsonObject = JSON.parseObject(result);
        if (jsonObject == null) {
            return false;
        }

        String status = jsonObject.getString("status");
        if ("ok".equalsIgnoreCase(status)) {
            return true;
        }
        return false;
    }
}
