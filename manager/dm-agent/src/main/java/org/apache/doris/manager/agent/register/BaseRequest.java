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
import org.apache.doris.manager.common.domain.RResult;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class BaseRequest {

    private static final Logger log = LoggerFactory.getLogger(BaseRequest.class);

    public static RResult sendRequest(String requestUrl, Map<String, Object> params) {
        CloseableHttpClient httpclient = HttpClients.createDefault();
        HttpPost httpPost = new HttpPost(requestUrl);

        RequestConfig requestConfig = RequestConfig.custom().
                setConnectTimeout(5000).
                setConnectionRequestTimeout(5000)
                .setSocketTimeout(5000).build();
        httpPost.setConfig(requestConfig);

        httpPost.setEntity(new StringEntity(JSON.toJSONString(params), "utf-8"));
        httpPost.addHeader("Content-Type", "application/json");

        CloseableHttpResponse response = null;
        String result = "";

        try {
            response = httpclient.execute(httpPost);
            result = EntityUtils.toString(response.getEntity());
        } catch (IOException e) {
            log.error("request url error:{},param:{}",requestUrl,params,e);
            throw new RuntimeException(e);
        }

        RResult res = JSON.parseObject(result, RResult.class);
        return res;
    }

}
