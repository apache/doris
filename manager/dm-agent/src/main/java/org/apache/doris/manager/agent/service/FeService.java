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

package org.apache.doris.manager.agent.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.doris.manager.agent.common.AgentConstants;
import org.apache.doris.manager.agent.exception.AgentException;
import org.apache.doris.manager.agent.register.AgentContext;
import org.apache.doris.manager.common.domain.ServiceRole;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.File;
import java.io.IOException;
import java.util.Objects;

@Slf4j
public class FeService extends Service {

    public FeService(String installDir) {
        super(ServiceRole.FE, installDir, installDir + AgentConstants.FE_CONFIG_FILE_RELATIVE_PATH);
        doLoad();
    }

    @Override
    public void doLoad() {
        String httpPortStr = getConfig().getProperty(AgentConstants.FE_CONFIG_KEY_HTTP_PORT);
        if (Objects.isNull(httpPortStr)) {
            throw new AgentException("get config failed, key:" + AgentConstants.FE_CONFIG_KEY_HTTP_PORT + ", configFile:" + getConfigFilePath());
        }
        httpPort = Integer.valueOf(httpPortStr);
    }

    @Override
    public boolean isHealth() {
        CloseableHttpClient httpclient = HttpClients.createDefault();

        String requestUrl = "http://" + AgentContext.getAgentIp() + ":" + httpPort + "/api/bootstrap";

        HttpGet httpget = new HttpGet(requestUrl);

        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(5000)
                .setConnectionRequestTimeout(5000)
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

        String code = jsonObject.getString("code");
        String msg = jsonObject.getString("msg");
        String status = jsonObject.getString("status");
        if (("0".equals(code) && "success".equalsIgnoreCase(msg)) || "OK".equalsIgnoreCase(status)) {
            return true;
        }
        return false;
    }

    public void createMetaDir() {
        String dir = null;
        String metaDir = getConfig().getProperty(AgentConstants.FE_CONFIG_KEY_META_DIR);
        if (Objects.nonNull(metaDir) && metaDir.contains("${DORIS_HOME}/")) {
            String subDir = metaDir.substring(metaDir.indexOf("}/") + 1);
            if (subDir.length() <= 1) {
                return;
            }
            dir = installDir + subDir;
        } else if (Objects.nonNull(metaDir) && metaDir.startsWith("/")) {
            dir = metaDir;
        } else {
            dir = installDir + File.separator + metaDir;
        }

        if (Objects.nonNull(dir)) {
            File file = new File(dir);
            if (!file.exists()) {
                boolean r = file.mkdirs();
                log.info("create meta path:{},ret:{}", dir, r);
            }
        }
    }

    @Override
    public String serviceProcessKeyword() {
        return AgentConstants.PROCESS_KEYWORD_FE;
    }
}
