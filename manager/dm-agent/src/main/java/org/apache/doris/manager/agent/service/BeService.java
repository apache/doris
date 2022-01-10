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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class BeService extends Service {

    private static final Logger TASKLOG = LoggerFactory.getLogger(AgentConstants.LOG_TYPE_TASK);

    public BeService(String installDir) {
        super(ServiceRole.BE, installDir, installDir + AgentConstants.BE_CONFIG_FILE_RELATIVE_PATH);
        doLoad();
    }

    private static List<String> parseStorageDirs(String storageRootPath) {
        ArrayList<String> list = new ArrayList<>();
        String[] splitArr = storageRootPath.split(";");
        for (String split : splitArr) {
            if (split.trim().length() <= 0) {
                continue;
            }

            int lastIndex = split.indexOf(",");
            if (lastIndex == -1) {
                list.add(split);
            } else {
                list.add(split.substring(0, lastIndex));
            }
        }
        return list;
    }

    @Override
    public void doLoad() {
        String httpPortStr = getConfig().getProperty(AgentConstants.BE_CONFIG_KEY_HTTP_PORT);
        if (Objects.isNull(httpPortStr)) {
            throw new AgentException("get config failed, key:" + AgentConstants.BE_CONFIG_KEY_HTTP_PORT + ", configFile:" + getConfigFilePath());
        }
        httpPort = Integer.valueOf(httpPortStr);
    }

    @Override
    public boolean isHealth() {
        CloseableHttpClient httpclient = HttpClients.createDefault();

        String requestUrl = "http://" + AgentContext.getAgentIp() + ":" + httpPort + "/api/health";

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

        String status = jsonObject.getString("status");
        if ("ok".equalsIgnoreCase(status)) {
            return true;
        }
        return false;
    }

    public void createStrorageDir() {
        String storageVal = null;
        String storageValInConfig = getConfig().getProperty(AgentConstants.BE_CONFIG_KEY_STORAGE_DIR);
        if (Objects.nonNull(storageValInConfig) && storageValInConfig.contains("${DORIS_HOME}/")) {
            String subDir = storageValInConfig.substring(storageValInConfig.indexOf("}/") + 1);
            if (subDir.length() <= 1 || subDir.contains("${") || subDir.contains(".") || subDir.contains(";")) {
                return;
            }
            storageVal = installDir + subDir;
        } else if (Objects.nonNull(storageValInConfig) && storageValInConfig.startsWith("/")) {
            storageVal = storageValInConfig;
        } else {
            TASKLOG.info("be storage path must be start ${DORIS_HOME} or /,path={}", storageVal);
            throw new AgentException("parse be storage path failed");
        }

        if (Objects.nonNull(storageVal)) {
            List<String> list = parseStorageDirs(storageVal);
            for (String dir : list) {
                File file = new File(dir);
                if (!file.exists()) {
                    boolean r = file.mkdirs();
                    TASKLOG.info("create storage path:{},ret:{}", dir, r);
                }
            }
        }
    }

    @Override
    public String serviceProcessKeyword() {
        return AgentConstants.PROCESS_KEYWORD_BE;
    }
}
