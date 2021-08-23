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

package org.apache.doris.stack.connector;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import org.apache.doris.stack.model.palo.PaloResponseEntity;
import org.apache.doris.stack.entity.ClusterInfoEntity;
import org.apache.doris.stack.exception.PaloRequestException;
import org.apache.doris.stack.rest.ResponseEntityBuilder;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
@Slf4j
public class PaloForwardManagerClient extends PaloClient {
    protected HttpClientPoolManager poolManager;

    @Autowired
    public PaloForwardManagerClient(HttpClientPoolManager poolManager) {
        this.poolManager = poolManager;
    }

    public Object forwardGet(String url, ClusterInfoEntity entity) throws Exception {
        log.debug("get request, url is {}.", url);

        Map<String, String> headers = Maps.newHashMap();
        setHeaders(headers);
        setAuthHeaders(headers, entity.getUser(), entity.getPasswd());
        PaloResponseEntity response;
        try {
            response = poolManager.doGet(url, headers);
        } catch (JSONException e) {
            return ResponseEntityBuilder.notFound("not fount path");
        }
        if (response.getCode() != REQUEST_SUCCESS_CODE) {
            throw new PaloRequestException("get exception:" + response.getData() + " url:" + url);
        }

        return ResponseEntityBuilder.ok(JSON.parse(response.getData()));
    }

    public Object forwardPost(String url, String requestBody, ClusterInfoEntity entity) throws Exception {
        log.debug("post request, url is {}.", url);

        Map<String, String> headers = Maps.newHashMap();
        setHeaders(headers);
        setAuthHeaders(headers, entity.getUser(), entity.getPasswd());
        headers.put("Content-Type", "application/json");
        PaloResponseEntity response;
        try {
            response = poolManager.forwardPost(url, headers, requestBody);
        } catch (JSONException e) {
            return ResponseEntityBuilder.notFound("not fount path");
        }
        if (response.getCode() != REQUEST_SUCCESS_CODE) {
            throw new PaloRequestException("post exception:" + response.getData());
        }
        return ResponseEntityBuilder.ok(JSON.parse(response.getData()));
    }
}
