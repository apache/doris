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

import org.apache.doris.stack.model.palo.PaloResponseEntity;
import org.apache.doris.stack.entity.ClusterInfoEntity;
import org.apache.doris.stack.exception.PaloRequestException;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
@Slf4j
public class PaloLoginClient extends PaloClient {
    protected HttpClientPoolManager poolManager;

    @Autowired
    public PaloLoginClient(HttpClientPoolManager poolManager) {
        this.poolManager = poolManager;
    }

    public boolean loginPalo(ClusterInfoEntity entity) throws Exception {
        String url = getHostUrl(entity.getAddress(), entity.getHttpPort());
        url += "/rest/v1/login";
        log.debug("Send login palo request, url is {}.", url);
        Map<String, String> headers = Maps.newHashMap();
        setHeaders(headers);
        setPostHeaders(headers);
        setAuthHeaders(headers, entity.getUser(), entity.getPasswd());

        PaloResponseEntity response = poolManager.doPost(url, headers, null);
        if (response.getCode() != LOGIN_SUCCESS_CODE) {
            throw new PaloRequestException("Login palo error:" + response.getData());
        }
        log.info("Login palo cluster {} successful.", entity.getAddress());
        return true;
    }
}
