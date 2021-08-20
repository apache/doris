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
import org.apache.doris.stack.model.palo.ClusterOverviewInfo;
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
public class PaloStatisticClient extends PaloClient {
    protected HttpClientPoolManager poolManager;

    @Autowired
    public PaloStatisticClient(HttpClientPoolManager poolManager) {
        this.poolManager = poolManager;
    }

    public ClusterOverviewInfo getClusterInfo(ClusterInfoEntity entity) throws Exception {
        String url = getHostUrl(entity.getAddress(), entity.getHttpPort());
        StringBuffer buffer = new StringBuffer();
        buffer.append(url);
        buffer.append("/rest/v2/api/cluster_overview");
        url = buffer.toString();
        log.debug("get cluster info request, url is {}.", url);

        Map<String, String> headers = Maps.newHashMap();
        setHeaders(headers);
        setAuthHeaders(headers, entity.getUser(), entity.getPasswd());

        PaloResponseEntity response = poolManager.doGet(url, headers);
        if (response.getCode() != REQUEST_SUCCESS_CODE) {
            throw new PaloRequestException("Get cluster overview info exception:" + response.getData());
        }
        return JSON.parseObject(response.getData(), ClusterOverviewInfo.class);
    }
}
