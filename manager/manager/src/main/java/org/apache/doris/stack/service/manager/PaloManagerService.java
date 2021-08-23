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

package org.apache.doris.stack.service.manager;

import org.apache.doris.stack.component.ClusterUserComponent;
import org.apache.doris.stack.connector.PaloForwardManagerClient;
import org.apache.doris.stack.entity.ClusterInfoEntity;
import org.apache.doris.stack.service.BaseService;
import org.apache.doris.stack.service.user.AuthenticationService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@Service
@Slf4j
public class PaloManagerService extends BaseService {
    private static final String HTTP = "http://";

    @Autowired
    private AuthenticationService authenticationService;

    @Autowired
    private PaloForwardManagerClient paloForwardManagerClient;

    @Autowired
    private ClusterUserComponent clusterUserComponent;

    private ClusterInfoEntity checkAndHandleCluster(HttpServletRequest request, HttpServletResponse response) throws Exception {
        int studioUserId = authenticationService.checkUserAuthWithCookie(request, response);
        authenticationService.checkUserIsAdmin(studioUserId);

        return clusterUserComponent.getClusterByUserId(studioUserId);
    }

    public Object get(HttpServletRequest request, HttpServletResponse response, String requestPath) throws Exception {
        ClusterInfoEntity entity = checkAndHandleCluster(request, response);
        return paloForwardManagerClient.forwardGet(
                concatUrl(entity.getAddress(), entity.getHttpPort(), requestPath), entity);
    }

    public Object post(HttpServletRequest request, HttpServletResponse response, String requestPath,
                       String requestBody) throws Exception {
        ClusterInfoEntity entity = checkAndHandleCluster(request, response);
        return paloForwardManagerClient.forwardPost(
                concatUrl(entity.getAddress(), entity.getHttpPort(), requestPath), requestBody, entity);
    }

    private String concatUrl(String host, int httpPort, String requestPath) {
        return HTTP + host + ":" + httpPort + requestPath;
    }
}
