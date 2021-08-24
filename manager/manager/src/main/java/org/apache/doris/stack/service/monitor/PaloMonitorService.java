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

package org.apache.doris.stack.service.monitor;

import org.apache.doris.stack.model.palo.ClusterMonitorInfo;
import org.apache.doris.stack.model.request.monitor.ClusterMonitorReq;
import org.apache.doris.stack.component.ClusterUserComponent;
import org.apache.doris.stack.connector.PaloMonitorClient;
import org.apache.doris.stack.entity.ClusterInfoEntity;
import org.apache.doris.stack.service.BaseService;
import org.apache.doris.stack.service.user.AuthenticationService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@Service
@Slf4j
public class PaloMonitorService extends BaseService {

    @Autowired
    private AuthenticationService authenticationService;

    @Autowired
    private PaloMonitorClient paloMonitorClient;

    @Autowired
    private ClusterUserComponent clusterUserComponent;

    private ClusterMonitorInfo checkAndHandleCluster(HttpServletRequest request, HttpServletResponse response) throws Exception {
        int studioUserId = authenticationService.checkUserAuthWithCookie(request, response);
        authenticationService.checkUserIsAdmin(studioUserId);
        ClusterMonitorInfo info = new ClusterMonitorInfo();

        ClusterInfoEntity clusterInfo = clusterUserComponent.getClusterByUserId(studioUserId);
        info.setHost(clusterInfo.getAddress());
        info.setHttpPort(clusterInfo.getHttpPort());

        return info;
    }

    /**
     * @param req
     * @param info
     * @return
     */
    private ClusterMonitorInfo assembleInfo(ClusterMonitorReq req, ClusterMonitorInfo info) {
        if (req.getNodes() != null) {
            info.setNodes(req.getNodes());
        }
        info.setStart(req.getStart());
        info.setEnd(req.getEnd());
        if (req.getPointNum() != null) {
            info.setPointNum(req.getPointNum());
        }
        if (!StringUtils.isEmpty(req.getQuantile())) {
            info.setQuantile(req.getQuantile());
        }
        return info;
    }

    public Object nodeNum(ClusterMonitorReq req, HttpServletRequest request, HttpServletResponse response) throws Exception {
        ClusterMonitorInfo info = checkAndHandleCluster(request, response);
        return paloMonitorClient.getNodeNum(info);
    }

    public Object disksCapacity(ClusterMonitorReq req, HttpServletRequest request, HttpServletResponse response) throws Exception {
        ClusterMonitorInfo info = checkAndHandleCluster(request, response);
        return paloMonitorClient.getDisksCapacity(info);
    }

    public Object feList(ClusterMonitorReq req, HttpServletRequest request, HttpServletResponse response) throws Exception {
        ClusterMonitorInfo info = checkAndHandleCluster(request, response);
        return paloMonitorClient.getFeList(info);
    }

    public Object beList(ClusterMonitorReq req, HttpServletRequest request, HttpServletResponse response) throws Exception {
        ClusterMonitorInfo info = checkAndHandleCluster(request, response);
        return paloMonitorClient.getBeList(info);
    }

    public Object qps(ClusterMonitorReq req, HttpServletRequest request, HttpServletResponse response) throws Exception {
        ClusterMonitorInfo info = checkAndHandleCluster(request, response);
        assembleInfo(req, info);
        return paloMonitorClient.getQPS(info);
    }

    public Object queryLatency(ClusterMonitorReq req, HttpServletRequest request, HttpServletResponse response) throws Exception {
        ClusterMonitorInfo info = checkAndHandleCluster(request, response);
        assembleInfo(req, info);
        return paloMonitorClient.getQueryLatency(info);
    }

    public Object queryErrRate(ClusterMonitorReq req, HttpServletRequest request, HttpServletResponse response) throws Exception {
        ClusterMonitorInfo info = checkAndHandleCluster(request, response);
        assembleInfo(req, info);
        return paloMonitorClient.getQueryErrRate(info);
    }

    public Object connTotal(ClusterMonitorReq req, HttpServletRequest request, HttpServletResponse response) throws Exception {
        ClusterMonitorInfo info = checkAndHandleCluster(request, response);
        assembleInfo(req, info);
        return paloMonitorClient.getConnTotal(info);
    }

    public Object txnStatus(ClusterMonitorReq req, HttpServletRequest request, HttpServletResponse response) throws Exception {
        ClusterMonitorInfo info = checkAndHandleCluster(request, response);
        assembleInfo(req, info);
        return paloMonitorClient.getTxnStatus(info);
    }

    public Object beCpuIdle(ClusterMonitorReq req, HttpServletRequest request, HttpServletResponse response) throws Exception {
        ClusterMonitorInfo info = checkAndHandleCluster(request, response);
        assembleInfo(req, info);
        return paloMonitorClient.getBeCpuIdle(info);
    }

    public Object beMem(ClusterMonitorReq req, HttpServletRequest request, HttpServletResponse response) throws Exception {
        ClusterMonitorInfo info = checkAndHandleCluster(request, response);
        assembleInfo(req, info);
        return paloMonitorClient.getBeMem(info);
    }

    public Object beDiskIO(ClusterMonitorReq req, HttpServletRequest request, HttpServletResponse response) throws Exception {
        ClusterMonitorInfo info = checkAndHandleCluster(request, response);
        assembleInfo(req, info);
        return paloMonitorClient.getBeDiskIO(info);
    }

    public Object beBaseCompactionScore(ClusterMonitorReq req, HttpServletRequest request, HttpServletResponse response) throws Exception {
        ClusterMonitorInfo info = checkAndHandleCluster(request, response);
        assembleInfo(req, info);
        return paloMonitorClient.getBeBaseCompactionScore(info);
    }

    public Object beCumuCompactionScore(ClusterMonitorReq req, HttpServletRequest request, HttpServletResponse response) throws Exception {
        ClusterMonitorInfo info = checkAndHandleCluster(request, response);
        assembleInfo(req, info);
        return paloMonitorClient.getBeCumuCompactionScore(info);
    }

}
