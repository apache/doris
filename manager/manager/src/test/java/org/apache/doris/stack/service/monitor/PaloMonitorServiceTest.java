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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import org.apache.doris.stack.model.request.monitor.ClusterMonitorReq;
import org.apache.doris.stack.component.ClusterUserComponent;
import org.apache.doris.stack.connector.PaloMonitorClient;
import org.apache.doris.stack.entity.ClusterInfoEntity;
import org.apache.doris.stack.service.user.AuthenticationService;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.util.Lists;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.List;

@RunWith(JUnit4.class)
@Slf4j
public class PaloMonitorServiceTest {
    @InjectMocks
    private PaloMonitorService monitorService;

    @Mock
    private AuthenticationService authenticationService;

    @Mock
    private PaloMonitorClient paloMonitorClient;

    @Mock
    private ClusterUserComponent clusterUserComponent;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    /**
     * Test monitoring forwarding interface
     */
    @Test
    public void monitorTest() {
        log.debug("Get doris monitor test.");
        int userId = 1;
        int clusterId = 2;
         // mock cluster
        ClusterInfoEntity clusterInfo = new ClusterInfoEntity();
        clusterInfo.setId(clusterId);
        clusterInfo.setName("doris1");
        clusterInfo.setAddress("10.23.32.32");
        clusterInfo.setHttpPort(8030);
        clusterInfo.setQueryPort(8031);
        clusterInfo.setUser("admin");
        clusterInfo.setPasswd("1234");
        clusterInfo.setTimezone("Asia/Shanghai");

        // Testing interfaces that do not require requesting content
        try {
            when(authenticationService.checkUserAuthWithCookie(any(), any())).thenReturn(userId);
            when(clusterUserComponent.getClusterByUserId(userId)).thenReturn(clusterInfo);
            monitorService.nodeNum(null, null, null);
            monitorService.disksCapacity(null, null, null);
            monitorService.feList(null, null, null);
            monitorService.beList(null, null, null);
        } catch (Exception e) {
            log.debug("Get doris monitor test error.");
            e.printStackTrace();
        }

        // Test the interface that needs to request content
        // mock request
        ClusterMonitorReq monitorReq = new ClusterMonitorReq();
        monitorReq.setStart(1L);
        monitorReq.setEnd(3L);
        monitorReq.setPointNum(3);
        monitorReq.setQuantile("quantile");
        List<String> nodes = Lists.newArrayList("node1", "node2");
        monitorReq.setNodes(nodes);

        // Testing interfaces that do not require requesting content
        try {
            when(authenticationService.checkUserAuthWithCookie(any(), any())).thenReturn(userId);
            when(clusterUserComponent.getClusterByUserId(userId)).thenReturn(clusterInfo);
            monitorService.qps(monitorReq, null, null);
            monitorService.queryLatency(monitorReq, null, null);
            monitorService.queryErrRate(monitorReq, null, null);
            monitorService.connTotal(monitorReq, null, null);
            monitorService.txnStatus(monitorReq, null, null);
            monitorService.beCpuIdle(monitorReq, null, null);
            monitorService.beMem(monitorReq, null, null);
            monitorService.beDiskIO(monitorReq, null, null);
            monitorService.beBaseCompactionScore(monitorReq, null, null);
            monitorService.beCumuCompactionScore(monitorReq, null, null);
        } catch (Exception e) {
            log.debug("Get doris monitor test error.");
            e.printStackTrace();
        }
    }
}
