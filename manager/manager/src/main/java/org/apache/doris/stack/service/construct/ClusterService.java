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

package org.apache.doris.stack.service.construct;

import org.apache.doris.stack.model.palo.ClusterOverviewInfo;
import org.apache.doris.stack.model.response.construct.ClusterOverviewResp;
import org.apache.doris.stack.component.ClusterUserComponent;
import org.apache.doris.stack.connector.PaloStatisticClient;
import org.apache.doris.stack.entity.ClusterInfoEntity;
import org.apache.doris.stack.service.BaseService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class ClusterService extends BaseService {

    private ClusterUserComponent clusterUserComponent;

    private PaloStatisticClient statisticClient;

    @Autowired
    public ClusterService(ClusterUserComponent clusterUserComponent,
                          PaloStatisticClient statisticClient) {
        this.clusterUserComponent = clusterUserComponent;
        this.statisticClient = statisticClient;
    }

    /**
     * Obtain the operation and maintenance information of the user's Doris cluster
     * @return
     */
    public ClusterOverviewResp getClusterOpInfo(int userId) throws Exception {

        ClusterInfoEntity clusterInfo = clusterUserComponent.getClusterByUserId(userId);
        ClusterOverviewInfo result = statisticClient.getClusterInfo(clusterInfo);

        ClusterOverviewResp resp = new ClusterOverviewResp(result);
        log.info("Get Palo cluster info success.");
        return resp;
    }
}
