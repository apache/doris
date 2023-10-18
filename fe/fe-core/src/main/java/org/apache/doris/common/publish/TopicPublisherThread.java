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

package org.apache.doris.common.publish;

import org.apache.doris.common.ClientPool;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.BackendService;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPublishTopicRequest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class TopicPublisherThread extends MasterDaemon {

    private static final Logger LOG = LogManager.getLogger(TopicPublisherThread.class);

    private SystemInfoService clusterInfoService;

    public TopicPublisherThread() {}

    public TopicPublisherThread(String name, long intervalMs,
            SystemInfoService clusterInfoService) {
        super(name, intervalMs);
        this.clusterInfoService = clusterInfoService;
    }

    private List<TopicPublisher> topicPublisherList = new ArrayList<>();

    public void addToTopicPublisherList(TopicPublisher topicPublisher) {
        this.topicPublisherList.add(topicPublisher);
    }

    @Override
    protected void runAfterCatalogReady() {
        if (!Config.enable_workload_group) {
            return;
        }
        LOG.info("begin publish topic info");
        // step 1: get all publish topic info
        TPublishTopicRequest request = new TPublishTopicRequest();
        for (TopicPublisher topicPublisher : topicPublisherList) {
            topicPublisher.getTopicInfo(request);
        }

        // step 2: publish topic info to all be
        Collection<Backend> nodesToPublish = clusterInfoService.getIdToBackend().values();
        for (Backend be : nodesToPublish) {
            try {
                TNetworkAddress addr = new TNetworkAddress(be.getHost(), be.getBePort());
                BackendService.Client client = null;
                try {
                    client = ClientPool.backendPool.borrowObject(addr);
                } catch (Exception e) {
                    LOG.warn("Fetch a agent client failed. backend=[{}] reason=[{}]", addr, e);
                    continue;
                }
                try {
                    client.publishTopicInfo(request);
                } catch (TException e) {
                    LOG.warn("rpc publish_topic_info failed ", e);
                    continue;
                }

                LOG.info("publish topic info to be {} success", be.getHost());
            } catch (Exception e) {
                LOG.warn("topic publish be {} error happens: ", be.getHost(), e);
            }
        }
    }

}
