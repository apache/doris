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
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.BackendService;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPublishTopicRequest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;

public class TopicPublisherThread extends MasterDaemon {

    private static final Logger LOG = LogManager.getLogger(TopicPublisherThread.class);

    private SystemInfoService clusterInfoService;

    private ExecutorService executor = ThreadPoolManager
            .newDaemonFixedThreadPool(6, 256, "topic-publish-thread", true);

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
        AckResponseHandler handler = new AckResponseHandler(nodesToPublish);
        for (Backend be : nodesToPublish) {
            executor.submit(new TopicPublishWorker(request, be, handler));
        }
        try {
            int timeoutMs = Config.publish_topic_info_interval_ms / 3 * 2;
            if (!handler.awaitAllInMs(timeoutMs)) {
                Backend[] backends = handler.pendingNodes();
                if (backends.length > 0) {
                    LOG.warn("timed out waiting for all nodes to publish. (pending nodes: {})",
                            Arrays.toString(backends));
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public class TopicPublishWorker implements Runnable {
        private TPublishTopicRequest request;
        private Backend be;
        private ResponseHandler handler;

        public TopicPublishWorker(TPublishTopicRequest request, Backend node, ResponseHandler handler) {
            this.request = request;
            this.be = node;
            this.handler = handler;
        }

        @Override
        public void run() {
            long beginTime = System.currentTimeMillis();
            try {
                TNetworkAddress addr = new TNetworkAddress(be.getHost(), be.getBePort());
                BackendService.Client client = ClientPool.backendPool.borrowObject(addr);
                client.publishTopicInfo(request);
                LOG.info("publish topic info to be {} success, time cost={} ms",
                        be.getHost(), (System.currentTimeMillis() - beginTime));
            } catch (Exception e) {
                LOG.warn("publish topic info to be {} error happens: , time cost={} ms",
                        be.getHost(), (System.currentTimeMillis() - beginTime), e);
            } finally {
                handler.onResponse(be);
            }
        }
    }

}
