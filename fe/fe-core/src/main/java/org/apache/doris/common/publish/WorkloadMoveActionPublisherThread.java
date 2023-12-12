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
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.common.util.Daemon;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.BackendService;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPublishTopicRequest;
import org.apache.doris.thrift.TTopicInfoType;
import org.apache.doris.thrift.TWorkloadMoveQueryToGroupAction;
import org.apache.doris.thrift.TopicInfo;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;

public class WorkloadMoveActionPublisherThread extends Daemon {

    private ExecutorService executor = ThreadPoolManager
            .newDaemonFixedThreadPool(4, 256, "move-action-publish-thread", true);

    private static final Logger LOG = LogManager.getLogger(WorkloadMoveActionPublisherThread.class);

    public static List<TWorkloadMoveQueryToGroupAction> moveActionTaskList = new ArrayList();

    public static synchronized void putMoveAction(TWorkloadMoveQueryToGroupAction moveAction) {
        moveActionTaskList.add(moveAction);
    }

    public static synchronized List<TWorkloadMoveQueryToGroupAction> getCurrentMoveActionList() {
        List<TWorkloadMoveQueryToGroupAction> retList = moveActionTaskList;
        moveActionTaskList = new ArrayList<TWorkloadMoveQueryToGroupAction>();
        return retList;
    }

    private SystemInfoService clusterInfoService;

    public WorkloadMoveActionPublisherThread(String name, long intervalMs,
            SystemInfoService clusterInfoService) {
        super(name, intervalMs);
        this.clusterInfoService = clusterInfoService;
    }

    @Override
    protected final void runOneCycle() {
        List<TWorkloadMoveQueryToGroupAction> currentList
                = WorkloadMoveActionPublisherThread.getCurrentMoveActionList();
        if (currentList.size() == 0) {
            LOG.info("no move action, skip publish");
            return;
        }
        Collection<Backend> currentBeToPublish = clusterInfoService.getIdToBackend().values();
        AckResponseHandler handler = new AckResponseHandler(currentBeToPublish);
        for (Backend be : currentBeToPublish) {
            executor.submit(new WorkloadMoveActionTask(currentList, be, handler));
        }
    }

    public class WorkloadMoveActionTask implements Runnable {

        private List<TWorkloadMoveQueryToGroupAction> moveActionList;

        private Backend be;

        private ResponseHandler handler;

        public WorkloadMoveActionTask(List<TWorkloadMoveQueryToGroupAction> moveActionList, Backend be,
                ResponseHandler handler) {
            this.moveActionList = moveActionList;
            this.be = be;
            this.handler = handler;
        }

        @Override
        public void run() {
            long beginTime = System.currentTimeMillis();
            try {
                List<TopicInfo> topicInfoList = new ArrayList();
                TopicInfo topicInfo = new TopicInfo();
                topicInfo.setMoveActionList(moveActionList);
                topicInfoList.add(topicInfo);

                TPublishTopicRequest req = new TPublishTopicRequest();
                req.putToTopicMap(TTopicInfoType.MOVE_QUERY_TO_GROUP, topicInfoList);

                TNetworkAddress addr = new TNetworkAddress(be.getHost(), be.getBePort());
                BackendService.Client client = ClientPool.backendPool.borrowObject(addr);
                client.publishTopicInfo(req);
                LOG.info("publish move action topic to be {} success, time cost={} ms",
                        be.getHost(), (System.currentTimeMillis() - beginTime));
            } catch (Exception e) {
                LOG.warn("publish move action topic to be {} error happens: , time cost={} ms",
                        be.getHost(), (System.currentTimeMillis() - beginTime), e);
            } finally {
                handler.onResponse(be);
            }
        }
    }
}
