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
import org.apache.doris.thrift.TopicInfo;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

public class WorkloadActionPublishThread extends Daemon {

    private ExecutorService executor = ThreadPoolManager
            .newDaemonFixedThreadPool(4, 256, "workload-action-publish-thread", true);

    private static final Logger LOG = LogManager.getLogger(WorkloadActionPublishThread.class);

    public static Map<TTopicInfoType, List<TopicInfo>> workloadActionToplicInfoMap
            = new HashMap<TTopicInfoType, List<TopicInfo>>();

    public static synchronized void putWorkloadAction(TTopicInfoType type, TopicInfo topicInfo) {
        List<TopicInfo> list = workloadActionToplicInfoMap.get(type);
        if (list == null) {
            list = new ArrayList<TopicInfo>();
            workloadActionToplicInfoMap.put(type, list);
        }
        list.add(topicInfo);
    }

    public static synchronized Map<TTopicInfoType, List<TopicInfo>> getCurrentWorkloadActionMap() {
        Map<TTopicInfoType, List<TopicInfo>> retMap = workloadActionToplicInfoMap;
        workloadActionToplicInfoMap = new HashMap<TTopicInfoType, List<TopicInfo>>();
        return retMap;
    }

    private SystemInfoService clusterInfoService;

    public WorkloadActionPublishThread(String name, long intervalMs,
            SystemInfoService clusterInfoService) {
        super(name, intervalMs);
        this.clusterInfoService = clusterInfoService;
    }

    @Override
    protected final void runOneCycle() {
        Map<TTopicInfoType, List<TopicInfo>> actionMap
                = WorkloadActionPublishThread.getCurrentWorkloadActionMap();
        if (actionMap.size() == 0) {
            LOG.info("no workload action found, skip publish");
            return;
        }
        Collection<Backend> currentBeToPublish = clusterInfoService.getIdToBackend().values();
        AckResponseHandler handler = new AckResponseHandler(currentBeToPublish);
        TPublishTopicRequest request = new TPublishTopicRequest();
        request.setTopicMap(actionMap);
        for (Backend be : currentBeToPublish) {
            executor.submit(new WorkloadMoveActionTask(request, be, handler));
        }
    }

    public class WorkloadMoveActionTask implements Runnable {

        private TPublishTopicRequest request;

        private Backend be;

        private ResponseHandler handler;

        public WorkloadMoveActionTask(TPublishTopicRequest request, Backend be,
                ResponseHandler handler) {
            this.request = request;
            this.be = be;
            this.handler = handler;
        }

        @Override
        public void run() {
            long beginTime = System.currentTimeMillis();
            try {
                TNetworkAddress addr = new TNetworkAddress(be.getHost(), be.getBePort());
                BackendService.Client client = ClientPool.backendPool.borrowObject(addr);
                client.publishTopicInfo(request);
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
