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

import org.apache.doris.catalog.Env;
import org.apache.doris.resource.workloadgroup.WorkloadGroupMgr;
import org.apache.doris.thrift.TPublishTopicRequest;
import org.apache.doris.thrift.TTopicInfoType;
import org.apache.doris.thrift.TopicInfo;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class WorkloadGroupPublisher implements TopicPublisher {

    private static final Logger LOG = LogManager.getLogger(WorkloadGroupPublisher.class);

    private Env env;

    public WorkloadGroupPublisher(Env env) {
        this.env = env;
    }

    @Override
    public void getTopicInfo(TPublishTopicRequest req) {
        List<TopicInfo> list = env.getWorkloadGroupMgr().getPublishTopicInfo();
        if (list.size() == 0) {
            LOG.warn("[topic_publish]currently, doris at least has one workload group named "
                    + WorkloadGroupMgr.DEFAULT_GROUP_NAME
                    + ", so get a size 0 here is an error, should check it.");
        } else {
            req.putToTopicMap(TTopicInfoType.WORKLOAD_GROUP, list);
        }
    }
}
