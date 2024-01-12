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

package org.apache.doris.resource.workloadschedpolicy;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.publish.TopicPublisher;
import org.apache.doris.thrift.TPublishTopicRequest;
import org.apache.doris.thrift.TTopicInfoType;
import org.apache.doris.thrift.TopicInfo;

import java.util.List;

public class WorkloadSchedPolicyPublisher implements TopicPublisher {

    private Env env;

    public WorkloadSchedPolicyPublisher(Env env) {
        this.env = env;
    }

    @Override
    public void getTopicInfo(TPublishTopicRequest req) {
        List<TopicInfo> list = env.getWorkloadSchedPolicyMgr().getPublishTopicInfoList();
        req.putToTopicMap(TTopicInfoType.WORKLOAD_SCHED_POLICY, list);
    }

}
