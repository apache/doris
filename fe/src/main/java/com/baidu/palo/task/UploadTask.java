// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.baidu.palo.task;

import com.baidu.palo.catalog.BrokerMgr.BrokerAddress;
import com.baidu.palo.thrift.TNetworkAddress;
import com.baidu.palo.thrift.TResourceInfo;
import com.baidu.palo.thrift.TTaskType;
import com.baidu.palo.thrift.TUploadReq;

import java.util.Map;

public class UploadTask extends AgentTask {

    private long jobId;

    private Map<String, String> srcToDestPath;
    private BrokerAddress brokerAddress;
    private Map<String, String> brokerProperties;

    public UploadTask(TResourceInfo resourceInfo, long backendId, long signature, long jobId, Long dbId,
            Map<String, String> srcToDestPath, BrokerAddress brokerAddr, Map<String, String> brokerProperties) {
        super(resourceInfo, backendId, TTaskType.UPLOAD, dbId, -1, -1, -1, -1, signature);
        this.jobId = jobId;
        this.srcToDestPath = srcToDestPath;
        this.brokerAddress = brokerAddr;
        this.brokerProperties = brokerProperties;
    }

    public long getJobId() {
        return jobId;
    }

    public Map<String, String> getSrcToDestPath() {
        return srcToDestPath;
    }

    public BrokerAddress getBrokerAddress() {
        return brokerAddress;
    }

    public Map<String, String> getBrokerProperties() {
        return brokerProperties;
    }

    public TUploadReq toThrift() {
        TNetworkAddress address = new TNetworkAddress(brokerAddress.ip, brokerAddress.port);
        TUploadReq request = new TUploadReq(jobId, srcToDestPath, address);
        request.setBroker_prop(brokerProperties);
        return request;
    }
}
