// Modifications copyright (C) 2018, Baidu.com, Inc.
// Copyright 2018 The Apache Software Foundation

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

package com.baidu.palo.task;

import com.baidu.palo.catalog.BrokerMgr.BrokerAddress;
import com.baidu.palo.thrift.TDownloadReq;
import com.baidu.palo.thrift.TNetworkAddress;
import com.baidu.palo.thrift.TResourceInfo;
import com.baidu.palo.thrift.TTaskType;

import java.util.Map;

public class DownloadTask extends AgentTask {

    private long jobId;
    private Map<String, String> srcToDestPath;
    private BrokerAddress brokerAddr;
    private Map<String, String> brokerProperties;

    public DownloadTask(TResourceInfo resourceInfo, long backendId, long signature, long jobId, long dbId,
            Map<String, String> srcToDestPath, BrokerAddress brokerAddr, Map<String, String> brokerProperties) {
        super(resourceInfo, backendId, signature, TTaskType.DOWNLOAD, dbId, -1, -1, -1, -1);
        this.jobId = jobId;
        this.srcToDestPath = srcToDestPath;
        this.brokerAddr = brokerAddr;
        this.brokerProperties = brokerProperties;
    }

    public long getJobId() {
        return jobId;
    }

    public Map<String, String> getSrcToDestPath() {
        return srcToDestPath;
    }

    public BrokerAddress getBrokerAddr() {
        return brokerAddr;
    }

    public Map<String, String> getBrokerProperties() {
        return brokerProperties;
    }

    public TDownloadReq toThrift() {
        TNetworkAddress address = new TNetworkAddress(brokerAddr.ip, brokerAddr.port);
        TDownloadReq req = new TDownloadReq(jobId, srcToDestPath, address);
        req.setBroker_prop(brokerProperties);
        return req;
    }
}
