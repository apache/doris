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

package org.apache.doris.task;

import org.apache.doris.analysis.StorageBackend;
import org.apache.doris.catalog.FsBroker;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TResourceInfo;
import org.apache.doris.thrift.TTaskType;
import org.apache.doris.thrift.TUploadReq;

import java.util.HashMap;
import java.util.Map;

public class UploadTask extends AgentTask {

    private long jobId;

    private Map<String, String> srcToDestPath;
    private FsBroker broker;
    private Map<String, String> brokerProperties;
    private StorageBackend.StorageType storageType;
    private String location;

    public UploadTask(TResourceInfo resourceInfo, long backendId, long signature, long jobId, Long dbId,
            Map<String, String> srcToDestPath, FsBroker broker, Map<String, String> brokerProperties,
            StorageBackend.StorageType storageType, String location) {
        super(resourceInfo, backendId, TTaskType.UPLOAD, dbId, -1, -1, -1, -1, signature);
        this.jobId = jobId;
        this.srcToDestPath = srcToDestPath;
        this.broker = broker;
        this.brokerProperties = brokerProperties;
        this.storageType = storageType;
        this.location = location;
    }

    public long getJobId() {
        return jobId;
    }

    public Map<String, String> getSrcToDestPath() {
        return srcToDestPath;
    }

    public FsBroker getBrokerAddress() {
        return broker;
    }

    public Map<String, String> getBrokerProperties() {
        return brokerProperties;
    }

    public void updateBrokerProperties(Map<String, String> brokerProperties) {
        this.brokerProperties = new HashMap<>(brokerProperties);
    }

    public TUploadReq toThrift() {
        TNetworkAddress address = new TNetworkAddress(broker.host, broker.port);
        TUploadReq request = new TUploadReq(jobId, srcToDestPath, address);
        request.setBrokerProp(brokerProperties);
        request.setStorageBackend(storageType.toThrift());
        request.setLocation(location);
        return request;
    }
}
