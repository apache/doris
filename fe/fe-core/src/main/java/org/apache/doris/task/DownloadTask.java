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
import org.apache.doris.thrift.TDownloadReq;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TRemoteTabletSnapshot;
import org.apache.doris.thrift.TResourceInfo;
import org.apache.doris.thrift.TTaskType;

import java.util.List;
import java.util.Map;

public class DownloadTask extends AgentTask {

    private long jobId;
    private Map<String, String> srcToDestPath;
    private FsBroker brokerAddr;
    private Map<String, String> brokerProperties;
    private StorageBackend.StorageType storageType;
    private String location;
    private List<TRemoteTabletSnapshot> remoteTabletSnapshots;
    private boolean isFromLocalSnapshot = false;


    public DownloadTask(TResourceInfo resourceInfo, long backendId, long signature, long jobId, long dbId,
            Map<String, String> srcToDestPath, FsBroker brokerAddr, Map<String, String> brokerProperties,
            StorageBackend.StorageType storageType, String location) {
        super(resourceInfo, backendId, TTaskType.DOWNLOAD, dbId, -1, -1, -1, -1, signature);
        this.jobId = jobId;
        this.srcToDestPath = srcToDestPath;
        this.brokerAddr = brokerAddr;
        this.brokerProperties = brokerProperties;
        this.storageType = storageType;
        this.location = location;
        this.isFromLocalSnapshot = false;
    }

    public DownloadTask(TResourceInfo resourceInfo, long backendId, long signature, long jobId, long dbId,
                        List<TRemoteTabletSnapshot> remoteTabletSnapshots) {
        super(resourceInfo, backendId, TTaskType.DOWNLOAD, dbId, -1, -1, -1, -1, signature);
        this.jobId = jobId;
        this.srcToDestPath = new java.util.HashMap<String, String>();
        this.remoteTabletSnapshots = remoteTabletSnapshots;
        this.isFromLocalSnapshot = true;
    }

    public long getJobId() {
        return jobId;
    }

    public Map<String, String> getSrcToDestPath() {
        return srcToDestPath;
    }

    public FsBroker getBrokerAddr() {
        return brokerAddr;
    }

    public Map<String, String> getBrokerProperties() {
        return brokerProperties;
    }

    public void updateBrokerProperties(Map<String, String> brokerProperties) {
        this.brokerProperties = new java.util.HashMap<>(brokerProperties);
    }

    public TDownloadReq toThrift() {
        // these fields are required
        // 1: required i64 job_id
        // 2: required map<string, string> src_dest_map
        // 3: required Types.TNetworkAddress broker_addr
        TDownloadReq req;
        if (isFromLocalSnapshot) {
            TNetworkAddress brokerAddr = new TNetworkAddress("", 0); // mock broker address
            req = new TDownloadReq(jobId, srcToDestPath, brokerAddr);
            req.setRemoteTabletSnapshots(remoteTabletSnapshots);
        } else {
            TNetworkAddress address = new TNetworkAddress(brokerAddr.host, brokerAddr.port);
            req = new TDownloadReq(jobId, srcToDestPath, address);
            req.setBrokerProp(brokerProperties);
            req.setStorageBackend(storageType.toThrift());
            req.setLocation(location);
        }
        return req;
    }
}
