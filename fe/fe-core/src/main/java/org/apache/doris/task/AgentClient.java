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

import org.apache.doris.common.ClientPool;
import org.apache.doris.common.Status;
import org.apache.doris.thrift.BackendService;
import org.apache.doris.thrift.TAgentResult;
import org.apache.doris.thrift.TAgentServiceVersion;
import org.apache.doris.thrift.TCheckStorageFormatResult;
import org.apache.doris.thrift.TMiniLoadEtlStatusRequest;
import org.apache.doris.thrift.TMiniLoadEtlStatusResult;
import org.apache.doris.thrift.TMiniLoadEtlTaskRequest;
import org.apache.doris.thrift.TDeleteEtlFilesRequest;
import org.apache.doris.thrift.TExportStatusResult;
import org.apache.doris.thrift.TExportTaskRequest;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TSnapshotRequest;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TUniqueId;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class AgentClient {
    private static final Logger LOG = LogManager.getLogger(AgentClient.class);

    private String host;
    private int port;
    
    private BackendService.Client client;
    private TNetworkAddress address;
    private boolean ok;

    public AgentClient(String host, int port) {
        this.host = host;
        this.port = port;
    }
    
    public TAgentResult submitEtlTask(TMiniLoadEtlTaskRequest request) {
        TAgentResult result = null;
        LOG.debug("submit etl task. request: {}", request);
        try {
            borrowClient();
            // submit etl task
            result = client.submitEtlTask(request);
            ok = true;
        } catch (Exception e) {
            LOG.warn("submit etl task error", e);
        } finally {
            returnClient();
        }
        return result;
    }
    
    public TAgentResult makeSnapshot(TSnapshotRequest request) {
        TAgentResult result = null;
        LOG.debug("submit make snapshot task. request: {}", request);
        try {
            borrowClient();
            // submit make snapshot task
            result = client.makeSnapshot(request);
            ok = true;
        } catch (Exception e) {
            LOG.warn("submit make snapshot error", e);
        } finally {
            returnClient();
        }
        return result;
    }
    
    public TAgentResult releaseSnapshot(String snapshotPath) {
        TAgentResult result = null;
        LOG.debug("submit release snapshot task. snapshotPath: {}", snapshotPath);
        try {
            borrowClient();
            // submit release snapshot task
            result = client.releaseSnapshot(snapshotPath);
            ok = true;
        } catch (Exception e) {
            LOG.warn("submit release snapshot error", e);
        } finally {
            returnClient();
        }
        return result;
    }
    
    public Status submitExportTask(TExportTaskRequest request) {
        Status result = Status.CANCELLED;
        LOG.debug("submit export task. request: {}", request);
        try {
            borrowClient();
            // submit export task
            TStatus status = client.submitExportTask(request);
            result = new Status(status);
        } catch (Exception e) {
            LOG.warn("submit export task error", e);
        } finally {
            returnClient();
        }
        return result;
    }
    
    public TMiniLoadEtlStatusResult getEtlStatus(long jobId, long taskId) {
        TMiniLoadEtlStatusResult result = null;
        TMiniLoadEtlStatusRequest request = new TMiniLoadEtlStatusRequest(TAgentServiceVersion.V1, 
                new TUniqueId(jobId, taskId));
        LOG.debug("get mini load etl task status. request: {}", request);
        try {
            borrowClient();
            // get etl status
            result = client.getEtlStatus(request);
            ok = true;
        } catch (Exception e) {
            LOG.warn("get etl status error", e);
        } finally {
            returnClient();
        }
        return result;
    }
    
    public TExportStatusResult getExportStatus(long jobId, long taskId) {
        TExportStatusResult result = null;
        TUniqueId request = new TUniqueId(jobId, taskId);
        LOG.debug("get export task status. request: {}", request);
        try {
            borrowClient();
            // get export status
            result = client.getExportStatus(request);
            ok = true;
        } catch (Exception e) {
            LOG.warn("get export status error", e);
        } finally {
            returnClient();
        }
        return result;
    }
    
    public Status eraseExportTask(long jobId, long taskId) {
        Status result = Status.CANCELLED;
        TUniqueId request = new TUniqueId(jobId, taskId);
        LOG.debug("erase export task. request: {}", request);
        try {
            borrowClient();
            // erase export task
            TStatus status = client.eraseExportTask(request);
            result = new Status(status);
        } catch (Exception e) {
            LOG.warn("submit export task error", e);
        } finally {
            returnClient();
        }
        return result;
    }

    public TCheckStorageFormatResult checkStorageFormat() {
        TCheckStorageFormatResult result = null;
        LOG.debug("submit make snapshot task.");
        try {
            borrowClient();
            result = client.checkStorageFormat();
            ok = true;
        } catch (Exception e) {
            LOG.warn("checkStorageFormat error", e);
        } finally {
            returnClient();
        }
        return result;
    }

    public void deleteEtlFiles(long dbId, long jobId, String dbName, String label) {
        TDeleteEtlFilesRequest request = new TDeleteEtlFilesRequest(TAgentServiceVersion.V1, 
                new TUniqueId(dbId, jobId), dbName, label);
        LOG.debug("delete etl files. request: {}", request);
        try {
            borrowClient();
            // delete etl files
            client.deleteEtlFiles(request);
            ok = true;
        } catch (Exception e) {
            LOG.warn("delete etl files error", e);
        } finally {
            returnClient();
        }
    }
    
    private void borrowClient() throws Exception {
        // create agent client
        ok = false;
        address = new TNetworkAddress(host, port);
        client = ClientPool.backendPool.borrowObject(address);
    }
    
    private void returnClient() {
        if (ok) {
            ClientPool.backendPool.returnObject(address, client);
        } else {
            ClientPool.backendPool.invalidateObject(address, client);
        }
    }

}
