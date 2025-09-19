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

package org.apache.doris.cloud.snapshot;

import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.NotImplementedException;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.rpc.RpcException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.Constructor;

public class CloudSnapshotHandler extends MasterDaemon {

    private static final Logger LOG = LogManager.getLogger(CloudSnapshotHandler.class);

    public CloudSnapshotHandler() {
        super("cloud snapshot handler", Config.cloud_snapshot_handler_interval_second * 1000);
    }

    public static CloudSnapshotHandler getInstance() {
        try {
            Class<CloudSnapshotHandler> theClass = (Class<CloudSnapshotHandler>) Class.forName(
                    Config.cloud_snapshot_handler_class);
            Constructor<CloudSnapshotHandler> constructor = theClass.getDeclaredConstructor();
            return constructor.newInstance();
        } catch (Exception e) {
            LOG.error("failed to create cloud snapshot handler, class name: {}", Config.cloud_snapshot_handler_class,
                    e);
            System.exit(-1);
            return null;
        }
    }

    public void initialize() {
        // do nothing
    }

    @Override
    protected void runAfterCatalogReady() {
        // do nothing
    }

    public void submitJob(long ttl, String label) throws Exception {
        throw new NotImplementedException("submitJob is not implemented");
    }

    public synchronized void refreshAutoSnapshotJob() throws Exception {
        throw new NotImplementedException("refreshAutoSnapshotJob is not implemented");
    }

    public void cloneSnapshot(String clusterSnapshotFile) throws Exception {
        throw new NotImplementedException("cloneSnapshot is not implemented");
    }

    public Cloud.ListSnapshotResponse listSnapshot(boolean includeAborted) throws DdlException {
        try {
            Cloud.ListSnapshotRequest request = Cloud.ListSnapshotRequest.newBuilder()
                    .setCloudUniqueId(Config.cloud_unique_id).setIncludeAborted(includeAborted).build();
            Cloud.ListSnapshotResponse response = MetaServiceProxy.getInstance().listSnapshot(request);
            if (response.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
                LOG.warn("listSnapshot response: {} ", response);
                throw new DdlException(response.getStatus().getMsg());
            }
            return response;
        } catch (RpcException e) {
            throw new DdlException(e.getMessage());
        }
    }
}
