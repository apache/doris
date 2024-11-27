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

package org.apache.doris.tablefunction;

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.analysis.StorageBackend.StorageType;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.proto.InternalService;
import org.apache.doris.proto.InternalService.PGlobResponse;
import org.apache.doris.proto.InternalService.PGlobResponse.PFileInfo;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TNetworkAddress;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * The implement of table valued function
 * local("file_path" = "path/to/file.txt", "backend_id" = "be_id").
 */
public class LocalTableValuedFunction extends ExternalFileTableValuedFunction {
    private static final Logger LOG = LogManager.getLogger(LocalTableValuedFunction.class);
    public static final String NAME = "local";
    public static final String PROP_FILE_PATH = "file_path";
    public static final String PROP_BACKEND_ID = "backend_id";
    public static final String PROP_SHARED_STORAGE = "shared_storage";

    private static final ImmutableSet<String> LOCATION_PROPERTIES = new ImmutableSet.Builder<String>()
            .add(PROP_FILE_PATH)
            .build();

    // This backend is user specified backend for listing files, fetching file schema and executing query.
    private long backendId;
    // This backend if for listing files and fetching file schema.
    // If "backendId" is set, "backendIdForRequest" will be set to "backendId",
    // otherwise, "backendIdForRequest" will be set to one of the available backends.
    private long backendIdForRequest = -1;
    private boolean sharedStorage = false;

    public LocalTableValuedFunction(Map<String, String> properties) throws AnalysisException {
        // 1. analyze common properties
        Map<String, String> otherProps = super.parseCommonProperties(properties);

        // 2. analyze location properties
        for (String key : LOCATION_PROPERTIES) {
            if (!otherProps.containsKey(key)) {
                throw new AnalysisException(String.format("Property '%s' is required.", key));
            }
        }
        filePath = otherProps.get(PROP_FILE_PATH);
        backendId = Long.parseLong(otherProps.getOrDefault(PROP_BACKEND_ID, "-1"));
        sharedStorage = Boolean.parseBoolean(otherProps.getOrDefault(PROP_SHARED_STORAGE, "false"));

        // If not shared storage, backend_id is required
        // If is shared storage, backend_id can still be set, so that we can query data on single BE node.
        // Otherwise, if shared storage is true, we may use multi BE nodes.
        if (backendId == -1 && !sharedStorage) {
            throw new AnalysisException("'backend_id' is required when 'shared_storage' is false.");
        }

        // 3. parse file
        getFileListFromBackend();
    }

    private void getFileListFromBackend() throws AnalysisException {
        Backend be = null;
        if (backendId != -1) {
            be = Env.getCurrentSystemInfo().getBackend(backendId);
            backendIdForRequest = backendId;
        } else {
            Preconditions.checkState(sharedStorage);
            List<Long> beIds = Env.getCurrentSystemInfo().getAllBackendIds(true);
            if (beIds.isEmpty()) {
                throw new AnalysisException("No available backend");
            }
            Collections.shuffle(beIds);
            be = Env.getCurrentSystemInfo().getBackend(beIds.get(0));
            backendIdForRequest = be.getId();
        }
        if (be == null) {
            throw new AnalysisException("backend not found with backend_id = " + backendId);
        }

        BackendServiceProxy proxy = BackendServiceProxy.getInstance();
        TNetworkAddress address = be.getBrpcAddress();
        InternalService.PGlobRequest.Builder requestBuilder = InternalService.PGlobRequest.newBuilder();
        requestBuilder.setPattern(filePath);
        try {
            Future<PGlobResponse> response = proxy.glob(address, requestBuilder.build());
            PGlobResponse globResponse = response.get(5, TimeUnit.SECONDS);
            if (globResponse.getStatus().getStatusCode() != 0) {
                throw new AnalysisException(
                        "error code: " + globResponse.getStatus().getStatusCode()
                                + ", error msg: " + globResponse.getStatus().getErrorMsgsList());
            }
            for (PFileInfo file : globResponse.getFilesList()) {
                fileStatuses.add(new TBrokerFileStatus(file.getFile().trim(), false, file.getSize(), true));
                LOG.info("get file from backend success. file: {}, size: {}", file.getFile(), file.getSize());
            }
        } catch (Exception e) {
            throw new AnalysisException("get file list from backend failed. " + e.getMessage());
        }
    }

    @Override
    public TFileType getTFileType() {
        return TFileType.FILE_LOCAL;
    }

    @Override
    public String getFilePath() {
        return filePath;
    }

    @Override
    public BrokerDesc getBrokerDesc() {
        return new BrokerDesc("LocalTvfBroker", StorageType.LOCAL, locationProperties);
    }

    @Override
    public String getTableName() {
        return "LocalTableValuedFunction";
    }

    public Long getBackendId() {
        return backendId;
    }

    @Override
    protected Backend getBackend() {
        return Env.getCurrentSystemInfo().getBackend(backendIdForRequest);
    }
}
