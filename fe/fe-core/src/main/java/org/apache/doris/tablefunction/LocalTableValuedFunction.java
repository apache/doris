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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.apache.commons.collections.map.CaseInsensitiveMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
    public static final String FILE_PATH = "file_path";
    public static final String BACKEND_ID = "backend_id";

    private static final ImmutableSet<String> LOCATION_PROPERTIES = new ImmutableSet.Builder<String>()
            .add(FILE_PATH)
            .add(BACKEND_ID)
            .build();

    private long backendId;

    public LocalTableValuedFunction(Map<String, String> params) throws AnalysisException {
        Map<String, String> fileParams = new CaseInsensitiveMap();
        locationProperties = Maps.newHashMap();
        for (String key : params.keySet()) {
            String lowerKey = key.toLowerCase();
            if (FILE_FORMAT_PROPERTIES.contains(lowerKey)) {
                fileParams.put(lowerKey, params.get(key));
            } else if (LOCATION_PROPERTIES.contains(lowerKey)) {
                locationProperties.put(lowerKey, params.get(key));
            } else {
                throw new AnalysisException(key + " is invalid property");
            }
        }

        for (String key : LOCATION_PROPERTIES) {
            if (!locationProperties.containsKey(key)) {
                throw new AnalysisException(String.format("Configuration '%s' is required.", key));
            }
        }

        filePath = locationProperties.get(FILE_PATH);
        backendId = Long.parseLong(locationProperties.get(BACKEND_ID));
        super.parseProperties(fileParams);

        getFileListFromBackend();
    }

    private void getFileListFromBackend() throws AnalysisException {
        Backend be = Env.getCurrentSystemInfo().getBackend(backendId);
        if (be == null) {
            throw new AnalysisException("backend not found with backend_id = " + backendId);
        }

        BackendServiceProxy proxy = BackendServiceProxy.getInstance();
        TNetworkAddress address = be.getBrpcAdress();
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
        return Env.getCurrentSystemInfo().getBackend(backendId);
    }
}

