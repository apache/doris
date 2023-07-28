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
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TFileType;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.apache.commons.collections.map.CaseInsensitiveMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

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

    private String filePath;
    private long backendId;

    public LocalTableValuedFunction(Map<String, String> params) throws AnalysisException {
        Map<String, String> fileFormatParams = new CaseInsensitiveMap();
        locationProperties = Maps.newHashMap();
        for (String key : params.keySet()) {
            if (FILE_FORMAT_PROPERTIES.contains(key.toLowerCase())) {
                fileFormatParams.put(key, params.get(key));
            } else if (LOCATION_PROPERTIES.contains(key.toLowerCase())) {
                locationProperties.put(key.toLowerCase(), params.get(key));
            } else {
                throw new AnalysisException(key + " is invalid property");
            }
        }

        if (!locationProperties.containsKey(FILE_PATH)) {
            throw new AnalysisException(String.format("Configuration '%s' is required.", FILE_PATH));
        }
        if (!locationProperties.containsKey(BACKEND_ID)) {
            throw new AnalysisException(String.format("Configuration '%s' is required.", BACKEND_ID));
        }

        filePath = locationProperties.get(FILE_PATH);
        backendId = Long.parseLong(locationProperties.get(BACKEND_ID));

        if (Env.getCurrentSystemInfo().getBackend(backendId) == null) {
            throw new AnalysisException("backend not found with backend_id = " + backendId);
        }

        parseProperties(fileFormatParams);

        // create a fileStatus with infinite size for local file instead of call parseFile.
        fileStatuses.add(new TBrokerFileStatus(filePath, false, Long.MAX_VALUE, false));
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
