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

package org.apache.doris.datasource.property.storage;

import org.apache.doris.common.UserException;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.collections.MapUtils;
import org.apache.hadoop.conf.Configuration;

import java.util.Map;

public class LocalProperties extends StorageProperties {
    public static final String PROP_FILE_PATH = "file_path";

    // This backend is user specified backend for listing files, fetching file schema and executing query.
    private long backendId;
    // This backend if for listing files and fetching file schema.
    // If "backendId" is set, "backendIdForRequest" will be set to "backendId",
    // otherwise, "backendIdForRequest" will be set to one of the available backends.
    private long backendIdForRequest = -1;
    private boolean sharedStorage = false;

    private static final ImmutableSet<String> LOCATION_PROPERTIES = new ImmutableSet.Builder<String>()
            .add(PROP_FILE_PATH)
            .build();

    public LocalProperties(Map<String, String> origProps) {
        super(Type.LOCAL, origProps);
    }

    public static boolean guessIsMe(Map<String, String> props) {
        if (MapUtils.isEmpty(props)) {
            return false;
        }
        if (LOCATION_PROPERTIES.stream().anyMatch(props::containsKey)) {
            return true;
        }
        return false;
    }

    @Override
    public Map<String, String> getBackendConfigProperties() {
        return origProps;
    }

    @Override
    public String validateAndNormalizeUri(String url) throws UserException {
        return url;
    }

    @Override
    public String validateAndGetUri(Map<String, String> loadProps) throws UserException {
        return loadProps.get(PROP_FILE_PATH);
    }

    @Override
    public String getStorageName() {
        return "local";
    }

    @Override
    public void initializeHadoopStorageConfig() {
        hadoopStorageConfig = new Configuration();
        hadoopStorageConfig.set("fs.local.impl", "org.apache.hadoop.fs.LocalFileSystem");
        hadoopStorageConfig.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
    }
}
