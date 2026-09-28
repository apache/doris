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

package org.apache.doris.datasource.lance.metadata;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** Access parameters resolved for one read; credentials are not versioned dataset metadata. */
public final class LanceTableAccess {
    private final String datasetUri;
    private final Map<String, String> storageOptions;

    public LanceTableAccess(String datasetUri, Map<String, String> storageOptions) {
        this.datasetUri = datasetUri;
        this.storageOptions = Collections.unmodifiableMap(new HashMap<>(storageOptions));
    }

    public String getDatasetUri() {
        return datasetUri;
    }

    public Map<String, String> getStorageOptions() {
        return storageOptions;
    }
}
