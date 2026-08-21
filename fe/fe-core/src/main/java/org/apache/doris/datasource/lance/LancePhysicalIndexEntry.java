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

package org.apache.doris.datasource.lance;

import org.apache.commons.lang3.StringUtils;

/** Immutable physical index entry read from one Lance dataset snapshot. */
public final class LancePhysicalIndexEntry {
    private final String name;
    private final String uuid;
    private final long datasetVersion;

    public LancePhysicalIndexEntry(String name, String uuid, long datasetVersion) {
        if (StringUtils.isBlank(name)) {
            throw new IllegalArgumentException("name must not be null or blank");
        }
        if (StringUtils.isBlank(uuid)) {
            throw new IllegalArgumentException("uuid must not be null or blank");
        }
        if (datasetVersion < 0) {
            throw new IllegalArgumentException("dataset version must not be negative");
        }
        this.name = name;
        this.uuid = uuid;
        this.datasetVersion = datasetVersion;
    }

    public String getName() {
        return name;
    }

    public String getUuid() {
        return uuid;
    }

    public long getDatasetVersion() {
        return datasetVersion;
    }
}
