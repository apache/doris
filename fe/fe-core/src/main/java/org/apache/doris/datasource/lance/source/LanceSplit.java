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

package org.apache.doris.datasource.lance.source;

import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.FileSplit;
import org.apache.doris.datasource.TableFormatType;

import java.util.Collections;

/**
 * A Lance scan split. Catalog and S3 scans use one fixed-version fragment per split, while a
 * backend-local TVF uses one whole-dataset split and resolves the latest version during execution.
 */
public class LanceSplit extends FileSplit {
    private final String datasetUri;
    private final long version;
    private final long fragmentId;
    private final boolean hasFragmentId;

    public LanceSplit(String datasetUri, long version, long fragmentId, long rowCount) {
        super(LocationPath.of(datasetUri), 0, 0, 0, 0, null, Collections.emptyList());
        this.datasetUri = datasetUri;
        this.version = version;
        this.fragmentId = fragmentId;
        this.hasFragmentId = true;
        this.tableFormatType = TableFormatType.LANCE;
        this.selfSplitWeight = Math.max(rowCount, 1);
    }

    private LanceSplit(String datasetUri) {
        super(LocationPath.of(datasetUri), 0, 0, 0, 0, null, Collections.emptyList());
        this.datasetUri = datasetUri;
        this.version = 0;
        this.fragmentId = -1;
        this.hasFragmentId = false;
        this.tableFormatType = TableFormatType.LANCE;
        this.selfSplitWeight = 1L;
    }

    public static LanceSplit wholeDatasetAtLatest(String datasetUri) {
        return new LanceSplit(datasetUri);
    }

    public String getDatasetUri() {
        return datasetUri;
    }

    public long getVersion() {
        return version;
    }

    public long getFragmentId() {
        return fragmentId;
    }

    public boolean hasFragmentId() {
        return hasFragmentId;
    }

    @Override
    public String getConsistentHashString() {
        return hasFragmentId
                ? datasetUri + "#" + version + "#" + fragmentId
                : datasetUri + "#latest#all";
    }
}
