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

package org.apache.doris.datasource;

import org.apache.doris.common.util.MasterDaemon;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * `SplitSource` is obtained by RPC call of `FrontendServiceImpl#fetchSplitBatch`.
 * Each `SplitSource` is reference by its unique ID. `SplitSourceManager` provides the register, get, and remove
 * function to manage the split sources. Each source remains strongly reachable until the query explicitly
 * releases it through {@link FileQueryScanNode#stop()}.
 */
public class SplitSourceManager extends MasterDaemon {
    private final Map<Long, SplitSource> splits = new ConcurrentHashMap<>();

    public void registerSplitSource(SplitSource splitSource) {
        splits.put(splitSource.getUniqueId(), splitSource);
    }

    public void removeSplitSource(long uniqueId) {
        splits.remove(uniqueId);
    }

    public SplitSource getSplitSource(long uniqueId) {
        return splits.get(uniqueId);
    }

    @Override
    protected void runAfterCatalogReady() {
    }
}
