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

import org.apache.doris.thrift.TLanceScanParams;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

/** Builds immutable FILE read sources in the same ID namespace on every query backend. */
public final class LanceBlobReadSourcePlanner {
    private LanceBlobReadSourcePlanner() {
    }

    public static boolean isRemoteDataset(String uri) {
        String scheme = URI.create(uri).getScheme();
        return scheme != null && uri.contains("://")
                && !scheme.equalsIgnoreCase("file") && !scheme.equalsIgnoreCase("memory");
    }

    public static int assign(TLanceScanParams params, List<String> columns, int nextSourceId) {
        params.unsetBlobReadSourceIds();
        if (columns.isEmpty()) {
            return nextSourceId;
        }
        if (!params.isSetScanNodeId() || params.getScanNodeId() < 0
                || !params.isSetDatasetUri() || !isRemoteDataset(params.getDatasetUri())
                || !params.isSetVersion() || params.getVersion() <= 0) {
            throw new IllegalStateException("Lance BLOB reads require a remote dataset and a fixed snapshot");
        }
        Map<String, Integer> sources = new TreeMap<>();
        for (String column : new TreeSet<>(columns)) {
            if (nextSourceId <= 0 || nextSourceId == Integer.MAX_VALUE) {
                throw new IllegalStateException("Too many FILE read sources in one query");
            }
            sources.put(column, nextSourceId++);
        }
        params.setBlobReadSourceIds(sources);
        return nextSourceId;
    }
}
