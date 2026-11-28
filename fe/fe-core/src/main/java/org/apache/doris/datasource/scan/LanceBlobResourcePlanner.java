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

package org.apache.doris.datasource.scan;

import org.apache.doris.common.Config;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TLanceBlobResource;
import org.apache.doris.thrift.TLanceScanParams;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

/** Builds query-local BLOB resources and assigns their IDs before fragment serialization. */
public final class LanceBlobResourcePlanner {
    private LanceBlobResourcePlanner() {
    }

    public static Map<Integer, TLanceBlobResource> plan(
            Collection<? extends ScanNode> scanNodes, int beExecVersion) {
        Map<Integer, FileQueryScanNode> fileScans = new TreeMap<>();
        for (ScanNode scan : scanNodes) {
            if (scan instanceof FileQueryScanNode) {
                fileScans.put(scan.getId().asInt(), (FileQueryScanNode) scan);
            }
        }
        List<TFileScanRangeParams> params = new ArrayList<>();
        for (FileQueryScanNode scan : fileScans.values()) {
            params.add(scan.getFileScanRangeParams());
        }
        return planParams(params, beExecVersion);
    }

    static Map<Integer, TLanceBlobResource> planParams(
            Collection<TFileScanRangeParams> scanParamsList, int beExecVersion) {
        Map<Integer, TLanceBlobResource> resources = new TreeMap<>();
        int nextSourceId = 1;
        for (TFileScanRangeParams scanParams : scanParamsList) {
            if (!scanParams.isSetLanceScanParams()) {
                continue;
            }
            TLanceScanParams lance = scanParams.getLanceScanParams();
            if (!lance.isSetBlobReadSourceIds() || lance.getBlobReadSourceIds().isEmpty()) {
                continue;
            }
            nextSourceId = assign(lance, new ArrayList<>(lance.getBlobReadSourceIds().keySet()),
                    nextSourceId);
            if (beExecVersion < Config.LANCE_BLOB_MIN_BE_EXEC_VERSION) {
                throw new IllegalStateException("Lance BLOB reads require be_exec_version >= "
                        + Config.LANCE_BLOB_MIN_BE_EXEC_VERSION + ", got " + beExecVersion);
            }
            for (Map.Entry<String, Integer> source : lance.getBlobReadSourceIds().entrySet()) {
                TLanceBlobResource resource = new TLanceBlobResource()
                        .setDatasetUri(lance.getDatasetUri()).setVersion(lance.getVersion())
                        .setColumnName(source.getKey())
                        .setStorageOptions(lance.isSetLanceStorageOptions()
                                ? new TreeMap<>(lance.getLanceStorageOptions()) : new TreeMap<>());
                resources.put(source.getValue(), resource);
            }
        }
        return resources;
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
        if (!params.isSetDatasetUri() || !isRemoteDataset(params.getDatasetUri())
                || !params.isSetVersion() || params.getVersion() <= 0) {
            throw new IllegalStateException("Lance BLOB reads require a remote dataset and a fixed snapshot");
        }
        Map<String, Integer> sources = new TreeMap<>();
        for (String column : new TreeSet<>(columns)) {
            if (nextSourceId <= 0 || nextSourceId == Integer.MAX_VALUE) {
                throw new IllegalStateException("Too many Lance BLOB resources in one query");
            }
            sources.put(column, nextSourceId++);
        }
        params.setBlobReadSourceIds(sources);
        return nextSourceId;
    }
}
