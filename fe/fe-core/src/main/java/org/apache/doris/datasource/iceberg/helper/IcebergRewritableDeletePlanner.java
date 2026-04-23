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

package org.apache.doris.datasource.iceberg.helper;

import org.apache.doris.common.UserException;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.iceberg.IcebergUtils;
import org.apache.doris.datasource.iceberg.source.IcebergScanNode;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.thrift.TIcebergRewritableDeleteFileSet;

import org.apache.iceberg.DeleteFile;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class IcebergRewritableDeletePlanner {
    private static final int ICEBERG_DELETION_VECTOR_MIN_VERSION = 3;

    private IcebergRewritableDeletePlanner() {
    }

    public static IcebergRewritableDeletePlan collectForDelete(
            IcebergExternalTable table, NereidsPlanner planner) throws UserException {
        return collect(table, planner);
    }

    public static IcebergRewritableDeletePlan collectForMerge(
            IcebergExternalTable table, NereidsPlanner planner) throws UserException {
        return collect(table, planner);
    }

    private static IcebergRewritableDeletePlan collect(
            IcebergExternalTable table, NereidsPlanner planner) {
        if (table == null
                || planner == null
                || IcebergUtils.getFormatVersion(table.getIcebergTable()) < ICEBERG_DELETION_VECTOR_MIN_VERSION) {
            return IcebergRewritableDeletePlan.empty();
        }

        List<TIcebergRewritableDeleteFileSet> thriftDeleteFileSets = new ArrayList<>();
        Map<String, List<DeleteFile>> deleteFilesByReferencedDataFile = new LinkedHashMap<>();

        for (ScanNode scanNode : planner.getScanNodes()) {
            if (!(scanNode instanceof IcebergScanNode)) {
                continue;
            }
            IcebergScanNode icebergScanNode = (IcebergScanNode) scanNode;

            deleteFilesByReferencedDataFile.putAll(icebergScanNode.deleteFilesByReferencedDataFile);
            icebergScanNode.deleteFilesDescByReferencedDataFile.forEach(
                    (key, value) -> {
                        TIcebergRewritableDeleteFileSet deleteFileSet = new TIcebergRewritableDeleteFileSet();
                        deleteFileSet.setReferencedDataFilePath(key);
                        deleteFileSet.setDeleteFiles(value);
                        thriftDeleteFileSets.add(deleteFileSet);
                    }
            );
        }

        if (thriftDeleteFileSets.isEmpty()) {
            return IcebergRewritableDeletePlan.empty();
        }
        return new IcebergRewritableDeletePlan(
                Collections.unmodifiableList(thriftDeleteFileSets),
                Collections.unmodifiableMap(deleteFilesByReferencedDataFile));
    }
}
