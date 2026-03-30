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

import org.apache.doris.thrift.TIcebergRewritableDeleteFileSet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.DeleteFile;

import java.util.List;
import java.util.Map;

public final class IcebergRewritableDeletePlan {
    private static final IcebergRewritableDeletePlan EMPTY =
            new IcebergRewritableDeletePlan(ImmutableList.of(), ImmutableMap.of());

    private final List<TIcebergRewritableDeleteFileSet> thriftDeleteFileSets;
    private final Map<String, List<DeleteFile>> deleteFilesByReferencedDataFile;

    public IcebergRewritableDeletePlan(
            List<TIcebergRewritableDeleteFileSet> thriftDeleteFileSets,
            Map<String, List<DeleteFile>> deleteFilesByReferencedDataFile) {
        this.thriftDeleteFileSets = thriftDeleteFileSets;
        this.deleteFilesByReferencedDataFile = deleteFilesByReferencedDataFile;
    }

    public static IcebergRewritableDeletePlan empty() {
        return EMPTY;
    }

    public List<TIcebergRewritableDeleteFileSet> getThriftDeleteFileSets() {
        return thriftDeleteFileSets;
    }

    public Map<String, List<DeleteFile>> getDeleteFilesByReferencedDataFile() {
        return deleteFilesByReferencedDataFile;
    }
}
