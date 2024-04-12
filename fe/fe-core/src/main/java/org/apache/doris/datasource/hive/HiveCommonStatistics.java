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

package org.apache.doris.datasource.hive;

public class HiveCommonStatistics {
    public static final HiveCommonStatistics EMPTY = new HiveCommonStatistics(0L, 0L, 0L);

    private final long rowCount;
    private final long fileCount;
    private final long totalFileBytes;

    public HiveCommonStatistics(long rowCount, long fileCount, long totalFileBytes) {
        this.fileCount = fileCount;
        this.rowCount = rowCount;
        this.totalFileBytes = totalFileBytes;
    }

    public long getRowCount() {
        return rowCount;
    }

    public long getFileCount() {
        return fileCount;
    }

    public long getTotalFileBytes() {
        return totalFileBytes;
    }
}
