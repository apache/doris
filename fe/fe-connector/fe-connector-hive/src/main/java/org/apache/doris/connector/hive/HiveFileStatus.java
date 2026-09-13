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

package org.apache.doris.connector.hive;

/**
 * An immutable, slim view of one data file in a partition directory: exactly the three fields the scan planner
 * ({@link HiveScanPlanProvider#splitFile}) and the row-count estimate
 * ({@link HiveConnectorMetadata#estimateDataSizeByListingFiles}) read from a Hadoop {@code FileStatus}. It is the
 * value element cached by {@link HiveFileListingCache}, deliberately decoupled from the (heavier, Hadoop-internal)
 * {@code FileStatus} so the cache holds only immutable, small values (the codebase-wide metadata-cache convention).
 */
public final class HiveFileStatus {

    private final String path;
    private final long length;
    private final long modificationTime;

    public HiveFileStatus(String path, long length, long modificationTime) {
        this.path = path;
        this.length = length;
        this.modificationTime = modificationTime;
    }

    /** The file's full path (e.g. {@code s3://wh/db/t/dt=1/000000_0}). */
    public String getPath() {
        return path;
    }

    /** The file length in bytes. */
    public long getLength() {
        return length;
    }

    /** The file's last-modification time in millis (BE uses it to detect a changed split). */
    public long getModificationTime() {
        return modificationTime;
    }
}
