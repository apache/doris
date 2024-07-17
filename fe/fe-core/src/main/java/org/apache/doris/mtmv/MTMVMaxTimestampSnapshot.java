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

package org.apache.doris.mtmv;

import com.google.common.base.Objects;
import com.google.gson.annotations.SerializedName;

/**
 * The version cannot be obtained from the hive table,
 * so the update time is used instead of the version
 */
public class MTMVMaxTimestampSnapshot implements MTMVSnapshotIf {
    // partitionName corresponding to timestamp
    // The reason why both timestamp and partitionName are stored is to avoid
    // deleting the partition corresponding to timestamp
    @SerializedName("pn")
    private String partitionName;
    // The maximum modify time in all partitions
    @SerializedName("t")
    private long timestamp;

    public MTMVMaxTimestampSnapshot(String partitionName, long timestamp) {
        this.partitionName = partitionName;
        this.timestamp = timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MTMVMaxTimestampSnapshot that = (MTMVMaxTimestampSnapshot) o;
        return timestamp == that.timestamp && Objects.equal(partitionName, that.partitionName);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(partitionName, timestamp);
    }

    @Override
    public String toString() {
        return "MTMVMaxTimestampSnapshot{"
                + "partitionName='" + partitionName + '\''
                + ", timestamp=" + timestamp
                + '}';
    }
}
