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
public class MTMVTimestampSnapshot implements MTMVSnapshotIf {
    @SerializedName("t")
    private long timestamp;

    public MTMVTimestampSnapshot(long timestamp) {
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
        MTMVTimestampSnapshot that = (MTMVTimestampSnapshot) o;
        return timestamp == that.timestamp;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(timestamp);
    }

    @Override
    public String toString() {
        return "MTMVTimestampSnapshot{"
                + "timestamp=" + timestamp
                + '}';
    }
}
