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

public class MTMVVersionSnapshot implements MTMVSnapshotIf {
    @SerializedName("v")
    private long version;

    // The partition version after insert overwrite is 1,
    // which may cause the upper level materialized view to be unaware of changes in the data at the bottom level.
    // However, the partition ID after overwrite will change, so the partition ID should be added.
    // only for partition, table will always 0
    @SerializedName("id")
    private long id;

    public MTMVVersionSnapshot(long version) {
        this.version = version;
    }

    public MTMVVersionSnapshot(long version, long id) {
        this.version = version;
        this.id = id;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MTMVVersionSnapshot that = (MTMVVersionSnapshot) o;
        return version == that.version && id == that.id;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(version, id);
    }

    @Override
    public String toString() {
        return "MTMVVersionSnapshot{"
                + "version=" + version
                + ", id=" + id
                + '}';
    }
}
