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

package org.apache.doris.mtmv.ivm;

import com.google.gson.annotations.SerializedName;

/**
 * Thin persistent binding between one base table and its IVM stream.
 *
 * <p>Tracks the consumed TSO (persisted) and latest TSO (transient, read
 * from OlapTable before each incremental refresh). When consumedTso == latestTso
 * there is no delta to apply for this table.
 */
public class IvmStreamRef {

    /** Last consumed TSO (timestamp ordering). Persisted via editlog. */
    @SerializedName("ct")
    private long consumedTso;

    /** Latest TSO read from the base table. Transient — populated before each refresh. */
    private transient long latestTso;

    public IvmStreamRef() {
        this.consumedTso = 0;
        this.latestTso = 0;
    }

    public IvmStreamRef(long consumedTso) {
        this.consumedTso = consumedTso;
        this.latestTso = 0;
    }

    public long getConsumedTso() {
        return consumedTso;
    }

    public void setConsumedTso(long consumedTso) {
        this.consumedTso = consumedTso;
    }

    public long getLatestTso() {
        return latestTso;
    }

    public void setLatestTso(long latestTso) {
        this.latestTso = latestTso;
    }

    /** Returns true when there is no new data to consume. */
    public boolean isUpToDate() {
        return consumedTso == latestTso;
    }

    @Override
    public String toString() {
        return "IvmStreamRef{"
                + "consumedTso=" + consumedTso
                + ", latestTso=" + latestTso
                + '}';
    }
}
