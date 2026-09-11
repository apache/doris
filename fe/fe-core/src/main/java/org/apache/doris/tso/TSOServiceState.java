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

package org.apache.doris.tso;

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/** The durable allocation window and readable transaction prefix, published as one snapshot. */
public final class TSOServiceState implements Writable {
    // Keep the old TSOTimestamp JSON fields and image checksum for journal/image compatibility.
    @SerializedName("physicalTimestamp")
    private final long windowEndPhysicalTime;
    @SerializedName("logicalCounter")
    private final long logicalCounter = 0;
    @SerializedName("committedTso")
    private final long committedTso;

    public TSOServiceState(long windowEndPhysicalTime, long committedTso) {
        this.windowEndPhysicalTime = windowEndPhysicalTime;
        this.committedTso = committedTso;
    }

    public long getPhysicalTimestamp() {
        return windowEndPhysicalTime;
    }

    /** Zero means no readable prefix has been established, including records written by older FEs. */
    public long getCommittedTso() {
        return committedTso;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static TSOServiceState read(DataInput in) throws IOException {
        TSOServiceState state = GsonUtils.GSON.fromJson(Text.readString(in), TSOServiceState.class);
        if (state == null) {
            throw new IOException("failed to deserialize TSO service state from journal/image");
        }
        return state;
    }
}
