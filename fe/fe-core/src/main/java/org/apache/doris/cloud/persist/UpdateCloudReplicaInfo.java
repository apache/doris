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

package org.apache.doris.cloud.persist;


import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;
import lombok.Getter;
import lombok.Setter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
public class UpdateCloudReplicaInfo implements Writable {
    @SerializedName(value = "dbId")
    private long dbId;
    @SerializedName(value = "tableId")
    private long tableId;
    @SerializedName(value = "partitionId")
    private long partitionId;
    @SerializedName(value = "indexId")
    private long indexId;
    @SerializedName(value = "tabletId")
    private long tabletId;
    @SerializedName(value = "replicaId")
    private long replicaId;

    @SerializedName(value = "clusterId")
    private String clusterId;
    @SerializedName(value = "beId")
    private long beId;

    @SerializedName(value = "tabletIds")
    private List<Long> tabletIds = new ArrayList<Long>();

    @SerializedName(value = "beIds")
    private List<Long> beIds = new ArrayList<Long>();

    @SerializedName(value = "rids")
    private List<Long> replicaIds = new ArrayList<>();

    public UpdateCloudReplicaInfo() {
    }

    public UpdateCloudReplicaInfo(long dbId, long tableId, long partitionId, long indexId,
                                  long tabletId, long replicaId, String clusterId, long beId) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.partitionId = partitionId;
        this.indexId = indexId;
        this.tabletId = tabletId;
        this.replicaId = replicaId;
        this.clusterId = clusterId;
        this.beId = beId;

        this.beIds = null;
        this.tabletIds = null;
    }

    public UpdateCloudReplicaInfo(long dbId, long tableId, long partitionId, long indexId,
                                  String clusterId, List<Long> beIds, List<Long> tabletIds) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.partitionId = partitionId;
        this.indexId = indexId;
        this.clusterId = clusterId;
        this.beIds = beIds;
        this.tabletIds = tabletIds;

        this.tabletId = -1;
        this.replicaId = -1;
        this.beId = -1;
    }

    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this, UpdateCloudReplicaInfo.class);
        Text.writeString(out, json);
    }

    public static UpdateCloudReplicaInfo read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, UpdateCloudReplicaInfo.class);
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("database id: ").append(dbId);
        sb.append(" table id: ").append(tableId);
        sb.append(" partition id: ").append(partitionId);
        sb.append(" index id: ").append(indexId);
        sb.append(" tablet id: ").append(tabletId);
        sb.append(" replica id: ").append(replicaId);
        sb.append(" cluster: ").append(clusterId);
        sb.append(" backend id: ").append(beId);

        if (tabletId == -1) {
            if (beIds != null && !beIds.isEmpty()) {
                sb.append(" be id list: ");
                for (long id : beIds) {
                    sb.append(" ").append(id);
                }

                sb.append(" tablet id list: ");
                for (long id : tabletIds) {
                    sb.append(" ").append(id);
                }

                sb.append(" replica id list: ");
                for (long id : replicaIds) {
                    sb.append(" ").append(id);
                }
            }
        }

        return sb.toString();
    }

}
