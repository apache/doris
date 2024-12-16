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

package org.apache.doris.backup;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public class SnapshotInfo implements Writable {
    @SerializedName("db")
    private long dbId;
    @SerializedName("tbl")
    private long tblId;
    @SerializedName("p")
    private long partitionId;
    @SerializedName("ind")
    private long indexId;
    @SerializedName("tab")
    private long tabletId;
    @SerializedName("be")
    private long beId;
    @SerializedName("sh")
    private int schemaHash;
    // eg: /path/to/your/be/data/snapshot/20180410102311.0.86400/
    @SerializedName("path")
    private String path;
    // eg:
    // 10006_0_1_0_0.dat
    // 10006_2_2_0_0.idx
    // 10006.hdr
    @SerializedName("f")
    private List<String> files = Lists.newArrayList();

    public SnapshotInfo() {
        // for persist
    }

    public SnapshotInfo(long dbId, long tblId, long partitionId, long indexId, long tabletId,
            long beId, int schemaHash, String path, List<String> files) {
        this.dbId = dbId;
        this.tblId = tblId;
        this.partitionId = partitionId;
        this.indexId = indexId;
        this.tabletId = tabletId;
        this.beId = beId;
        this.schemaHash = schemaHash;
        this.path = path;
        this.files = files;
    }

    public long getDbId() {
        return dbId;
    }

    public long getTblId() {
        return tblId;
    }

    public long getPartitionId() {
        return partitionId;
    }

    public long getIndexId() {
        return indexId;
    }

    public long getTabletId() {
        return tabletId;
    }

    public long getBeId() {
        return beId;
    }

    public int getSchemaHash() {
        return schemaHash;
    }

    public String getPath() {
        return path;
    }

    public List<String> getFiles() {
        return files;
    }

    public void setFiles(List<String> files) {
        this.files = files;
    }

    public String getTabletPath() {
        String basePath = Joiner.on("/").join(path, tabletId, schemaHash);
        return basePath;
    }

    public static SnapshotInfo read(DataInput in) throws IOException {
        if (Env.getCurrentEnvJournalVersion() < FeMetaVersion.VERSION_135) {
            SnapshotInfo info = new SnapshotInfo();
            info.readFields(in);
            return info;
        } else {
            String json = Text.readString(in);
            return GsonUtils.GSON.fromJson(json, SnapshotInfo.class);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public void readFields(DataInput in) throws IOException {
        dbId = in.readLong();
        tblId = in.readLong();
        partitionId = in.readLong();
        indexId = in.readLong();
        tabletId = in.readLong();
        beId = in.readLong();
        schemaHash = in.readInt();
        path = Text.readString(in);

        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            files.add(Text.readString(in));
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("tablet id: ").append(tabletId);
        sb.append(", be id: ").append(beId);
        sb.append(", path: ").append(path);
        sb.append(", files:").append(files);
        return sb.toString();
    }
}
