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

package org.apache.doris.persist;

import org.apache.doris.catalog.Partition;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TruncateTableInfo implements Writable {
    @SerializedName(value = "dbId")
    private long dbId;
    @SerializedName(value = "db")
    private String db;
    @SerializedName(value = "tblId")
    private long tblId;
    @SerializedName(value = "table")
    private String table;
    @SerializedName(value = "partitions")
    private List<Partition> partitions = Lists.newArrayList();
    @SerializedName(value = "isEntireTable")
    private boolean isEntireTable = false;
    @SerializedName(value = "rawSql")
    private String rawSql = "";
    @SerializedName(value = "op")
    private Map<Long, String> oldPartitions = new HashMap<>();

    public TruncateTableInfo() {

    }

    public TruncateTableInfo(long dbId, String db, long tblId, String table, List<Partition> partitions,
            boolean isEntireTable, String rawSql, List<Partition> oldPartitions) {
        this.dbId = dbId;
        this.db = db;
        this.tblId = tblId;
        this.table = table;
        this.partitions = partitions;
        this.isEntireTable = isEntireTable;
        this.rawSql = rawSql;
        for (Partition partition : oldPartitions) {
            this.oldPartitions.put(partition.getId(), partition.getName());
        }
    }

    public long getDbId() {
        return dbId;
    }

    public String getDb() {
        return db;
    }

    public long getTblId() {
        return tblId;
    }

    public String getTable() {
        return table;
    }

    public List<Partition> getPartitions() {
        return partitions;
    }

    public Map<Long, String> getOldPartitions() {
        return oldPartitions == null ? new HashMap<>() : oldPartitions;
    }

    public boolean isEntireTable() {
        return isEntireTable;
    }

    public String getRawSql() {
        return rawSql;
    }

    public static TruncateTableInfo read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, TruncateTableInfo.class);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public String toJson() {
        return GsonUtils.GSON.toJson(this);
    }

    @Override
    public String toString() {
        return "TruncateTableInfo{"
                + "dbId=" + dbId
                + ", db='" + db + '\''
                + ", tblId=" + tblId
                + ", table='" + table + '\''
                + ", isEntireTable=" + isEntireTable
                + ", rawSql='" + rawSql + '\''
                + ", partitions_size=" + partitions.size()
                + '}';
    }
}
