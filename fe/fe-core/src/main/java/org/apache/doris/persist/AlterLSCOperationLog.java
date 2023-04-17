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

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.proto.InternalService.PFetchColIdsResponse;
import org.apache.doris.proto.InternalService.PFetchColIdsResponse.Builder;
import org.apache.doris.proto.InternalService.PFetchColIdsResponse.PFetchColIdsResultEntry;

import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class AlterLSCOperationLog implements Writable {

    @SerializedName(value = "dbId")
    private Long dbId;

    @SerializedName(value = "tableId")
    private Long tableId;

    @SerializedName(value = "indexIds")
    private List<Long> indexIds;

    @SerializedName(value = "colNameToIds")
    private List<Map<String, Integer>> colNameToIds;

    public AlterLSCOperationLog(long dbId, long tableId, PFetchColIdsResponse response) {
        this.dbId = dbId;
        this.tableId = tableId;
        indexIds = new ArrayList<>();
        colNameToIds = new ArrayList<>();
        final List<PFetchColIdsResultEntry> entriesList = Preconditions.checkNotNull(response.getEntriesList());
        entriesList.forEach(entry -> {
            indexIds.add(entry.getIndexId());
            colNameToIds.add(entry.getColNameToIdMap());
        });
    }

    public Long getDbId() {
        return dbId;
    }

    public Long getTableId() {
        return tableId;
    }

    public List<Long> getIndexIds() {
        return indexIds;
    }

    public List<Map<String, Integer>> getColNameToIds() {
        return colNameToIds;
    }

    public PFetchColIdsResponse getFetchColIdsResponse() {
        final List<Long> idxIds = Preconditions.checkNotNull(indexIds);
        final List<Map<String, Integer>> colInfos = Preconditions.checkNotNull(colNameToIds);
        Preconditions.checkState(idxIds.size() == colInfos.size());
        final Builder builder = PFetchColIdsResponse.newBuilder();
        for (int i = 0; i < idxIds.size(); i++) {
            final PFetchColIdsResultEntry entry = PFetchColIdsResultEntry.newBuilder()
                    .setIndexId(idxIds.get(i))
                    .putAllColNameToId(colInfos.get(i))
                    .build();
            builder.addEntries(entry);
        }
        return builder.build();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static AlterLSCOperationLog read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), AlterLSCOperationLog.class);
    }
}
