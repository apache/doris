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

import org.apache.doris.analysis.IndexDef;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Index;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * PersistInfo for Table properties
 */
public class TableAddOrDropIndexIndicesInfo implements Writable {
    @SerializedName(value = "dbId")
    private long dbId;
    @SerializedName(value = "tableId")
    private long tableId;
    @SerializedName(value = "indexSchemaMap")
    private Map<Long, LinkedList<Column>> indexSchemaMap;
    @SerializedName(value = "indexes")
    private List<Index> indexes;
    @SerializedName(value = "alterInvertedIndexes")
    private List<Index> alterInvertedIndexes;
    @SerializedName(value = "alterVectorIndexes")
    private List<Index> alterVectorIndexes;
    @SerializedName(value = "isDropInvertedIndex")
    private boolean isDropInvertedIndex;
    @SerializedName(value = "jobId")
    private long jobId;
    @SerializedName(value = "rawSql")
    private String rawSql;

    public TableAddOrDropIndexIndicesInfo(String rawSql, long dbId, long tableId,
                                          Map<Long, LinkedList<Column>> indexSchemaMap, List<Index> indexes,
                                          List<Index> alterIndexes, boolean isDropIndex,
                                          long jobId) {
        this.rawSql = rawSql;
        this.dbId = dbId;
        this.tableId = tableId;
        this.indexSchemaMap = indexSchemaMap;
        this.indexes = indexes;
        this.alterInvertedIndexes = alterIndexes.stream().filter(idx ->
            idx.getIndexType() == IndexDef.IndexType.INVERTED).collect(Collectors.toList());
        this.alterVectorIndexes = alterIndexes.stream().filter(idx ->
            idx.getIndexType() == IndexDef.IndexType.VECTOR).collect(Collectors.toList());
        this.isDropInvertedIndex = isDropIndex;
        this.jobId = jobId;
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public Map<Long, LinkedList<Column>> getIndexSchemaMap() {
        return indexSchemaMap;
    }

    public List<Index> getIndexes() {
        return indexes;
    }

    public List<Index> getAlterInvertedIndexes() {
        return alterInvertedIndexes;
    }

    public List<Index> getAlterVectorIndexes() {
        return alterVectorIndexes;
    }

    public boolean getIsDropInvertedIndex() {
        return isDropInvertedIndex;
    }

    public long getJobId() {
        return jobId;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static TableAddOrDropIndexIndicesInfo read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), TableAddOrDropIndexIndicesInfo.class);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (!(obj instanceof TableAddOrDropIndexIndicesInfo)) {
            return false;
        }

        TableAddOrDropIndexIndicesInfo info = (TableAddOrDropIndexIndicesInfo) obj;

        return (dbId == info.dbId && tableId == tableId
                && indexSchemaMap.equals(info.indexSchemaMap)
                && indexes.equals(info.indexes)
                && alterInvertedIndexes.equals(info.alterInvertedIndexes)
                && isDropInvertedIndex == info.isDropInvertedIndex
                && jobId == info.jobId);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(" dbId: ").append(dbId);
        sb.append(" tableId: ").append(tableId);
        sb.append(" indexSchemaMap: ").append(indexSchemaMap);
        sb.append(" indexes: ").append(indexes);
        sb.append(" alterInvertedIndexes: ").append(alterInvertedIndexes);
        sb.append(" alterVectorIndexes: ").append(alterVectorIndexes);
        sb.append(" isDropInvertedIndex: ").append(isDropInvertedIndex);
        sb.append(" jobId: ").append(jobId);
        return sb.toString();
    }
}
