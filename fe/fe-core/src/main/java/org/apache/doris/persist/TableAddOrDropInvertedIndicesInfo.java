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

/**
 * PersistInfo for Table properties
 */
public class TableAddOrDropInvertedIndicesInfo implements Writable {
    @SerializedName(value = "dbId")
    private long dbId;
    @SerializedName(value = "tableId")
    private long tableId;
    @SerializedName(value = "indexSchemaMap")
    private Map<Long, LinkedList<Column>> indexSchemaMap;
    @SerializedName(value = "propertyMap")
    private Map<String, String> propertyMap;
    @SerializedName(value = "indexes")
    private List<Index> indexes;
    @SerializedName(value = "alterInvertedIndexes")
    private List<Index> alterInvertedIndexes;
    @SerializedName(value = "isDropInvertedIndex")
    private boolean isDropInvertedIndex;
    @SerializedName(value = "oriIndexes")
    private List<Index> oriIndexes;
    @SerializedName(value = "jobId")
    private long jobId;

    public TableAddOrDropInvertedIndicesInfo(long dbId, long tableId,
            Map<Long, LinkedList<Column>> indexSchemaMap, Map<String, String> propertyMap,
            List<Index> indexes, List<Index> alterInvertedIndexes, boolean isDropInvertedIndex,
            List<Index> oriIndexes, long jobId) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.indexSchemaMap = indexSchemaMap;
        this.propertyMap = propertyMap;
        this.indexes = indexes;
        this.alterInvertedIndexes = alterInvertedIndexes;
        this.isDropInvertedIndex = isDropInvertedIndex;
        this.oriIndexes = oriIndexes;
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

    public Map<String, String> getPropertyMap() {
        return propertyMap;
    }

    public List<Index> getIndexes() {
        return indexes;
    }

    public List<Index> getAlterInvertedIndexes() {
        return alterInvertedIndexes;
    }

    public boolean getIsDropInvertedIndex() {
        return isDropInvertedIndex;
    }

    public List<Index> getOriIndexes() {
        return oriIndexes;
    }

    public long getJobId() {
        return jobId;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static TableAddOrDropInvertedIndicesInfo read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), TableAddOrDropInvertedIndicesInfo.class);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (!(obj instanceof TableAddOrDropInvertedIndicesInfo)) {
            return false;
        }

        TableAddOrDropInvertedIndicesInfo info = (TableAddOrDropInvertedIndicesInfo) obj;

        return (dbId == info.dbId && tableId == tableId
                && indexSchemaMap.equals(info.indexSchemaMap)
                && propertyMap.equals(info.propertyMap)
                && indexes.equals(info.indexes)
                && alterInvertedIndexes.equals(info.alterInvertedIndexes)
                && isDropInvertedIndex == info.isDropInvertedIndex
                && oriIndexes.equals(info.oriIndexes) && jobId == info.jobId);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(" dbId: ").append(dbId);
        sb.append(" tableId: ").append(tableId);
        sb.append(" indexSchemaMap: ").append(indexSchemaMap);
        sb.append(" propertyMap: ").append(propertyMap);
        sb.append(" indexes: ").append(indexes);
        sb.append(" alterInvertedIndexes: ").append(alterInvertedIndexes);
        sb.append(" isDropInvertedIndex: ").append(isDropInvertedIndex);
        sb.append(" oriIndexes: ").append(oriIndexes);
        sb.append(" jobId: ").append(jobId);
        return sb.toString();
    }
}
