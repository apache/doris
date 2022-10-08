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

package org.apache.doris.analysis;

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TSortType;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class DataSortInfo implements Writable {
    public static final String DATA_SORT_PROPERTY_PREFIX = "data_sort";
    public static final String DATA_SORT_TYPE = "data_sort.sort_type";
    public static final String DATA_SORT_COL_NUM = "data_sort.col_num";

    @SerializedName(value = "sort_type")
    private TSortType sortType;
    @SerializedName(value = "col_num")
    private int colNum;

    public DataSortInfo() {

    }

    public DataSortInfo(Map<String, String> properties) {
        if (properties != null && !properties.isEmpty()) {
            if (properties.get(DATA_SORT_TYPE).equalsIgnoreCase("ZORDER")) {
                this.sortType = TSortType.ZORDER;
            } else {
                this.sortType = TSortType.LEXICAL;
            }
            this.colNum = Integer.parseInt(properties.get(DATA_SORT_COL_NUM));
        }
    }

    public DataSortInfo(TSortType sortType, int colNum) {
        this.sortType = sortType;
        this.colNum = colNum;
    }

    public TSortType getSortType() {
        return sortType;
    }

    public void setSortType(TSortType sortType) {
        this.sortType = sortType;
    }

    public int getColNum() {
        return colNum;
    }

    public void setColNum(int colNum) {
        this.colNum = colNum;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static DataSortInfo read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, DataSortInfo.class);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DataSortInfo that = (DataSortInfo) o;
        return colNum == that.colNum && sortType == that.sortType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(sortType, colNum);
    }

    public String toSql() {
        String res = ",\n\"" + DATA_SORT_TYPE + "\" = \"" + this.sortType + "\""
                + ",\n\"" + DATA_SORT_COL_NUM + "\" = \"" + this.colNum + "\"";
        return res;
    }
}
