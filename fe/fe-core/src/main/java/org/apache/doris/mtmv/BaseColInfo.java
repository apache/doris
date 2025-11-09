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

import com.google.gson.annotations.SerializedName;

import java.util.Objects;

public class BaseColInfo {
    @SerializedName("ti")
    private BaseTableInfo tableInfo;
    @SerializedName("rn")
    private String colName;

    public BaseColInfo(String colName, BaseTableInfo tableInfo) {
        this.colName = colName;
        this.tableInfo = tableInfo;
    }

    public String getColName() {
        return colName;
    }

    public void setColName(String colName) {
        this.colName = colName;
    }

    public BaseTableInfo getTableInfo() {
        return tableInfo;
    }

    public void setTableInfo(BaseTableInfo tableInfo) {
        this.tableInfo = tableInfo;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BaseColInfo that = (BaseColInfo) o;
        return Objects.equals(tableInfo, that.tableInfo) && Objects.equals(colName, that.colName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableInfo, colName);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("BaseColInfo{");
        sb.append("colName='").append(colName).append('\'');
        sb.append(", tableInfo=").append(tableInfo);
        sb.append('}');
        return sb.toString();
    }
}
