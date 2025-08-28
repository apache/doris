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

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Represents the information needed for table's branch or tag operation.
 * This class is used for serialization and deserialization of table branch or tag information.
 */
public class TableBranchOrTagInfo implements Writable {
    @SerializedName(value = "ctl")
    private String ctlName;
    @SerializedName(value = "db")
    private String dbName;
    @SerializedName(value = "tbl")
    private String tblName;

    public TableBranchOrTagInfo(String ctlName, String dbName, String tblName) {
        this.ctlName = ctlName;
        this.dbName = dbName;
        this.tblName = tblName;
    }

    public String getCtlName() {
        return ctlName;
    }

    public String getDbName() {
        return dbName;
    }

    public String getTblName() {
        return tblName;
    }

    public static TableBranchOrTagInfo read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, TableBranchOrTagInfo.class);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof TableBranchOrTagInfo)) {
            return false;
        }
        TableBranchOrTagInfo other = (TableBranchOrTagInfo) obj;
        return ctlName.equals(other.ctlName) && dbName.equals(other.dbName) && tblName.equals(other.tblName);
    }
}
