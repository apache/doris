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

package org.apache.doris.catalog;

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;

import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;

/**
 * This class is used to save the authorization info which needs to be persisted.
 * The stmt of http connection info is not included.
 * <p>
 * When the table has been deleted, the table name could not be found by table id.
 * The job could not check the privilege without table name.
 * This class is used to resolve this problem.
 * The auth info needs to be persisted by job.
 * The job checks the privilege by auth info.
 */
public class AuthorizationInfo implements Writable {
    @SerializedName(value = "dn")
    private String dbName;
    @SerializedName(value = "tn")
    private Set<String> tableNameList;

    // only for persist
    public AuthorizationInfo() {
    }

    public AuthorizationInfo(String dbName, Set<String> tableNameList) {
        this.dbName = dbName;
        this.tableNameList = tableNameList;
    }

    public String getDbName() {
        return dbName;
    }

    public Set<String> getTableNameList() {
        return tableNameList;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        if (dbName == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            Text.writeString(out, dbName);
        }
        if (tableNameList == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeInt(tableNameList.size());
            for (String tableName : tableNameList) {
                Text.writeString(out, tableName);
            }
        }
    }

    public void readFields(DataInput in) throws IOException {
        if (in.readBoolean()) {
            dbName = Text.readString(in);
        }
        if (in.readBoolean()) {
            tableNameList = Sets.newHashSet();
            int size = in.readInt();
            for (int i = 0; i < size; i++) {
                tableNameList.add(Text.readString(in));
            }
        }
    }

    @Override
    public String toString() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }
}
