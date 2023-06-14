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

package org.apache.doris.hplsql.store;

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TStoredProcedure;

import com.google.gson.annotations.SerializedName;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@AllArgsConstructor
@Getter
public class StoredProcedure implements Writable {
    @SerializedName(value = "name")
    private String name;

    @SerializedName(value = "catalogName")
    private String catalogName;

    @SerializedName(value = "dbName")
    private String dbName;

    @SerializedName(value = "ownerName")
    private String ownerName;

    @SerializedName(value = "source")
    private String source;

    public TStoredProcedure toThrift() {
        return new TStoredProcedure().setName(name).setCatalogName(catalogName).setDbName(dbName)
                .setOwnerName(ownerName).setSource(source);
    }

    public static StoredProcedure fromThrift(TStoredProcedure procedure) {
        return new StoredProcedure(procedure.getName(), procedure.getCatalogName(), procedure.getDbName(),
                procedure.getOwnerName(), procedure.source);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static StoredProcedure read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, StoredProcedure.class);
    }
}
