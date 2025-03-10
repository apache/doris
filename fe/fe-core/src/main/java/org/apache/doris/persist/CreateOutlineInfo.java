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

import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.base.Strings;
import com.google.gson.annotations.SerializedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class CreateOutlineInfo implements Writable, GsonPostProcessable {
    public static final Logger LOG = LoggerFactory.getLogger(CreateTableInfo.class);

    @SerializedName(value = "ctl")
    private String ctlName;
    @SerializedName(value = "dbName")
    private String dbName;
    @SerializedName(value = "tbl")
    private String outlineName;

    public CreateOutlineInfo() {
        // for persist
    }

    public CreateOutlineInfo(String dbName, String outlineName) {
        this.ctlName = InternalCatalog.INTERNAL_CATALOG_NAME;
        this.dbName = dbName;
        this.outlineName = outlineName;
    }

    public String getCtlName() {
        return ctlName;
    }

    public String getDbName() {
        return dbName;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static CreateOutlineInfo read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), CreateOutlineInfo.class);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dbName);
    }

    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof CreateOutlineInfo)) {
            return false;
        }

        CreateOutlineInfo info = (CreateOutlineInfo) obj;

        return (dbName.equals(info.dbName))
            && (outlineName.equals(info.outlineName));
    }

    public String toJson() {
        return GsonUtils.GSON.toJson(this);
    }

    @Override
    public String toString() {
        // In previous versions, ctlName and tblName is not set, so it may be null.
        return String.format("%s.%s.%s",
            Strings.isNullOrEmpty(ctlName) ? InternalCatalog.INTERNAL_CATALOG_NAME : ctlName,
            dbName,
            outlineName);
    }

    @Override
    public void gsonPostProcess() throws IOException {
        dbName = ClusterNamespace.getNameFromFullName(dbName);
    }
}

