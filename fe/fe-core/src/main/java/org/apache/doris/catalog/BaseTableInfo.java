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

import org.apache.doris.cluster.ClusterNamespace;

import com.google.common.base.Objects;
import com.google.gson.annotations.SerializedName;
import lombok.Data;

@Data
public class BaseTableInfo {
    @SerializedName("tn")
    private String tableName;
    @SerializedName("dn")
    private String dbName;
    @SerializedName("cn")
    private String ctlName;

    public BaseTableInfo(String tableName, String dbName, String ctlName) {
        this.tableName = java.util.Objects.requireNonNull(tableName, "require tableName object");
        this.dbName = ClusterNamespace
                .getNameFromFullName(java.util.Objects.requireNonNull(dbName, "require dbName object"));
        this.ctlName = java.util.Objects.requireNonNull(ctlName, "require ctlName object");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BaseTableInfo that = (BaseTableInfo) o;
        return Objects.equal(tableName, that.tableName) &&
                Objects.equal(dbName, that.dbName) &&
                Objects.equal(ctlName, that.ctlName);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(tableName, dbName, ctlName);
    }
}
