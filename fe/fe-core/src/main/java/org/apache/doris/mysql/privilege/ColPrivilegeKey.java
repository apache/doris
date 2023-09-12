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

package org.apache.doris.mysql.privilege;

import org.apache.doris.cluster.ClusterNamespace;

import com.google.common.base.Objects;
import com.google.gson.annotations.SerializedName;

public class ColPrivilegeKey {

    @SerializedName(value = "privilegeIdx")
    private int privilegeIdx;
    @SerializedName(value = "ctl")
    private String ctl;
    @SerializedName(value = "db")
    private String db;
    @SerializedName(value = "tbl")
    private String tbl;

    public ColPrivilegeKey(Privilege privilege, String ctl, String db, String tbl) {
        this.privilegeIdx = privilege.getIdx();
        this.ctl = ClusterNamespace.getNameFromFullName(ctl);
        this.db = ClusterNamespace.getNameFromFullName(db);
        this.tbl = ClusterNamespace.getNameFromFullName(tbl);
    }

    public Privilege getPrivilege() {
        return Privilege.getPriv(privilegeIdx);
    }

    public int getPrivilegeIdx() {
        return privilegeIdx;
    }

    public void setPrivilegeIdx(int privilegeIdx) {
        this.privilegeIdx = privilegeIdx;
    }

    public String getCtl() {
        return ctl;
    }

    public void setCtl(String ctl) {
        this.ctl = ctl;
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public String getTbl() {
        return tbl;
    }

    public void setTbl(String tbl) {
        this.tbl = tbl;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ColPrivilegeKey that = (ColPrivilegeKey) o;
        return privilegeIdx == that.privilegeIdx
                && Objects.equal(ctl, that.ctl)
                && Objects.equal(db, that.db)
                && Objects.equal(tbl, that.tbl);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(privilegeIdx, ctl, db, tbl);
    }
}
