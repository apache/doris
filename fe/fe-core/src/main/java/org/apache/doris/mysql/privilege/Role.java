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

import org.apache.doris.analysis.ResourcePattern;
import org.apache.doris.analysis.TablePattern;
import org.apache.doris.catalog.AuthorizationInfo;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.mysql.privilege.Auth.PrivLevel;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

public class Role implements Writable {
    // operator is responsible for operating cluster, such as add/drop node
    public static String OPERATOR_ROLE = "operator";
    // admin is like DBA, who has all privileges except for NODE privilege held by operator
    public static String ADMIN_ROLE = "admin";

    public static Role OPERATOR = new Role(OPERATOR_ROLE,
            TablePattern.ALL, PrivBitSet.of(Privilege.NODE_PRIV, Privilege.ADMIN_PRIV),
            ResourcePattern.ALL, PrivBitSet.of(Privilege.NODE_PRIV, Privilege.ADMIN_PRIV));
    public static Role ADMIN = new Role(ADMIN_ROLE,
            TablePattern.ALL, PrivBitSet.of(Privilege.ADMIN_PRIV),
            ResourcePattern.ALL, PrivBitSet.of(Privilege.ADMIN_PRIV));

    private String roleName;
    private Map<TablePattern, PrivBitSet> tblPatternToPrivs = Maps.newConcurrentMap();
    private Map<ResourcePattern, PrivBitSet> resourcePatternToPrivs = Maps.newConcurrentMap();

    private GlobalPrivTable globalPrivTable = new GlobalPrivTable();
    private CatalogPrivTable catalogPrivTable = new CatalogPrivTable();
    private DbPrivTable dbPrivTable = new DbPrivTable();
    private TablePrivTable tablePrivTable = new TablePrivTable();
    private ResourcePrivTable resourcePrivTable = new ResourcePrivTable();

    private Role() {

    }

    public Role(String roleName) {
        this.roleName = roleName;
    }

    public Role(String roleName, TablePattern tablePattern, PrivBitSet privs) {
        this.roleName = roleName;
        this.tblPatternToPrivs.put(tablePattern, privs);
    }

    public Role(String roleName, ResourcePattern resourcePattern, PrivBitSet privs) {
        this.roleName = roleName;
        this.resourcePatternToPrivs.put(resourcePattern, privs);
    }

    public Role(String roleName, TablePattern tablePattern, PrivBitSet tablePrivs,
            ResourcePattern resourcePattern, PrivBitSet resourcePrivs) {
        this.roleName = roleName;
        this.tblPatternToPrivs.put(tablePattern, tablePrivs);
        this.resourcePatternToPrivs.put(resourcePattern, resourcePrivs);
    }

    public String getRoleName() {
        return roleName;
    }

    public Map<TablePattern, PrivBitSet> getTblPatternToPrivs() {
        return tblPatternToPrivs;
    }

    public Map<ResourcePattern, PrivBitSet> getResourcePatternToPrivs() {
        return resourcePatternToPrivs;
    }

    // merge role not check role name.
    public void mergeNotCheck(Role other) {
        for (Map.Entry<TablePattern, PrivBitSet> entry : other.getTblPatternToPrivs().entrySet()) {
            if (tblPatternToPrivs.containsKey(entry.getKey())) {
                PrivBitSet existPrivs = tblPatternToPrivs.get(entry.getKey());
                existPrivs.or(entry.getValue());
            } else {
                tblPatternToPrivs.put(entry.getKey(), entry.getValue());
            }
        }
        for (Map.Entry<ResourcePattern, PrivBitSet> entry : other.resourcePatternToPrivs.entrySet()) {
            if (resourcePatternToPrivs.containsKey(entry.getKey())) {
                PrivBitSet existPrivs = resourcePatternToPrivs.get(entry.getKey());
                existPrivs.or(entry.getValue());
            } else {
                resourcePatternToPrivs.put(entry.getKey(), entry.getValue());
            }
        }
    }

    public void merge(Role other) {
        Preconditions.checkState(roleName.equalsIgnoreCase(other.getRoleName()));
        mergeNotCheck(other);
    }


    public static Role read(DataInput in) throws IOException {
        Role role = new Role();
        role.readFields(in);
        return role;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, roleName);
        out.writeInt(tblPatternToPrivs.size());
        for (Map.Entry<TablePattern, PrivBitSet> entry : tblPatternToPrivs.entrySet()) {
            entry.getKey().write(out);
            entry.getValue().write(out);
        }
        out.writeInt(resourcePatternToPrivs.size());
        for (Map.Entry<ResourcePattern, PrivBitSet> entry : resourcePatternToPrivs.entrySet()) {
            entry.getKey().write(out);
            entry.getValue().write(out);
        }
    }

    public void readFields(DataInput in) throws IOException {
        roleName = Text.readString(in);
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            TablePattern tblPattern = TablePattern.read(in);
            PrivBitSet privs = PrivBitSet.read(in);
            tblPatternToPrivs.put(tblPattern, privs);
        }
        size = in.readInt();
        for (int i = 0; i < size; i++) {
            ResourcePattern resourcePattern = ResourcePattern.read(in);
            PrivBitSet privs = PrivBitSet.read(in);
            resourcePatternToPrivs.put(resourcePattern, privs);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("role: ").append(roleName).append(", db table privs: ").append(tblPatternToPrivs);
        sb.append(", resource privs: ").append(resourcePatternToPrivs);
        return sb.toString();
    }

    public boolean checkGlobalPriv(PrivPredicate wanted) {
        return true;
    }

    public boolean checkCtlPriv(String ctl, PrivPredicate wanted) {
        return true;
    }

    public boolean checkDbPriv(String ctl, String db, PrivPredicate wanted) {
        return true;
    }

    public boolean checkTblPriv(String ctl, String db, String tbl, PrivPredicate wanted) {
        return true;
    }

    public boolean checkResourcePriv(String resourceName, PrivPredicate wanted) {
        return true;
    }

    public boolean checkPrivByAuthInfo(AuthorizationInfo authInfo, PrivPredicate wanted) {
        return true;
    }

    public boolean checkHasPrivInternal(PrivPredicate priv, PrivLevel[] levels) {
        return true;
    }
}
