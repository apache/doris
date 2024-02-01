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

import org.apache.doris.mysql.privilege.Privilege;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.List;

// Indicates the access permission of a user to a resource
// better than the current support for both permissions, so the current comparison is done using a simple priority flag
public enum AccessPrivilege {
    READ_ONLY(1, "READ_ONLY"),
    READ_WRITE(2, "READ_WRITE"),
    ALL(3, "ALL"),
    NODE_PRIV(4, "Privilege for cluster node operations"),
    GRANT_PRIV(5, "Privilege for granting privilege"),
    SELECT_PRIV(6, "Privilege for select data in tables"),
    LOAD_PRIV(7, "Privilege for loading data into tables"),
    ALTER_PRIV(8, "Privilege for alter database or table"),
    CREATE_PRIV(9, "Privilege for creating database or table"),
    DROP_PRIV(10, "Privilege for dropping database or table"),
    ADMIN_PRIV(11, "All privileges except NODE_PRIV"),
    USAGE_PRIV(12, "Privilege for use resource"),
    SHOW_VIEW_PRIV(13, "Privilege for show view");

    private int flag;
    private String desc;

    private AccessPrivilege(int flag, String desc) {
        this.flag = flag;
        this.desc = desc;
    }

    public List<Privilege> toDorisPrivilege() {
        Preconditions.checkState(flag > 0 && flag < 14);
        switch (flag) {
            case 1:
            case 6:
                return Lists.newArrayList(Privilege.SELECT_PRIV);
            case 2:
            case 3:
                return Lists.newArrayList(Privilege.SELECT_PRIV, Privilege.LOAD_PRIV,
                        Privilege.ALTER_PRIV, Privilege.CREATE_PRIV,
                        Privilege.DROP_PRIV);
            case 4:
                return Lists.newArrayList(Privilege.NODE_PRIV);
            case 5:
                return Lists.newArrayList(Privilege.GRANT_PRIV);
            case 7:
                return Lists.newArrayList(Privilege.LOAD_PRIV);
            case 8:
                return Lists.newArrayList(Privilege.ALTER_PRIV);
            case 9:
                return Lists.newArrayList(Privilege.CREATE_PRIV);
            case 10:
                return Lists.newArrayList(Privilege.DROP_PRIV);
            case 11:
                return Lists.newArrayList(Privilege.ADMIN_PRIV);
            case 12:
                return Lists.newArrayList(Privilege.USAGE_PRIV);
            case 13:
                return Lists.newArrayList(Privilege.SHOW_VIEW_PRIV);
            default:
                return null;
        }
    }

    // Used to restrict which permissions support column permissions
    public boolean canHasColPriv() {
        return this == SELECT_PRIV;
    }

    public static AccessPrivilege fromName(String privStr) {
        try {
            return AccessPrivilege.valueOf(privStr.toUpperCase());
        } catch (Exception e) {
            return null;
        }
    }

    public static AccessPrivilege merge(List<AccessPrivilege> privileges) {
        if (privileges == null || privileges.isEmpty()) {
            return null;
        }

        AccessPrivilege privilege = null;
        for (AccessPrivilege iter : privileges) {
            if (privilege == null) {
                privilege = iter;
            } else {
                if (iter.flag > privilege.flag) {
                    privilege = iter;
                }
            }
        }

        return privilege;
    }

    @Override
    public String toString() {
        return desc;
    }
}
