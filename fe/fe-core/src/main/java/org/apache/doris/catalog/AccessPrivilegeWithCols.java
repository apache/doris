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

import org.apache.doris.analysis.TablePattern;
import org.apache.doris.mysql.privilege.ColPrivilegeKey;
import org.apache.doris.mysql.privilege.Privilege;

import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class AccessPrivilegeWithCols {
    private AccessPrivilege accessPrivilege;
    private List<String> cols;

    public AccessPrivilegeWithCols(AccessPrivilege accessPrivilege, List<String> cols) {
        this.accessPrivilege = accessPrivilege;
        this.cols = cols;
    }

    public AccessPrivilegeWithCols(AccessPrivilege accessPrivilege) {
        this.accessPrivilege = accessPrivilege;
    }

    public AccessPrivilege getAccessPrivilege() {
        return accessPrivilege;
    }

    public void setAccessPrivilege(AccessPrivilege accessPrivilege) {
        this.accessPrivilege = accessPrivilege;
    }

    public List<String> getCols() {
        return cols;
    }

    public void setCols(List<String> cols) {
        this.cols = cols;
    }

    @Override
    public String toString() {
        return "AccessPrivilegeWithCols{"
                + "accessPrivilege=" + accessPrivilege
                + ", cols=" + cols
                + '}';
    }

    public void transferAccessPrivilegeToDoris(Set<Privilege> privileges,
            Map<ColPrivilegeKey, Set<String>> colPrivileges, TablePattern tblPattern) {
        List<Privilege> dorisPrivileges = accessPrivilege.toDorisPrivilege();
        // if has no cols,represents the permissions assigned to the entire table
        if (CollectionUtils.isEmpty(cols)) {
            privileges.addAll(dorisPrivileges);
        } else {
            // if has cols, represents the permissions assigned to the cols
            for (Privilege privilege : dorisPrivileges) {
                ColPrivilegeKey colPrivilegeKey = new ColPrivilegeKey(privilege, tblPattern.getQualifiedCtl(),
                        tblPattern.getQualifiedDb(), tblPattern.getTbl());
                if (colPrivileges.containsKey(colPrivilegeKey)) {
                    colPrivileges.get(colPrivilegeKey).addAll(cols);
                } else {
                    colPrivileges.put(colPrivilegeKey, Sets.newHashSet(cols));
                }
            }
        }
    }
}
