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

import java.util.List;

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
}
