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

package org.apache.doris.analysis;

import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.UserException;

public class DropRoleStmt extends DdlStmt {

    private String role;

    public DropRoleStmt(String role) {
        this.role = role;
    }

    public String getQualifiedRole() {
        return role;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        FeNameFormat.checkRoleName(role, false /* can not be superuser */, "Can not drop role");
        role = ClusterNamespace.getFullName(analyzer.getClusterName(), role);
    }

    @Override
    public String toSql() {
        return "DROP ROLE " + role;
    }
}
