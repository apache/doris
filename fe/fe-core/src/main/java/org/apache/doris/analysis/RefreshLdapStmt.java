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

import org.apache.doris.common.UserException;

public class RefreshLdapStmt extends DdlStmt implements NotFallbackInParser {

    private boolean isAll;

    private String user;

    RefreshLdapStmt(boolean isAll, String user) {
        this.isAll = isAll;
        this.user = user;
    }

    public boolean getIsAll() {
        return isAll;
    }

    public String getUser() {
        return user;
    }

    @Override
    public void analyze() throws UserException {
        super.analyze();
    }

    @Override
    public String toSql() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("REFRESH LDAP ");
        if (isAll) {
            stringBuilder.append("ALL");
        } else {
            stringBuilder.append("`").append(user).append("`");
        }
        return stringBuilder.toString();
    }

    @Override
    public StmtType stmtType() {
        return StmtType.REFRESH;
    }

}
