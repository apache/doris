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

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.Expression;

public class RangerRowFilterPolicy implements RowFilterPolicy {
    private UserIdentity userIdentity;
    private String ctl;
    private String db;
    private String tbl;
    private long policyId;
    private long policyVersion;
    private String filterExpr;

    public RangerRowFilterPolicy(UserIdentity userIdentity, String ctl, String db, String tbl, long policyId,
            long policyVersion, String filterExpr) {
        this.userIdentity = userIdentity;
        this.ctl = ctl;
        this.db = db;
        this.tbl = tbl;
        this.policyId = policyId;
        this.policyVersion = policyVersion;
        this.filterExpr = filterExpr;
    }

    public UserIdentity getUserIdentity() {
        return userIdentity;
    }

    public String getCtl() {
        return ctl;
    }

    public String getDb() {
        return db;
    }

    public String getTbl() {
        return tbl;
    }

    public long getPolicyId() {
        return policyId;
    }

    public long getPolicyVersion() {
        return policyVersion;
    }

    public String getFilterExpr() {
        return filterExpr;
    }

    @Override
    public Expression getFilterExpression() {
        NereidsParser nereidsParser = new NereidsParser();
        return nereidsParser.parseExpression(filterExpr);
    }

    @Override
    public String getPolicyIdent() {
        return getPolicyId() + ":" + getPolicyVersion();
    }

    @Override
    public String toString() {
        return "RangerRowFilterPolicy{"
                + "userIdentity=" + userIdentity
                + ", ctl='" + ctl + '\''
                + ", db='" + db + '\''
                + ", tbl='" + tbl + '\''
                + ", policyId=" + policyId
                + ", policyVersion=" + policyVersion
                + ", filterExpr='" + filterExpr + '\''
                + '}';
    }
}
