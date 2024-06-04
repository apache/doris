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

public class RangerDataMaskPolicy implements DataMaskPolicy {
    private UserIdentity userIdentity;
    private String ctl;
    private String db;
    private String tbl;
    private String col;
    private long policyId;
    private long policyVersion;
    private String maskType;
    private String maskTypeDef;

    public RangerDataMaskPolicy(UserIdentity userIdentity, String ctl, String db, String tbl, String col,
            long policyId,
            long policyVersion, String maskType, String maskTypeDef) {
        this.userIdentity = userIdentity;
        this.ctl = ctl;
        this.db = db;
        this.tbl = tbl;
        this.col = col;
        this.policyId = policyId;
        this.policyVersion = policyVersion;
        this.maskType = maskType;
        this.maskTypeDef = maskTypeDef;
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

    public String getCol() {
        return col;
    }

    public long getPolicyId() {
        return policyId;
    }

    public long getPolicyVersion() {
        return policyVersion;
    }

    public String getMaskType() {
        return maskType;
    }

    @Override
    public String getMaskTypeDef() {
        return maskTypeDef;
    }

    @Override
    public String getPolicyIdent() {
        return getPolicyId() + ":" + getPolicyVersion();
    }

    @Override
    public String toString() {
        return "RangerDataMaskPolicy{"
                + "userIdentity=" + userIdentity
                + ", ctl='" + ctl + '\''
                + ", db='" + db + '\''
                + ", tbl='" + tbl + '\''
                + ", col='" + col + '\''
                + ", policyId=" + policyId
                + ", policyVersion=" + policyVersion
                + ", maskType='" + maskType + '\''
                + ", maskTypeDef='" + maskTypeDef + '\''
                + '}';
    }
}
