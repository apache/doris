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

package org.apache.doris.policy;

import org.apache.doris.analysis.CreateTablePolicyStmt;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;

import com.google.gson.annotations.SerializedName;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import lombok.Data;

/**
 * Save policy for filtering data.
 **/
@Data
public abstract class Policy implements Writable, GsonPostProcessable {

    private static final Logger LOG = LogManager.getLogger(Policy.class);

    @SerializedName(value = "type")
    protected PolicyTypeEnum type;

    @SerializedName(value = "policyName")
    protected String policyName;

    /**
     * Use for Serialization/deserialization.
     **/
    @SerializedName(value = "originStmt")
    protected String originStmt;

    public Policy() {}

    public Policy(final PolicyTypeEnum type, final String policyName, String originStmt) {
        this.type = type;
        this.policyName = policyName;
        this.originStmt = originStmt;
    }
    /**
     * Trans stmt to Policy.
     **/
    public static Policy fromCreateStmt(CreateTablePolicyStmt stmt) throws AnalysisException {
        String curDb = stmt.getTableName().getDb();
        if (curDb == null) {
            curDb = ConnectContext.get().getDatabase();
        }
        Database db = Catalog.getCurrentCatalog().getDbOrAnalysisException(curDb);
        UserIdentity userIdent = stmt.getUser();
        userIdent.analyze(ConnectContext.get().getClusterName());
        switch (stmt.getType()) {
            case ROW:
            default:
                Table table = db.getTableOrAnalysisException(stmt.getTableName().getTbl());
                return new TablePolicy(stmt.getType(), stmt.getPolicyName(), db.getId(), userIdent,
                    stmt.getOrigStmt().originStmt, table.getId(), stmt.getFilterType(),
                    stmt.getWherePredicate());
        }
    }

    /**
     * Use for SHOW POLICY.
     **/
    public abstract List<String> getShowInfo() throws AnalysisException;

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    /**
     * Read policy from file.
     **/
    public static Policy read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, Policy.class);
    }

    // it is used to check whether this policy is in PolicyMgr
    public boolean matchPolicy(Policy policy) {
       return policy.getType().equals(type)
            && StringUtils.equals(policy.getPolicyName(), policyName);
    }

    public boolean matchPolicy(DropPolicyLog dropPolicyLog) {
        return dropPolicyLog.getType().equals(type)
            && StringUtils.equals(dropPolicyLog.getPolicyName(), policyName);
    }

    public abstract boolean isInvalid();

}
