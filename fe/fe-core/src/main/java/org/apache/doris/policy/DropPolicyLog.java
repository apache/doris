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

import org.apache.doris.analysis.DropPolicyStmt;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Use for transmission drop policy log.
 **/
@AllArgsConstructor
@Getter
@Setter
public class DropPolicyLog implements Writable {

    @Deprecated
    @SerializedName(value = "dbId")
    private long dbId;

    @Deprecated
    @SerializedName(value = "tableId")
    private long tableId;

    @SerializedName(value = "ctlName")
    private String ctlName;
    @SerializedName(value = "dbName")
    private String dbName;
    @SerializedName(value = "tableName")
    private String tableName;

    @SerializedName(value = "type")
    private PolicyTypeEnum type;

    @SerializedName(value = "policyName")
    private String policyName;

    @SerializedName(value = "user")
    private UserIdentity user;

    @SerializedName(value = "roleName")
    private String roleName;

    public DropPolicyLog(PolicyTypeEnum type, String policyName) {
        this.type = type;
        this.policyName = policyName;
    }

    public DropPolicyLog(String ctlName, String dbName, String tableName, PolicyTypeEnum type, String policyName,
            UserIdentity user, String roleName) {
        this.ctlName = ctlName;
        this.dbName = dbName;
        this.tableName = tableName;
        this.type = type;
        this.policyName = policyName;
        this.user = user;
        this.roleName = roleName;
    }

    /**
     * Generate delete logs through stmt.
     **/
    public static DropPolicyLog fromDropStmt(DropPolicyStmt stmt) throws AnalysisException {
        switch (stmt.getType()) {
            case STORAGE:
                return new DropPolicyLog(stmt.getType(), stmt.getPolicyName());
            case ROW:
                return new DropPolicyLog(stmt.getTableName().getCtl(), stmt.getTableName().getDb(),
                        stmt.getTableName().getTbl(), stmt.getType(),
                        stmt.getPolicyName(), stmt.getUser(), stmt.getRoleName());
            default:
                throw new AnalysisException("Invalid policy type: " + stmt.getType().name());
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static DropPolicyLog read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), DropPolicyLog.class);
    }
}
