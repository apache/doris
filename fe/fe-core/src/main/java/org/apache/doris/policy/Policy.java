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

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Setter;

import org.apache.doris.analysis.CreatePolicyStmt;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

@Data
@AllArgsConstructor
public class Policy implements Writable {
    private static final Logger LOG = LogManager.getLogger(Policy.class);

    @SerializedName(value = "dbId")
    private long dbId;

    @SerializedName(value = "tableId")
    private long tableId;

    @SerializedName(value = "policyName")
    private String policyName;

    /**
     * ROW
     **/
    @SerializedName(value = "type")
    private String type;

    /**
     * PERMISSIVE | RESTRICTIVE, If multiple types exist, the last type prevails
     **/
    @SerializedName(value = "filterType")
    private final FilterType filterType;

    /**
     * filter sql
     **/
    @Setter
    private Expr wherePredicate;

    /**
     * bind user
     **/
    @SerializedName(value = "user")
    private final String user;

    public static Policy fromCreateStmt(CreatePolicyStmt stmt) throws AnalysisException {
        String curDb = stmt.getTableName().getDb();
        if (curDb == null) {
            curDb = ConnectContext.get().getDatabase();
        }
        Database db = Catalog.getCurrentCatalog().getDbOrAnalysisException(curDb);
        Table table = db.getTableOrAnalysisException(stmt.getTableName().getTbl());
        UserIdentity userIdent = stmt.getUserIdent();
        userIdent.analyze(ConnectContext.get().getClusterName());
        return new Policy(db.getId(), table.getId(), stmt.getPolicyName(), stmt.getType(), stmt.getFilterType(), stmt.getWherePredicate(), userIdent.getQualifiedUser());
    }

    public List<String> getShowInfo() throws AnalysisException {
        return Lists.newArrayList(this.policyName, Catalog.getCurrentCatalog().getDbOrAnalysisException(this.dbId).getTableOrAnalysisException(this.tableId).getName(), this.type, this.filterType.name(), this.wherePredicate != null ? this.wherePredicate.toSql() : null, this.user);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Expr.writeTo(wherePredicate, out);
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static Policy read(DataInput in) throws IOException {
        Expr expr = Expr.readIn(in);
        String json = Text.readString(in);
        Policy policy = GsonUtils.GSON.fromJson(json, Policy.class);
        policy.setWherePredicate(expr);
        return policy;
    }
}
