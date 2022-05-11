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

import org.apache.doris.analysis.CreatePolicyStmt;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.SqlParser;
import org.apache.doris.analysis.SqlScanner;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.SqlParserUtils;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.StringReader;
import java.util.List;

/**
 * Save policy for filtering data.
 **/
@Data
@AllArgsConstructor
public class Policy implements Writable, GsonPostProcessable {

    public static final String ROW_POLICY = "ROW";

    private static final Logger LOG = LogManager.getLogger(Policy.class);

    @SerializedName(value = "dbId")
    private long dbId;

    @SerializedName(value = "tableId")
    private long tableId;

    @SerializedName(value = "policyName")
    private String policyName;

    /**
     * ROW.
     **/
    @SerializedName(value = "type")
    private PolicyTypeEnum type;

    /**
     * PERMISSIVE | RESTRICTIVE, If multiple types exist, the last type prevails.
     **/
    @SerializedName(value = "filterType")
    private final FilterType filterType;

    private Expr wherePredicate;

    /**
     * Policy bind user.
     **/
    @SerializedName(value = "user")
    private final UserIdentity user;

    /**
     * Use for Serialization/deserialization.
     **/
    @SerializedName(value = "originStmt")
    private String originStmt;

    /**
     * Trans stmt to Policy.
     **/
    public static Policy fromCreateStmt(CreatePolicyStmt stmt) throws AnalysisException {
        String curDb = stmt.getTableName().getDb();
        if (curDb == null) {
            curDb = ConnectContext.get().getDatabase();
        }
        Database db = Catalog.getCurrentCatalog().getDbOrAnalysisException(curDb);
        Table table = db.getTableOrAnalysisException(stmt.getTableName().getTbl());
        UserIdentity userIdent = stmt.getUser();
        userIdent.analyze(ConnectContext.get().getClusterName());
        return new Policy(db.getId(), table.getId(), stmt.getPolicyName(), stmt.getType(), stmt.getFilterType(),
                stmt.getWherePredicate(), userIdent, stmt.getOrigStmt().originStmt);
    }

    /**
     * Use for SHOW POLICY.
     **/
    public List<String> getShowInfo() throws AnalysisException {
        Database database = Catalog.getCurrentCatalog().getDbOrAnalysisException(this.dbId);
        Table table = database.getTableOrAnalysisException(this.tableId);
        return Lists.newArrayList(this.policyName, database.getFullName(), table.getName(), this.type.name(),
                this.filterType.name(), this.wherePredicate.toSql(), this.user.getQualifiedUser(), this.originStmt);
    }

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

    @Override
    public void gsonPostProcess() throws IOException {
        if (wherePredicate != null) {
            return;
        }
        try {
            SqlScanner input = new SqlScanner(new StringReader(originStmt), 0L);
            SqlParser parser = new SqlParser(input);
            CreatePolicyStmt stmt = (CreatePolicyStmt) SqlParserUtils.getFirstStmt(parser);
            wherePredicate = stmt.getWherePredicate();
        } catch (Exception e) {
            throw new IOException("policy parse originStmt error", e);
        }
    }

    @Override
    public Policy clone() {
        return new Policy(this.dbId, this.tableId, this.policyName, this.type, this.filterType, this.wherePredicate,
                this.user, this.originStmt);
    }
}
