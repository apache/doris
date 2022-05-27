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
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.SqlParser;
import org.apache.doris.analysis.SqlScanner;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.util.SqlParserUtils;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.IOException;
import java.io.StringReader;
import java.util.List;

import lombok.Data;

/**
 * Save policy for filtering data.
 **/
@Data
public class TablePolicy extends Policy {

    private static final Logger LOG = LogManager.getLogger(TablePolicy.class);

    /**
     * Policy bind user.
     **/
    @SerializedName(value = "user")
    private UserIdentity user = null;

    @SerializedName(value = "dbId")
    protected long dbId = -1;

    @SerializedName(value = "tableId")
    private long tableId = -1;

    /**
     * PERMISSIVE | RESTRICTIVE, If multiple types exist, the last type prevails.
     **/
    @SerializedName(value = "filterType")
    private FilterType filterType = null;

    private Expr wherePredicate = null;

    public TablePolicy() {}

    public TablePolicy(final PolicyTypeEnum type, final String policyName, long dbId,
                  UserIdentity user, String originStmt, final long tableId,
                  final FilterType filterType, final Expr wherePredicate) {
        super(type, policyName, originStmt);
        this.user = user;
        this.dbId = dbId;
        this.tableId = tableId;
        this.filterType = filterType;
        this.wherePredicate = wherePredicate;
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

    /**
     * Read Table Policy from file.
     **/
    public static TablePolicy read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, TablePolicy.class);
    }

    @Override
    public void gsonPostProcess() throws IOException {
        if (wherePredicate != null) {
            return;
        }
        try {
            SqlScanner input = new SqlScanner(new StringReader(originStmt), 0L);
            SqlParser parser = new SqlParser(input);
            CreateTablePolicyStmt stmt = (CreateTablePolicyStmt) SqlParserUtils.getFirstStmt(parser);
            wherePredicate = stmt.getWherePredicate();
        } catch (Exception e) {
            throw new IOException("table policy parse originStmt error", e);
        }
    }

    @Override
    public TablePolicy clone() {
        return new TablePolicy(this.type, this.policyName, this.dbId, this.user, this.originStmt, this.tableId,
                               this.filterType, this.wherePredicate);
    }

    private boolean checkMatched(long dbId, long tableId, PolicyTypeEnum type, String policyName, UserIdentity user) {
        return super.checkMatched(type, policyName)
                && (dbId == -1 || dbId == this.dbId)
                && (tableId == -1 || tableId == this.tableId)
                && (user == null || this.user == null
                        || StringUtils.equals(user.getQualifiedUser(), this.user.getQualifiedUser()));
    }

    @Override
    public boolean matchPolicy(Policy policy) {
        if (!(policy instanceof TablePolicy)) {
            return false;
        }
        TablePolicy tablePolicy = (TablePolicy) policy;
        return checkMatched(tablePolicy.getDbId(), tablePolicy.getTableId(), tablePolicy.getType(),
                            tablePolicy.getPolicyName(), tablePolicy.getUser());
    }

    @Override
    public boolean matchPolicy(DropPolicyLog dropPolicyLog) {
        return checkMatched(dropPolicyLog.getDbId(), dropPolicyLog.getTableId(), dropPolicyLog.getType(),
                            dropPolicyLog.getPolicyName(), dropPolicyLog.getUser());
    }

    @Override
    public boolean isInvalid() {
        return (wherePredicate == null);
    }
}
