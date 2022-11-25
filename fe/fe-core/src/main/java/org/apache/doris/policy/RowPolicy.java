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
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.SqlParserUtils;
import org.apache.doris.qe.ShowResultSetMetaData;

import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import lombok.Data;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.StringReader;
import java.util.List;

/**
 * Save policy for filtering data.
 **/
@Data
public class RowPolicy extends Policy {

    public static final ShowResultSetMetaData ROW_META_DATA =
            ShowResultSetMetaData.builder()
                .addColumn(new Column("PolicyName", ScalarType.createVarchar(100)))
                .addColumn(new Column("DbName", ScalarType.createVarchar(100)))
                .addColumn(new Column("TableName", ScalarType.createVarchar(100)))
                .addColumn(new Column("Type", ScalarType.createVarchar(20)))
                .addColumn(new Column("FilterType", ScalarType.createVarchar(20)))
                .addColumn(new Column("WherePredicate", ScalarType.createVarchar(65535)))
                .addColumn(new Column("User", ScalarType.createVarchar(20)))
                .addColumn(new Column("OriginStmt", ScalarType.createVarchar(65535)))
                .build();

    private static final Logger LOG = LogManager.getLogger(RowPolicy.class);

    /**
     * Policy bind user.
     **/
    @SerializedName(value = "user")
    private UserIdentity user = null;

    @SerializedName(value = "dbId")
    private long dbId = -1;

    @SerializedName(value = "tableId")
    private long tableId = -1;

    /**
     * PERMISSIVE | RESTRICTIVE, If multiple types exist, the last type prevails.
     **/
    @SerializedName(value = "filterType")
    private FilterType filterType = null;

    /**
     * Use for Serialization/deserialization.
     **/
    @SerializedName(value = "originStmt")
    private String originStmt;

    private Expr wherePredicate = null;

    public RowPolicy() {
        super(PolicyTypeEnum.ROW);
    }

    /**
     * Policy for Table. Policy of ROW or others.
     *
     * @param policyId policy id
     * @param policyName policy name
     * @param dbId database i
     * @param user username
     * @param originStmt origin stmt
     * @param tableId table id
     * @param filterType filter type
     * @param wherePredicate where predicate
     */
    public RowPolicy(long policyId, final String policyName, long dbId, UserIdentity user, String originStmt,
            final long tableId, final FilterType filterType, final Expr wherePredicate) {
        super(policyId, PolicyTypeEnum.ROW, policyName);
        this.user = user;
        this.dbId = dbId;
        this.tableId = tableId;
        this.filterType = filterType;
        this.originStmt = originStmt;
        this.wherePredicate = wherePredicate;
    }

    /**
     * Use for SHOW POLICY.
     **/
    public List<String> getShowInfo() throws AnalysisException {
        Database database = Env.getCurrentInternalCatalog().getDbOrAnalysisException(this.dbId);
        Table table = database.getTableOrAnalysisException(this.tableId);
        return Lists.newArrayList(this.policyName, database.getFullName(), table.getName(), this.type.name(),
                this.filterType.name(), this.wherePredicate.toSql(), this.user.getQualifiedUser(), this.originStmt);
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
            throw new IOException("table policy parse originStmt error", e);
        }
    }

    @Override
    public RowPolicy clone() {
        return new RowPolicy(this.policyId, this.policyName, this.dbId, this.user, this.originStmt, this.tableId,
                this.filterType, this.wherePredicate);
    }

    private boolean checkMatched(long dbId, long tableId, PolicyTypeEnum type,
                                 String policyName, UserIdentity user) {
        return super.checkMatched(type, policyName)
                && (dbId == -1 || dbId == this.dbId)
                && (tableId == -1 || tableId == this.tableId)
                && (user == null || this.user == null
                        || StringUtils.equals(user.getQualifiedUser(), this.user.getQualifiedUser()));
    }

    @Override
    public boolean matchPolicy(Policy checkedPolicyCondition) {
        if (!(checkedPolicyCondition instanceof RowPolicy)) {
            return false;
        }
        RowPolicy rowPolicy = (RowPolicy) checkedPolicyCondition;
        return checkMatched(rowPolicy.getDbId(), rowPolicy.getTableId(), rowPolicy.getType(),
                            rowPolicy.getPolicyName(), rowPolicy.getUser());
    }

    @Override
    public boolean matchPolicy(DropPolicyLog checkedDropPolicyLogCondition) {
        return checkMatched(checkedDropPolicyLogCondition.getDbId(), checkedDropPolicyLogCondition.getTableId(),
                            checkedDropPolicyLogCondition.getType(), checkedDropPolicyLogCondition.getPolicyName(),
                            checkedDropPolicyLogCondition.getUser());
    }

    @Override
    public boolean isInvalid() {
        return (wherePredicate == null);
    }
}
