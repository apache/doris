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
import org.apache.doris.common.util.SqlParserUtils;

import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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

    @SerializedName(value = "tableId")
    private long tableId;

    /**
     * PERMISSIVE | RESTRICTIVE, If multiple types exist, the last type prevails.
     **/
    @SerializedName(value = "filterType")
    private final FilterType filterType;

    private Expr wherePredicate;

    public TablePolicy(final PolicyTypeEnum type, final String policyName, long dbId,
                  UserIdentity user, String originStmt, final long tableId,
                  final FilterType filterType, final Expr wherePredicate) {
        super(type, policyName, dbId, user, originStmt);
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

    @Override
    public boolean matchPolicy(Policy policy) {
        if (!(policy instanceof TablePolicy)) {
            return false;
        }
        TablePolicy tablePolicy = (TablePolicy) policy;
        return tablePolicy.getTableId() == tableId
                && tablePolicy.getType().equals(type)
                && StringUtils.equals(tablePolicy.getPolicyName(), policyName)
                && (tablePolicy.getUser() == null || user == null
                        || StringUtils.equals(tablePolicy.getUser().getQualifiedUser(), user.getQualifiedUser()));
    }

    @Override
    public boolean matchPolicy(DropPolicyLog dropPolicyLog) {
        return dropPolicyLog.getTableId() == tableId
            && dropPolicyLog.getType().equals(type)
            && StringUtils.equals(dropPolicyLog.getPolicyName(), policyName)
            && (dropPolicyLog.getUser() == null || user == null
            || StringUtils.equals(dropPolicyLog.getUser().getQualifiedUser(), user.getQualifiedUser()));
    }

    @Override
    public boolean isInvalid() {
        return (wherePredicate == null);
    }
}
