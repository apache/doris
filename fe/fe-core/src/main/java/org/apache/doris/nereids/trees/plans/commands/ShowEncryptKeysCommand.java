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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.EncryptKey;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.util.ListComparator;
import org.apache.doris.common.util.OrderByPair;
import org.apache.doris.common.util.Util;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Represents the command for SHOW ENCRYPTKEYS.
 */
public class ShowEncryptKeysCommand extends ShowCommand {
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("EncryptKey Name", ScalarType.createVarchar(20)))
                    .addColumn(new Column("EncryptKey String", ScalarType.createVarchar(1024)))
                    .build();

    private String dbName;
    private final String likeString;

    public ShowEncryptKeysCommand(String databaseName, String likeString) {
        super(PlanType.SHOW_ENCRYPT_KEYS_COMMAND);
        this.dbName = databaseName;
        this.likeString = likeString;
    }

    private void validate(ConnectContext ctx) throws AnalysisException {
        if (Strings.isNullOrEmpty(dbName)) {
            dbName = ctx.getDatabase();
            if (Strings.isNullOrEmpty(dbName)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }
        }

        // check auth
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                    PrivPredicate.ADMIN.getPrivs().toString());
        }
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
        Util.prohibitExternalCatalog(ctx.getDefaultCatalog(), this.getClass().getSimpleName());
        DatabaseIf db = ctx.getCurrentCatalog().getDbOrAnalysisException(dbName);
        List<List<String>> resultRowSet = Lists.newArrayList();
        if (db instanceof Database) {
            List<EncryptKey> encryptKeys = ((Database) db).getEncryptKeys();
            List<List<Comparable>> rowSet = Lists.newArrayList();
            for (EncryptKey encryptKey : encryptKeys) {
                List<Comparable> row = encryptKey.getInfo();
                // like predicate
                if (likeString == null || like(encryptKey.getEncryptKeyName().getKeyName())) {
                    rowSet.add(row);
                }

            }

            // sort function rows by first column asc
            ListComparator<List<Comparable>> comparator = null;
            OrderByPair orderByPair = new OrderByPair(0, false);
            comparator = new ListComparator<>(orderByPair);
            Collections.sort(rowSet, comparator);

            Set<String> encryptKeyNameSet = new HashSet<>();
            for (List<Comparable> row : rowSet) {
                List<String> resultRow = Lists.newArrayList();
                for (Comparable column : row) {
                    resultRow.add(column.toString());
                }
                resultRowSet.add(resultRow);
                encryptKeyNameSet.add(resultRow.get(0));
            }
        }

        return new ShowResultSet(META_DATA, resultRowSet);
    }

    private boolean like(String str) {
        str = str.toLowerCase();
        return str.matches(likeString.replace(".", "\\.").replace("?", ".").replace("%", ".*").toLowerCase());
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowEncryptKeysCommand(this, context);
    }

    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }
}

