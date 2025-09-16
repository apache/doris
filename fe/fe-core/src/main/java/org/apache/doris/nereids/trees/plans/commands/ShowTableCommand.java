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
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.InfoSchemaDb;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.CaseSensibility;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.PatternMatcher;
import org.apache.doris.common.PatternMatcherWrapper;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.AliasInfo;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.GlobalVariable;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * ShowTableCommand
 */
public class ShowTableCommand extends ShowCommand {
    private static final String NAME_COL_PREFIX = "Tables_in_";
    private static final String TYPE_COL = "Table_type";
    private static final String STORAGE_FORMAT_COL = "Storage_format";
    private static final String INVERTED_INDEX_STORAGE_FORMAT_COL = "Inverted_index_storage_format";
    private String db;
    private String catalog;
    private final boolean isVerbose;
    private final String likePattern;
    private String whereClause;

    public ShowTableCommand(String db, String catalog, boolean isVerbose, PlanType planType) {
        this(db, catalog, isVerbose, null, null, planType);
    }

    /**
     * ShowTableCommand
     */
    public ShowTableCommand(String db, String catalog, boolean isVerbose,
            String likePattern, String whereClause, PlanType planType) {
        super(planType);
        this.catalog = catalog;
        this.db = db;
        this.isVerbose = isVerbose;
        this.likePattern = likePattern;
        this.whereClause = whereClause;
    }

    /**
     * validate
     */
    public void validate(ConnectContext ctx) throws AnalysisException {
        if (Strings.isNullOrEmpty(db)) {
            db = ctx.getDatabase();
            if (Strings.isNullOrEmpty(db)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }
        }
        if (Strings.isNullOrEmpty(catalog)) {
            catalog = ctx.getDefaultCatalog();
            if (Strings.isNullOrEmpty(catalog)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_WRONG_NAME_FOR_CATALOG);
            }
        }

        // we do not check db privs here. because user may not have any db privs,
        // but if it has privs of tbls inside this db,it should be allowed to see this db.
    }

    /**
     * isShowTablesCaseSensitive
     */
    public boolean isShowTablesCaseSensitive() {
        if (GlobalVariable.lowerCaseTableNames == 0) {
            return CaseSensibility.TABLE.getCaseSensibility();
        }
        return false;
    }

    private ShowResultSet executeWhere(ConnectContext ctx, StmtExecutor executor)
            throws AnalysisException {
        List<AliasInfo> selectList = new ArrayList<>();
        selectList.add(AliasInfo.of("TABLE_NAME",
                NAME_COL_PREFIX + ClusterNamespace.getNameFromFullName(db)));
        if (isVerbose) {
            selectList.add(AliasInfo.of("TABLE_TYPE", TYPE_COL));
        }

        TableNameInfo fullTblName = new TableNameInfo(catalog, InfoSchemaDb.DATABASE_NAME, "tables");

        if (type.equals(PlanType.SHOW_VIEWS)) {
            whereClause = whereClause + " and `ENGINE` = '" + TableIf.TableType.VIEW.toEngineName() + "'";
        }

        // We need to use TABLE_SCHEMA as a condition to query When querying external catalogs.
        // This also applies to the internal catalog.
        LogicalPlan plan = Utils.buildLogicalPlan(selectList, fullTblName,
                whereClause + " and `TABLE_SCHEMA` = '" + db + "'");
        List<List<String>> rows = Utils.executePlan(ctx, executor, plan);
        return new ShowResultSet(getMetaData(), rows);
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
        if (whereClause != null) {
            return executeWhere(ctx, executor);
        }
        List<List<String>> rows = Lists.newArrayList();
        DatabaseIf<TableIf> dbIf = ctx.getEnv().getCatalogMgr()
                .getCatalogOrAnalysisException(catalog)
                .getDbOrAnalysisException(db);
        PatternMatcher matcher = null;
        if (likePattern != null) {
            matcher = PatternMatcherWrapper.createMysqlPattern(likePattern, isShowTablesCaseSensitive());
        }
        for (TableIf tbl : dbIf.getTables()) {
            if (type.equals(PlanType.SHOW_VIEWS) && !tbl.getEngine().equals(TableIf.TableType.VIEW.toEngineName())) {
                continue;
            }
            if (matcher != null && !matcher.match(tbl.getName())) {
                continue;
            }
            if (tbl.isTemporary()) {
                continue;
            }
            // check tbl privs
            if (!Env.getCurrentEnv().getAccessManager()
                    .checkTblPriv(ConnectContext.get(), catalog, dbIf.getFullName(), tbl.getName(),
                            PrivPredicate.SHOW)) {
                continue;
            }
            if (isVerbose) {
                String storageFormat = "NONE";
                String invertedIndexFileStorageFormat = "NONE";
                if (tbl instanceof OlapTable) {
                    storageFormat = ((OlapTable) tbl).getStorageFormat().toString();
                    invertedIndexFileStorageFormat = ((OlapTable) tbl).getInvertedIndexFileStorageFormat().toString();
                }
                rows.add(Lists.newArrayList(tbl.getName(), tbl.getMysqlType(), storageFormat,
                        invertedIndexFileStorageFormat));
            } else {
                rows.add(Lists.newArrayList(tbl.getName()));
            }
        }
        // sort by table name
        rows.sort(Comparator.comparing(x -> x.get(0)));
        return new ShowResultSet(getMetaData(), rows);
    }

    /**
     * getMetaData
     */
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        builder.addColumn(
                new Column(NAME_COL_PREFIX + ClusterNamespace.getNameFromFullName(db), ScalarType.createVarchar(20)));
        if (isVerbose) {
            builder.addColumn(new Column(TYPE_COL, ScalarType.createVarchar(20)));
            // TODO: using where can only show two columns, maybe this is a bug?
            if (whereClause == null) {
                builder.addColumn(new Column(STORAGE_FORMAT_COL, ScalarType.createVarchar(20)));
                builder.addColumn(new Column(INVERTED_INDEX_STORAGE_FORMAT_COL, ScalarType.createVarchar(20)));
            }
        }
        return builder.build();
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowTableCommand(this, context);
    }
}
