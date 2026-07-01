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

package org.apache.doris.tablefunction;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.iceberg.IcebergSysExternalTable;
import org.apache.doris.datasource.iceberg.source.IcebergScanNode;
import org.apache.doris.datasource.systable.SysTableResolver;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * Iceberg metadata table valued function.
 * iceberg_meta("table" = "ctl.db.tbl", "query_type" = "position_deletes").
 */
public class IcebergTableValuedFunction extends TableValuedFunctionIf {
    public static final String NAME = "iceberg_meta";
    private static final String TABLE = "table";
    private static final String QUERY_TYPE = "query_type";
    private static final ImmutableSet<String> PROPERTIES_SET = ImmutableSet.of(TABLE, QUERY_TYPE);

    private final TableNameInfo icebergTableName;
    private final String queryType;
    private final IcebergSysExternalTable sysExternalTable;

    public IcebergTableValuedFunction(Map<String, String> params) throws AnalysisException {
        Map<String, String> validParams = Maps.newHashMap();
        for (String key : params.keySet()) {
            if (!PROPERTIES_SET.contains(key.toLowerCase(Locale.ROOT))) {
                throw new AnalysisException("'" + key + "' is invalid property");
            }
            validParams.put(key.toLowerCase(Locale.ROOT), params.get(key));
        }

        String tableName = validParams.get(TABLE);
        String queryTypeString = validParams.get(QUERY_TYPE);
        if (tableName == null || queryTypeString == null) {
            throw new AnalysisException("Invalid iceberg metadata query");
        }
        String[] names = tableName.split("\\.");
        if (names.length != 3) {
            throw new AnalysisException(
                    "The iceberg table name contains the catalogName, databaseName, and tableName");
        }
        this.icebergTableName = new TableNameInfo(names[0], names[1], names[2]);
        this.queryType = queryTypeString.toLowerCase(Locale.ROOT);
        checkTableAuth(ConnectContext.get());

        CatalogIf<?> catalog = Env.getCurrentEnv().getCatalogMgr().getCatalogOrAnalysisException(names[0]);
        DatabaseIf<?> database = catalog.getDbOrAnalysisException(names[1]);
        TableIf table = database.getTableOrAnalysisException(names[2]);
        if (!(table instanceof IcebergExternalTable)) {
            throw new AnalysisException("Iceberg metadata query only supports iceberg external table: " + tableName);
        }

        Optional<SysTableResolver.SysTablePlan> plan = SysTableResolver.resolveForPlan(
                table, names[0], names[1], names[2] + "$" + queryType);
        if (!plan.isPresent() || !plan.get().isNative()
                || !(plan.get().getSysExternalTable() instanceof IcebergSysExternalTable)) {
            throw new AnalysisException("Unsupported iceberg metadata query type: " + queryTypeString);
        }
        this.sysExternalTable = (IcebergSysExternalTable) plan.get().getSysExternalTable();
    }

    @Override
    public String getTableName() {
        return "IcebergMetadataTableValuedFunction";
    }

    @Override
    public List<Column> getTableColumns() {
        return sysExternalTable.getFullSchema();
    }

    @Override
    public ScanNode getScanNode(PlanNodeId id, TupleDescriptor desc, SessionVariable sv) {
        return new IcebergScanNode(id, desc, sysExternalTable, sv,
                ScanContext.builder().clusterName(sv.resolveCloudClusterName()).build());
    }

    @Override
    public void checkAuth(ConnectContext ctx) {
        try {
            checkTableAuth(ctx);
        } catch (AnalysisException e) {
            throw new org.apache.doris.nereids.exceptions.AnalysisException(e.getMessage(), e);
        }
    }

    private void checkTableAuth(ConnectContext ctx) throws AnalysisException {
        if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(ctx, icebergTableName, PrivPredicate.SELECT)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "SELECT",
                    ctx.getQualifiedUser(), ctx.getRemoteIP(),
                    icebergTableName.getDb() + ": " + icebergTableName.getTbl());
        }
    }
}
