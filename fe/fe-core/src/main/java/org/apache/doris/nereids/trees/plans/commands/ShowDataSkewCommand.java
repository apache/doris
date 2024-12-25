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

import org.apache.doris.analysis.PartitionNames;
import org.apache.doris.analysis.RedirectStatus;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MetadataViewer;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.util.Util;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.TableRefInfo;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * show table id command
 */
public class ShowDataSkewCommand extends ShowCommand {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("PartitionName").add("BucketIdx").add("AvgRowCount").add("AvgDataSize")
            .add("Graph").add("Percent")
            .build();

    private final TableRefInfo tableRefInfo;

    /**
     * constructor
     */
    public ShowDataSkewCommand(TableRefInfo tableRefInfo) {
        super(PlanType.SHOW_DATA_SKEW_COMMAND);
        this.tableRefInfo = tableRefInfo;
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        tableRefInfo.analyze(ctx);
        // disallow external catalog
        Util.prohibitExternalCatalog(tableRefInfo.getTableNameInfo().getCtl(), this.getClass().getSimpleName());
        if (!Env.getCurrentEnv().getAccessManager()
                .checkTblPriv(ctx, tableRefInfo.getTableNameInfo().getCtl(),
                    tableRefInfo.getTableNameInfo().getDb(), tableRefInfo.getTableNameInfo().getTbl(),
                    PrivPredicate.SHOW)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "SHOW DATA SKEW",
                    ctx.getQualifiedUser(),
                    ctx.getRemoteIP(),
                    tableRefInfo.getTableNameInfo().getDb() + "." + tableRefInfo.getTableNameInfo().getTbl());
        }
        try {
            tableRefInfo.analyze(ctx);
            Util.prohibitExternalCatalog(tableRefInfo.getTableNameInfo().getCtl(), this.getClass().getSimpleName());

            PartitionNames partitionNames = (tableRefInfo.getPartitionNamesInfo() != null)
                     ? tableRefInfo.getPartitionNamesInfo().translateToLegacyPartitionNames() : null;
            List<List<String>> results = MetadataViewer.getDataSkew(tableRefInfo.getTableNameInfo().getDb(),
                    tableRefInfo.getTableNameInfo().getTbl(), partitionNames);
            return new ShowResultSet(getMetaData(), results);
        } catch (DdlException e) {
            throw new AnalysisException(e.getMessage());
        }

    }

    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowDataSkewCommand(this, context);
    }

    @Override
    public RedirectStatus toRedirectStatus() {
        return RedirectStatus.FORWARD_NO_SYNC;
    }
}
