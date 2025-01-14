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
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.system.Backend;
import org.apache.doris.task.AgentClient;
import org.apache.doris.thrift.TCheckStorageFormatResult;

import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * show tablet storage format command
 */
public class ShowTabletStorageFormatCommand extends ShowCommand {
    public static final Logger LOG = LogManager.getLogger(ShowTabletStorageFormatCommand.class);
    private final boolean verbose;

    /**
     * constructor
     */
    public ShowTabletStorageFormatCommand(boolean verbose) {
        super(PlanType.SHOW_TABLET_STORAGE_FORMAT_COMMAND);
        this.verbose = verbose;
    }

    /**
     * get Meta for show
     */
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        if (verbose) {
            builder.addColumn(new Column("BackendId", ScalarType.createVarchar(30)))
                    .addColumn(new Column("TabletId", ScalarType.createVarchar(30)))
                    .addColumn(new Column("StorageFormat", ScalarType.createVarchar(30)));
        } else {
            builder.addColumn(new Column("BackendId", ScalarType.createVarchar(30)))
                    .addColumn(new Column("V1Count", ScalarType.createVarchar(30)))
                    .addColumn(new Column("V2Count", ScalarType.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                    PrivPredicate.ADMIN.getPrivs().toString());
        }

        List<List<String>> resultRowSet = Lists.newArrayList();
        for (Backend be : Env.getCurrentSystemInfo().getAllBackendsByAllCluster().values()) {
            if (be.isQueryAvailable() && be.isLoadAvailable()) {
                AgentClient client = new AgentClient(be.getHost(), be.getBePort());
                TCheckStorageFormatResult result = client.checkStorageFormat();
                if (result == null) {
                    throw new AnalysisException("get tablet data from backend: " + be.getId() + "error.");
                }
                if (verbose) {
                    for (long tabletId : result.getV1Tablets()) {
                        List<String> row = new ArrayList<>();
                        row.add(String.valueOf(be.getId()));
                        row.add(String.valueOf(tabletId));
                        row.add("V1");
                        resultRowSet.add(row);
                    }
                    for (long tabletId : result.getV2Tablets()) {
                        List<String> row = new ArrayList<>();
                        row.add(String.valueOf(be.getId()));
                        row.add(String.valueOf(tabletId));
                        row.add("V2");
                        resultRowSet.add(row);
                    }
                } else {
                    List<String> row = new ArrayList<>();
                    row.add(String.valueOf(be.getId()));
                    row.add(String.valueOf(result.getV1Tablets().size()));
                    row.add(String.valueOf(result.getV2Tablets().size()));
                    resultRowSet.add(row);
                }
            }
        }
        return new ShowResultSet(getMetaData(), resultRowSet);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowTabletStorageFormatCommand(this, context);
    }

    @Override
    protected void checkSupportedInCloudMode(ConnectContext ctx) throws DdlException {
        LOG.info("show tablet storage format not supported in cloud mode");
        throw new DdlException("Unsupported operation");
    }
}
