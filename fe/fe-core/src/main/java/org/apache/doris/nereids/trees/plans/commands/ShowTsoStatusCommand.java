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

import org.apache.doris.analysis.RedirectStatus;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.tso.TSOService;
import org.apache.doris.tso.TSOTimestamp;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Command for SHOW TSO STATUS.
 */
public class ShowTsoStatusCommand extends ShowCommand implements ForwardNoSync {
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("window_end_physical_time",
                            ScalarType.createType(PrimitiveType.BIGINT)))
                    .addColumn(new Column("current_tso", ScalarType.createType(PrimitiveType.BIGINT)))
                    .addColumn(new Column("current_tso_physical_time",
                            ScalarType.createType(PrimitiveType.BIGINT)))
                    .addColumn(new Column("current_tso_logical_counter",
                            ScalarType.createType(PrimitiveType.BIGINT)))
                    .build();

    public ShowTsoStatusCommand() {
        super(PlanType.SHOW_TSO_STATUS_COMMAND);
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws AnalysisException {
        if (!Config.enable_feature_binlog) {
            throw new AnalysisException("TSO feature is disabled, please check enable_feature_binlog");
        }

        TSOService tsoService = Env.getCurrentEnv().getTSOService();
        TSOService.TSOStatusSnapshot statusSnapshot = tsoService.getStatusSnapshot();
        if (!statusSnapshot.isInitialized()) {
            throw new AnalysisException("TSO timestamp is not calibrated, please check");
        }

        long currentTso = statusSnapshot.getCurrentTso();
        List<List<String>> rows = ImmutableList.of(ImmutableList.of(
                String.valueOf(statusSnapshot.getWindowEndPhysicalTime()),
                String.valueOf(currentTso),
                String.valueOf(TSOTimestamp.extractPhysicalTime(currentTso)),
                String.valueOf(TSOTimestamp.extractLogicalCounter(currentTso))));
        return new ShowResultSet(getMetaData(), rows);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowTsoStatusCommand(this, context);
    }

    @Override
    public RedirectStatus toRedirectStatus() {
        return RedirectStatus.FORWARD_NO_SYNC;
    }
}
