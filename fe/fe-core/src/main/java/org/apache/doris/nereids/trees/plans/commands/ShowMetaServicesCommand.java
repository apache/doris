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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.doris.analysis.RedirectStatus;
import org.apache.doris.common.Column;
import org.apache.doris.common.ScalarType;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

/**
 * Represents the command for SHOW META-SERVICES in cloud mode.
 */
public class ShowMetaServicesCommand extends ShowCommand {
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Host", ScalarType.createVarchar(50)))
                    .addColumn(new Column("Port", ScalarType.createVarchar(10)))
                    .addColumn(new Column("Role", ScalarType.createVarchar(30)))
                    .addColumn(new Column("Recycler", ScalarType.createVarchar(30)))
                    .build();

    /**
     * Constructs a new ShowMetaServicesCommand.
     */
    public ShowMetaServicesCommand() {
        super(PlanType.SHOW_META_SERVICES_COMMAND);
    }

    /**
     * Executes the SHOW META-SERVICES command and returns the result set.
     *
     * @param ctx ConnectContext for the query execution
     * @param executor StmtExecutor for the query execution
     * @return ShowResultSet containing meta-services information
     * @throws Exception if execution fails
     */
    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        List<List<String>> rows = new ArrayList<>();
        // Simulated data, to be replaced with ServiceRegistryPB read
        // (key 0x02 "system" "meta-service" "registry")
        rows.add(Arrays.asList("127.0.0.1", "5000", "meta-service", "role_recycler"));
        return new ShowResultSet(META_DATA, rows);
    }

    /**
     * Accepts a PlanVisitor to process this command.
     *
     * @param visitor PlanVisitor instance
     * @param context Visitor context
     * @return Result of the visitor
     */
    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowMetaServicesCommand(this, context);
    }

    /**
     * Returns the redirect status for this command.
     *
     * @return RedirectStatus.NO_FORWARD
     */
    @Override
    public RedirectStatus toRedirectStatus() {
        return RedirectStatus.NO_FORWARD;
    }

    /**
     * Returns the metadata for the result set.
     *
     * @return ShowResultSetMetaData for the command
     */
    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }
}

