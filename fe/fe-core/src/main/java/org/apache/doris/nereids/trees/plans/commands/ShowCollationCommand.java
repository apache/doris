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
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * Represents the command for SHOW COLLATION
 */
public class ShowCollationCommand extends ShowCommand {
    private static final ShowResultSetMetaData COLLATION_META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Collation", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Charset", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Id", ScalarType.createVarchar(10)))
                    .addColumn(new Column("Default", ScalarType.createVarchar(10)))
                    .addColumn(new Column("Compiled", ScalarType.createVarchar(10)))
                    .addColumn(new Column("Sortlen", ScalarType.createVarchar(10)))
                    .build();

    private final String wild;

    public ShowCollationCommand(String wild) {
        super(PlanType.SHOW_COLLATION_COMMAND);
        this.wild = wild;
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        List<List<String>> rows = Lists.newArrayList();
        List<String> utf8mb40900Bin = Lists.newArrayList();
        // | utf8mb4_0900_bin | utf8mb4 | 309 | Yes | Yes | 1 |
        utf8mb40900Bin.add(ctx.getSessionVariable().getCollationConnection());
        utf8mb40900Bin.add(ctx.getSessionVariable().getCharsetServer());
        utf8mb40900Bin.add("309");
        utf8mb40900Bin.add("Yes");
        utf8mb40900Bin.add("Yes");
        utf8mb40900Bin.add("1");
        rows.add(utf8mb40900Bin);
        // ATTN: we must have this collation for compatible with some bi tools
        List<String> utf8mb3GeneralCi = Lists.newArrayList();
        // | utf8mb3_general_ci | utf8mb3 | 33 | Yes | Yes | 1 |
        utf8mb3GeneralCi.add("utf8mb3_general_ci");
        utf8mb3GeneralCi.add("utf8mb3");
        utf8mb3GeneralCi.add("33");
        utf8mb3GeneralCi.add("Yes");
        utf8mb3GeneralCi.add("Yes");
        utf8mb3GeneralCi.add("1");
        rows.add(utf8mb3GeneralCi);
        // Set the result set and send it using the executor
        return new ShowResultSet(COLLATION_META_DATA, rows);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowCollationCommand(this, context);
    }

    @Override
    public String toString() {
        return "SHOW COLLATION" + (wild != null ? " LIKE '" + wild + "'" : "");
    }
}
