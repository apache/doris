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
 * Represents the command for SHOW CHARSETG
 */
public class ShowCharsetCommand extends ShowCommand {
    private static final ShowResultSetMetaData CHARSET_META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Charset", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Description", ScalarType.createVarchar(60)))
                    .addColumn(new Column("Default collation", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Maxlen", ScalarType.createVarchar(10)))
                    .build();

    public ShowCharsetCommand() {
        super(PlanType.SHOW_CHARSET_COMMAND);
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return CHARSET_META_DATA;
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        List<List<String>> rows = Lists.newArrayList();
        List<String> row = Lists.newArrayList();
        // | utf8mb4 | UTF-8 Unicode | utf8mb4_general_ci | 4|
        row.add(ctx.getSessionVariable().getCharsetServer());
        row.add("UTF-8 Unicode");
        row.add(ctx.getSessionVariable().getCollationConnection());
        row.add("4");
        rows.add(row);

        return new ShowResultSet(getMetaData(), rows);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowCharsetCommand(this, context);
    }
}
