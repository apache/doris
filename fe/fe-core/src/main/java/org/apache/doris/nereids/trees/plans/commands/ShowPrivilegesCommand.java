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
import org.apache.doris.mysql.privilege.Privilege;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * show privileges command
 */
public class ShowPrivilegesCommand extends ShowCommand {
    private static final ShowResultSetMetaData META_DATA;

    static {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();

        builder.addColumn(new Column("Privilege", ScalarType.createVarchar(100)));
        builder.addColumn(new Column("Context", ScalarType.createVarchar(100)));
        builder.addColumn(new Column("Comment", ScalarType.createVarchar(100)));

        META_DATA = builder.build();
    }

    /**
     * constructor
     */

    public ShowPrivilegesCommand() {
        super(PlanType.SHOW_PRIVILEGES_COMMAND);
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        List<List<String>> infos = Lists.newArrayList();
        Privilege[] values = Privilege.values();
        for (Privilege privilege : values) {
            if (!privilege.isDeprecated()) {
                infos.add(Lists.newArrayList(privilege.getName(), privilege.getContext(), privilege.getDesc()));
            }
        }
        return new ShowResultSet(META_DATA, infos);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowPrivilegesCommand(this, context);
    }
}
