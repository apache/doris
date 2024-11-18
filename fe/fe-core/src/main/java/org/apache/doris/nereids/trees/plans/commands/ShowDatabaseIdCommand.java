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
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * show database id command
 */
public class ShowDatabaseIdCommand extends ShowCommand{
  public static final Logger LOG = LogManager.getLogger(ShowFrontendsCommand.class);
  private final Long dbId;

  /**
   * constructor
   */
  public ShowDatabaseIdCommand(Long dbId) {
    super(PlanType.SHOW_DATABASE_ID_COMMAND);
    this.dbId = dbId;
  }

  @Override
  public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
      List<List<String>> rows = Lists.newArrayList();
      DatabaseIf database = ctx.getCurrentCatalog().getDbNullable(dbId);
      if (database != null) {
          List<String> row = new ArrayList<>();
          row.add(database.getFullName());
          rows.add(row);
      }
      ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
      builder.addColumn(new Column("DBName", ScalarType.createVarchar(30)));
      return new ShowResultSet(builder.build(), rows);
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
      return visitor.visitShowDatabaseIdCommand(this, context);
  }
}
