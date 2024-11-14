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

package org.apache.doris.nereids.trees.plans.commands.load;

import org.apache.doris.analysis.CreateRoutineLoadStmt;
import org.apache.doris.catalog.Env;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.commands.NoForward;
import org.apache.doris.nereids.trees.plans.commands.info.CreateRoutineLoadInfo;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import java.util.Objects;

/**
 Create routine Load statement,  continually load data from a streaming app

 syntax:
      CREATE ROUTINE LOAD [database.]name on table
      [load properties]
      [PROPERTIES
      (
          desired_concurrent_number = xxx,
          max_error_number = xxx,
          k1 = v1,
          ...
          kn = vn
      )]
      FROM type of routine load
      [(
          k1 = v1,
          ...
          kn = vn
      )]

      load properties:
          load property [[,] load property] ...

      load property:
          column separator | columns_mapping | partitions | where

      column separator:
          COLUMNS TERMINATED BY xxx
      columns_mapping:
          COLUMNS (c1, c2, c3 = c1 + c2)
      partitions:
          PARTITIONS (p1, p2, p3)
      where:
          WHERE c1 > 1

      type of routine load:
          KAFKA
*/
public class CreateRoutineLoadCommand extends Command implements NoForward {
    CreateRoutineLoadInfo createRoutineLoadInfo;

    public CreateRoutineLoadCommand(CreateRoutineLoadInfo createRoutineLoadInfo) {
        super(PlanType.CREATE_ROUTINE_LOAD_COMMAND);
        this.createRoutineLoadInfo = Objects.requireNonNull(createRoutineLoadInfo, "require CreateTableInfo object");
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        createRoutineLoadInfo.validate(ctx);
        CreateRoutineLoadStmt createRoutineLoadStmt = createRoutineLoadInfo.translateToLegacyStmt(ctx);
        Env.getCurrentEnv().getRoutineLoadManager().createRoutineLoadJob(createRoutineLoadStmt);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCreateRoutineLoadCommand(this, context);
    }
}
