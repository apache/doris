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

package org.apache.doris.nereids.trees.plans;

import org.apache.doris.analysis.ExplainOptions;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand.ExplainLevel;

/**
 * Planner for prepare command. Used for PrepareCommand to get output slots metadata.
 * In prepare stage, we only need to do analyze stage of plan to get the output slots metadata.
 * And we skip all the other stages to avoid errors.
 */
public class PrepareCommandPlanner extends NereidsPlanner {
    public PrepareCommandPlanner(StatementContext statementContext) {
        super(statementContext);
    }

    protected ExplainLevel getExplainLevel(ExplainOptions explainOptions) {
        return ExplainLevel.ANALYZED_PLAN;
    }

}
