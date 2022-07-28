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

import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

/**
 * explain command.
 */
public class ExplainCommand implements Command {

    /**
     * explain level.
     */
    public enum ExplainLevel {
        NORMAL,
        VERBOSE,
        GRAPH,
        ;
    }

    private final ExplainLevel level;
    private final LogicalPlan logicalPlan;

    public ExplainCommand(ExplainLevel level, LogicalPlan logicalPlan) {
        this.level = level;
        this.logicalPlan = logicalPlan;
    }

    public ExplainLevel getLevel() {
        return level;
    }

    public LogicalPlan getLogicalPlan() {
        return logicalPlan;
    }
}
