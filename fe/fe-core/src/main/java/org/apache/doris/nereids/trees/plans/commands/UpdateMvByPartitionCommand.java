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

import org.apache.doris.nereids.trees.plans.commands.insert.InsertOverwriteTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Optional;

/**
 * Update mv by partition
 */
public class UpdateMvByPartitionCommand extends InsertOverwriteTableCommand {
    private static final Logger LOG = LogManager.getLogger(UpdateMvByPartitionCommand.class);

    public UpdateMvByPartitionCommand(LogicalPlan logicalQuery) {
        super(logicalQuery, Optional.empty(), Optional.empty(), Optional.empty());
    }

    @Override
    public boolean isForceDropPartition() {
        // After refreshing the data in MTMV, it will be synchronized with the base table
        // and there is no need to put it in the recycle bin
        return true;
    }
}
