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

package org.apache.doris.nereids.trees.plans.visitor;

import org.apache.doris.nereids.trees.plans.commands.AlterMTMVCommand;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.commands.CreateMTMVCommand;
import org.apache.doris.nereids.trees.plans.commands.CreatePolicyCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateTableCommand;
import org.apache.doris.nereids.trees.plans.commands.DeleteCommand;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand;
import org.apache.doris.nereids.trees.plans.commands.ExportCommand;
import org.apache.doris.nereids.trees.plans.commands.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.commands.RefreshMTMVCommand;
import org.apache.doris.nereids.trees.plans.commands.LoadCommand;
import org.apache.doris.nereids.trees.plans.commands.UpdateCommand;

/** CommandVisitor. */
public interface CommandVisitor<R, C> {

    R visitCommand(Command command, C context);

    default R visitExplainCommand(ExplainCommand explain, C context) {
        return visitCommand(explain, context);
    }

    default R visitCreatePolicyCommand(CreatePolicyCommand createPolicy, C context) {
        return visitCommand(createPolicy, context);
    }

    default R visitInsertIntoTableCommand(InsertIntoTableCommand insertIntoSelectCommand,
            C context) {
        return visitCommand(insertIntoSelectCommand, context);
    }

    default R visitUpdateCommand(UpdateCommand updateCommand, C context) {
        return visitCommand(updateCommand, context);
    }

    default R visitDeleteCommand(DeleteCommand deleteCommand, C context) {
        return visitCommand(deleteCommand, context);
    }

    default R visitLoadCommand(LoadCommand loadCommand, C context) {
        return visitCommand(loadCommand, context);
    }

    default R visitExportCommand(ExportCommand exportCommand, C context) {
        return visitCommand(exportCommand, context);
    }

    default R visitCreateMTMVCommand(CreateMTMVCommand createMTMVCommand, C context) {
        return visitCommand(createMTMVCommand, context);
    }

    default R visitAlterMTMVCommand(AlterMTMVCommand alterMTMVCommand, C context) {
        return visitCommand(alterMTMVCommand, context);
    }

    default R visitRefreshMTMVCommand(RefreshMTMVCommand refreshMTMVCommand, C context) {
        return visitCommand(refreshMTMVCommand, context);
    }

    default R visitCreateTableCommand(CreateTableCommand createTableCommand, C context) {
        return visitCommand(createTableCommand, context);
    }
}
