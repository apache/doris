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

import org.apache.doris.nereids.trees.plans.commands.AddConstraintCommand;
import org.apache.doris.nereids.trees.plans.commands.AlterMTMVCommand;
import org.apache.doris.nereids.trees.plans.commands.BatchInsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.commands.CallCommand;
import org.apache.doris.nereids.trees.plans.commands.CancelMTMVTaskCommand;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.commands.CreateMTMVCommand;
import org.apache.doris.nereids.trees.plans.commands.CreatePolicyCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateTableCommand;
import org.apache.doris.nereids.trees.plans.commands.DeleteFromCommand;
import org.apache.doris.nereids.trees.plans.commands.DeleteFromUsingCommand;
import org.apache.doris.nereids.trees.plans.commands.DropConstraintCommand;
import org.apache.doris.nereids.trees.plans.commands.DropMTMVCommand;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand;
import org.apache.doris.nereids.trees.plans.commands.ExportCommand;
import org.apache.doris.nereids.trees.plans.commands.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.commands.InsertOverwriteTableCommand;
import org.apache.doris.nereids.trees.plans.commands.LoadCommand;
import org.apache.doris.nereids.trees.plans.commands.PauseMTMVCommand;
import org.apache.doris.nereids.trees.plans.commands.RefreshMTMVCommand;
import org.apache.doris.nereids.trees.plans.commands.ResumeMTMVCommand;
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

    default R visitInsertIntoTableCommand(InsertIntoTableCommand insertIntoTableCommand,
            C context) {
        return visitCommand(insertIntoTableCommand, context);
    }

    default R visitInsertOverwriteTableCommand(InsertOverwriteTableCommand insertOverwriteTableCommand,
            C context) {
        return visitCommand(insertOverwriteTableCommand, context);
    }

    default R visitBatchInsertIntoTableCommand(BatchInsertIntoTableCommand batchInsertIntoTableCommand,
            C context) {
        return visitCommand(batchInsertIntoTableCommand, context);
    }

    default R visitUpdateCommand(UpdateCommand updateCommand, C context) {
        return visitCommand(updateCommand, context);
    }

    default R visitDeleteFromCommand(DeleteFromCommand deleteFromCommand, C context) {
        return visitCommand(deleteFromCommand, context);
    }

    default R visitDeleteFromUsingCommand(DeleteFromUsingCommand deleteFromUsingCommand, C context) {
        return visitCommand(deleteFromUsingCommand, context);
    }

    default R visitLoadCommand(LoadCommand loadCommand, C context) {
        return visitCommand(loadCommand, context);
    }

    default R visitExportCommand(ExportCommand exportCommand, C context) {
        return visitCommand(exportCommand, context);
    }

    default R visitCreateTableCommand(CreateTableCommand createTableCommand, C context) {
        return visitCommand(createTableCommand, context);
    }

    default R visitCreateMTMVCommand(CreateMTMVCommand createMTMVCommand, C context) {
        return visitCommand(createMTMVCommand, context);
    }

    default R visitAlterMTMVCommand(AlterMTMVCommand alterMTMVCommand, C context) {
        return visitCommand(alterMTMVCommand, context);
    }

    default R visitAddConstraintCommand(AddConstraintCommand addConstraintCommand, C context) {
        return visitCommand(addConstraintCommand, context);
    }

    default R visitDropConstraintCommand(DropConstraintCommand dropConstraintCommand, C context) {
        return visitCommand(dropConstraintCommand, context);
    }

    default R visitRefreshMTMVCommand(RefreshMTMVCommand refreshMTMVCommand, C context) {
        return visitCommand(refreshMTMVCommand, context);
    }

    default R visitDropMTMVCommand(DropMTMVCommand dropMTMVCommand, C context) {
        return visitCommand(dropMTMVCommand, context);
    }

    default R visitPauseMTMVCommand(PauseMTMVCommand pauseMTMVCommand, C context) {
        return visitCommand(pauseMTMVCommand, context);
    }

    default R visitResumeMTMVCommand(ResumeMTMVCommand resumeMTMVCommand, C context) {
        return visitCommand(resumeMTMVCommand, context);
    }

    default R visitCancelMTMVTaskCommand(CancelMTMVTaskCommand cancelMTMVTaskCommand, C context) {
        return visitCommand(cancelMTMVTaskCommand, context);
    }

    default R visitCallCommand(CallCommand callCommand, C context) {
        return visitCommand(callCommand, context);
    }
}
