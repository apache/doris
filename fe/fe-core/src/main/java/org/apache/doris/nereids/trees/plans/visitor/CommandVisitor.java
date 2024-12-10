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
import org.apache.doris.nereids.trees.plans.commands.AdminCheckTabletsCommand;
import org.apache.doris.nereids.trees.plans.commands.AdminCompactTableCommand;
import org.apache.doris.nereids.trees.plans.commands.AdminShowReplicaStatusCommand;
import org.apache.doris.nereids.trees.plans.commands.AlterJobStatusCommand;
import org.apache.doris.nereids.trees.plans.commands.AlterMTMVCommand;
import org.apache.doris.nereids.trees.plans.commands.AlterRoleCommand;
import org.apache.doris.nereids.trees.plans.commands.AlterSqlBlockRuleCommand;
import org.apache.doris.nereids.trees.plans.commands.AlterViewCommand;
import org.apache.doris.nereids.trees.plans.commands.AlterWorkloadGroupCommand;
import org.apache.doris.nereids.trees.plans.commands.AlterWorkloadPolicyCommand;
import org.apache.doris.nereids.trees.plans.commands.CallCommand;
import org.apache.doris.nereids.trees.plans.commands.CancelExportCommand;
import org.apache.doris.nereids.trees.plans.commands.CancelJobTaskCommand;
import org.apache.doris.nereids.trees.plans.commands.CancelLoadCommand;
import org.apache.doris.nereids.trees.plans.commands.CancelMTMVTaskCommand;
import org.apache.doris.nereids.trees.plans.commands.CancelWarmUpJobCommand;
import org.apache.doris.nereids.trees.plans.commands.CleanAllProfileCommand;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.commands.CreateEncryptkeyCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateFileCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateJobCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateMTMVCommand;
import org.apache.doris.nereids.trees.plans.commands.CreatePolicyCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateProcedureCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateRoleCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateSqlBlockRuleCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateTableCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateTableLikeCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateViewCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateWorkloadGroupCommand;
import org.apache.doris.nereids.trees.plans.commands.DeleteFromCommand;
import org.apache.doris.nereids.trees.plans.commands.DeleteFromUsingCommand;
import org.apache.doris.nereids.trees.plans.commands.DropCatalogRecycleBinCommand;
import org.apache.doris.nereids.trees.plans.commands.DropConstraintCommand;
import org.apache.doris.nereids.trees.plans.commands.DropEncryptkeyCommand;
import org.apache.doris.nereids.trees.plans.commands.DropFileCommand;
import org.apache.doris.nereids.trees.plans.commands.DropJobCommand;
import org.apache.doris.nereids.trees.plans.commands.DropMTMVCommand;
import org.apache.doris.nereids.trees.plans.commands.DropProcedureCommand;
import org.apache.doris.nereids.trees.plans.commands.DropRepositoryCommand;
import org.apache.doris.nereids.trees.plans.commands.DropRoleCommand;
import org.apache.doris.nereids.trees.plans.commands.DropSqlBlockRuleCommand;
import org.apache.doris.nereids.trees.plans.commands.DropUserCommand;
import org.apache.doris.nereids.trees.plans.commands.DropWorkloadGroupCommand;
import org.apache.doris.nereids.trees.plans.commands.DropWorkloadPolicyCommand;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand;
import org.apache.doris.nereids.trees.plans.commands.ExportCommand;
import org.apache.doris.nereids.trees.plans.commands.LoadCommand;
import org.apache.doris.nereids.trees.plans.commands.PauseJobCommand;
import org.apache.doris.nereids.trees.plans.commands.PauseMTMVCommand;
import org.apache.doris.nereids.trees.plans.commands.RecoverDatabaseCommand;
import org.apache.doris.nereids.trees.plans.commands.RecoverPartitionCommand;
import org.apache.doris.nereids.trees.plans.commands.RecoverTableCommand;
import org.apache.doris.nereids.trees.plans.commands.RefreshMTMVCommand;
import org.apache.doris.nereids.trees.plans.commands.ReplayCommand;
import org.apache.doris.nereids.trees.plans.commands.ResumeJobCommand;
import org.apache.doris.nereids.trees.plans.commands.ResumeMTMVCommand;
import org.apache.doris.nereids.trees.plans.commands.SetDefaultStorageVaultCommand;
import org.apache.doris.nereids.trees.plans.commands.SetOptionsCommand;
import org.apache.doris.nereids.trees.plans.commands.SetTransactionCommand;
import org.apache.doris.nereids.trees.plans.commands.SetUserPropertiesCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowAuthorsCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowBackendsCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowBrokerCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowCollationCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowConfigCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowConstraintsCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowCreateCatalogCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowCreateDatabaseCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowCreateMTMVCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowCreateMaterializedViewCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowCreateProcedureCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowCreateRepositoryCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowCreateTableCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowCreateViewCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowDataSkewCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowDatabaseIdCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowDeleteCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowDiagnoseTabletCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowDynamicPartitionCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowEncryptKeysCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowEventsCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowFrontendsCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowGrantsCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowLastInsertCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowLoadProfileCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowPartitionIdCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowPluginsCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowPrivilegesCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowProcCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowProcedureStatusCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowProcessListCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowReplicaDistributionCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowRepositoriesCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowRolesCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowSmallFilesCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowSqlBlockRuleCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowStorageEnginesCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowTableCreationCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowTableIdCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowTabletStorageFormatCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowTabletsBelongCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowTrashCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowTriggersCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowVariablesCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowViewCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowWhiteListCommand;
import org.apache.doris.nereids.trees.plans.commands.SyncCommand;
import org.apache.doris.nereids.trees.plans.commands.UnsetDefaultStorageVaultCommand;
import org.apache.doris.nereids.trees.plans.commands.UnsetVariableCommand;
import org.apache.doris.nereids.trees.plans.commands.UnsupportedCommand;
import org.apache.doris.nereids.trees.plans.commands.UpdateCommand;
import org.apache.doris.nereids.trees.plans.commands.insert.BatchInsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertOverwriteTableCommand;
import org.apache.doris.nereids.trees.plans.commands.load.CreateRoutineLoadCommand;
import org.apache.doris.nereids.trees.plans.commands.refresh.RefreshCatalogCommand;
import org.apache.doris.nereids.trees.plans.commands.refresh.RefreshDatabaseCommand;

/** CommandVisitor. */
public interface CommandVisitor<R, C> {

    R visitCommand(Command command, C context);

    default R visitExplainCommand(ExplainCommand explain, C context) {
        return visitCommand(explain, context);
    }

    default R visitReplayCommand(ReplayCommand replay, C context) {
        return visitCommand(replay, context);
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

    default R visitCreateEncryptKeyCommand(CreateEncryptkeyCommand createEncryptKeyCommand, C context) {
        return visitCommand(createEncryptKeyCommand, context);
    }

    default R visitCreateTableCommand(CreateTableCommand createTableCommand, C context) {
        return visitCommand(createTableCommand, context);
    }

    default R visitCreateMTMVCommand(CreateMTMVCommand createMTMVCommand, C context) {
        return visitCommand(createMTMVCommand, context);
    }

    default R visitCreateJobCommand(CreateJobCommand createJobCommand, C context) {
        return visitCommand(createJobCommand, context);
    }

    default R visitCreateFileCommand(CreateFileCommand createFileCommand, C context) {
        return visitCommand(createFileCommand, context);
    }

    default R visitAlterMTMVCommand(AlterMTMVCommand alterMTMVCommand, C context) {
        return visitCommand(alterMTMVCommand, context);
    }

    default R visitAddConstraintCommand(AddConstraintCommand addConstraintCommand, C context) {
        return visitCommand(addConstraintCommand, context);
    }

    default R visitAdminCompactTableCommand(AdminCompactTableCommand adminCompactTableCommand, C context) {
        return visitCommand(adminCompactTableCommand, context);
    }

    default R visitDropConstraintCommand(DropConstraintCommand dropConstraintCommand, C context) {
        return visitCommand(dropConstraintCommand, context);
    }

    default R visitDropJobCommand(DropJobCommand dropJobCommand, C context) {
        return visitCommand(dropJobCommand, context);
    }

    default R visitShowConstraintsCommand(ShowConstraintsCommand showConstraintsCommand, C context) {
        return visitCommand(showConstraintsCommand, context);
    }

    default R visitRefreshMTMVCommand(RefreshMTMVCommand refreshMTMVCommand, C context) {
        return visitCommand(refreshMTMVCommand, context);
    }

    default R visitDropMTMVCommand(DropMTMVCommand dropMTMVCommand, C context) {
        return visitCommand(dropMTMVCommand, context);
    }

    default R visitPauseJobCommand(PauseJobCommand pauseJobCommand, C context) {
        return visitCommand(pauseJobCommand, context);
    }

    default R visitPauseMTMVCommand(PauseMTMVCommand pauseMTMVCommand, C context) {
        return visitCommand(pauseMTMVCommand, context);
    }

    default R visitResumeJobCommand(ResumeJobCommand resumeJobCommand, C context) {
        return visitCommand(resumeJobCommand, context);
    }

    default R visitResumeMTMVCommand(ResumeMTMVCommand resumeMTMVCommand, C context) {
        return visitCommand(resumeMTMVCommand, context);
    }

    default R visitShowCreateMTMVCommand(ShowCreateMTMVCommand showCreateMTMVCommand, C context) {
        return visitCommand(showCreateMTMVCommand, context);
    }

    default R visitCancelLoadCommand(CancelLoadCommand cancelLoadCommand, C context) {
        return visitCommand(cancelLoadCommand, context);
    }

    default R visitCancelExportCommand(CancelExportCommand cancelExportCommand, C context) {
        return visitCommand(cancelExportCommand, context);
    }

    default R visitCancelWarmUpJobCommand(CancelWarmUpJobCommand cancelWarmUpJobCommand, C context) {
        return visitCommand(cancelWarmUpJobCommand, context);
    }

    default R visitCancelMTMVTaskCommand(CancelMTMVTaskCommand cancelMTMVTaskCommand, C context) {
        return visitCommand(cancelMTMVTaskCommand, context);
    }

    default R visitCancelTaskCommand(CancelJobTaskCommand cancelJobTaskCommand, C context) {
        return visitCommand(cancelJobTaskCommand, context);
    }

    default R visitCallCommand(CallCommand callCommand, C context) {
        return visitCommand(callCommand, context);
    }

    default R visitCreateProcedureCommand(CreateProcedureCommand createProcedureCommand, C context) {
        return visitCommand(createProcedureCommand, context);
    }

    default R visitDropProcedureCommand(DropProcedureCommand dropProcedureCommand, C context) {
        return visitCommand(dropProcedureCommand, context);
    }

    default R visitShowProcedureStatusCommand(ShowProcedureStatusCommand showProcedureStatusCommand, C context) {
        return visitCommand(showProcedureStatusCommand, context);
    }

    default R visitShowCreateProcedureCommand(ShowCreateProcedureCommand showCreateProcedureCommand, C context) {
        return visitCommand(showCreateProcedureCommand, context);
    }

    default R visitCreateViewCommand(CreateViewCommand createViewCommand, C context) {
        return visitCommand(createViewCommand, context);
    }

    default R visitAlterJobStatusCommand(AlterJobStatusCommand alterJobStatusCommand, C context) {
        return visitCommand(alterJobStatusCommand, context);
    }

    default R visitAlterViewCommand(AlterViewCommand alterViewCommand, C context) {
        return visitCommand(alterViewCommand, context);
    }

    default R visitDropCatalogRecycleBinCommand(DropCatalogRecycleBinCommand dropCatalogRecycleBinCommand, C context) {
        return visitCommand(dropCatalogRecycleBinCommand, context);
    }

    default R visitUnsupportedCommand(UnsupportedCommand unsupportedCommand, C context) {
        return visitCommand(unsupportedCommand, context);
    }

    default R visitUnsetVariableCommand(UnsetVariableCommand unsetVariableCommand, C context) {
        return visitCommand(unsetVariableCommand, context);
    }

    default R visitUnsetDefaultStorageVaultCommand(UnsetDefaultStorageVaultCommand unsetDefaultStorageVaultCommand,
                                                   C context) {
        return visitCommand(unsetDefaultStorageVaultCommand, context);
    }

    default R visitCreateTableLikeCommand(CreateTableLikeCommand createTableLikeCommand, C context) {
        return visitCommand(createTableLikeCommand, context);
    }

    default R visitShowAuthorsCommand(ShowAuthorsCommand showAuthorsCommand, C context) {
        return visitCommand(showAuthorsCommand, context);
    }

    default R visitShowConfigCommand(ShowConfigCommand showConfigCommand, C context) {
        return visitCommand(showConfigCommand, context);
    }

    default R visitSetOptionsCommand(SetOptionsCommand setOptionsCommand, C context) {
        return visitCommand(setOptionsCommand, context);
    }

    default R visitSetTransactionCommand(SetTransactionCommand setTransactionCommand, C context) {
        return visitCommand(setTransactionCommand, context);
    }

    default R visitSetUserPropertiesCommand(SetUserPropertiesCommand setUserPropertiesCommand, C context) {
        return visitCommand(setUserPropertiesCommand, context);
    }

    default R visitSetDefaultStorageVault(SetDefaultStorageVaultCommand setDefaultStorageVaultCommand, C context) {
        return visitCommand(setDefaultStorageVaultCommand, context);
    }

    default R visitRefreshCatalogCommand(RefreshCatalogCommand refreshCatalogCommand, C context) {
        return visitCommand(refreshCatalogCommand, context);
    }

    default R visitShowCreateRepositoryCommand(ShowCreateRepositoryCommand showCreateRepositoryCommand, C context) {
        return visitCommand(showCreateRepositoryCommand, context);
    }

    default R visitShowLastInsertCommand(ShowLastInsertCommand showLastInsertCommand, C context) {
        return visitCommand(showLastInsertCommand, context);
    }

    default R visitShowGrantsCommand(ShowGrantsCommand showGrantsCommand, C context) {
        return visitCommand(showGrantsCommand, context);
    }

    default R visitShowPartitionIdCommand(ShowPartitionIdCommand showPartitionIdCommand, C context) {
        return visitCommand(showPartitionIdCommand, context);
    }

    default R visitShowVariablesCommand(ShowVariablesCommand showVariablesCommand, C context) {
        return visitCommand(showVariablesCommand, context);
    }

    default R visitShowViewCommand(ShowViewCommand showViewCommand, C context) {
        return visitCommand(showViewCommand, context);
    }

    default R visitRefreshDatabaseCommand(RefreshDatabaseCommand refreshDatabaseCommand, C context) {
        return visitCommand(refreshDatabaseCommand, context);
    }

    default R visitShowBackendsCommand(ShowBackendsCommand showBackendsCommand, C context) {
        return visitCommand(showBackendsCommand, context);
    }

    default R visitShowCreateTableCommand(ShowCreateTableCommand showCreateTableCommand, C context) {
        return visitCommand(showCreateTableCommand, context);
    }

    default R visitShowSmallFilesCommand(ShowSmallFilesCommand showSmallFilesCommand, C context) {
        return visitCommand(showSmallFilesCommand, context);
    }

    default R visitShowSqlBlockRuleCommand(ShowSqlBlockRuleCommand showblockruleCommand, C context) {
        return visitCommand(showblockruleCommand, context);
    }

    default R visitShowPluginsCommand(ShowPluginsCommand showPluginsCommand, C context) {
        return visitCommand(showPluginsCommand, context);
    }

    default R visitShowTrashCommand(ShowTrashCommand showTrashCommand, C context) {
        return visitCommand(showTrashCommand, context);
    }

    default R visitShowTriggersCommand(ShowTriggersCommand showTriggersCommand, C context) {
        return visitCommand(showTriggersCommand, context);
    }

    default R visitAdminShowReplicaStatusCommand(AdminShowReplicaStatusCommand adminShowReplicaStatusCommand,
                                                    C context) {
        return visitCommand(adminShowReplicaStatusCommand, context);
    }

    default R visitShowRepositoriesCommand(ShowRepositoriesCommand showRepositoriesCommand, C context) {
        return visitCommand(showRepositoriesCommand, context);
    }

    default R visitShowRolesCommand(ShowRolesCommand showRolesCommand, C context) {
        return visitCommand(showRolesCommand, context);
    }

    default R visitShowProcCommand(ShowProcCommand showProcCommand, C context) {
        return visitCommand(showProcCommand, context);
    }

    default R visitShowStorageEnginesCommand(ShowStorageEnginesCommand showStorageEnginesCommand, C context) {
        return visitCommand(showStorageEnginesCommand, context);
    }

    default R visitShowCreateCatalogCommand(ShowCreateCatalogCommand showCreateCatalogCommand, C context) {
        return visitCommand(showCreateCatalogCommand, context);
    }

    default R visitShowCreateMaterializedViewCommand(ShowCreateMaterializedViewCommand showCreateMtlzViewCommand,
                        C context) {
        return visitCommand(showCreateMtlzViewCommand, context);
    }

    default R visitShowCreateDatabaseCommand(ShowCreateDatabaseCommand showCreateDatabaseCommand, C context) {
        return visitCommand(showCreateDatabaseCommand, context);
    }

    default R visitShowCreateViewCommand(ShowCreateViewCommand showCreateViewCommand, C context) {
        return visitCommand(showCreateViewCommand, context);
    }

    default R visitAlterRoleCommand(AlterRoleCommand alterRoleCommand, C context) {
        return visitCommand(alterRoleCommand, context);
    }

    default R visitShowDatabaseIdCommand(ShowDatabaseIdCommand showDatabaseIdCommand, C context) {
        return visitCommand(showDatabaseIdCommand, context);
    }

    default R visitAlterWorkloadGroupCommand(AlterWorkloadGroupCommand alterWorkloadGroupCommand, C context) {
        return visitCommand(alterWorkloadGroupCommand, context);
    }

    default R visitAlterWorkloadPolicyCommand(AlterWorkloadPolicyCommand alterWorkloadPolicyCommand, C context) {
        return visitCommand(alterWorkloadPolicyCommand, context);
    }

    default R visitCleanAllProfileCommand(CleanAllProfileCommand cleanAllProfileCommand, C context) {
        return visitCommand(cleanAllProfileCommand, context);
    }

    default R visitShowFrontendsCommand(ShowFrontendsCommand showFrontendsCommand, C context) {
        return visitCommand(showFrontendsCommand, context);
    }

    default R visitShowDynamicPartitionCommand(ShowDynamicPartitionCommand showDynamicPartitionCommand, C context) {
        return visitCommand(showDynamicPartitionCommand, context);
    }

    default R visitShowWhiteListCommand(ShowWhiteListCommand whiteListCommand, C context) {
        return visitCommand(whiteListCommand, context);
    }

    default R visitRecoverDatabaseCommand(RecoverDatabaseCommand recoverDatabaseCommand, C context) {
        return visitCommand(recoverDatabaseCommand, context);
    }

    default R visitShowDiagnoseTabletCommand(ShowDiagnoseTabletCommand showDiagnoseTabletCommand, C context) {
        return visitCommand(showDiagnoseTabletCommand, context);
    }

    default R visitRecoverTableCommand(RecoverTableCommand recoverTableCommand, C context) {
        return visitCommand(recoverTableCommand, context);
    }

    default R visitRecoverPartitionCommand(RecoverPartitionCommand recoverPartitionCommand, C context) {
        return visitCommand(recoverPartitionCommand, context);
    }

    default R visitShowBrokerCommand(ShowBrokerCommand showBrokerCommand, C context) {
        return visitCommand(showBrokerCommand, context);
    }

    default R visitShowLoadProfileCommand(ShowLoadProfileCommand showLoadProfileCommand, C context) {
        return visitCommand(showLoadProfileCommand, context);
    }

    default R visitAlterSqlBlockRuleCommand(AlterSqlBlockRuleCommand cmd, C context) {
        return visitCommand(cmd, context);
    }

    default R visitCreateSqlBlockRuleCommand(CreateSqlBlockRuleCommand cmd, C context) {
        return visitCommand(cmd, context);
    }

    default R visitDropRepositoryCommand(DropRepositoryCommand cmd, C context) {
        return visitCommand(cmd, context);
    }

    default R visitCreateRoleCommand(CreateRoleCommand createRoleCommand, C context) {
        return visitCommand(createRoleCommand, context);
    }

    default R visitDropRoleCommand(DropRoleCommand dropRoleCommand, C context) {
        return visitCommand(dropRoleCommand, context);
    }

    default R visitDropEncryptKeyCommand(DropEncryptkeyCommand dropEncryptkeyCommand, C context) {
        return visitCommand(dropEncryptkeyCommand, context);
    }

    default R visitDropFileCommand(DropFileCommand dropFileCommand, C context) {
        return visitCommand(dropFileCommand, context);
    }

    default R visitDropSqlBlockRuleCommand(DropSqlBlockRuleCommand dropSqlBlockRuleCommand, C context) {
        return visitCommand(dropSqlBlockRuleCommand, context);
    }

    default R visitDropUserCommand(DropUserCommand dropUserCommand, C context) {
        return visitCommand(dropUserCommand, context);
    }

    default R visitDropWorkloadGroupCommand(DropWorkloadGroupCommand dropWorkloadGroupCommand, C context) {
        return visitCommand(dropWorkloadGroupCommand, context);
    }

    default R visitShowReplicaDistributionCommand(ShowReplicaDistributionCommand showReplicaDistributedCommand,
                                                    C context) {
        return visitCommand(showReplicaDistributedCommand, context);
    }

    default R visitDropWorkloadPolicyCommand(DropWorkloadPolicyCommand dropWorkloadPolicyCommand, C context) {
        return visitCommand(dropWorkloadPolicyCommand, context);
    }

    default R visitShowTableIdCommand(ShowTableIdCommand showTableIdCommand, C context) {
        return visitCommand(showTableIdCommand, context);
    }

    default R visitCreateWorkloadGroupCommand(CreateWorkloadGroupCommand createWorkloadGroupCommand, C context) {
        return visitCommand(createWorkloadGroupCommand, context);
    }

    default R visitShowEncryptKeysCommand(ShowEncryptKeysCommand showEncryptKeysCommand, C context) {
        return visitCommand(showEncryptKeysCommand, context);
    }

    default R visitSyncCommand(SyncCommand syncCommand, C context) {
        return visitCommand(syncCommand, context);
    }

    default R visitShowEventsCommand(ShowEventsCommand showEventsCommand, C context) {
        return visitCommand(showEventsCommand, context);
    }

    default R visitShowDeleteCommand(ShowDeleteCommand showDeleteCommand, C context) {
        return visitCommand(showDeleteCommand, context);
    }

    default R visitShowPrivilegesCommand(ShowPrivilegesCommand showPrivilegesCommand, C context) {
        return visitCommand(showPrivilegesCommand, context);
    }

    default R visitShowTabletsBelongCommand(ShowTabletsBelongCommand showTabletBelongCommand, C context) {
        return visitCommand(showTabletBelongCommand, context);
    }

    default R visitShowCollationCommand(ShowCollationCommand showCollationCommand, C context) {
        return visitCommand(showCollationCommand, context);
    }

    default R visitCreateRoutineLoadCommand(CreateRoutineLoadCommand createRoutineLoadCommand, C context) {
        return visitCommand(createRoutineLoadCommand, context);
    }

    default R visitShowProcessListCommand(ShowProcessListCommand showProcessListCommand, C context) {
        return visitCommand(showProcessListCommand, context);
    }

    default R visitAdminCheckTabletsCommand(AdminCheckTabletsCommand adminCheckTabletsCommand, C context) {
        return visitCommand(adminCheckTabletsCommand, context);
    }

    default R visitShowDataSkewCommand(ShowDataSkewCommand showDataSkewCommand, C context) {
        return visitCommand(showDataSkewCommand, context);
    }

    default R visitShowTableCreationCommand(ShowTableCreationCommand showTableCreationCommand, C context) {
        return visitCommand(showTableCreationCommand, context);
    }

    default R visitShowTabletStorageFormatCommand(ShowTabletStorageFormatCommand showTabletStorageFormatCommand,
                                                  C context) {
        return visitCommand(showTabletStorageFormatCommand, context);
    }
}
