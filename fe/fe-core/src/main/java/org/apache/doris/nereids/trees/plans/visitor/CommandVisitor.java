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
import org.apache.doris.nereids.trees.plans.commands.AdminCancelRebalanceDiskCommand;
import org.apache.doris.nereids.trees.plans.commands.AdminCancelRepairTableCommand;
import org.apache.doris.nereids.trees.plans.commands.AdminCheckTabletsCommand;
import org.apache.doris.nereids.trees.plans.commands.AdminCleanTrashCommand;
import org.apache.doris.nereids.trees.plans.commands.AdminCompactTableCommand;
import org.apache.doris.nereids.trees.plans.commands.AdminCopyTabletCommand;
import org.apache.doris.nereids.trees.plans.commands.AdminRebalanceDiskCommand;
import org.apache.doris.nereids.trees.plans.commands.AdminRepairTableCommand;
import org.apache.doris.nereids.trees.plans.commands.AdminSetFrontendConfigCommand;
import org.apache.doris.nereids.trees.plans.commands.AdminSetPartitionVersionCommand;
import org.apache.doris.nereids.trees.plans.commands.AdminSetReplicaStatusCommand;
import org.apache.doris.nereids.trees.plans.commands.AdminSetReplicaVersionCommand;
import org.apache.doris.nereids.trees.plans.commands.AdminSetTableStatusCommand;
import org.apache.doris.nereids.trees.plans.commands.AlterCatalogCommentCommand;
import org.apache.doris.nereids.trees.plans.commands.AlterCatalogPropertiesCommand;
import org.apache.doris.nereids.trees.plans.commands.AlterCatalogRenameCommand;
import org.apache.doris.nereids.trees.plans.commands.AlterColocateGroupCommand;
import org.apache.doris.nereids.trees.plans.commands.AlterColumnStatsCommand;
import org.apache.doris.nereids.trees.plans.commands.AlterDatabasePropertiesCommand;
import org.apache.doris.nereids.trees.plans.commands.AlterJobStatusCommand;
import org.apache.doris.nereids.trees.plans.commands.AlterMTMVCommand;
import org.apache.doris.nereids.trees.plans.commands.AlterResourceCommand;
import org.apache.doris.nereids.trees.plans.commands.AlterRoleCommand;
import org.apache.doris.nereids.trees.plans.commands.AlterRoutineLoadCommand;
import org.apache.doris.nereids.trees.plans.commands.AlterSqlBlockRuleCommand;
import org.apache.doris.nereids.trees.plans.commands.AlterStoragePolicyCommand;
import org.apache.doris.nereids.trees.plans.commands.AlterTableCommand;
import org.apache.doris.nereids.trees.plans.commands.AlterTableStatsCommand;
import org.apache.doris.nereids.trees.plans.commands.AlterUserCommand;
import org.apache.doris.nereids.trees.plans.commands.AlterViewCommand;
import org.apache.doris.nereids.trees.plans.commands.AlterWorkloadGroupCommand;
import org.apache.doris.nereids.trees.plans.commands.AlterWorkloadPolicyCommand;
import org.apache.doris.nereids.trees.plans.commands.BackupCommand;
import org.apache.doris.nereids.trees.plans.commands.CallCommand;
import org.apache.doris.nereids.trees.plans.commands.CancelAlterTableCommand;
import org.apache.doris.nereids.trees.plans.commands.CancelBackupCommand;
import org.apache.doris.nereids.trees.plans.commands.CancelBuildIndexCommand;
import org.apache.doris.nereids.trees.plans.commands.CancelDecommissionBackendCommand;
import org.apache.doris.nereids.trees.plans.commands.CancelExportCommand;
import org.apache.doris.nereids.trees.plans.commands.CancelJobTaskCommand;
import org.apache.doris.nereids.trees.plans.commands.CancelLoadCommand;
import org.apache.doris.nereids.trees.plans.commands.CancelMTMVTaskCommand;
import org.apache.doris.nereids.trees.plans.commands.CancelWarmUpJobCommand;
import org.apache.doris.nereids.trees.plans.commands.CleanAllProfileCommand;
import org.apache.doris.nereids.trees.plans.commands.CleanQueryStatsCommand;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.commands.CopyIntoCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateCatalogCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateDatabaseCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateDictionaryCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateEncryptkeyCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateFileCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateFunctionCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateIndexAnalyzerCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateIndexTokenFilterCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateIndexTokenizerCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateJobCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateMTMVCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateMaterializedViewCommand;
import org.apache.doris.nereids.trees.plans.commands.CreatePolicyCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateProcedureCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateRepositoryCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateResourceCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateRoleCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateSqlBlockRuleCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateStageCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateStorageVaultCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateTableCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateTableLikeCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateUserCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateViewCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateWorkloadGroupCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateWorkloadPolicyCommand;
import org.apache.doris.nereids.trees.plans.commands.DeleteFromCommand;
import org.apache.doris.nereids.trees.plans.commands.DeleteFromUsingCommand;
import org.apache.doris.nereids.trees.plans.commands.DescribeCommand;
import org.apache.doris.nereids.trees.plans.commands.DropAnalyzeJobCommand;
import org.apache.doris.nereids.trees.plans.commands.DropCachedStatsCommand;
import org.apache.doris.nereids.trees.plans.commands.DropCatalogCommand;
import org.apache.doris.nereids.trees.plans.commands.DropCatalogRecycleBinCommand;
import org.apache.doris.nereids.trees.plans.commands.DropConstraintCommand;
import org.apache.doris.nereids.trees.plans.commands.DropDatabaseCommand;
import org.apache.doris.nereids.trees.plans.commands.DropDictionaryCommand;
import org.apache.doris.nereids.trees.plans.commands.DropEncryptkeyCommand;
import org.apache.doris.nereids.trees.plans.commands.DropExpiredStatsCommand;
import org.apache.doris.nereids.trees.plans.commands.DropFileCommand;
import org.apache.doris.nereids.trees.plans.commands.DropFunctionCommand;
import org.apache.doris.nereids.trees.plans.commands.DropIndexAnalyzerCommand;
import org.apache.doris.nereids.trees.plans.commands.DropIndexTokenFilterCommand;
import org.apache.doris.nereids.trees.plans.commands.DropIndexTokenizerCommand;
import org.apache.doris.nereids.trees.plans.commands.DropJobCommand;
import org.apache.doris.nereids.trees.plans.commands.DropMTMVCommand;
import org.apache.doris.nereids.trees.plans.commands.DropMaterializedViewCommand;
import org.apache.doris.nereids.trees.plans.commands.DropProcedureCommand;
import org.apache.doris.nereids.trees.plans.commands.DropRepositoryCommand;
import org.apache.doris.nereids.trees.plans.commands.DropResourceCommand;
import org.apache.doris.nereids.trees.plans.commands.DropRoleCommand;
import org.apache.doris.nereids.trees.plans.commands.DropRowPolicyCommand;
import org.apache.doris.nereids.trees.plans.commands.DropSqlBlockRuleCommand;
import org.apache.doris.nereids.trees.plans.commands.DropStageCommand;
import org.apache.doris.nereids.trees.plans.commands.DropStatsCommand;
import org.apache.doris.nereids.trees.plans.commands.DropStoragePolicyCommand;
import org.apache.doris.nereids.trees.plans.commands.DropTableCommand;
import org.apache.doris.nereids.trees.plans.commands.DropUserCommand;
import org.apache.doris.nereids.trees.plans.commands.DropViewCommand;
import org.apache.doris.nereids.trees.plans.commands.DropWorkloadGroupCommand;
import org.apache.doris.nereids.trees.plans.commands.DropWorkloadPolicyCommand;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand;
import org.apache.doris.nereids.trees.plans.commands.ExplainDictionaryCommand;
import org.apache.doris.nereids.trees.plans.commands.ExportCommand;
import org.apache.doris.nereids.trees.plans.commands.GrantResourcePrivilegeCommand;
import org.apache.doris.nereids.trees.plans.commands.GrantRoleCommand;
import org.apache.doris.nereids.trees.plans.commands.GrantTablePrivilegeCommand;
import org.apache.doris.nereids.trees.plans.commands.HelpCommand;
import org.apache.doris.nereids.trees.plans.commands.InstallPluginCommand;
import org.apache.doris.nereids.trees.plans.commands.KillAnalyzeJobCommand;
import org.apache.doris.nereids.trees.plans.commands.KillConnectionCommand;
import org.apache.doris.nereids.trees.plans.commands.KillQueryCommand;
import org.apache.doris.nereids.trees.plans.commands.LoadCommand;
import org.apache.doris.nereids.trees.plans.commands.LockTablesCommand;
import org.apache.doris.nereids.trees.plans.commands.PauseJobCommand;
import org.apache.doris.nereids.trees.plans.commands.PauseMTMVCommand;
import org.apache.doris.nereids.trees.plans.commands.RecoverDatabaseCommand;
import org.apache.doris.nereids.trees.plans.commands.RecoverPartitionCommand;
import org.apache.doris.nereids.trees.plans.commands.RecoverTableCommand;
import org.apache.doris.nereids.trees.plans.commands.RefreshMTMVCommand;
import org.apache.doris.nereids.trees.plans.commands.ReplayCommand;
import org.apache.doris.nereids.trees.plans.commands.RestoreCommand;
import org.apache.doris.nereids.trees.plans.commands.ResumeJobCommand;
import org.apache.doris.nereids.trees.plans.commands.ResumeMTMVCommand;
import org.apache.doris.nereids.trees.plans.commands.RevokeResourcePrivilegeCommand;
import org.apache.doris.nereids.trees.plans.commands.RevokeRoleCommand;
import org.apache.doris.nereids.trees.plans.commands.RevokeTablePrivilegeCommand;
import org.apache.doris.nereids.trees.plans.commands.SetDefaultStorageVaultCommand;
import org.apache.doris.nereids.trees.plans.commands.SetOptionsCommand;
import org.apache.doris.nereids.trees.plans.commands.SetTransactionCommand;
import org.apache.doris.nereids.trees.plans.commands.SetUserPropertiesCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowAlterTableCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowAnalyzeCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowAnalyzeTaskCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowAuthorsCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowBackendsCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowBackupCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowBrokerCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowBuildIndexCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowCatalogCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowCatalogRecycleBinCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowCharsetCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowClustersCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowCollationCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowColumnHistogramStatsCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowColumnStatsCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowColumnsCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowConfigCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowConstraintsCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowConvertLSCCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowCopyCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowCreateCatalogCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowCreateDatabaseCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowCreateFunctionCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowCreateLoadCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowCreateMTMVCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowCreateMaterializedViewCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowCreateProcedureCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowCreateRepositoryCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowCreateStorageVaultCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowCreateTableCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowCreateUserCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowCreateViewCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowDataCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowDataSkewCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowDataTypesCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowDatabaseIdCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowDatabasesCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowDeleteCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowDiagnoseTabletCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowDictionariesCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowDynamicPartitionCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowEncryptKeysCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowEventsCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowExportCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowFrontendsCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowFunctionsCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowGrantsCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowIndexAnalyzerCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowIndexCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowIndexStatsCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowIndexTokenFilterCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowIndexTokenizerCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowLastInsertCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowLoadCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowLoadProfileCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowLoadWarningsCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowOpenTablesCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowPartitionIdCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowPartitionsCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowPluginsCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowPrivilegesCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowProcCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowProcedureStatusCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowProcessListCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowQueryProfileCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowQueryStatsCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowQueuedAnalyzeJobsCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowReplicaDistributionCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowReplicaStatusCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowRepositoriesCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowResourcesCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowRestoreCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowRolesCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowRoutineLoadCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowRoutineLoadTaskCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowRowPolicyCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowSmallFilesCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowSnapshotCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowSqlBlockRuleCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowStagesCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowStatusCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowStorageEnginesCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowStoragePolicyCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowStorageVaultCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowTableCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowTableCreationCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowTableIdCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowTableStatsCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowTableStatusCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowTabletIdCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowTabletStorageFormatCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowTabletsBelongCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowTabletsFromTableCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowTransactionCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowTrashCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowTriggersCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowTypeCastCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowUserPropertyCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowVariablesCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowViewCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowWarmUpCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowWarningErrorCountCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowWarningErrorsCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowWhiteListCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowWorkloadGroupsCommand;
import org.apache.doris.nereids.trees.plans.commands.StartTransactionCommand;
import org.apache.doris.nereids.trees.plans.commands.SyncCommand;
import org.apache.doris.nereids.trees.plans.commands.TransactionBeginCommand;
import org.apache.doris.nereids.trees.plans.commands.TransactionCommitCommand;
import org.apache.doris.nereids.trees.plans.commands.TransactionRollbackCommand;
import org.apache.doris.nereids.trees.plans.commands.TruncateTableCommand;
import org.apache.doris.nereids.trees.plans.commands.UninstallPluginCommand;
import org.apache.doris.nereids.trees.plans.commands.UnlockTablesCommand;
import org.apache.doris.nereids.trees.plans.commands.UnsetDefaultStorageVaultCommand;
import org.apache.doris.nereids.trees.plans.commands.UnsetVariableCommand;
import org.apache.doris.nereids.trees.plans.commands.UnsupportedCommand;
import org.apache.doris.nereids.trees.plans.commands.UpdateCommand;
import org.apache.doris.nereids.trees.plans.commands.WarmUpClusterCommand;
import org.apache.doris.nereids.trees.plans.commands.alter.AlterDatabaseRenameCommand;
import org.apache.doris.nereids.trees.plans.commands.alter.AlterDatabaseSetQuotaCommand;
import org.apache.doris.nereids.trees.plans.commands.alter.AlterRepositoryCommand;
import org.apache.doris.nereids.trees.plans.commands.clean.CleanLabelCommand;
import org.apache.doris.nereids.trees.plans.commands.insert.BatchInsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertOverwriteTableCommand;
import org.apache.doris.nereids.trees.plans.commands.load.CreateRoutineLoadCommand;
import org.apache.doris.nereids.trees.plans.commands.load.MysqlLoadCommand;
import org.apache.doris.nereids.trees.plans.commands.load.PauseRoutineLoadCommand;
import org.apache.doris.nereids.trees.plans.commands.load.ResumeRoutineLoadCommand;
import org.apache.doris.nereids.trees.plans.commands.load.ShowCreateRoutineLoadCommand;
import org.apache.doris.nereids.trees.plans.commands.load.StopRoutineLoadCommand;
import org.apache.doris.nereids.trees.plans.commands.refresh.RefreshCatalogCommand;
import org.apache.doris.nereids.trees.plans.commands.refresh.RefreshDatabaseCommand;
import org.apache.doris.nereids.trees.plans.commands.refresh.RefreshDictionaryCommand;
import org.apache.doris.nereids.trees.plans.commands.refresh.RefreshLdapCommand;
import org.apache.doris.nereids.trees.plans.commands.refresh.RefreshTableCommand;
import org.apache.doris.nereids.trees.plans.commands.use.SwitchCommand;
import org.apache.doris.nereids.trees.plans.commands.use.UseCloudClusterCommand;
import org.apache.doris.nereids.trees.plans.commands.use.UseCommand;

/** CommandVisitor. */
public interface CommandVisitor<R, C> {

    R visitCommand(Command command, C context);

    default R visitExplainCommand(ExplainCommand explain, C context) {
        return visitCommand(explain, context);
    }

    default R visitExplainDictionaryCommand(ExplainDictionaryCommand explainDictionary, C context) {
        return visitCommand(explainDictionary, context);
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

    default R visitCopyIntoCommand(CopyIntoCommand copyIntoCommand, C context) {
        return visitCommand(copyIntoCommand, context);
    }

    default R visitCreateDictionaryCommand(CreateDictionaryCommand createDictionaryCommand, C context) {
        return visitCommand(createDictionaryCommand, context);
    }

    default R visitCreateEncryptKeyCommand(CreateEncryptkeyCommand createEncryptKeyCommand, C context) {
        return visitCommand(createEncryptKeyCommand, context);
    }

    default R visitCreateFunctionCommand(CreateFunctionCommand createFunctionCommand, C context) {
        return visitCommand(createFunctionCommand, context);
    }

    default R visitDropFunctionCommand(DropFunctionCommand dropFunctionCommand, C context) {
        return visitCommand(dropFunctionCommand, context);
    }

    default R visitCreateTableCommand(CreateTableCommand createTableCommand, C context) {
        return visitCommand(createTableCommand, context);
    }

    default R visitCreateMTMVCommand(CreateMTMVCommand createMTMVCommand, C context) {
        return visitCommand(createMTMVCommand, context);
    }

    default R visitCreateMaterializedViewCommand(CreateMaterializedViewCommand createSyncMVCommand, C context) {
        return visitCommand(createSyncMVCommand, context);
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

    default R visitAdminCleanTrashCommand(AdminCleanTrashCommand adminCleanTrashCommand, C context) {
        return visitCommand(adminCleanTrashCommand, context);
    }

    default R visitAdminSetTableStatusCommand(AdminSetTableStatusCommand cmd, C context) {
        return visitCommand(cmd, context);
    }

    default R visitDropConstraintCommand(DropConstraintCommand dropConstraintCommand, C context) {
        return visitCommand(dropConstraintCommand, context);
    }

    default R visitDropDictionaryCommand(DropDictionaryCommand dropDictionaryCommand, C context) {
        return visitCommand(dropDictionaryCommand, context);
    }

    default R visitAlterResourceCommand(AlterResourceCommand alterResourceCommand, C context) {
        return visitCommand(alterResourceCommand, context);
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

    default R visitShowStorageVaultCommand(ShowStorageVaultCommand showStorageVaultCommand, C context) {
        return visitCommand(showStorageVaultCommand, context);
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

    default R visitShowWarningErrorCountCommand(ShowWarningErrorCountCommand showWarnErrorCountCommand, C context) {
        return visitCommand(showWarnErrorCountCommand, context);
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

    default R visitCreateCatalogCommand(CreateCatalogCommand createCatalogCommand, C context) {
        return visitCommand(createCatalogCommand, context);
    }

    default R visitShowWarningErrorsCommand(ShowWarningErrorsCommand showWarningErrorsCommand, C context) {
        return visitCommand(showWarningErrorsCommand, context);
    }

    default R visitShowCreateProcedureCommand(ShowCreateProcedureCommand showCreateProcedureCommand, C context) {
        return visitCommand(showCreateProcedureCommand, context);
    }

    default R visitHelpCommand(HelpCommand helpCommand, C context) {
        return visitCommand(helpCommand, context);
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

    default R visitShowColumnsCommand(ShowColumnsCommand showColumnsCommand, C context) {
        return visitCommand(showColumnsCommand, context);
    }

    default R visitDropCatalogCommand(DropCatalogCommand dropCatalogCommand, C context) {
        return visitCommand(dropCatalogCommand, context);
    }

    default R visitAlterCatalogCommentCommand(AlterCatalogCommentCommand alterCatalogCommentCommand, C context) {
        return visitCommand(alterCatalogCommentCommand, context);
    }

    default R visitDropCatalogRecycleBinCommand(DropCatalogRecycleBinCommand dropCatalogRecycleBinCommand, C context) {
        return visitCommand(dropCatalogRecycleBinCommand, context);
    }

    default R visitShowStagesCommand(ShowStagesCommand showStagesCommand, C context) {
        return visitCommand(showStagesCommand, context);
    }

    default R visitUnsupportedCommand(UnsupportedCommand unsupportedCommand, C context) {
        return visitCommand(unsupportedCommand, context);
    }

    default R visitUnsupportedStartTransactionCommand(StartTransactionCommand unsupportedStartTransactionCommand,
                                                      C context) {
        return visitCommand(unsupportedStartTransactionCommand, context);
    }

    default R visitShowAnalyzeTaskCommand(ShowAnalyzeTaskCommand showAnalyzeTaskCommand, C context) {
        return visitCommand(showAnalyzeTaskCommand, context);
    }

    default R visitUnsetVariableCommand(UnsetVariableCommand unsetVariableCommand, C context) {
        return visitCommand(unsetVariableCommand, context);
    }

    default R visitUnsetDefaultStorageVaultCommand(UnsetDefaultStorageVaultCommand unsetDefaultStorageVaultCommand,
                                                   C context) {
        return visitCommand(unsetDefaultStorageVaultCommand, context);
    }

    default R visitCreateDatabaseCommand(CreateDatabaseCommand command, C context) {
        return visitCommand(command, context);
    }

    default R visitCreateRepositoryCommand(CreateRepositoryCommand command, C context) {
        return visitCommand(command, context);
    }

    default R visitCreateStorageVaultCommand(CreateStorageVaultCommand createStorageVaultCommand, C context) {
        return visitCommand(createStorageVaultCommand, context);
    }

    default R visitCreateTableLikeCommand(CreateTableLikeCommand createTableLikeCommand, C context) {
        return visitCommand(createTableLikeCommand, context);
    }

    default R visitShowAuthorsCommand(ShowAuthorsCommand showAuthorsCommand, C context) {
        return visitCommand(showAuthorsCommand, context);
    }

    default R visitShowAlterTableCommand(ShowAlterTableCommand showAlterTableCommand, C context) {
        return visitCommand(showAlterTableCommand, context);
    }

    default R visitShowDictionariesCommand(ShowDictionariesCommand showDictionariesCommand, C context) {
        return visitCommand(showDictionariesCommand, context);
    }

    default R visitShowConfigCommand(ShowConfigCommand showConfigCommand, C context) {
        return visitCommand(showConfigCommand, context);
    }

    default R visitSetOptionsCommand(SetOptionsCommand setOptionsCommand, C context) {
        return visitCommand(setOptionsCommand, context);
    }

    default R visitShowColumnStatsCommand(ShowColumnStatsCommand showColumnStatsCommand, C context) {
        return visitCommand(showColumnStatsCommand, context);
    }

    default R visitSetTransactionCommand(SetTransactionCommand setTransactionCommand, C context) {
        return visitCommand(setTransactionCommand, context);
    }

    default R visitSetUserPropertiesCommand(SetUserPropertiesCommand setUserPropertiesCommand, C context) {
        return visitCommand(setUserPropertiesCommand, context);
    }

    default R visitAlterCatalogRenameCommand(AlterCatalogRenameCommand alterCatalogRenameCommand, C context) {
        return visitCommand(alterCatalogRenameCommand, context);
    }

    default R visitSetDefaultStorageVault(SetDefaultStorageVaultCommand setDefaultStorageVaultCommand, C context) {
        return visitCommand(setDefaultStorageVaultCommand, context);
    }

    default R visitDropStoragePolicyCommand(DropStoragePolicyCommand dropStoragePolicyCommand, C context) {
        return visitCommand(dropStoragePolicyCommand, context);
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

    default R visitAlterTableCommand(AlterTableCommand alterTableCommand, C context) {
        return visitCommand(alterTableCommand, context);
    }

    default R visitShowGrantsCommand(ShowGrantsCommand showGrantsCommand, C context) {
        return visitCommand(showGrantsCommand, context);
    }

    default R visitShowStatusCommand(ShowStatusCommand showStatusCommand, C context) {
        return visitCommand(showStatusCommand, context);
    }

    default R visitShowPartitionIdCommand(ShowPartitionIdCommand showPartitionIdCommand, C context) {
        return visitCommand(showPartitionIdCommand, context);
    }

    default R visitShowPartitionsCommand(ShowPartitionsCommand showPartitionsCommand, C context) {
        return visitCommand(showPartitionsCommand, context);
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

    default R visitRefreshTableCommand(RefreshTableCommand refreshTableCommand, C context) {
        return visitCommand(refreshTableCommand, context);
    }

    default R visitRefreshDictionaryCommand(RefreshDictionaryCommand refreshDictionaryCommand, C context) {
        return visitCommand(refreshDictionaryCommand, context);
    }

    default R visitShowBackendsCommand(ShowBackendsCommand showBackendsCommand, C context) {
        return visitCommand(showBackendsCommand, context);
    }

    default R visitShowBackupCommand(ShowBackupCommand showBackupCommand, C context) {
        return visitCommand(showBackupCommand, context);
    }

    default R visitShowCreateTableCommand(ShowCreateTableCommand showCreateTableCommand, C context) {
        return visitCommand(showCreateTableCommand, context);
    }

    default R visitShowSmallFilesCommand(ShowSmallFilesCommand showSmallFilesCommand, C context) {
        return visitCommand(showSmallFilesCommand, context);
    }

    default R visitShowSnapshotCommand(ShowSnapshotCommand showSnapshotCommand, C context) {
        return visitCommand(showSnapshotCommand, context);
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

    default R visitShowTypeCastCommand(ShowTypeCastCommand showTypeCastCommand, C context) {
        return visitCommand(showTypeCastCommand, context);
    }

    default R visitShowRepositoriesCommand(ShowRepositoriesCommand showRepositoriesCommand, C context) {
        return visitCommand(showRepositoriesCommand, context);
    }

    default R visitShowResourcesCommand(ShowResourcesCommand showResourcesCommand, C context) {
        return visitCommand(showResourcesCommand, context);
    }

    default R visitShowRestoreCommand(ShowRestoreCommand showRestoreCommand, C context) {
        return visitCommand(showRestoreCommand, context);
    }

    default R visitShowRolesCommand(ShowRolesCommand showRolesCommand, C context) {
        return visitCommand(showRolesCommand, context);
    }

    default R visitShowProcCommand(ShowProcCommand showProcCommand, C context) {
        return visitCommand(showProcCommand, context);
    }

    default R visitShowDataCommand(ShowDataCommand showDataCommand, C context) {
        return visitCommand(showDataCommand, context);
    }

    default R visitShowStorageEnginesCommand(ShowStorageEnginesCommand showStorageEnginesCommand, C context) {
        return visitCommand(showStorageEnginesCommand, context);
    }

    default R visitShowCreateCatalogCommand(ShowCreateCatalogCommand showCreateCatalogCommand, C context) {
        return visitCommand(showCreateCatalogCommand, context);
    }

    default R visitShowCatalogCommand(ShowCatalogCommand showCatalogCommand, C context) {
        return visitCommand(showCatalogCommand, context);
    }

    default R visitShowCreateMaterializedViewCommand(ShowCreateMaterializedViewCommand showCreateMtlzViewCommand,
                        C context) {
        return visitCommand(showCreateMtlzViewCommand, context);
    }

    default R visitShowCreateDatabaseCommand(ShowCreateDatabaseCommand showCreateDatabaseCommand, C context) {
        return visitCommand(showCreateDatabaseCommand, context);
    }

    default R visitShowCreateFunctionCommand(ShowCreateFunctionCommand showCreateFunctionCommand, C context) {
        return visitCommand(showCreateFunctionCommand, context);
    }

    default R visitShowCreateUserCommand(ShowCreateUserCommand showCreateUserCommand, C context) {
        return visitCommand(showCreateUserCommand, context);
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

    default R visitCleanLabelCommand(CleanLabelCommand cleanLabelCommand, C context) {
        return visitCommand(cleanLabelCommand, context);
    }

    default R visitShowDataTypesCommand(ShowDataTypesCommand showDataTypesCommand, C context) {
        return visitCommand(showDataTypesCommand, context);
    }

    default R visitShowFrontendsCommand(ShowFrontendsCommand showFrontendsCommand, C context) {
        return visitCommand(showFrontendsCommand, context);
    }

    default R visitShowFunctionsCommand(ShowFunctionsCommand showFunctionsCommand, C context) {
        return visitCommand(showFunctionsCommand, context);
    }

    default R visitAdminRebalanceDiskCommand(AdminRebalanceDiskCommand adminRebalanceDiskCommand, C context) {
        return visitCommand(adminRebalanceDiskCommand, context);
    }

    default R visitAdminCancelRebalanceDiskCommand(AdminCancelRebalanceDiskCommand command, C context) {
        return visitCommand(command, context);
    }

    default R visitCreateWorkloadPolicyCommand(CreateWorkloadPolicyCommand createWorkloadPolicyCommand, C context) {
        return visitCommand(createWorkloadPolicyCommand, context);
    }

    default R visitInstallPluginCommand(InstallPluginCommand command, C context) {
        return visitCommand(command, context);
    }

    default R visitShowDynamicPartitionCommand(ShowDynamicPartitionCommand showDynamicPartitionCommand, C context) {
        return visitCommand(showDynamicPartitionCommand, context);
    }

    default R visitShowWhiteListCommand(ShowWhiteListCommand whiteListCommand, C context) {
        return visitCommand(whiteListCommand, context);
    }

    default R visitAlterCatalogPropertiesCommand(AlterCatalogPropertiesCommand alterCatalogPropsCmd, C context) {
        return visitCommand(alterCatalogPropsCmd, context);
    }

    default R visitAlterDatabasePropertiesCommand(AlterDatabasePropertiesCommand alterDatabasePropsCmd, C context) {
        return visitCommand(alterDatabasePropsCmd, context);
    }

    default R visitUninstallPluginCommand(UninstallPluginCommand command, C context) {
        return visitCommand(command, context);
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

    default R visitShowStoragePolicyCommand(ShowStoragePolicyCommand showStoragePolicyCommand, C context) {
        return visitCommand(showStoragePolicyCommand, context);
    }

    default R visitRecoverPartitionCommand(RecoverPartitionCommand recoverPartitionCommand, C context) {
        return visitCommand(recoverPartitionCommand, context);
    }

    default R visitShowBrokerCommand(ShowBrokerCommand showBrokerCommand, C context) {
        return visitCommand(showBrokerCommand, context);
    }

    default R visitShowBuildIndexCommand(ShowBuildIndexCommand showBuildIndexCommand, C context) {
        return visitCommand(showBuildIndexCommand, context);
    }

    default R visitShowLoadCommand(ShowLoadCommand showLoadCommand, C context) {
        return visitCommand(showLoadCommand, context);
    }

    default R visitShowLoadProfileCommand(ShowLoadProfileCommand showLoadProfileCommand, C context) {
        return visitCommand(showLoadProfileCommand, context);
    }

    default R visitShowLoadWarningsCommand(ShowLoadWarningsCommand showLoadWarningsCommand, C context) {
        return visitCommand(showLoadWarningsCommand, context);
    }

    default R visitAlterStoragePolicyCommand(AlterStoragePolicyCommand alterStoragePolicyCommand, C context) {
        return visitCommand(alterStoragePolicyCommand, context);
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

    default R visitDropTableCommand(DropTableCommand dropTableCommand, C context) {
        return visitCommand(dropTableCommand, context);
    }

    default R visitDropViewCommand(DropViewCommand command, C context) {
        return visitCommand(command, context);
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

    default R visitShowCharsetCommand(ShowCharsetCommand showCharsetCommand, C context) {
        return visitCommand(showCharsetCommand, context);
    }

    default R visitShowCreateLoadCommand(ShowCreateLoadCommand showCreateLoadCommand, C context) {
        return visitCommand(showCreateLoadCommand, context);
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

    default R visitShowExportCommand(ShowExportCommand showExportCommand, C context) {
        return visitCommand(showExportCommand, context);
    }

    default R visitShowDeleteCommand(ShowDeleteCommand showDeleteCommand, C context) {
        return visitCommand(showDeleteCommand, context);
    }

    default R visitShowOpenTablesCommand(ShowOpenTablesCommand showOpenTablesCommand, C context) {
        return visitCommand(showOpenTablesCommand, context);
    }

    default R visitShowPrivilegesCommand(ShowPrivilegesCommand showPrivilegesCommand, C context) {
        return visitCommand(showPrivilegesCommand, context);
    }

    default R visitShowUserPropertyCommand(ShowUserPropertyCommand showUserpropertyCommand, C context) {
        return visitCommand(showUserpropertyCommand, context);
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

    default R visitShowQueryProfileCommand(ShowQueryProfileCommand showQueryProfileCommand,
                                           C context) {
        return visitCommand(showQueryProfileCommand, context);
    }

    default R visitShowQueryStatsCommand(ShowQueryStatsCommand showQueryStatsCommand,
            C context) {
        return visitCommand(showQueryStatsCommand, context);
    }

    default R visitShowConvertLscCommand(ShowConvertLSCCommand showConvertLSCCommand, C context) {
        return visitCommand(showConvertLSCCommand, context);
    }

    default R visitShowClustersCommand(ShowClustersCommand showClustersCommand, C context) {
        return visitCommand(showClustersCommand, context);
    }

    default R visitSwitchCommand(SwitchCommand switchCommand, C context) {
        return visitCommand(switchCommand, context);
    }

    default R visitUseCommand(UseCommand useCommand, C context) {
        return visitCommand(useCommand, context);
    }

    default R visitLockTablesCommand(LockTablesCommand lockTablesCommand, C context) {
        return visitCommand(lockTablesCommand, context);
    }

    default R visitAlterDatabaseRenameCommand(AlterDatabaseRenameCommand alterDatabaseRenameCommand, C context) {
        return visitCommand(alterDatabaseRenameCommand, context);
    }

    default R visitKillQueryCommand(KillQueryCommand killQueryCommand, C context) {
        return visitCommand(killQueryCommand, context);
    }

    default R visitKillConnectionCommand(KillConnectionCommand killConnectionCommand, C context) {
        return visitCommand(killConnectionCommand, context);
    }

    default R visitAlterDatabaseSetQuotaCommand(AlterDatabaseSetQuotaCommand alterDatabaseSetQuotaCommand, C context) {
        return visitCommand(alterDatabaseSetQuotaCommand, context);
    }

    default R visitDropDatabaseCommand(DropDatabaseCommand dropDatabaseCommand, C context) {
        return visitCommand(dropDatabaseCommand, context);
    }

    default R visitAlterRepositoryCommand(AlterRepositoryCommand alterRepositoryCommand,
                                          C context) {
        return visitCommand(alterRepositoryCommand, context);
    }

    default R visitShowRowPolicyCommand(ShowRowPolicyCommand showRowPolicyCommand, C context) {
        return visitCommand(showRowPolicyCommand, context);
    }

    default R visitShowAnalyzeCommand(ShowAnalyzeCommand showAnalyzeCommand, C context) {
        return visitCommand(showAnalyzeCommand, context);
    }

    default R visitShowQueuedAnalyzeJobsCommand(ShowQueuedAnalyzeJobsCommand showQueuedAnalyzeJobsCommand, C context) {
        return visitCommand(showQueuedAnalyzeJobsCommand, context);
    }

    default R visitShowColumnHistogramStatsCommand(ShowColumnHistogramStatsCommand showColumnHistogramStatCommand,
                                                   C context) {
        return visitCommand(showColumnHistogramStatCommand, context);
    }

    default R visitDescribeCommand(DescribeCommand describeCommand, C context) {
        return visitCommand(describeCommand, context);
    }

    default R visitAlterUserCommand(AlterUserCommand alterUserCommand, C context) {
        return visitCommand(alterUserCommand, context);
    }

    default R visitShowTableStatusCommand(ShowTableStatusCommand showTableStatusCommand, C context) {
        return visitCommand(showTableStatusCommand, context);
    }

    default R visitShowDatabasesCommand(ShowDatabasesCommand showDatabasesCommand, C context) {
        return visitCommand(showDatabasesCommand, context);
    }

    default R visitShowTableCommand(ShowTableCommand showTableCommand, C context) {
        return visitCommand(showTableCommand, context);
    }

    default R visitShowTableStatsCommand(ShowTableStatsCommand showTableStatsCommand, C context) {
        return visitCommand(showTableStatsCommand, context);
    }

    default R visitShowTabletsFromTableCommand(ShowTabletsFromTableCommand showTabletsFromTableCommand, C context) {
        return visitCommand(showTabletsFromTableCommand, context);
    }

    default R visitDropStatsCommand(DropStatsCommand dropStatsCommand, C context) {
        return visitCommand(dropStatsCommand, context);
    }

    default R visitUnlockTablesCommand(UnlockTablesCommand unlockTablesCommand, C context) {
        return visitCommand(unlockTablesCommand, context);
    }

    default R visitDropCachedStatsCommand(DropCachedStatsCommand dropCachedStatsCommand, C context) {
        return visitCommand(dropCachedStatsCommand, context);
    }

    default R visitDropExpiredStatsCommand(DropExpiredStatsCommand dropExpiredStatsCommand, C context) {
        return visitCommand(dropExpiredStatsCommand, context);
    }

    default R visitShowIndexStatsCommand(ShowIndexStatsCommand showIndexStatsCommand, C context) {
        return visitCommand(showIndexStatsCommand, context);
    }

    default R visitShowIndexCommand(ShowIndexCommand showIndexCommand, C context) {
        return visitCommand(showIndexCommand, context);
    }

    default R visitShowTabletIdCommand(ShowTabletIdCommand showTabletIdCommand, C context) {
        return visitCommand(showTabletIdCommand, context);
    }

    default R visitShowCatalogRecycleBinCommand(ShowCatalogRecycleBinCommand showCatalogRecycleBinCommand, C context) {
        return visitCommand(showCatalogRecycleBinCommand, context);
    }

    default R visitAlterTableStatsCommand(AlterTableStatsCommand alterTableStatsCommand, C context) {
        return visitCommand(alterTableStatsCommand, context);
    }

    default R visitAlterColumnStatsCommand(AlterColumnStatsCommand alterColumnStatsCommand, C context) {
        return visitCommand(alterColumnStatsCommand, context);
    }

    default R visitMysqlLoadCommand(MysqlLoadCommand mysqlLoadCommand, C context) {
        return visitCommand(mysqlLoadCommand, context);
    }

    default R visitCancelAlterTable(CancelAlterTableCommand cancelAlterTableCommand, C context) {
        return visitCommand(cancelAlterTableCommand, context);
    }

    default R visitAdminCancelRepairTableCommand(AdminCancelRepairTableCommand adminCancelRepairTableCommand,
                                                     C context) {
        return visitCommand(adminCancelRepairTableCommand, context);
    }

    default R visitAdminRepairTableCommand(AdminRepairTableCommand adminRepairTableCommand, C context) {
        return visitCommand(adminRepairTableCommand, context);
    }

    default R visitAdminSetReplicaStatusCommand(AdminSetReplicaStatusCommand adminSetReplicaStatusCommand, C context) {
        return visitCommand(adminSetReplicaStatusCommand, context);
    }

    default R visitAdminCopyTabletCommand(AdminCopyTabletCommand adminCopyTabletCommand, C context) {
        return visitCommand(adminCopyTabletCommand, context);
    }

    default R visitShowCreateRoutineLoadCommand(ShowCreateRoutineLoadCommand showCreateRoutineLoadCommand, C context) {
        return visitCommand(showCreateRoutineLoadCommand, context);
    }

    default R visitPauseRoutineLoadCommand(PauseRoutineLoadCommand routineLoadCommand, C context) {
        return visitCommand(routineLoadCommand, context);
    }

    default R visitResumeRoutineLoadCommand(ResumeRoutineLoadCommand resumeRoutineLoadCommand, C context) {
        return visitCommand(resumeRoutineLoadCommand, context);
    }

    default R visitStopRoutineLoadCommand(StopRoutineLoadCommand stopRoutineLoadCommand, C context) {
        return visitCommand(stopRoutineLoadCommand, context);
    }

    default R visitCleanQueryStatsCommand(CleanQueryStatsCommand cleanQueryStatsCommand, C context) {
        return visitCommand(cleanQueryStatsCommand, context);
    }

    default R visitDropResourceCommand(DropResourceCommand dropResourceCommand, C context) {
        return visitCommand(dropResourceCommand, context);
    }

    default R visitDropRowPolicyCommand(DropRowPolicyCommand dropRowPolicyCommand, C context) {
        return visitCommand(dropRowPolicyCommand, context);
    }

    default R visitTransactionBeginCommand(TransactionBeginCommand transactionBeginCommand, C context) {
        return visitCommand(transactionBeginCommand, context);
    }

    default R visitTransactionCommitCommand(TransactionCommitCommand transactionCommitCommand, C context) {
        return visitCommand(transactionCommitCommand, context);
    }

    default R visitTransactionRollbackCommand(TransactionRollbackCommand transactionRollbackCommand, C context) {
        return visitCommand(transactionRollbackCommand, context);
    }

    default R visitKillAnalyzeJobCommand(KillAnalyzeJobCommand killAnalyzeJobCommand, C context) {
        return visitCommand(killAnalyzeJobCommand, context);
    }

    default R visitDropAnalyzeJobCommand(DropAnalyzeJobCommand dropAnalyzeJobCommand, C context) {
        return visitCommand(dropAnalyzeJobCommand, context);
    }

    default R visitAlterRoutineLoadCommand(AlterRoutineLoadCommand alterRoutineLoadCommand, C context) {
        return visitCommand(alterRoutineLoadCommand, context);
    }

    default R visitColocateGroupCommand(AlterColocateGroupCommand alterColocateGroupCommand, C context) {
        return visitCommand(alterColocateGroupCommand, context);
    }

    default R visitCancelBackupCommand(CancelBackupCommand cancelBackupCommand, C context) {
        return visitCommand(cancelBackupCommand, context);
    }

    default R visitCancelBuildIndexCommand(CancelBuildIndexCommand cancelBuildIndexCommand, C context) {
        return visitCommand(cancelBuildIndexCommand, context);
    }

    default R visitCreateUserCommand(CreateUserCommand createUserCommand, C context) {
        return visitCommand(createUserCommand, context);
    }

    default R visitWarmUpClusterCommand(WarmUpClusterCommand warmUpClusterCommand, C context) {
        return visitCommand(warmUpClusterCommand, context);
    }

    default R visitCreateResourceCommand(CreateResourceCommand createResourceCommand, C context) {
        return visitCommand(createResourceCommand, context);
    }

    default R visitRestoreCommand(RestoreCommand restoreCommand, C context) {
        return visitCommand(restoreCommand, context);
    }

    default R visitBackupCommand(BackupCommand backupCommand, C context) {
        return visitCommand(backupCommand, context);
    }

    default R visitRefreshLdapCommand(RefreshLdapCommand command, C context) {
        return visitCommand(command, context);
    }

    default R visitCreateStageCommand(CreateStageCommand createStageCommand, C context) {
        return visitCommand(createStageCommand, context);
    }

    default R visitAdminSetReplicaVersionCommand(AdminSetReplicaVersionCommand command, C context) {
        return visitCommand(command, context);
    }

    default R visitDropStageCommand(DropStageCommand dropStageCommand, C context) {
        return visitCommand(dropStageCommand, context);
    }

    default R visitCancelDecommissionCommand(CancelDecommissionBackendCommand cancelDecommissionBackendCommand,
                                             C context) {
        return visitCommand(cancelDecommissionBackendCommand, context);
    }

    default R visitShowTransactionCommand(ShowTransactionCommand showTransactionCommand, C context) {
        return visitCommand(showTransactionCommand, context);
    }

    default R visitShowReplicaStatusCommand(ShowReplicaStatusCommand showReplicaStatusCommand, C context) {
        return visitCommand(showReplicaStatusCommand, context);
    }

    default R visitShowWorkloadGroupsCommand(ShowWorkloadGroupsCommand showWorkloadGroupCommand, C context) {
        return visitCommand(showWorkloadGroupCommand, context);
    }

    default R visitTruncateTableCommand(TruncateTableCommand truncateTableCommand, C context) {
        return visitCommand(truncateTableCommand, context);
    }

    default R visitShowCopyCommand(ShowCopyCommand showCopyCommand, C context) {
        return visitCommand(showCopyCommand, context);
    }

    default R visitGrantRoleCommand(GrantRoleCommand grantRoleCommand, C context) {
        return visitCommand(grantRoleCommand, context);
    }

    default R visitUseCloudClusterCommand(UseCloudClusterCommand command, C context) {
        return visitCommand(command, context);
    }

    default R visitGrantTablePrivilegeCommand(GrantTablePrivilegeCommand grantTablePrivilegeCommand, C context) {
        return visitCommand(grantTablePrivilegeCommand, context);
    }

    default R visitGrantResourcePrivilegeCommand(GrantResourcePrivilegeCommand command, C context) {
        return visitCommand(command, context);
    }

    default R visitAdminSetFrontendConfigCommand(AdminSetFrontendConfigCommand command, C context) {
        return visitCommand(command, context);
    }

    default R visitAdminSetPartitionVersionCommand(AdminSetPartitionVersionCommand command, C context) {
        return visitCommand(command, context);
    }

    default R visitRevokeRoleCommand(RevokeRoleCommand revokeRoleCommand, C context) {
        return visitCommand(revokeRoleCommand, context);
    }

    default R visitShowRoutineLoadCommand(ShowRoutineLoadCommand showRoutineLoadCommand, C context) {
        return visitCommand(showRoutineLoadCommand, context);
    }

    default R visitShowRoutineLoadTaskCommand(ShowRoutineLoadTaskCommand showRoutineLoadTaskCommand, C context) {
        return visitCommand(showRoutineLoadTaskCommand, context);
    }

    default R visitShowWarmupCommand(ShowWarmUpCommand showWarmUpCommand, C context) {
        return visitCommand(showWarmUpCommand, context);
    }

    default R visitRevokeResourcePrivilegeCommand(RevokeResourcePrivilegeCommand command, C context) {
        return visitCommand(command, context);
    }

    default R visitRevokeTablePrivilegeCommand(RevokeTablePrivilegeCommand revokeTablePrivilegeCommand, C context) {
        return visitCommand(revokeTablePrivilegeCommand, context);
    }

    default R visitCreateIndexAnalyzerCommand(
            CreateIndexAnalyzerCommand createIndexAnalyzerCommand, C context) {
        return visitCommand(createIndexAnalyzerCommand, context);
    }

    default R visitCreateIndexTokenizerCommand(
            CreateIndexTokenizerCommand createIndexTokenizerCommand, C context) {
        return visitCommand(createIndexTokenizerCommand, context);
    }

    default R visitCreateIndexTokenFilterCommand(
            CreateIndexTokenFilterCommand createIndexTokenFilterCommand, C context) {
        return visitCommand(createIndexTokenFilterCommand, context);
    }

    default R visitDropIndexAnalyzerCommand(
            DropIndexAnalyzerCommand dropIndexAnalyzerCommand, C context) {
        return visitCommand(dropIndexAnalyzerCommand, context);
    }

    default R visitShowCreateStorageVaultCommand(ShowCreateStorageVaultCommand command, C context) {
        return visitCommand(command, context);
    }

    default R visitDropIndexTokenizerCommand(
            DropIndexTokenizerCommand dropIndexTokenizerCommand, C context) {
        return visitCommand(dropIndexTokenizerCommand, context);
    }

    default R visitDropIndexTokenFilterCommand(
            DropIndexTokenFilterCommand dropIndexTokenFilterCommand, C context) {
        return visitCommand(dropIndexTokenFilterCommand, context);
    }

    default R visitShowIndexAnalyzerCommand(
            ShowIndexAnalyzerCommand showIndexAnalyzerCommand, C context) {
        return visitCommand(showIndexAnalyzerCommand, context);
    }

    default R visitShowIndexTokenizerCommand(
            ShowIndexTokenizerCommand showIndexTokenizerCommand, C context) {
        return visitCommand(showIndexTokenizerCommand, context);
    }

    default R visitShowIndexTokenFilterCommand(
            ShowIndexTokenFilterCommand showIndexTokenFilterCommand, C context) {
        return visitCommand(showIndexTokenFilterCommand, context);
    }

    default R visitDropMaterializedViewCommand(DropMaterializedViewCommand dropMaterializedViewCommand, C context) {
        return visitCommand(dropMaterializedViewCommand, context);
    }
}
