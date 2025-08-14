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

package org.apache.doris.nereids.parser;

import org.apache.doris.alter.QuotaType;
import org.apache.doris.analysis.AnalyzeProperties;
import org.apache.doris.analysis.ArithmeticExpr.Operator;
import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.analysis.ColumnNullableType;
import org.apache.doris.analysis.ColumnPosition;
import org.apache.doris.analysis.DbName;
import org.apache.doris.analysis.EncryptKeyName;
import org.apache.doris.analysis.FunctionName;
import org.apache.doris.analysis.LabelName;
import org.apache.doris.analysis.LockTable;
import org.apache.doris.analysis.PartitionNames;
import org.apache.doris.analysis.PassVar;
import org.apache.doris.analysis.PasswordOptions;
import org.apache.doris.analysis.ResourceDesc;
import org.apache.doris.analysis.ResourcePattern;
import org.apache.doris.analysis.ResourceTypeEnum;
import org.apache.doris.analysis.Separator;
import org.apache.doris.analysis.SetType;
import org.apache.doris.analysis.StageAndPattern;
import org.apache.doris.analysis.StorageBackend;
import org.apache.doris.analysis.TableName;
import org.apache.doris.analysis.TablePattern;
import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.analysis.TableValuedFunctionRef;
import org.apache.doris.analysis.UserDesc;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.analysis.WorkloadGroupPattern;
import org.apache.doris.catalog.AccessPrivilege;
import org.apache.doris.catalog.AccessPrivilegeWithCols;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.BuiltinAggregateFunctions;
import org.apache.doris.catalog.BuiltinTableGeneratingFunctions;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.InfoSchemaDb;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.cloud.stage.StageUtil;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.dictionary.LayoutType;
import org.apache.doris.job.common.IntervalUnit;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.mtmv.MTMVPartitionInfo.MTMVPartitionType;
import org.apache.doris.mtmv.MTMVRefreshEnum.BuildMode;
import org.apache.doris.mtmv.MTMVRefreshEnum.RefreshMethod;
import org.apache.doris.mtmv.MTMVRefreshEnum.RefreshTrigger;
import org.apache.doris.mtmv.MTMVRefreshInfo;
import org.apache.doris.mtmv.MTMVRefreshSchedule;
import org.apache.doris.mtmv.MTMVRefreshTriggerInfo;
import org.apache.doris.nereids.DorisParser;
import org.apache.doris.nereids.DorisParser.AddBackendClauseContext;
import org.apache.doris.nereids.DorisParser.AddBrokerClauseContext;
import org.apache.doris.nereids.DorisParser.AddColumnClauseContext;
import org.apache.doris.nereids.DorisParser.AddColumnsClauseContext;
import org.apache.doris.nereids.DorisParser.AddConstraintContext;
import org.apache.doris.nereids.DorisParser.AddIndexClauseContext;
import org.apache.doris.nereids.DorisParser.AddPartitionClauseContext;
import org.apache.doris.nereids.DorisParser.AddRollupClauseContext;
import org.apache.doris.nereids.DorisParser.AdminCancelRebalanceDiskContext;
import org.apache.doris.nereids.DorisParser.AdminCheckTabletsContext;
import org.apache.doris.nereids.DorisParser.AdminCompactTableContext;
import org.apache.doris.nereids.DorisParser.AdminDiagnoseTabletContext;
import org.apache.doris.nereids.DorisParser.AdminRebalanceDiskContext;
import org.apache.doris.nereids.DorisParser.AdminSetEncryptionRootKeyContext;
import org.apache.doris.nereids.DorisParser.AdminSetTableStatusContext;
import org.apache.doris.nereids.DorisParser.AdminShowReplicaDistributionContext;
import org.apache.doris.nereids.DorisParser.AdminShowTabletStorageFormatContext;
import org.apache.doris.nereids.DorisParser.AggClauseContext;
import org.apache.doris.nereids.DorisParser.AggStateDataTypeContext;
import org.apache.doris.nereids.DorisParser.AliasQueryContext;
import org.apache.doris.nereids.DorisParser.AliasedQueryContext;
import org.apache.doris.nereids.DorisParser.AlterCatalogCommentContext;
import org.apache.doris.nereids.DorisParser.AlterCatalogPropertiesContext;
import org.apache.doris.nereids.DorisParser.AlterCatalogRenameContext;
import org.apache.doris.nereids.DorisParser.AlterDatabasePropertiesContext;
import org.apache.doris.nereids.DorisParser.AlterDatabaseRenameContext;
import org.apache.doris.nereids.DorisParser.AlterDatabaseSetQuotaContext;
import org.apache.doris.nereids.DorisParser.AlterMTMVContext;
import org.apache.doris.nereids.DorisParser.AlterMultiPartitionClauseContext;
import org.apache.doris.nereids.DorisParser.AlterRepositoryContext;
import org.apache.doris.nereids.DorisParser.AlterResourceContext;
import org.apache.doris.nereids.DorisParser.AlterRoleContext;
import org.apache.doris.nereids.DorisParser.AlterSqlBlockRuleContext;
import org.apache.doris.nereids.DorisParser.AlterStoragePolicyContext;
import org.apache.doris.nereids.DorisParser.AlterStorageVaultContext;
import org.apache.doris.nereids.DorisParser.AlterSystemRenameComputeGroupContext;
import org.apache.doris.nereids.DorisParser.AlterTableAddRollupContext;
import org.apache.doris.nereids.DorisParser.AlterTableClauseContext;
import org.apache.doris.nereids.DorisParser.AlterTableContext;
import org.apache.doris.nereids.DorisParser.AlterTableDropRollupContext;
import org.apache.doris.nereids.DorisParser.AlterViewContext;
import org.apache.doris.nereids.DorisParser.AlterWorkloadGroupContext;
import org.apache.doris.nereids.DorisParser.AlterWorkloadPolicyContext;
import org.apache.doris.nereids.DorisParser.ArithmeticBinaryContext;
import org.apache.doris.nereids.DorisParser.ArithmeticUnaryContext;
import org.apache.doris.nereids.DorisParser.ArrayLiteralContext;
import org.apache.doris.nereids.DorisParser.ArraySliceContext;
import org.apache.doris.nereids.DorisParser.BaseTableRefContext;
import org.apache.doris.nereids.DorisParser.BooleanExpressionContext;
import org.apache.doris.nereids.DorisParser.BooleanLiteralContext;
import org.apache.doris.nereids.DorisParser.BracketRelationHintContext;
import org.apache.doris.nereids.DorisParser.BuildIndexContext;
import org.apache.doris.nereids.DorisParser.BuildModeContext;
import org.apache.doris.nereids.DorisParser.CallProcedureContext;
import org.apache.doris.nereids.DorisParser.CancelMTMVTaskContext;
import org.apache.doris.nereids.DorisParser.CastDataTypeContext;
import org.apache.doris.nereids.DorisParser.CleanAllProfileContext;
import org.apache.doris.nereids.DorisParser.CleanLabelContext;
import org.apache.doris.nereids.DorisParser.CollateContext;
import org.apache.doris.nereids.DorisParser.ColumnDefContext;
import org.apache.doris.nereids.DorisParser.ColumnDefsContext;
import org.apache.doris.nereids.DorisParser.ColumnReferenceContext;
import org.apache.doris.nereids.DorisParser.CommentRelationHintContext;
import org.apache.doris.nereids.DorisParser.ComparisonContext;
import org.apache.doris.nereids.DorisParser.ComplexColTypeContext;
import org.apache.doris.nereids.DorisParser.ComplexColTypeListContext;
import org.apache.doris.nereids.DorisParser.ComplexDataTypeContext;
import org.apache.doris.nereids.DorisParser.ConstantContext;
import org.apache.doris.nereids.DorisParser.CreateAliasFunctionContext;
import org.apache.doris.nereids.DorisParser.CreateCatalogContext;
import org.apache.doris.nereids.DorisParser.CreateDictionaryContext;
import org.apache.doris.nereids.DorisParser.CreateEncryptkeyContext;
import org.apache.doris.nereids.DorisParser.CreateFileContext;
import org.apache.doris.nereids.DorisParser.CreateIndexAnalyzerContext;
import org.apache.doris.nereids.DorisParser.CreateIndexContext;
import org.apache.doris.nereids.DorisParser.CreateIndexTokenFilterContext;
import org.apache.doris.nereids.DorisParser.CreateIndexTokenizerContext;
import org.apache.doris.nereids.DorisParser.CreateMTMVContext;
import org.apache.doris.nereids.DorisParser.CreateProcedureContext;
import org.apache.doris.nereids.DorisParser.CreateRoleContext;
import org.apache.doris.nereids.DorisParser.CreateRoutineLoadContext;
import org.apache.doris.nereids.DorisParser.CreateRowPolicyContext;
import org.apache.doris.nereids.DorisParser.CreateSqlBlockRuleContext;
import org.apache.doris.nereids.DorisParser.CreateStoragePolicyContext;
import org.apache.doris.nereids.DorisParser.CreateTableContext;
import org.apache.doris.nereids.DorisParser.CreateTableLikeContext;
import org.apache.doris.nereids.DorisParser.CreateUserContext;
import org.apache.doris.nereids.DorisParser.CreateUserDefineFunctionContext;
import org.apache.doris.nereids.DorisParser.CreateViewContext;
import org.apache.doris.nereids.DorisParser.CreateWorkloadGroupContext;
import org.apache.doris.nereids.DorisParser.CreateWorkloadPolicyContext;
import org.apache.doris.nereids.DorisParser.CteContext;
import org.apache.doris.nereids.DorisParser.DataTypeListContext;
import org.apache.doris.nereids.DorisParser.DataTypeWithNullableContext;
import org.apache.doris.nereids.DorisParser.DecimalLiteralContext;
import org.apache.doris.nereids.DorisParser.DeleteContext;
import org.apache.doris.nereids.DorisParser.DereferenceContext;
import org.apache.doris.nereids.DorisParser.DescribeDictionaryContext;
import org.apache.doris.nereids.DorisParser.DictionaryColumnDefContext;
import org.apache.doris.nereids.DorisParser.DistributeTypeContext;
import org.apache.doris.nereids.DorisParser.DropAllBrokerClauseContext;
import org.apache.doris.nereids.DorisParser.DropBrokerClauseContext;
import org.apache.doris.nereids.DorisParser.DropCatalogContext;
import org.apache.doris.nereids.DorisParser.DropCatalogRecycleBinContext;
import org.apache.doris.nereids.DorisParser.DropColumnClauseContext;
import org.apache.doris.nereids.DorisParser.DropConstraintContext;
import org.apache.doris.nereids.DorisParser.DropDatabaseContext;
import org.apache.doris.nereids.DorisParser.DropDictionaryContext;
import org.apache.doris.nereids.DorisParser.DropEncryptkeyContext;
import org.apache.doris.nereids.DorisParser.DropFileContext;
import org.apache.doris.nereids.DorisParser.DropFunctionContext;
import org.apache.doris.nereids.DorisParser.DropIndexAnalyzerContext;
import org.apache.doris.nereids.DorisParser.DropIndexClauseContext;
import org.apache.doris.nereids.DorisParser.DropIndexContext;
import org.apache.doris.nereids.DorisParser.DropIndexTokenFilterContext;
import org.apache.doris.nereids.DorisParser.DropIndexTokenizerContext;
import org.apache.doris.nereids.DorisParser.DropMVContext;
import org.apache.doris.nereids.DorisParser.DropPartitionClauseContext;
import org.apache.doris.nereids.DorisParser.DropProcedureContext;
import org.apache.doris.nereids.DorisParser.DropRepositoryContext;
import org.apache.doris.nereids.DorisParser.DropRoleContext;
import org.apache.doris.nereids.DorisParser.DropRollupClauseContext;
import org.apache.doris.nereids.DorisParser.DropSqlBlockRuleContext;
import org.apache.doris.nereids.DorisParser.DropStoragePolicyContext;
import org.apache.doris.nereids.DorisParser.DropTableContext;
import org.apache.doris.nereids.DorisParser.DropUserContext;
import org.apache.doris.nereids.DorisParser.DropWorkloadGroupContext;
import org.apache.doris.nereids.DorisParser.DropWorkloadPolicyContext;
import org.apache.doris.nereids.DorisParser.ElementAtContext;
import org.apache.doris.nereids.DorisParser.EnableFeatureClauseContext;
import org.apache.doris.nereids.DorisParser.ExceptContext;
import org.apache.doris.nereids.DorisParser.ExceptOrReplaceContext;
import org.apache.doris.nereids.DorisParser.ExistContext;
import org.apache.doris.nereids.DorisParser.ExplainContext;
import org.apache.doris.nereids.DorisParser.ExportContext;
import org.apache.doris.nereids.DorisParser.ExpressionWithEofContext;
import org.apache.doris.nereids.DorisParser.ExpressionWithOrderContext;
import org.apache.doris.nereids.DorisParser.FixedPartitionDefContext;
import org.apache.doris.nereids.DorisParser.FromClauseContext;
import org.apache.doris.nereids.DorisParser.FunctionArgumentsContext;
import org.apache.doris.nereids.DorisParser.FunctionIdentifierContext;
import org.apache.doris.nereids.DorisParser.GroupConcatContext;
import org.apache.doris.nereids.DorisParser.GroupingElementContext;
import org.apache.doris.nereids.DorisParser.GroupingSetContext;
import org.apache.doris.nereids.DorisParser.HavingClauseContext;
import org.apache.doris.nereids.DorisParser.HelpContext;
import org.apache.doris.nereids.DorisParser.HintAssignmentContext;
import org.apache.doris.nereids.DorisParser.HintStatementContext;
import org.apache.doris.nereids.DorisParser.IdentifierContext;
import org.apache.doris.nereids.DorisParser.IdentifierListContext;
import org.apache.doris.nereids.DorisParser.IdentifierSeqContext;
import org.apache.doris.nereids.DorisParser.ImportColumnsContext;
import org.apache.doris.nereids.DorisParser.ImportDeleteOnContext;
import org.apache.doris.nereids.DorisParser.ImportPartitionsContext;
import org.apache.doris.nereids.DorisParser.ImportPrecedingFilterContext;
import org.apache.doris.nereids.DorisParser.ImportSequenceContext;
import org.apache.doris.nereids.DorisParser.ImportWhereContext;
import org.apache.doris.nereids.DorisParser.InPartitionDefContext;
import org.apache.doris.nereids.DorisParser.IndexDefContext;
import org.apache.doris.nereids.DorisParser.IndexDefsContext;
import org.apache.doris.nereids.DorisParser.InlineTableContext;
import org.apache.doris.nereids.DorisParser.InsertTableContext;
import org.apache.doris.nereids.DorisParser.InstallPluginContext;
import org.apache.doris.nereids.DorisParser.IntegerLiteralContext;
import org.apache.doris.nereids.DorisParser.IntervalContext;
import org.apache.doris.nereids.DorisParser.Is_not_null_predContext;
import org.apache.doris.nereids.DorisParser.IsnullContext;
import org.apache.doris.nereids.DorisParser.JoinCriteriaContext;
import org.apache.doris.nereids.DorisParser.JoinRelationContext;
import org.apache.doris.nereids.DorisParser.KillQueryContext;
import org.apache.doris.nereids.DorisParser.LambdaExpressionContext;
import org.apache.doris.nereids.DorisParser.LateralViewContext;
import org.apache.doris.nereids.DorisParser.LessThanPartitionDefContext;
import org.apache.doris.nereids.DorisParser.LimitClauseContext;
import org.apache.doris.nereids.DorisParser.LoadPropertyContext;
import org.apache.doris.nereids.DorisParser.LockTablesContext;
import org.apache.doris.nereids.DorisParser.LogicalBinaryContext;
import org.apache.doris.nereids.DorisParser.LogicalNotContext;
import org.apache.doris.nereids.DorisParser.MapLiteralContext;
import org.apache.doris.nereids.DorisParser.ModifyColumnClauseContext;
import org.apache.doris.nereids.DorisParser.ModifyColumnCommentClauseContext;
import org.apache.doris.nereids.DorisParser.ModifyDistributionClauseContext;
import org.apache.doris.nereids.DorisParser.ModifyEngineClauseContext;
import org.apache.doris.nereids.DorisParser.ModifyPartitionClauseContext;
import org.apache.doris.nereids.DorisParser.ModifyTableCommentClauseContext;
import org.apache.doris.nereids.DorisParser.MultiStatementsContext;
import org.apache.doris.nereids.DorisParser.MultipartIdentifierContext;
import org.apache.doris.nereids.DorisParser.MvPartitionContext;
import org.apache.doris.nereids.DorisParser.NamedExpressionContext;
import org.apache.doris.nereids.DorisParser.NamedExpressionSeqContext;
import org.apache.doris.nereids.DorisParser.NullLiteralContext;
import org.apache.doris.nereids.DorisParser.OptScanParamsContext;
import org.apache.doris.nereids.DorisParser.OutFileClauseContext;
import org.apache.doris.nereids.DorisParser.ParenthesizedExpressionContext;
import org.apache.doris.nereids.DorisParser.PartitionSpecContext;
import org.apache.doris.nereids.DorisParser.PartitionValueDefContext;
import org.apache.doris.nereids.DorisParser.PartitionValueListContext;
import org.apache.doris.nereids.DorisParser.PartitionsDefContext;
import org.apache.doris.nereids.DorisParser.PauseMTMVContext;
import org.apache.doris.nereids.DorisParser.PlanTypeContext;
import org.apache.doris.nereids.DorisParser.PredicateContext;
import org.apache.doris.nereids.DorisParser.PredicatedContext;
import org.apache.doris.nereids.DorisParser.PrimitiveDataTypeContext;
import org.apache.doris.nereids.DorisParser.PropertyClauseContext;
import org.apache.doris.nereids.DorisParser.PropertyItemContext;
import org.apache.doris.nereids.DorisParser.PropertyItemListContext;
import org.apache.doris.nereids.DorisParser.PropertyKeyContext;
import org.apache.doris.nereids.DorisParser.PropertyValueContext;
import org.apache.doris.nereids.DorisParser.QualifiedNameContext;
import org.apache.doris.nereids.DorisParser.QualifyClauseContext;
import org.apache.doris.nereids.DorisParser.QueryContext;
import org.apache.doris.nereids.DorisParser.QueryOrganizationContext;
import org.apache.doris.nereids.DorisParser.QueryTermContext;
import org.apache.doris.nereids.DorisParser.RecoverDatabaseContext;
import org.apache.doris.nereids.DorisParser.RecoverPartitionContext;
import org.apache.doris.nereids.DorisParser.RecoverTableContext;
import org.apache.doris.nereids.DorisParser.RefreshCatalogContext;
import org.apache.doris.nereids.DorisParser.RefreshDatabaseContext;
import org.apache.doris.nereids.DorisParser.RefreshDictionaryContext;
import org.apache.doris.nereids.DorisParser.RefreshMTMVContext;
import org.apache.doris.nereids.DorisParser.RefreshMethodContext;
import org.apache.doris.nereids.DorisParser.RefreshScheduleContext;
import org.apache.doris.nereids.DorisParser.RefreshTableContext;
import org.apache.doris.nereids.DorisParser.RefreshTriggerContext;
import org.apache.doris.nereids.DorisParser.RegularQuerySpecificationContext;
import org.apache.doris.nereids.DorisParser.RelationContext;
import org.apache.doris.nereids.DorisParser.RelationHintContext;
import org.apache.doris.nereids.DorisParser.RenameClauseContext;
import org.apache.doris.nereids.DorisParser.RenameColumnClauseContext;
import org.apache.doris.nereids.DorisParser.RenamePartitionClauseContext;
import org.apache.doris.nereids.DorisParser.RenameRollupClauseContext;
import org.apache.doris.nereids.DorisParser.ReorderColumnsClauseContext;
import org.apache.doris.nereids.DorisParser.ReplaceContext;
import org.apache.doris.nereids.DorisParser.ReplacePartitionClauseContext;
import org.apache.doris.nereids.DorisParser.ReplaceTableClauseContext;
import org.apache.doris.nereids.DorisParser.ResumeMTMVContext;
import org.apache.doris.nereids.DorisParser.RollupDefContext;
import org.apache.doris.nereids.DorisParser.RollupDefsContext;
import org.apache.doris.nereids.DorisParser.RowConstructorContext;
import org.apache.doris.nereids.DorisParser.RowConstructorItemContext;
import org.apache.doris.nereids.DorisParser.SampleByPercentileContext;
import org.apache.doris.nereids.DorisParser.SampleByRowsContext;
import org.apache.doris.nereids.DorisParser.SampleContext;
import org.apache.doris.nereids.DorisParser.SelectClauseContext;
import org.apache.doris.nereids.DorisParser.SelectColumnClauseContext;
import org.apache.doris.nereids.DorisParser.SelectHintContext;
import org.apache.doris.nereids.DorisParser.SeparatorContext;
import org.apache.doris.nereids.DorisParser.SetCharsetContext;
import org.apache.doris.nereids.DorisParser.SetCollateContext;
import org.apache.doris.nereids.DorisParser.SetDefaultStorageVaultContext;
import org.apache.doris.nereids.DorisParser.SetLdapAdminPasswordContext;
import org.apache.doris.nereids.DorisParser.SetNamesContext;
import org.apache.doris.nereids.DorisParser.SetOperationContext;
import org.apache.doris.nereids.DorisParser.SetOptionsContext;
import org.apache.doris.nereids.DorisParser.SetPasswordContext;
import org.apache.doris.nereids.DorisParser.SetSystemVariableContext;
import org.apache.doris.nereids.DorisParser.SetTransactionContext;
import org.apache.doris.nereids.DorisParser.SetUserPropertiesContext;
import org.apache.doris.nereids.DorisParser.SetUserVariableContext;
import org.apache.doris.nereids.DorisParser.SetVariableWithTypeContext;
import org.apache.doris.nereids.DorisParser.ShowAllPropertiesContext;
import org.apache.doris.nereids.DorisParser.ShowAlterTableContext;
import org.apache.doris.nereids.DorisParser.ShowAnalyzeContext;
import org.apache.doris.nereids.DorisParser.ShowAnalyzeTaskContext;
import org.apache.doris.nereids.DorisParser.ShowAuthorsContext;
import org.apache.doris.nereids.DorisParser.ShowBackendsContext;
import org.apache.doris.nereids.DorisParser.ShowBackupContext;
import org.apache.doris.nereids.DorisParser.ShowBrokerContext;
import org.apache.doris.nereids.DorisParser.ShowBuildIndexContext;
import org.apache.doris.nereids.DorisParser.ShowCatalogRecycleBinContext;
import org.apache.doris.nereids.DorisParser.ShowCharsetContext;
import org.apache.doris.nereids.DorisParser.ShowClustersContext;
import org.apache.doris.nereids.DorisParser.ShowCollationContext;
import org.apache.doris.nereids.DorisParser.ShowColumnHistogramStatsContext;
import org.apache.doris.nereids.DorisParser.ShowColumnStatsContext;
import org.apache.doris.nereids.DorisParser.ShowColumnsContext;
import org.apache.doris.nereids.DorisParser.ShowConfigContext;
import org.apache.doris.nereids.DorisParser.ShowConstraintContext;
import org.apache.doris.nereids.DorisParser.ShowConvertLscContext;
import org.apache.doris.nereids.DorisParser.ShowCreateCatalogContext;
import org.apache.doris.nereids.DorisParser.ShowCreateDatabaseContext;
import org.apache.doris.nereids.DorisParser.ShowCreateFunctionContext;
import org.apache.doris.nereids.DorisParser.ShowCreateLoadContext;
import org.apache.doris.nereids.DorisParser.ShowCreateMTMVContext;
import org.apache.doris.nereids.DorisParser.ShowCreateMaterializedViewContext;
import org.apache.doris.nereids.DorisParser.ShowCreateProcedureContext;
import org.apache.doris.nereids.DorisParser.ShowCreateRepositoryContext;
import org.apache.doris.nereids.DorisParser.ShowCreateTableContext;
import org.apache.doris.nereids.DorisParser.ShowCreateViewContext;
import org.apache.doris.nereids.DorisParser.ShowDataSkewContext;
import org.apache.doris.nereids.DorisParser.ShowDataTypesContext;
import org.apache.doris.nereids.DorisParser.ShowDatabaseIdContext;
import org.apache.doris.nereids.DorisParser.ShowDeleteContext;
import org.apache.doris.nereids.DorisParser.ShowDiagnoseTabletContext;
import org.apache.doris.nereids.DorisParser.ShowDictionariesContext;
import org.apache.doris.nereids.DorisParser.ShowDynamicPartitionContext;
import org.apache.doris.nereids.DorisParser.ShowEncryptKeysContext;
import org.apache.doris.nereids.DorisParser.ShowEventsContext;
import org.apache.doris.nereids.DorisParser.ShowExportContext;
import org.apache.doris.nereids.DorisParser.ShowFrontendsContext;
import org.apache.doris.nereids.DorisParser.ShowFunctionsContext;
import org.apache.doris.nereids.DorisParser.ShowGlobalFunctionsContext;
import org.apache.doris.nereids.DorisParser.ShowGrantsContext;
import org.apache.doris.nereids.DorisParser.ShowGrantsForUserContext;
import org.apache.doris.nereids.DorisParser.ShowIndexAnalyzerContext;
import org.apache.doris.nereids.DorisParser.ShowIndexTokenFilterContext;
import org.apache.doris.nereids.DorisParser.ShowIndexTokenizerContext;
import org.apache.doris.nereids.DorisParser.ShowLastInsertContext;
import org.apache.doris.nereids.DorisParser.ShowLoadContext;
import org.apache.doris.nereids.DorisParser.ShowLoadProfileContext;
import org.apache.doris.nereids.DorisParser.ShowOpenTablesContext;
import org.apache.doris.nereids.DorisParser.ShowPartitionIdContext;
import org.apache.doris.nereids.DorisParser.ShowPartitionsContext;
import org.apache.doris.nereids.DorisParser.ShowPluginsContext;
import org.apache.doris.nereids.DorisParser.ShowPrivilegesContext;
import org.apache.doris.nereids.DorisParser.ShowProcContext;
import org.apache.doris.nereids.DorisParser.ShowProcedureStatusContext;
import org.apache.doris.nereids.DorisParser.ShowProcessListContext;
import org.apache.doris.nereids.DorisParser.ShowQueryProfileContext;
import org.apache.doris.nereids.DorisParser.ShowQueryStatsContext;
import org.apache.doris.nereids.DorisParser.ShowQueuedAnalyzeJobsContext;
import org.apache.doris.nereids.DorisParser.ShowReplicaDistributionContext;
import org.apache.doris.nereids.DorisParser.ShowRepositoriesContext;
import org.apache.doris.nereids.DorisParser.ShowResourcesContext;
import org.apache.doris.nereids.DorisParser.ShowRestoreContext;
import org.apache.doris.nereids.DorisParser.ShowRolesContext;
import org.apache.doris.nereids.DorisParser.ShowRowPolicyContext;
import org.apache.doris.nereids.DorisParser.ShowSmallFilesContext;
import org.apache.doris.nereids.DorisParser.ShowSnapshotContext;
import org.apache.doris.nereids.DorisParser.ShowSqlBlockRuleContext;
import org.apache.doris.nereids.DorisParser.ShowStagesContext;
import org.apache.doris.nereids.DorisParser.ShowStatusContext;
import org.apache.doris.nereids.DorisParser.ShowStorageEnginesContext;
import org.apache.doris.nereids.DorisParser.ShowStoragePolicyContext;
import org.apache.doris.nereids.DorisParser.ShowStorageVaultContext;
import org.apache.doris.nereids.DorisParser.ShowTableCreationContext;
import org.apache.doris.nereids.DorisParser.ShowTableIdContext;
import org.apache.doris.nereids.DorisParser.ShowTabletStorageFormatContext;
import org.apache.doris.nereids.DorisParser.ShowTabletsBelongContext;
import org.apache.doris.nereids.DorisParser.ShowTrashContext;
import org.apache.doris.nereids.DorisParser.ShowTriggersContext;
import org.apache.doris.nereids.DorisParser.ShowUserPropertiesContext;
import org.apache.doris.nereids.DorisParser.ShowVariablesContext;
import org.apache.doris.nereids.DorisParser.ShowViewContext;
import org.apache.doris.nereids.DorisParser.ShowWarningErrorCountContext;
import org.apache.doris.nereids.DorisParser.ShowWarningErrorsContext;
import org.apache.doris.nereids.DorisParser.ShowWhitelistContext;
import org.apache.doris.nereids.DorisParser.SimpleColumnDefContext;
import org.apache.doris.nereids.DorisParser.SimpleColumnDefsContext;
import org.apache.doris.nereids.DorisParser.SingleStatementContext;
import org.apache.doris.nereids.DorisParser.SortClauseContext;
import org.apache.doris.nereids.DorisParser.SortItemContext;
import org.apache.doris.nereids.DorisParser.SpecifiedPartitionContext;
import org.apache.doris.nereids.DorisParser.StarContext;
import org.apache.doris.nereids.DorisParser.StatementDefaultContext;
import org.apache.doris.nereids.DorisParser.StatementScopeContext;
import org.apache.doris.nereids.DorisParser.StepPartitionDefContext;
import org.apache.doris.nereids.DorisParser.StringLiteralContext;
import org.apache.doris.nereids.DorisParser.StructLiteralContext;
import org.apache.doris.nereids.DorisParser.SubqueryContext;
import org.apache.doris.nereids.DorisParser.SubqueryExpressionContext;
import org.apache.doris.nereids.DorisParser.SupportedUnsetStatementContext;
import org.apache.doris.nereids.DorisParser.SwitchCatalogContext;
import org.apache.doris.nereids.DorisParser.SyncContext;
import org.apache.doris.nereids.DorisParser.SystemVariableContext;
import org.apache.doris.nereids.DorisParser.TableAliasContext;
import org.apache.doris.nereids.DorisParser.TableNameContext;
import org.apache.doris.nereids.DorisParser.TableSnapshotContext;
import org.apache.doris.nereids.DorisParser.TableValuedFunctionContext;
import org.apache.doris.nereids.DorisParser.TabletListContext;
import org.apache.doris.nereids.DorisParser.TrimContext;
import org.apache.doris.nereids.DorisParser.TypeConstructorContext;
import org.apache.doris.nereids.DorisParser.UninstallPluginContext;
import org.apache.doris.nereids.DorisParser.UnitIdentifierContext;
import org.apache.doris.nereids.DorisParser.UnlockTablesContext;
import org.apache.doris.nereids.DorisParser.UnsupportedStartTransactionContext;
import org.apache.doris.nereids.DorisParser.UpdateAssignmentContext;
import org.apache.doris.nereids.DorisParser.UpdateAssignmentSeqContext;
import org.apache.doris.nereids.DorisParser.UpdateContext;
import org.apache.doris.nereids.DorisParser.UseDatabaseContext;
import org.apache.doris.nereids.DorisParser.UserIdentifyContext;
import org.apache.doris.nereids.DorisParser.UserVariableContext;
import org.apache.doris.nereids.DorisParser.VariantContext;
import org.apache.doris.nereids.DorisParser.VariantPredefinedFieldsContext;
import org.apache.doris.nereids.DorisParser.VariantSubColTypeContext;
import org.apache.doris.nereids.DorisParser.VariantSubColTypeListContext;
import org.apache.doris.nereids.DorisParser.VariantTypeDefinitionsContext;
import org.apache.doris.nereids.DorisParser.WhereClauseContext;
import org.apache.doris.nereids.DorisParser.WindowFrameContext;
import org.apache.doris.nereids.DorisParser.WindowSpecContext;
import org.apache.doris.nereids.DorisParser.WithRemoteStorageSystemContext;
import org.apache.doris.nereids.DorisParserBaseVisitor;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundAlias;
import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.analyzer.UnboundInlineTable;
import org.apache.doris.nereids.analyzer.UnboundOneRowRelation;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.analyzer.UnboundResultSink;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.analyzer.UnboundStar;
import org.apache.doris.nereids.analyzer.UnboundTVFRelation;
import org.apache.doris.nereids.analyzer.UnboundTableSinkCreator;
import org.apache.doris.nereids.analyzer.UnboundVariable;
import org.apache.doris.nereids.analyzer.UnboundVariable.VariableType;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.exceptions.NotSupportedException;
import org.apache.doris.nereids.exceptions.ParseException;
import org.apache.doris.nereids.hint.DistributeHint;
import org.apache.doris.nereids.hint.JoinSkewInfo;
import org.apache.doris.nereids.load.NereidsDataDescription;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.properties.SelectHint;
import org.apache.doris.nereids.properties.SelectHintLeading;
import org.apache.doris.nereids.properties.SelectHintOrdered;
import org.apache.doris.nereids.properties.SelectHintSetVar;
import org.apache.doris.nereids.properties.SelectHintUseCboRule;
import org.apache.doris.nereids.properties.SelectHintUseMv;
import org.apache.doris.nereids.trees.TableSample;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.BitAnd;
import org.apache.doris.nereids.trees.expressions.BitNot;
import org.apache.doris.nereids.trees.expressions.BitOr;
import org.apache.doris.nereids.trees.expressions.BitXor;
import org.apache.doris.nereids.trees.expressions.CaseWhen;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.DefaultValueSlot;
import org.apache.doris.nereids.trees.expressions.Divide;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Exists;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.InSubquery;
import org.apache.doris.nereids.trees.expressions.IntegralDivide;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Like;
import org.apache.doris.nereids.trees.expressions.MatchAll;
import org.apache.doris.nereids.trees.expressions.MatchAny;
import org.apache.doris.nereids.trees.expressions.MatchPhrase;
import org.apache.doris.nereids.trees.expressions.MatchPhraseEdge;
import org.apache.doris.nereids.trees.expressions.MatchPhrasePrefix;
import org.apache.doris.nereids.trees.expressions.MatchRegexp;
import org.apache.doris.nereids.trees.expressions.Mod;
import org.apache.doris.nereids.trees.expressions.Multiply;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.OrderExpression;
import org.apache.doris.nereids.trees.expressions.Placeholder;
import org.apache.doris.nereids.trees.expressions.Properties;
import org.apache.doris.nereids.trees.expressions.Regexp;
import org.apache.doris.nereids.trees.expressions.ScalarSubquery;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.Subtract;
import org.apache.doris.nereids.trees.expressions.TimestampArithmetic;
import org.apache.doris.nereids.trees.expressions.WhenClause;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.WindowFrame;
import org.apache.doris.nereids.trees.expressions.functions.Function;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Array;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArraySlice;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Char;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ConvertTo;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CurrentDate;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CurrentTime;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CurrentUser;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ElementAt;
import org.apache.doris.nereids.trees.expressions.functions.scalar.EncryptKeyRef;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Lambda;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Now;
import org.apache.doris.nereids.trees.expressions.functions.scalar.SessionUser;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Xor;
import org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DecimalLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DecimalV3Literal;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Interval;
import org.apache.doris.nereids.trees.expressions.literal.LargeIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.MapLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.SmallIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StructLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.trees.plans.DistributeType;
import org.apache.doris.nereids.trees.plans.DistributeType.JoinDistributeType;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.LimitPhase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.Aggregate;
import org.apache.doris.nereids.trees.plans.algebra.InlineTable;
import org.apache.doris.nereids.trees.plans.algebra.OneRowRelation;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.commands.AddConstraintCommand;
import org.apache.doris.nereids.trees.plans.commands.AdminCancelRebalanceDiskCommand;
import org.apache.doris.nereids.trees.plans.commands.AdminCancelRepairTableCommand;
import org.apache.doris.nereids.trees.plans.commands.AdminCheckTabletsCommand;
import org.apache.doris.nereids.trees.plans.commands.AdminCleanTrashCommand;
import org.apache.doris.nereids.trees.plans.commands.AdminCompactTableCommand;
import org.apache.doris.nereids.trees.plans.commands.AdminCopyTabletCommand;
import org.apache.doris.nereids.trees.plans.commands.AdminRebalanceDiskCommand;
import org.apache.doris.nereids.trees.plans.commands.AdminRepairTableCommand;
import org.apache.doris.nereids.trees.plans.commands.AdminSetEncryptionRootKeyCommand;
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
import org.apache.doris.nereids.trees.plans.commands.AlterMTMVCommand;
import org.apache.doris.nereids.trees.plans.commands.AlterResourceCommand;
import org.apache.doris.nereids.trees.plans.commands.AlterRoleCommand;
import org.apache.doris.nereids.trees.plans.commands.AlterRoutineLoadCommand;
import org.apache.doris.nereids.trees.plans.commands.AlterSqlBlockRuleCommand;
import org.apache.doris.nereids.trees.plans.commands.AlterStoragePolicyCommand;
import org.apache.doris.nereids.trees.plans.commands.AlterStorageVaultCommand;
import org.apache.doris.nereids.trees.plans.commands.AlterSystemCommand;
import org.apache.doris.nereids.trees.plans.commands.AlterSystemRenameComputeGroupCommand;
import org.apache.doris.nereids.trees.plans.commands.AlterTableCommand;
import org.apache.doris.nereids.trees.plans.commands.AlterTableStatsCommand;
import org.apache.doris.nereids.trees.plans.commands.AlterUserCommand;
import org.apache.doris.nereids.trees.plans.commands.AlterViewCommand;
import org.apache.doris.nereids.trees.plans.commands.AlterWorkloadGroupCommand;
import org.apache.doris.nereids.trees.plans.commands.AlterWorkloadPolicyCommand;
import org.apache.doris.nereids.trees.plans.commands.AnalyzeDatabaseCommand;
import org.apache.doris.nereids.trees.plans.commands.AnalyzeTableCommand;
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
import org.apache.doris.nereids.trees.plans.commands.Constraint;
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
import org.apache.doris.nereids.trees.plans.commands.DropCatalogRecycleBinCommand.IdType;
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
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand.ExplainLevel;
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
import org.apache.doris.nereids.trees.plans.commands.UpdateCommand;
import org.apache.doris.nereids.trees.plans.commands.WarmUpClusterCommand;
import org.apache.doris.nereids.trees.plans.commands.alter.AlterDatabaseRenameCommand;
import org.apache.doris.nereids.trees.plans.commands.alter.AlterDatabaseSetQuotaCommand;
import org.apache.doris.nereids.trees.plans.commands.alter.AlterRepositoryCommand;
import org.apache.doris.nereids.trees.plans.commands.clean.CleanLabelCommand;
import org.apache.doris.nereids.trees.plans.commands.info.AddBackendOp;
import org.apache.doris.nereids.trees.plans.commands.info.AddBrokerOp;
import org.apache.doris.nereids.trees.plans.commands.info.AddColumnOp;
import org.apache.doris.nereids.trees.plans.commands.info.AddColumnsOp;
import org.apache.doris.nereids.trees.plans.commands.info.AddFollowerOp;
import org.apache.doris.nereids.trees.plans.commands.info.AddObserverOp;
import org.apache.doris.nereids.trees.plans.commands.info.AddPartitionOp;
import org.apache.doris.nereids.trees.plans.commands.info.AddRollupOp;
import org.apache.doris.nereids.trees.plans.commands.info.AlterLoadErrorUrlOp;
import org.apache.doris.nereids.trees.plans.commands.info.AlterMTMVInfo;
import org.apache.doris.nereids.trees.plans.commands.info.AlterMTMVPropertyInfo;
import org.apache.doris.nereids.trees.plans.commands.info.AlterMTMVRefreshInfo;
import org.apache.doris.nereids.trees.plans.commands.info.AlterMTMVRenameInfo;
import org.apache.doris.nereids.trees.plans.commands.info.AlterMTMVReplaceInfo;
import org.apache.doris.nereids.trees.plans.commands.info.AlterMultiPartitionOp;
import org.apache.doris.nereids.trees.plans.commands.info.AlterSystemOp;
import org.apache.doris.nereids.trees.plans.commands.info.AlterTableOp;
import org.apache.doris.nereids.trees.plans.commands.info.AlterUserInfo;
import org.apache.doris.nereids.trees.plans.commands.info.AlterViewInfo;
import org.apache.doris.nereids.trees.plans.commands.info.BranchOptions;
import org.apache.doris.nereids.trees.plans.commands.info.BuildIndexOp;
import org.apache.doris.nereids.trees.plans.commands.info.CancelMTMVTaskInfo;
import org.apache.doris.nereids.trees.plans.commands.info.ColocateGroupName;
import org.apache.doris.nereids.trees.plans.commands.info.ColumnDefinition;
import org.apache.doris.nereids.trees.plans.commands.info.CopyFromDesc;
import org.apache.doris.nereids.trees.plans.commands.info.CopyIntoInfo;
import org.apache.doris.nereids.trees.plans.commands.info.CreateIndexOp;
import org.apache.doris.nereids.trees.plans.commands.info.CreateJobInfo;
import org.apache.doris.nereids.trees.plans.commands.info.CreateMTMVInfo;
import org.apache.doris.nereids.trees.plans.commands.info.CreateOrReplaceBranchOp;
import org.apache.doris.nereids.trees.plans.commands.info.CreateOrReplaceTagOp;
import org.apache.doris.nereids.trees.plans.commands.info.CreateResourceInfo;
import org.apache.doris.nereids.trees.plans.commands.info.CreateRoutineLoadInfo;
import org.apache.doris.nereids.trees.plans.commands.info.CreateTableInfo;
import org.apache.doris.nereids.trees.plans.commands.info.CreateTableLikeInfo;
import org.apache.doris.nereids.trees.plans.commands.info.CreateUserInfo;
import org.apache.doris.nereids.trees.plans.commands.info.CreateViewInfo;
import org.apache.doris.nereids.trees.plans.commands.info.DMLCommandType;
import org.apache.doris.nereids.trees.plans.commands.info.DecommissionBackendOp;
import org.apache.doris.nereids.trees.plans.commands.info.DefaultValue;
import org.apache.doris.nereids.trees.plans.commands.info.DictionaryColumnDefinition;
import org.apache.doris.nereids.trees.plans.commands.info.DistributionDescriptor;
import org.apache.doris.nereids.trees.plans.commands.info.DropAllBrokerOp;
import org.apache.doris.nereids.trees.plans.commands.info.DropBackendOp;
import org.apache.doris.nereids.trees.plans.commands.info.DropBranchOp;
import org.apache.doris.nereids.trees.plans.commands.info.DropBrokerOp;
import org.apache.doris.nereids.trees.plans.commands.info.DropColumnOp;
import org.apache.doris.nereids.trees.plans.commands.info.DropDatabaseInfo;
import org.apache.doris.nereids.trees.plans.commands.info.DropFollowerOp;
import org.apache.doris.nereids.trees.plans.commands.info.DropIndexOp;
import org.apache.doris.nereids.trees.plans.commands.info.DropMTMVInfo;
import org.apache.doris.nereids.trees.plans.commands.info.DropObserverOp;
import org.apache.doris.nereids.trees.plans.commands.info.DropPartitionFromIndexOp;
import org.apache.doris.nereids.trees.plans.commands.info.DropPartitionOp;
import org.apache.doris.nereids.trees.plans.commands.info.DropRollupOp;
import org.apache.doris.nereids.trees.plans.commands.info.DropTagOp;
import org.apache.doris.nereids.trees.plans.commands.info.EnableFeatureOp;
import org.apache.doris.nereids.trees.plans.commands.info.FixedRangePartition;
import org.apache.doris.nereids.trees.plans.commands.info.FuncNameInfo;
import org.apache.doris.nereids.trees.plans.commands.info.FunctionArgTypesInfo;
import org.apache.doris.nereids.trees.plans.commands.info.GeneratedColumnDesc;
import org.apache.doris.nereids.trees.plans.commands.info.InPartition;
import org.apache.doris.nereids.trees.plans.commands.info.IndexDefinition;
import org.apache.doris.nereids.trees.plans.commands.info.LabelNameInfo;
import org.apache.doris.nereids.trees.plans.commands.info.LessThanPartition;
import org.apache.doris.nereids.trees.plans.commands.info.LockTableInfo;
import org.apache.doris.nereids.trees.plans.commands.info.MTMVPartitionDefinition;
import org.apache.doris.nereids.trees.plans.commands.info.ModifyBackendOp;
import org.apache.doris.nereids.trees.plans.commands.info.ModifyColumnCommentOp;
import org.apache.doris.nereids.trees.plans.commands.info.ModifyColumnOp;
import org.apache.doris.nereids.trees.plans.commands.info.ModifyDistributionOp;
import org.apache.doris.nereids.trees.plans.commands.info.ModifyEngineOp;
import org.apache.doris.nereids.trees.plans.commands.info.ModifyFrontendOrBackendHostNameOp;
import org.apache.doris.nereids.trees.plans.commands.info.ModifyFrontendOrBackendHostNameOp.ModifyOpType;
import org.apache.doris.nereids.trees.plans.commands.info.ModifyPartitionOp;
import org.apache.doris.nereids.trees.plans.commands.info.ModifyTableCommentOp;
import org.apache.doris.nereids.trees.plans.commands.info.ModifyTablePropertiesOp;
import org.apache.doris.nereids.trees.plans.commands.info.PartitionDefinition;
import org.apache.doris.nereids.trees.plans.commands.info.PartitionDefinition.MaxValue;
import org.apache.doris.nereids.trees.plans.commands.info.PartitionNamesInfo;
import org.apache.doris.nereids.trees.plans.commands.info.PartitionTableInfo;
import org.apache.doris.nereids.trees.plans.commands.info.PauseMTMVInfo;
import org.apache.doris.nereids.trees.plans.commands.info.RefreshMTMVInfo;
import org.apache.doris.nereids.trees.plans.commands.info.RenameColumnOp;
import org.apache.doris.nereids.trees.plans.commands.info.RenamePartitionOp;
import org.apache.doris.nereids.trees.plans.commands.info.RenameRollupOp;
import org.apache.doris.nereids.trees.plans.commands.info.RenameTableOp;
import org.apache.doris.nereids.trees.plans.commands.info.ReorderColumnsOp;
import org.apache.doris.nereids.trees.plans.commands.info.ReplacePartitionOp;
import org.apache.doris.nereids.trees.plans.commands.info.ReplaceTableOp;
import org.apache.doris.nereids.trees.plans.commands.info.ResumeMTMVInfo;
import org.apache.doris.nereids.trees.plans.commands.info.RollupDefinition;
import org.apache.doris.nereids.trees.plans.commands.info.SetCharsetAndCollateVarOp;
import org.apache.doris.nereids.trees.plans.commands.info.SetLdapPassVarOp;
import org.apache.doris.nereids.trees.plans.commands.info.SetNamesVarOp;
import org.apache.doris.nereids.trees.plans.commands.info.SetPassVarOp;
import org.apache.doris.nereids.trees.plans.commands.info.SetSessionVarOp;
import org.apache.doris.nereids.trees.plans.commands.info.SetUserDefinedVarOp;
import org.apache.doris.nereids.trees.plans.commands.info.SetUserPropertyVarOp;
import org.apache.doris.nereids.trees.plans.commands.info.SetVarOp;
import org.apache.doris.nereids.trees.plans.commands.info.ShowCreateMTMVInfo;
import org.apache.doris.nereids.trees.plans.commands.info.SimpleColumnDefinition;
import org.apache.doris.nereids.trees.plans.commands.info.StepPartition;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.nereids.trees.plans.commands.info.TableRefInfo;
import org.apache.doris.nereids.trees.plans.commands.info.TagOptions;
import org.apache.doris.nereids.trees.plans.commands.info.WarmUpItem;
import org.apache.doris.nereids.trees.plans.commands.insert.BatchInsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertOverwriteTableCommand;
import org.apache.doris.nereids.trees.plans.commands.load.CreateRoutineLoadCommand;
import org.apache.doris.nereids.trees.plans.commands.load.LoadColumnClause;
import org.apache.doris.nereids.trees.plans.commands.load.LoadColumnDesc;
import org.apache.doris.nereids.trees.plans.commands.load.LoadDeleteOnClause;
import org.apache.doris.nereids.trees.plans.commands.load.LoadPartitionNames;
import org.apache.doris.nereids.trees.plans.commands.load.LoadPrecedingFilterClause;
import org.apache.doris.nereids.trees.plans.commands.load.LoadProperty;
import org.apache.doris.nereids.trees.plans.commands.load.LoadSeparator;
import org.apache.doris.nereids.trees.plans.commands.load.LoadSequenceClause;
import org.apache.doris.nereids.trees.plans.commands.load.LoadWhereClause;
import org.apache.doris.nereids.trees.plans.commands.load.MysqlDataDescription;
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
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTE;
import org.apache.doris.nereids.trees.plans.logical.LogicalExcept;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalGenerate;
import org.apache.doris.nereids.trees.plans.logical.LogicalHaving;
import org.apache.doris.nereids.trees.plans.logical.LogicalIntersect;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPreAggOnHint;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalQualify;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.trees.plans.logical.LogicalSelectHint;
import org.apache.doris.nereids.trees.plans.logical.LogicalSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.trees.plans.logical.LogicalUsingJoin;
import org.apache.doris.nereids.types.AggStateType;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DateType;
import org.apache.doris.nereids.types.DateV2Type;
import org.apache.doris.nereids.types.LargeIntType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.types.VariantField;
import org.apache.doris.nereids.types.VariantType;
import org.apache.doris.nereids.types.coercion.CharacterType;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.RelationUtil;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.policy.FilterType;
import org.apache.doris.policy.PolicyTypeEnum;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SqlModeHelper;
import org.apache.doris.resource.workloadschedpolicy.WorkloadActionMeta;
import org.apache.doris.resource.workloadschedpolicy.WorkloadConditionMeta;
import org.apache.doris.statistics.AnalysisInfo;
import org.apache.doris.system.NodeType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.RuleNode;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Build a logical plan tree with unbounded nodes.
 */
@SuppressWarnings({"OptionalUsedAsFieldOrParameterType", "OptionalGetWithoutIsPresent"})
public class LogicalPlanBuilder extends DorisParserBaseVisitor<Object> {
    private static String JOB_NAME = "jobName";
    private static String TASK_ID = "taskId";

    // Sort the parameters with token position to keep the order with original placeholders
    // in prepared statement.Otherwise, the order maybe broken
    private final Map<Token, Placeholder> tokenPosToParameters = Maps.newTreeMap((pos1, pos2) -> {
        int line = pos1.getLine() - pos2.getLine();
        if (line != 0) {
            return line;
        }
        return pos1.getCharPositionInLine() - pos2.getCharPositionInLine();
    });

    private final Map<Integer, ParserRuleContext> selectHintMap;

    public LogicalPlanBuilder(Map<Integer, ParserRuleContext> selectHintMap) {
        this.selectHintMap = selectHintMap;
    }

    @SuppressWarnings("unchecked")
    protected <T> T typedVisit(ParseTree ctx) {
        return (T) ctx.accept(this);
    }

    /**
     * Override the default behavior for all visit methods. This will only return a non-null result
     * when the context has only one child. This is done because there is no generic method to
     * combine the results of the context children. In all other cases null is returned.
     */
    @Override
    public Object visitChildren(RuleNode node) {
        if (node.getChildCount() == 1) {
            return node.getChild(0).accept(this);
        } else {
            return null;
        }
    }

    @Override
    public LogicalPlan visitSingleStatement(SingleStatementContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> (LogicalPlan) visit(ctx.statement()));
    }

    @Override
    public Expression visitExpressionWithEof(ExpressionWithEofContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> typedVisit(ctx.expression()));
    }

    @Override
    public LogicalPlan visitStatementDefault(StatementDefaultContext ctx) {
        LogicalPlan plan = plan(ctx.query());
        if (ctx.outFileClause() != null) {
            plan = withOutFile(plan, ctx.outFileClause());
        } else {
            plan = new UnboundResultSink<>(plan);
        }
        return withExplain(plan, ctx.explain());
    }

    @Override
    public LogicalPlan visitCreateScheduledJob(DorisParser.CreateScheduledJobContext ctx) {
        Optional<String> label = ctx.label == null ? Optional.empty() : Optional.of(ctx.label.getText());
        Optional<String> atTime = ctx.atTime == null ? Optional.empty() : Optional.of(ctx.atTime.getText());
        Optional<Boolean> immediateStartOptional = ctx.CURRENT_TIMESTAMP() == null ? Optional.of(false) :
                Optional.of(true);
        Optional<String> startTime = ctx.startTime == null ? Optional.empty() : Optional.of(ctx.startTime.getText());
        Optional<String> endsTime = ctx.endsTime == null ? Optional.empty() : Optional.of(ctx.endsTime.getText());
        Optional<Long> interval = ctx.timeInterval == null ? Optional.empty() :
                Optional.of(Long.valueOf(ctx.timeInterval.getText()));
        Optional<String> intervalUnit = ctx.timeUnit == null ? Optional.empty() : Optional.of(ctx.timeUnit.getText());
        String comment =
                visitCommentSpec(ctx.commentSpec());
        String executeSql = getOriginSql(ctx.supportedDmlStatement());
        CreateJobInfo createJobInfo = new CreateJobInfo(label, atTime, interval, intervalUnit, startTime,
                endsTime, immediateStartOptional, comment, executeSql);
        return new CreateJobCommand(createJobInfo);
    }

    private void checkJobNameKey(String key, String keyFormat, DorisParser.SupportedJobStatementContext parseContext) {
        if (key.isEmpty() || !key.equalsIgnoreCase(keyFormat)) {
            throw new ParseException(keyFormat + " should be: '" + keyFormat + "'", parseContext);
        }
    }

    @Override
    public LogicalPlan visitPauseJob(DorisParser.PauseJobContext ctx) {
        checkJobNameKey(stripQuotes(ctx.jobNameKey.getText()), JOB_NAME, ctx);
        return new PauseJobCommand(stripQuotes(ctx.jobNameValue.getText()));
    }

    @Override
    public LogicalPlan visitDropJob(DorisParser.DropJobContext ctx) {
        checkJobNameKey(stripQuotes(ctx.jobNameKey.getText()), JOB_NAME, ctx);
        boolean ifExists = ctx.EXISTS() != null;
        return new DropJobCommand(stripQuotes(ctx.jobNameValue.getText()), ifExists);
    }

    @Override
    public LogicalPlan visitResumeJob(DorisParser.ResumeJobContext ctx) {
        checkJobNameKey(stripQuotes(ctx.jobNameKey.getText()), JOB_NAME, ctx);
        return new ResumeJobCommand(stripQuotes(ctx.jobNameValue.getText()));
    }

    @Override
    public LogicalPlan visitCancelJobTask(DorisParser.CancelJobTaskContext ctx) {
        checkJobNameKey(stripQuotes(ctx.jobNameKey.getText()), JOB_NAME, ctx);
        checkJobNameKey(stripQuotes(ctx.taskIdKey.getText()), TASK_ID, ctx);
        String jobName = stripQuotes(ctx.jobNameValue.getText());
        Long taskId = Long.valueOf(ctx.taskIdValue.getText());
        return new CancelJobTaskCommand(jobName, taskId);
    }

    private StageAndPattern getStageAndPattern(DorisParser.StageAndPatternContext ctx) {
        String stage = ctx.stage == null ? StageUtil.INTERNAL_STAGE : stripQuotes(ctx.stage.getText());
        if (ctx.pattern != null) {
            return new StageAndPattern(stage, stripQuotes(ctx.pattern.getText()));
        } else {
            return new StageAndPattern(stage, null);
        }
    }

    @Override
    public LogicalPlan visitCopyInto(DorisParser.CopyIntoContext ctx) {
        ImmutableList.Builder<String> tableName = ImmutableList.builder();
        if (null != ctx.name) {
            List<String> nameParts = visitMultipartIdentifier(ctx.name);
            tableName.addAll(nameParts);
        }
        List<String> columns = (null != ctx.columns) ? visitIdentifierList(ctx.columns) : null;
        StageAndPattern stageAndPattern = getStageAndPattern(ctx.stageAndPattern());
        CopyFromDesc copyFromDesc = null;
        if (null != ctx.SELECT()) {
            List<NamedExpression> projects = getNamedExpressions(ctx.selectColumnClause().namedExpressionSeq());
            Optional<Expression> where = Optional.empty();
            if (ctx.whereClause() != null) {
                where = Optional.of(getExpression(ctx.whereClause().booleanExpression()));
            }
            copyFromDesc = new CopyFromDesc(stageAndPattern, projects, where);
        } else {
            copyFromDesc = new CopyFromDesc(stageAndPattern);
        }
        Map<String, String> properties = visitPropertyClause(ctx.properties);
        copyFromDesc.setTargetColumns(columns);
        CopyIntoInfo copyInfoInfo = null;
        if (null != ctx.selectHint()) {
            if ((selectHintMap == null) || selectHintMap.isEmpty()) {
                throw new AnalysisException("hint should be in right place: " + ctx.getText());
            }
            List<ParserRuleContext> selectHintContexts = Lists.newArrayList();
            for (Integer key : selectHintMap.keySet()) {
                if (key > ctx.getStart().getStopIndex() && key < ctx.getStop().getStartIndex()) {
                    selectHintContexts.add(selectHintMap.get(key));
                }
            }
            if (selectHintContexts.size() != 1) {
                throw new AnalysisException("only one hint is allowed in: " + ctx.getText());
            }
            SelectHintContext selectHintContext = (SelectHintContext) selectHintContexts.get(0);
            Map<String, String> parameterNames = Maps.newLinkedHashMap();
            for (HintStatementContext hintStatement : selectHintContext.hintStatements) {
                String hintName = hintStatement.hintName().getText().toLowerCase(Locale.ROOT);
                if (!hintName.equalsIgnoreCase("set_var")) {
                    throw new AnalysisException("only set_var hint is allowed in: " + ctx.getText());
                }
                for (HintAssignmentContext kv : hintStatement.parameters) {
                    if (kv.key != null) {
                        String parameterName = visitIdentifierOrText(kv.key);
                        Optional<String> value = Optional.empty();
                        if (kv.constantValue != null) {
                            Literal literal = (Literal) visit(kv.constantValue);
                            value = Optional.ofNullable(literal.toLegacyLiteral().getStringValue());
                        } else if (kv.identifierValue != null) {
                            // maybe we should throw exception when the identifierValue is quoted identifier
                            value = Optional.ofNullable(kv.identifierValue.getText());
                        }
                        parameterNames.put(parameterName, value.get());
                    }
                }
            }
            Map<String, Map<String, String>> setVarHint = Maps.newLinkedHashMap();
            setVarHint.put("set_var", parameterNames);
            copyInfoInfo = new CopyIntoInfo(tableName.build(), copyFromDesc, properties, setVarHint);
        } else {
            copyInfoInfo = new CopyIntoInfo(tableName.build(), copyFromDesc, properties, null);
        }

        return new CopyIntoCommand(copyInfoInfo);
    }

    @Override
    public String visitCommentSpec(DorisParser.CommentSpecContext ctx) {
        String commentSpec = ctx == null ? "''" : ctx.STRING_LITERAL().getText();
        return LogicalPlanBuilderAssistant.escapeBackSlash(commentSpec.substring(1, commentSpec.length() - 1));
    }

    /**
     * This function may be used in some task like InsertTask, RefreshDictionary, etc. the target could be many type of
     * tables.
     */
    @Override
    public LogicalPlan visitInsertTable(InsertTableContext ctx) {
        boolean isOverwrite = ctx.INTO() == null;
        ImmutableList.Builder<String> tableName = ImmutableList.builder();
        if (null != ctx.tableName) {
            List<String> nameParts = visitMultipartIdentifier(ctx.tableName);
            tableName.addAll(nameParts);
        } else if (null != ctx.tableId) {
            // process group commit insert table command send by be
            TableName name = Env.getCurrentEnv().getCurrentCatalog()
                    .getTableNameByTableId(Long.valueOf(ctx.tableId.getText()));
            tableName.add(name.getDb());
            tableName.add(name.getTbl());
            ConnectContext.get().setDatabase(name.getDb());
        } else {
            throw new ParseException("tableName and tableId cannot both be null");
        }
        Optional<String> labelName = (ctx.labelName == null) ? Optional.empty() : Optional.of(ctx.labelName.getText());
        Optional<String> branchName = Optional.empty();
        if (ctx.optSpecBranch() != null) {
            branchName = Optional.of(ctx.optSpecBranch().name.getText());
        }
        List<String> colNames = ctx.cols == null ? ImmutableList.of() : visitIdentifierList(ctx.cols);
        // TODO visit partitionSpecCtx
        LogicalPlan plan = visitQuery(ctx.query());
        // partitionSpec may be NULL. means auto detect partition. only available when IOT
        Pair<Boolean, List<String>> partitionSpec = visitPartitionSpec(ctx.partitionSpec());
        // partitionSpec.second :
        // null - auto detect
        // zero - whole table
        // others - specific partitions
        boolean isAutoDetect = partitionSpec.second == null;
        LogicalSink<?> sink = UnboundTableSinkCreator.createUnboundTableSinkMaybeOverwrite(
                tableName.build(),
                colNames,
                ImmutableList.of(), // hints
                partitionSpec.first, // isTemp
                partitionSpec.second, // partition names
                isAutoDetect,
                isOverwrite,
                ConnectContext.get().getSessionVariable().isEnableUniqueKeyPartialUpdate(),
                ConnectContext.get().getSessionVariable().getPartialUpdateNewRowPolicy(),
                ctx.tableId == null ? DMLCommandType.INSERT : DMLCommandType.GROUP_COMMIT,
                plan);
        Optional<LogicalPlan> cte = Optional.empty();
        if (ctx.cte() != null) {
            cte = Optional.ofNullable(withCte(plan, ctx.cte()));
        }
        LogicalPlan command;
        if (isOverwrite) {
            command = new InsertOverwriteTableCommand(sink, labelName, cte, branchName);
        } else {
            if (ConnectContext.get() != null && ConnectContext.get().isTxnModel()
                    && sink.child() instanceof InlineTable
                    && sink.child().getExpressions().stream().allMatch(Expression::isConstant)) {
                // FIXME: In legacy, the `insert into select 1` is handled as `insert into values`.
                //  In nereids, the original way is throw an AnalysisException and fallback to legacy.
                //  Now handle it as `insert into select`(a separate load job), should fix it as the legacy.
                command = new BatchInsertIntoTableCommand(sink);
            } else {
                command = new InsertIntoTableCommand(sink, labelName, Optional.empty(), cte, true, branchName);
            }
        }
        return withExplain(command, ctx.explain());
    }

    /**
     * return a pair, first will be true if partitions is temp partition, select is a list to present partition list.
     */
    @Override
    public Pair<Boolean, List<String>> visitPartitionSpec(PartitionSpecContext ctx) {
        List<String> partitions = ImmutableList.of();
        boolean temporaryPartition = false;
        if (ctx != null) {
            temporaryPartition = ctx.TEMPORARY() != null;
            if (ctx.ASTERISK() != null) {
                partitions = null;
            } else if (ctx.partition != null) {
                partitions = ImmutableList.of(ctx.partition.getText());
            } else {
                partitions = visitIdentifierList(ctx.partitions);
            }
        }
        return Pair.of(temporaryPartition, partitions);
    }

    @Override
    public Command visitCreateMTMV(CreateMTMVContext ctx) {
        if (ctx.buildMode() == null && ctx.refreshMethod() == null && ctx.refreshTrigger() == null
                && ctx.cols == null && ctx.keys == null
                && ctx.HASH() == null && ctx.RANDOM() == null && ctx.BUCKETS() == null) {
            return visitCreateSyncMvCommand(ctx);
        }

        List<String> nameParts = visitMultipartIdentifier(ctx.mvName);
        BuildMode buildMode = visitBuildMode(ctx.buildMode());
        RefreshMethod refreshMethod = visitRefreshMethod(ctx.refreshMethod());
        MTMVRefreshTriggerInfo refreshTriggerInfo = visitRefreshTrigger(ctx.refreshTrigger());
        LogicalPlan logicalPlan = visitQuery(ctx.query());
        String querySql = getOriginSql(ctx.query());

        int bucketNum = FeConstants.default_bucket_num;
        if (ctx.INTEGER_VALUE() != null) {
            bucketNum = Integer.parseInt(ctx.INTEGER_VALUE().getText());
        }
        DistributionDescriptor desc;
        if (ctx.HASH() != null) {
            desc = new DistributionDescriptor(true, ctx.AUTO() != null, bucketNum,
                    visitIdentifierList(ctx.hashKeys));
        } else {
            desc = new DistributionDescriptor(false, ctx.AUTO() != null, bucketNum, null);
        }

        Map<String, String> properties = ctx.propertyClause() != null
                ? Maps.newHashMap(visitPropertyClause(ctx.propertyClause())) : Maps.newHashMap();
        String comment = ctx.STRING_LITERAL() == null ? "" : LogicalPlanBuilderAssistant.escapeBackSlash(
                ctx.STRING_LITERAL().getText().substring(1, ctx.STRING_LITERAL().getText().length() - 1));

        return new CreateMTMVCommand(new CreateMTMVInfo(ctx.EXISTS() != null, new TableNameInfo(nameParts),
                ctx.keys != null ? visitIdentifierList(ctx.keys) : ImmutableList.of(),
                comment,
                desc, properties, logicalPlan, querySql,
                new MTMVRefreshInfo(buildMode, refreshMethod, refreshTriggerInfo),
                ctx.cols == null ? Lists.newArrayList() : visitSimpleColumnDefs(ctx.cols),
                visitMTMVPartitionInfo(ctx.mvPartition())
        ));
    }

    private Command visitCreateSyncMvCommand(CreateMTMVContext ctx) {
        List<String> nameParts = visitMultipartIdentifier(ctx.mvName);
        LogicalPlan logicalPlan = new UnboundResultSink<>(visitQuery(ctx.query()));
        Map<String, String> properties = ctx.propertyClause() != null
                ? Maps.newHashMap(visitPropertyClause(ctx.propertyClause())) : Maps.newHashMap();
        return new CreateMaterializedViewCommand(new TableNameInfo(nameParts), logicalPlan, properties);
    }

    /**
     * get MTMVPartitionDefinition
     *
     * @param ctx MvPartitionContext
     * @return MTMVPartitionDefinition
     */
    public MTMVPartitionDefinition visitMTMVPartitionInfo(MvPartitionContext ctx) {
        MTMVPartitionDefinition mtmvPartitionDefinition = new MTMVPartitionDefinition();
        if (ctx == null) {
            mtmvPartitionDefinition.setPartitionType(MTMVPartitionType.SELF_MANAGE);
        } else if (ctx.partitionKey != null) {
            mtmvPartitionDefinition.setPartitionType(MTMVPartitionType.FOLLOW_BASE_TABLE);
            mtmvPartitionDefinition.setPartitionCol(ctx.partitionKey.getText());
        } else {
            mtmvPartitionDefinition.setPartitionType(MTMVPartitionType.EXPR);
            Expression functionCallExpression = visitFunctionCallExpression(ctx.partitionExpr);
            mtmvPartitionDefinition.setFunctionCallExpression(functionCallExpression);
        }
        return mtmvPartitionDefinition;
    }

    @Override
    public List<SimpleColumnDefinition> visitSimpleColumnDefs(SimpleColumnDefsContext ctx) {
        if (ctx == null) {
            return null;
        }
        return ctx.cols.stream()
                .map(this::visitSimpleColumnDef)
                .collect(ImmutableList.toImmutableList());
    }

    @Override
    public SimpleColumnDefinition visitSimpleColumnDef(SimpleColumnDefContext ctx) {
        String comment = ctx.STRING_LITERAL() == null ? "" : LogicalPlanBuilderAssistant.escapeBackSlash(
                ctx.STRING_LITERAL().getText().substring(1, ctx.STRING_LITERAL().getText().length() - 1));
        return new SimpleColumnDefinition(ctx.colName.getText(), comment);
    }

    /**
     * get originSql
     *
     * @param ctx context
     * @return originSql
     */
    public String getOriginSql(ParserRuleContext ctx) {
        int startIndex = ctx.start.getStartIndex();
        int stopIndex = ctx.stop.getStopIndex();
        org.antlr.v4.runtime.misc.Interval interval = new org.antlr.v4.runtime.misc.Interval(startIndex, stopIndex);
        return ctx.start.getInputStream().getText(interval);
    }

    @Override
    public MTMVRefreshTriggerInfo visitRefreshTrigger(RefreshTriggerContext ctx) {
        if (ctx == null) {
            return new MTMVRefreshTriggerInfo(RefreshTrigger.MANUAL);
        }
        if (ctx.MANUAL() != null) {
            return new MTMVRefreshTriggerInfo(RefreshTrigger.MANUAL);
        }
        if (ctx.COMMIT() != null) {
            return new MTMVRefreshTriggerInfo(RefreshTrigger.COMMIT);
        }
        if (ctx.SCHEDULE() != null) {
            return new MTMVRefreshTriggerInfo(RefreshTrigger.SCHEDULE, visitRefreshSchedule(ctx.refreshSchedule()));
        }
        return new MTMVRefreshTriggerInfo(RefreshTrigger.MANUAL);
    }

    @Override
    public ReplayCommand visitReplay(DorisParser.ReplayContext ctx) {
        if (ctx.replayCommand().replayType().DUMP() != null) {
            LogicalPlan plan = plan(ctx.replayCommand().replayType().query());
            return new ReplayCommand(PlanType.REPLAY_COMMAND, null, plan, ReplayCommand.ReplayType.DUMP);
        } else if (ctx.replayCommand().replayType().PLAY() != null) {
            String tmpPath = ctx.replayCommand().replayType().filePath.getText();
            String path = LogicalPlanBuilderAssistant.escapeBackSlash(tmpPath.substring(1, tmpPath.length() - 1));
            return new ReplayCommand(PlanType.REPLAY_COMMAND, path, null, ReplayCommand.ReplayType.PLAY);
        }
        return null;
    }

    @Override
    public MTMVRefreshSchedule visitRefreshSchedule(RefreshScheduleContext ctx) {
        int interval = Integer.parseInt(ctx.INTEGER_VALUE().getText());
        String startTime = ctx.STARTS() == null ? null
                : ctx.STRING_LITERAL().getText().substring(1, ctx.STRING_LITERAL().getText().length() - 1);
        IntervalUnit unit = visitMvRefreshUnit(ctx.refreshUnit);
        return new MTMVRefreshSchedule(startTime, interval, unit);
    }

    /**
     * get IntervalUnit,only enable_job_schedule_second_for_test is true, can use second
     *
     * @param ctx ctx
     * @return IntervalUnit
     */
    public IntervalUnit visitMvRefreshUnit(IdentifierContext ctx) {
        IntervalUnit intervalUnit = IntervalUnit.fromString(ctx.getText().toUpperCase());
        if (null == intervalUnit) {
            throw new AnalysisException("interval time unit can not be " + ctx.getText());
        }
        if (intervalUnit.equals(IntervalUnit.SECOND)
                && !Config.enable_job_schedule_second_for_test) {
            throw new AnalysisException("interval time unit can not be second");
        }
        return intervalUnit;
    }

    @Override
    public RefreshMethod visitRefreshMethod(RefreshMethodContext ctx) {
        if (ctx == null) {
            return RefreshMethod.AUTO;
        }
        return RefreshMethod.valueOf(ctx.getText().toUpperCase());
    }

    @Override
    public BuildMode visitBuildMode(BuildModeContext ctx) {
        if (ctx == null) {
            return BuildMode.IMMEDIATE;
        }
        if (ctx.DEFERRED() != null) {
            return BuildMode.DEFERRED;
        } else if (ctx.IMMEDIATE() != null) {
            return BuildMode.IMMEDIATE;
        }
        return BuildMode.IMMEDIATE;
    }

    @Override
    public RefreshMTMVCommand visitRefreshMTMV(RefreshMTMVContext ctx) {
        List<String> nameParts = visitMultipartIdentifier(ctx.mvName);
        List<String> partitions = ImmutableList.of();
        if (ctx.partitionSpec() != null) {
            if (ctx.partitionSpec().TEMPORARY() != null) {
                throw new AnalysisException("Not allowed to specify TEMPORARY ");
            }
            if (ctx.partitionSpec().partition != null) {
                partitions = ImmutableList.of(ctx.partitionSpec().partition.getText());
            } else {
                partitions = visitIdentifierList(ctx.partitionSpec().partitions);
            }
        }
        return new RefreshMTMVCommand(new RefreshMTMVInfo(new TableNameInfo(nameParts),
                partitions, ctx.COMPLETE() != null));
    }

    private DropMTMVCommand visitDropMTMV(DropMVContext ctx) {
        List<String> nameParts = visitMultipartIdentifier(ctx.mvName);
        return new DropMTMVCommand(new DropMTMVInfo(new TableNameInfo(nameParts), ctx.EXISTS() != null));
    }

    private DropMaterializedViewCommand visitDropSyncMv(DropMVContext ctx) {
        List<String> tableNameParts = visitMultipartIdentifier(ctx.tableName);
        List<String> mvNameParts = visitMultipartIdentifier(ctx.mvName);
        if (mvNameParts == null || mvNameParts.size() != 1) {
            throw new AnalysisException("The mvName name must be a single identifier");
        }
        return new DropMaterializedViewCommand(new TableNameInfo(tableNameParts), ctx.EXISTS() != null,
                mvNameParts.get(0));
    }

    @Override
    public Command visitDropMV(DropMVContext ctx) {
        if (ctx.tableName != null) {
            return visitDropSyncMv(ctx);
        } else {
            return visitDropMTMV(ctx);
        }
    }

    @Override
    public PauseMTMVCommand visitPauseMTMV(PauseMTMVContext ctx) {
        List<String> nameParts = visitMultipartIdentifier(ctx.mvName);
        return new PauseMTMVCommand(new PauseMTMVInfo(new TableNameInfo(nameParts)));
    }

    @Override
    public ResumeMTMVCommand visitResumeMTMV(ResumeMTMVContext ctx) {
        List<String> nameParts = visitMultipartIdentifier(ctx.mvName);
        return new ResumeMTMVCommand(new ResumeMTMVInfo(new TableNameInfo(nameParts)));
    }

    @Override
    public ShowCreateMTMVCommand visitShowCreateMTMV(ShowCreateMTMVContext ctx) {
        List<String> nameParts = visitMultipartIdentifier(ctx.mvName);
        return new ShowCreateMTMVCommand(new ShowCreateMTMVInfo(new TableNameInfo(nameParts)));
    }

    @Override
    public CancelExportCommand visitCancelExport(DorisParser.CancelExportContext ctx) {
        String databaseName = null;
        if (ctx.database != null) {
            databaseName = stripQuotes(ctx.database.getText());
        }
        Expression wildWhere = null;
        if (ctx.wildWhere() != null) {
            wildWhere = getWildWhere(ctx.wildWhere());
        }
        return new CancelExportCommand(databaseName, wildWhere);
    }

    @Override
    public CancelLoadCommand visitCancelLoad(DorisParser.CancelLoadContext ctx) {
        String databaseName = null;
        if (ctx.database != null) {
            databaseName = stripQuotes(ctx.database.getText());
        }
        Expression wildWhere = null;
        if (ctx.wildWhere() != null) {
            wildWhere = getWildWhere(ctx.wildWhere());
        }
        return new CancelLoadCommand(databaseName, wildWhere);
    }

    @Override
    public CancelWarmUpJobCommand visitCancelWarmUpJob(DorisParser.CancelWarmUpJobContext ctx) {
        Expression wildWhere = null;
        if (ctx.wildWhere() != null) {
            wildWhere = getWildWhere(ctx.wildWhere());
        }
        return new CancelWarmUpJobCommand(wildWhere);
    }

    @Override
    public CancelMTMVTaskCommand visitCancelMTMVTask(CancelMTMVTaskContext ctx) {
        List<String> nameParts = visitMultipartIdentifier(ctx.mvName);
        long taskId = Long.parseLong(ctx.taskId.getText());
        return new CancelMTMVTaskCommand(new CancelMTMVTaskInfo(new TableNameInfo(nameParts), taskId));
    }

    @Override
    public AdminCompactTableCommand visitAdminCompactTable(AdminCompactTableContext ctx) {
        TableRefInfo tableRefInfo = visitBaseTableRefContext(ctx.baseTableRef());
        EqualTo equalTo = null;
        if (ctx.WHERE() != null) {
            StringLiteral left = new StringLiteral(stripQuotes(ctx.TYPE().getText()));
            StringLiteral right = new StringLiteral(stripQuotes(ctx.STRING_LITERAL().getText()));
            equalTo = new EqualTo(left, right);
        }
        return new AdminCompactTableCommand(tableRefInfo, equalTo);
    }

    @Override
    public AlterMTMVCommand visitAlterMTMV(AlterMTMVContext ctx) {
        List<String> nameParts = visitMultipartIdentifier(ctx.mvName);
        TableNameInfo mvName = new TableNameInfo(nameParts);
        AlterMTMVInfo alterMTMVInfo = null;
        if (ctx.RENAME() != null) {
            alterMTMVInfo = new AlterMTMVRenameInfo(mvName, ctx.newName.getText());
        } else if (ctx.REFRESH() != null) {
            MTMVRefreshInfo refreshInfo = new MTMVRefreshInfo();
            if (ctx.refreshMethod() != null) {
                refreshInfo.setRefreshMethod(visitRefreshMethod(ctx.refreshMethod()));
            }
            if (ctx.refreshTrigger() != null) {
                refreshInfo.setRefreshTriggerInfo(visitRefreshTrigger(ctx.refreshTrigger()));
            }
            alterMTMVInfo = new AlterMTMVRefreshInfo(mvName, refreshInfo);
        } else if (ctx.SET() != null) {
            alterMTMVInfo = new AlterMTMVPropertyInfo(mvName,
                    Maps.newHashMap(visitPropertyItemList(ctx.fileProperties)));
        } else if (ctx.REPLACE() != null) {
            String newName = ctx.newName.getText();
            Map<String, String> properties = ctx.propertyClause() != null
                    ? Maps.newHashMap(visitPropertyClause(ctx.propertyClause())) : Maps.newHashMap();
            alterMTMVInfo = new AlterMTMVReplaceInfo(mvName, newName, properties);
        }
        return new AlterMTMVCommand(alterMTMVInfo);
    }

    @Override
    public Object visitUnsupportedStartTransaction(UnsupportedStartTransactionContext ctx) {
        return new StartTransactionCommand();
    }

    @Override
    public LogicalPlan visitAlterView(AlterViewContext ctx) {
        List<String> nameParts = visitMultipartIdentifier(ctx.name);
        String comment = ctx.commentSpec() == null ? null : visitCommentSpec(ctx.commentSpec());
        AlterViewInfo info;
        if (comment != null) {
            info = new AlterViewInfo(new TableNameInfo(nameParts), comment);
        } else {
            String querySql = getOriginSql(ctx.query());
            if (ctx.STRING_LITERAL() != null) {
                comment = ctx.STRING_LITERAL().getText();
                comment = LogicalPlanBuilderAssistant.escapeBackSlash(comment.substring(1, comment.length() - 1));
            }
            info = new AlterViewInfo(new TableNameInfo(nameParts), comment, querySql,
                    ctx.cols == null ? Lists.newArrayList() : visitSimpleColumnDefs(ctx.cols));
        }
        return new AlterViewCommand(info);
    }

    @Override
    public LogicalPlan visitAlterStorageVault(AlterStorageVaultContext ctx) {
        List<String> nameParts = this.visitMultipartIdentifier(ctx.name);
        String vaultName = nameParts.get(0);
        Map<String, String> properties = this.visitPropertyClause(ctx.properties);
        return new AlterStorageVaultCommand(vaultName, properties);
    }

    @Override
    public LogicalPlan visitAlterSystemRenameComputeGroup(AlterSystemRenameComputeGroupContext ctx) {
        return new AlterSystemRenameComputeGroupCommand(ctx.name.getText(), ctx.newName.getText());
    }

    @Override
    public LogicalPlan visitAdminSetEncryptionRootKey(AdminSetEncryptionRootKeyContext ctx) {
        Map<String, String> properties = visitPropertyItemList(ctx.propertyItemList());
        return new AdminSetEncryptionRootKeyCommand(properties);
    }

    @Override
    public LogicalPlan visitShowConstraint(ShowConstraintContext ctx) {
        List<String> parts = visitMultipartIdentifier(ctx.table);
        return new ShowConstraintsCommand(parts);
    }

    @Override
    public LogicalPlan visitAddConstraint(AddConstraintContext ctx) {
        List<String> parts = visitMultipartIdentifier(ctx.table);
        UnboundRelation curTable = new UnboundRelation(StatementScopeIdGenerator.newRelationId(), parts);
        ImmutableList<Slot> slots = ctx.constraint().slots.identifierSeq().ident.stream()
                .map(ident -> new UnboundSlot(ident.getText()))
                .collect(ImmutableList.toImmutableList());
        Constraint constraint;
        if (ctx.constraint().UNIQUE() != null) {
            constraint = Constraint.newUniqueConstraint(curTable, slots);
        } else if (ctx.constraint().PRIMARY() != null) {
            constraint = Constraint.newPrimaryKeyConstraint(curTable, slots);
        } else if (ctx.constraint().FOREIGN() != null) {
            ImmutableList<Slot> referencedSlots = ctx.constraint().referencedSlots.identifierSeq().ident.stream()
                    .map(ident -> new UnboundSlot(ident.getText()))
                    .collect(ImmutableList.toImmutableList());
            List<String> nameParts = visitMultipartIdentifier(ctx.constraint().referenceTable);
            LogicalPlan referenceTable = new UnboundRelation(
                    StatementScopeIdGenerator.newRelationId(),
                    nameParts
            );
            constraint = Constraint.newForeignKeyConstraint(curTable, slots, referenceTable, referencedSlots);
        } else {
            throw new AnalysisException("Unsupported constraint " + ctx.getText());
        }
        return new AddConstraintCommand(ctx.constraintName.getText().toLowerCase(), constraint);
    }

    @Override
    public LogicalPlan visitDropConstraint(DropConstraintContext ctx) {
        List<String> parts = visitMultipartIdentifier(ctx.table);
        UnboundRelation curTable = new UnboundRelation(StatementScopeIdGenerator.newRelationId(), parts);
        return new DropConstraintCommand(ctx.constraintName.getText().toLowerCase(), curTable);
    }

    @Override
    public LogicalPlan visitUpdate(UpdateContext ctx) {
        LogicalPlan query = LogicalPlanBuilderAssistant.withCheckPolicy(
                new UnboundRelation(
                        StatementScopeIdGenerator.newRelationId(),
                        visitMultipartIdentifier(ctx.tableName)
                )
        );
        query = withTableAlias(query, ctx.tableAlias());
        if (ctx.fromClause() != null) {
            query = withRelations(query, ctx.fromClause().relations().relation());
        }
        query = withFilter(query, Optional.ofNullable(ctx.whereClause()));
        String tableAlias = null;
        if (ctx.tableAlias().strictIdentifier() != null) {
            tableAlias = ctx.tableAlias().getText();
        }
        Optional<LogicalPlan> cte = Optional.empty();
        if (ctx.cte() != null) {
            cte = Optional.ofNullable(withCte(query, ctx.cte()));
        }
        return withExplain(new UpdateCommand(visitMultipartIdentifier(ctx.tableName), tableAlias,
                visitUpdateAssignmentSeq(ctx.updateAssignmentSeq()), query, cte), ctx.explain());
    }

    @Override
    public LogicalPlan visitDelete(DeleteContext ctx) {
        List<String> tableName = visitMultipartIdentifier(ctx.tableName);
        Pair<Boolean, List<String>> partitionSpec = visitPartitionSpec(ctx.partitionSpec());
        // TODO: now dont support delete auto detect partition.
        if (partitionSpec == null) {
            throw new ParseException("Now don't support auto detect partitions in deleting", ctx);
        }
        LogicalPlan query = withTableAlias(LogicalPlanBuilderAssistant.withCheckPolicy(
                new UnboundRelation(
                        StatementScopeIdGenerator.newRelationId(), tableName,
                        partitionSpec.second, partitionSpec.first)), ctx.tableAlias()
        );
        String tableAlias = null;
        if (ctx.tableAlias().strictIdentifier() != null) {
            tableAlias = ctx.tableAlias().getText();
        }

        Command deleteCommand;
        if (ctx.USING() == null && ctx.cte() == null) {
            query = withFilter(query, Optional.ofNullable(ctx.whereClause()));
            deleteCommand = new DeleteFromCommand(tableName, tableAlias, partitionSpec.first,
                    partitionSpec.second, query);
        } else {
            // convert to insert into select
            if (ctx.USING() != null) {
                query = withRelations(query, ctx.relations().relation());
            }
            query = withFilter(query, Optional.ofNullable(ctx.whereClause()));
            Optional<LogicalPlan> cte = Optional.empty();
            if (ctx.cte() != null) {
                cte = Optional.ofNullable(withCte(query, ctx.cte()));
            }
            deleteCommand = new DeleteFromUsingCommand(tableName, tableAlias,
                    partitionSpec.first, partitionSpec.second, query, cte);
        }
        if (ctx.explain() != null) {
            return withExplain(deleteCommand, ctx.explain());
        } else {
            return deleteCommand;
        }
    }

    @Override
    public LogicalPlan visitExport(ExportContext ctx) {
        // TODO: replace old class name like ExportStmt, BrokerDesc, Expr with new nereid class name
        List<String> tableName = visitMultipartIdentifier(ctx.tableName);
        List<String> partitions = ctx.partition == null ? ImmutableList.of() : visitIdentifierList(ctx.partition);

        // handle path string
        String tmpPath = ctx.filePath.getText();
        String path = LogicalPlanBuilderAssistant.escapeBackSlash(tmpPath.substring(1, tmpPath.length() - 1));

        Optional<Expression> expr = Optional.empty();
        if (ctx.whereClause() != null) {
            expr = Optional.of(getExpression(ctx.whereClause().booleanExpression()));
        }

        Map<String, String> filePropertiesMap = ImmutableMap.of();
        if (ctx.propertyClause() != null) {
            filePropertiesMap = visitPropertyClause(ctx.propertyClause());
        }

        Optional<BrokerDesc> brokerDesc = Optional.empty();
        if (ctx.withRemoteStorageSystem() != null) {
            brokerDesc = Optional.ofNullable(visitWithRemoteStorageSystem(ctx.withRemoteStorageSystem()));
        }
        return new ExportCommand(tableName, partitions, expr, path, filePropertiesMap, brokerDesc);
    }

    @Override
    public Map<String, String> visitPropertyClause(PropertyClauseContext ctx) {
        return ctx == null ? ImmutableMap.of() : visitPropertyItemList(ctx.fileProperties);
    }

    @Override
    public Map<String, String> visitPropertyItemList(PropertyItemListContext ctx) {
        if (ctx == null || ctx.properties == null) {
            return ImmutableMap.of();
        }
        Builder<String, String> propertiesMap = ImmutableMap.builder();
        for (PropertyItemContext argument : ctx.properties) {
            String key = parsePropertyKey(argument.key);
            String value = parsePropertyValue(argument.value);
            propertiesMap.put(key, value);
        }
        return propertiesMap.build();
    }

    @Override
    public BrokerDesc visitWithRemoteStorageSystem(WithRemoteStorageSystemContext ctx) {
        BrokerDesc brokerDesc = null;

        Map<String, String> brokerPropertiesMap = visitPropertyItemList(ctx.brokerProperties);

        if (ctx.S3() != null) {
            brokerDesc = new BrokerDesc("S3", StorageBackend.StorageType.S3, brokerPropertiesMap);
        } else if (ctx.HDFS() != null) {
            brokerDesc = new BrokerDesc("HDFS", StorageBackend.StorageType.HDFS, brokerPropertiesMap);
        } else if (ctx.LOCAL() != null) {
            brokerDesc = new BrokerDesc("HDFS", StorageBackend.StorageType.LOCAL, brokerPropertiesMap);
        } else if (ctx.BROKER() != null) {
            brokerDesc = new BrokerDesc(visitIdentifierOrText(ctx.brokerName), brokerPropertiesMap);
        }
        return brokerDesc;
    }

    @Override
    public ResourceDesc visitResourceDesc(DorisParser.ResourceDescContext ctx) {
        if (ctx == null) {
            return null;
        }

        Map<String, String> resourcePropertiesMap = visitPropertyItemList(ctx.propertyItemList());
        String resourceName = visitIdentifierOrText(ctx.resourceName);
        return new ResourceDesc(resourceName, resourcePropertiesMap);
    }

    /**
     * Visit multi-statements.
     */
    @Override
    public List<Pair<LogicalPlan, StatementContext>> visitMultiStatements(MultiStatementsContext ctx) {
        List<Pair<LogicalPlan, StatementContext>> logicalPlans = Lists.newArrayList();
        for (DorisParser.StatementContext statement : ctx.statement()) {
            StatementContext statementContext = new StatementContext();
            ConnectContext connectContext = ConnectContext.get();
            if (connectContext != null) {
                connectContext.setStatementContext(statementContext);
                statementContext.setConnectContext(connectContext);
            }
            logicalPlans.add(Pair.of(
                    ParserUtils.withOrigin(ctx, () -> (LogicalPlan) visit(statement)), statementContext));
            List<Placeholder> params = new ArrayList<>(tokenPosToParameters.values());
            statementContext.setPlaceholders(params);
            tokenPosToParameters.clear();
        }
        return logicalPlans;
    }

    /**
     * Visit load-statements.
     */
    @Override
    public LogicalPlan visitLoad(DorisParser.LoadContext ctx) {
        BrokerDesc brokerDesc = null;
        if (ctx.withRemoteStorageSystem() != null) {
            brokerDesc = visitWithRemoteStorageSystem(ctx.withRemoteStorageSystem());
        }

        ResourceDesc resourceDesc = null;
        if (ctx.withRemoteStorageSystem() != null && ctx.withRemoteStorageSystem().resourceDesc() != null) {
            resourceDesc = visitResourceDesc(ctx.withRemoteStorageSystem().resourceDesc());
        }

        List<NereidsDataDescription> dataDescriptions = new ArrayList<>();
        List<String> labelParts = visitMultipartIdentifier(ctx.lableName);
        String labelName = null;
        String labelDbName = null;
        if (ConnectContext.get() != null && ConnectContext.get().getDatabase() != null
                && ConnectContext.get().getDatabase().isEmpty() && labelParts.size() == 1) {
            throw new AnalysisException("Current database is not set.");
        } else if (labelParts.size() == 1) {
            labelName = labelParts.get(0);
        } else if (labelParts.size() == 2) {
            labelDbName = labelParts.get(0);
            labelName = labelParts.get(1);
        } else if (labelParts.size() == 3) {
            labelDbName = labelParts.get(1);
            labelName = labelParts.get(2);
        } else {
            throw new AnalysisException("labelParts in load should be [ctl.][db.]label");
        }

        for (DorisParser.DataDescContext ddc : ctx.dataDescs) {
            List<String> nameParts = Lists.newArrayList();
            if (labelDbName != null) {
                nameParts.add(labelDbName);
            }
            nameParts.add(ddc.targetTableName.getText());
            List<String> fullTableName = RelationUtil.getQualifierName(ConnectContext.get(), nameParts);
            // fullTableName is always [catalog],[db],[table]
            String tableName = fullTableName.get(2);
            List<String> colNames = (ddc.columns == null ? null : visitIdentifierList(ddc.columns));
            List<String> columnsFromPath = (ddc.columnsFromPath == null ? null
                    : visitIdentifierList(ddc.columnsFromPath.identifierList()));
            // we need a mutable list in NereidsDataDescription's co constructor
            List<String> mutableColNames = colNames == null ? null : new ArrayList<>(colNames);
            List<String> mutableColumnsFromPath = columnsFromPath == null ? null : new ArrayList<>(columnsFromPath);

            PartitionNames partitionNames = null;
            if (ddc.partitionSpec() != null) {
                Pair<Boolean, List<String>> partitionSpec = visitPartitionSpec(ddc.partitionSpec());
                partitionNames = new PartitionNames(partitionSpec.first, partitionSpec.second);
            }
            // TODO: multi location
            List<String> multiFilePaths = new ArrayList<>();
            for (Token filePath : ddc.filePaths) {
                multiFilePaths.add(filePath.getText().substring(1, filePath.getText().length() - 1));
            }
            List<String> filePaths = ddc.filePath == null ? null : multiFilePaths;
            List<Expression> colMappings;
            if (ddc.columnMapping == null) {
                colMappings = null;
            } else {
                colMappings = new ArrayList<>();
                for (DorisParser.MappingExprContext mappingExpr : ddc.columnMapping.mappingSet) {
                    String colName = mappingExpr.mappingCol.getText();
                    UnboundSlot colSlot = new UnboundSlot(Lists.newArrayList(colName));
                    Expression expr = getExpression(mappingExpr.expression());
                    EqualTo equalTo = new EqualTo(colSlot, expr);
                    colMappings.add(equalTo);
                }
            }

            LoadTask.MergeType mergeType = ddc.mergeType() == null ? LoadTask.MergeType.APPEND
                    : LoadTask.MergeType.valueOf(ddc.mergeType().getText());

            String fileFormat = ddc.format == null ? null : visitIdentifierOrText(ddc.format);
            String compressType = ddc.compressType == null ? null : visitIdentifierOrText(ddc.compressType);

            // separator
            String lineDelimiter = ddc.separator == null
                    ? null : LogicalPlanBuilderAssistant.escapeBackSlash(
                            ddc.separator.getText().substring(1, ddc.separator.getText().length() - 1));
            String columnSeparator = ddc.comma == null
                    ? null : LogicalPlanBuilderAssistant.escapeBackSlash(
                            ddc.comma.getText().substring(1, ddc.comma.getText().length() - 1));
            String srcTable = ddc.sourceTableName == null ? null : ddc.sourceTableName.getText();
            Map<String, String> dataProperties = ddc.propertyClause() == null ? new HashMap<>()
                    : visitPropertyClause(ddc.propertyClause());
            dataDescriptions.add(new NereidsDataDescription(
                    tableName,
                    partitionNames,
                    filePaths,
                    mutableColNames,
                    new Separator(columnSeparator),
                    new Separator(lineDelimiter),
                    fileFormat,
                    compressType,
                    srcTable,
                    mutableColumnsFromPath,
                    false,
                    colMappings,
                    ddc.preFilter == null ? null : getExpression(ddc.preFilter.expression()),
                    ddc.where == null ? null : getExpression(ddc.where.booleanExpression()),
                    mergeType,
                    ddc.deleteOn == null ? null : getExpression(ddc.deleteOn.expression()),
                    ddc.sequenceColumn == null ? null : ddc.sequenceColumn.identifier().getText(),
                    dataProperties));
        }
        Map<String, String> properties = Collections.emptyMap();
        if (ctx.propertyClause() != null) {
            properties = visitPropertyItemList(ctx.propertyClause().propertyItemList());
        }
        Map<String, String> mutableProperties = new HashMap<>(properties);
        String commentSpec = ctx.commentSpec() == null ? "''" : ctx.commentSpec().STRING_LITERAL().getText();
        String comment =
                LogicalPlanBuilderAssistant.escapeBackSlash(commentSpec.substring(1, commentSpec.length() - 1));
        return new LoadCommand(new LabelName(labelDbName, labelName), dataDescriptions, brokerDesc,
                resourceDesc, mutableProperties, comment);
    }

    /* ********************************************************************************************
     * Plan parsing
     * ******************************************************************************************** */

    /**
     * process lateral view, add a {@link LogicalGenerate} on plan.
     */
    protected LogicalPlan withGenerate(LogicalPlan plan, LateralViewContext ctx) {
        if (ctx.LATERAL() == null) {
            return plan;
        }
        String generateName = ctx.tableName.getText();
        // if later view explode map type, we need to add a project to convert map to struct
        String columnName = ctx.columnNames.get(0).getText();
        List<String> expandColumnNames = ImmutableList.of();

        // explode can pass multiple columns
        // then use struct to return the result of the expansion of multiple columns.
        if (ctx.columnNames.size() > 1
                || BuiltinTableGeneratingFunctions.INSTANCE.getReturnManyColumnFunctions()
                .contains(ctx.functionName.getText())) {
            columnName = ConnectContext.get() != null
                    ? ConnectContext.get().getStatementContext().generateColumnName() : "expand_cols";
            expandColumnNames = ctx.columnNames.stream()
                    .map(RuleContext::getText).collect(ImmutableList.toImmutableList());
        }
        String functionName = ctx.functionName.getText();
        List<Expression> arguments = ctx.expression().stream()
                .<Expression>map(this::typedVisit)
                .collect(ImmutableList.toImmutableList());
        Function unboundFunction = new UnboundFunction(functionName, arguments);
        return new LogicalGenerate<>(ImmutableList.of(unboundFunction),
                ImmutableList.of(new UnboundSlot(generateName, columnName)),
                ImmutableList.of(expandColumnNames), plan);
    }

    /**
     * process CTE and store the results in a logical plan node LogicalCTE
     */
    private LogicalPlan withCte(LogicalPlan plan, CteContext ctx) {
        if (ctx == null) {
            return plan;
        }
        return new LogicalCTE<>((List) visit(ctx.aliasQuery(), LogicalSubQueryAlias.class), plan);
    }

    /**
     * process CTE's alias queries and column aliases
     */
    @Override
    public LogicalSubQueryAlias<Plan> visitAliasQuery(AliasQueryContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            LogicalPlan queryPlan = plan(ctx.query());
            Optional<List<String>> columnNames = optionalVisit(ctx.columnAliases(), () ->
                    ctx.columnAliases().identifier().stream()
                            .map(RuleContext::getText)
                            .collect(ImmutableList.toImmutableList())
            );
            return new LogicalSubQueryAlias<>(ctx.identifier().getText(), columnNames, queryPlan);
        });
    }

    /**
     * process LoadProperty in routine load
     */
    public LoadProperty visitLoadProperty(LoadPropertyContext ctx) {
        LoadProperty loadProperty = null;
        if (ctx instanceof SeparatorContext) {
            String separator = stripQuotes(((SeparatorContext) ctx).STRING_LITERAL().getText());
            loadProperty = new LoadSeparator(separator);
        } else if (ctx instanceof ImportColumnsContext) {
            List<LoadColumnDesc> descList = new ArrayList<>();
            for (DorisParser.ImportColumnDescContext loadColumnDescCtx : ((ImportColumnsContext) ctx)
                    .importColumnsStatement().importColumnDesc()) {
                LoadColumnDesc desc;
                if (loadColumnDescCtx.booleanExpression() != null) {
                    desc = new LoadColumnDesc(loadColumnDescCtx.name.getText(),
                            getExpression(loadColumnDescCtx.booleanExpression()));
                } else {
                    desc = new LoadColumnDesc(loadColumnDescCtx.name.getText());
                }
                descList.add(desc);
            }
            loadProperty = new LoadColumnClause(descList);
        } else if (ctx instanceof ImportDeleteOnContext) {
            loadProperty = new LoadDeleteOnClause(getExpression(((ImportDeleteOnContext) ctx)
                    .importDeleteOnStatement().booleanExpression()));
        } else if (ctx instanceof ImportPartitionsContext) {
            Pair<Boolean, List<String>> partitionSpec = visitPartitionSpec(
                    ((ImportPartitionsContext) ctx).partitionSpec());
            loadProperty = new LoadPartitionNames(partitionSpec.first, partitionSpec.second);
        } else if (ctx instanceof ImportPrecedingFilterContext) {
            loadProperty = new LoadPrecedingFilterClause(getExpression(((ImportPrecedingFilterContext) ctx)
                    .importPrecedingFilterStatement().booleanExpression()));
        } else if (ctx instanceof ImportSequenceContext) {
            loadProperty = new LoadSequenceClause(((ImportSequenceContext) ctx)
                    .importSequenceStatement().identifier().getText());
        } else if (ctx instanceof ImportWhereContext) {
            loadProperty = new LoadWhereClause(getExpression(((ImportWhereContext) ctx)
                    .importWhereStatement().booleanExpression()));
        }
        return loadProperty;
    }

    @Override
    public LogicalPlan visitCreateRoutineLoad(CreateRoutineLoadContext ctx) {
        List<String> labelParts = visitMultipartIdentifier(ctx.label);
        String labelName = null;
        String labelDbName = null;
        if (ConnectContext.get().getDatabase().isEmpty() && labelParts.size() == 1) {
            throw new AnalysisException("Current database is not set.");
        } else if (labelParts.size() == 1) {
            labelName = labelParts.get(0);
        } else if (labelParts.size() == 2) {
            labelDbName = labelParts.get(0);
            labelName = labelParts.get(1);
        } else {
            throw new AnalysisException("labelParts in load should be [db.]label");
        }
        LabelNameInfo jobLabelInfo = new LabelNameInfo(labelDbName, labelName);
        String tableName = null;
        if (ctx.table != null) {
            tableName = ctx.table.getText();
        }
        Map<String, String> properties = ctx.propertyClause() != null
                // NOTICE: we should not generate immutable map here, because it will be modified when analyzing.
                ? Maps.newHashMap(visitPropertyClause(ctx.propertyClause()))
                : Maps.newHashMap();
        String type = ctx.type.getText();
        Map<String, String> customProperties = ctx.customProperties != null
                // NOTICE: we should not generate immutable map here, because it will be modified when analyzing.
                ? Maps.newHashMap(visitPropertyItemList(ctx.customProperties))
                : Maps.newHashMap();
        LoadTask.MergeType mergeType = LoadTask.MergeType.APPEND;
        if (ctx.WITH() != null) {
            if (ctx.DELETE() != null) {
                mergeType = LoadTask.MergeType.DELETE;
            } else if (ctx.MERGE() != null) {
                mergeType = LoadTask.MergeType.MERGE;
            }
        }
        String comment = visitCommentSpec(ctx.commentSpec());
        Map<String, LoadProperty> loadPropertyMap = new HashMap<>();
        for (DorisParser.LoadPropertyContext oneLoadPropertyCOntext : ctx.loadProperty()) {
            LoadProperty loadProperty = visitLoadProperty(oneLoadPropertyCOntext);
            if (loadProperty == null) {
                throw new AnalysisException("invalid clause of routine load");
            }
            if (loadPropertyMap.get(loadProperty.getClass().getName()) != null) {
                throw new AnalysisException("repeat setting of clause load property: "
                        + loadProperty.getClass().getName());
            } else {
                loadPropertyMap.put(loadProperty.getClass().getName(), loadProperty);
            }
        }
        CreateRoutineLoadInfo createRoutineLoadInfo = new CreateRoutineLoadInfo(jobLabelInfo, tableName,
                loadPropertyMap, properties, type, customProperties, mergeType, comment);
        return new CreateRoutineLoadCommand(createRoutineLoadInfo);

    }

    @Override
    public Command visitCreateRowPolicy(CreateRowPolicyContext ctx) {
        FilterType filterType = FilterType.of(ctx.type.getText());
        List<String> nameParts = visitMultipartIdentifier(ctx.table);
        return new CreatePolicyCommand(PolicyTypeEnum.ROW, ctx.name.getText(),
                ctx.EXISTS() != null, new TableNameInfo(nameParts), Optional.of(filterType),
                ctx.user == null ? null : visitUserIdentify(ctx.user),
                ctx.roleName == null ? null : ctx.roleName.getText(),
                Optional.of(getExpression(ctx.booleanExpression())), ImmutableMap.of());
    }

    @Override
    public Command visitCreateStoragePolicy(CreateStoragePolicyContext ctx) {
        Map<String, String> properties = ctx.properties != null
                ? Maps.newHashMap(visitPropertyClause(ctx.properties))
                : Maps.newHashMap();
        return new CreatePolicyCommand(PolicyTypeEnum.STORAGE, ctx.name.getText(),
                ctx.EXISTS() != null, null, Optional.empty(),
                null, null, Optional.empty(), properties);
    }

    @Override
    public String visitIdentifierOrText(DorisParser.IdentifierOrTextContext ctx) {
        if (ctx.STRING_LITERAL() != null) {
            return ctx.STRING_LITERAL().getText().substring(1, ctx.STRING_LITERAL().getText().length() - 1);
        } else {
            return ctx.identifier().getText();
        }
    }

    @Override
    public String visitIdentifierOrTextOrAsterisk(DorisParser.IdentifierOrTextOrAsteriskContext ctx) {
        if (ctx.ASTERISK() != null) {
            return stripQuotes(ctx.ASTERISK().getText());
        } else if (ctx.STRING_LITERAL() != null) {
            return stripQuotes(ctx.STRING_LITERAL().getText());
        } else {
            return stripQuotes(ctx.identifier().getText());
        }
    }

    @Override
    public List<String> visitMultipartIdentifierOrAsterisk(DorisParser.MultipartIdentifierOrAsteriskContext ctx) {
        return ctx.parts.stream()
                .map(RuleContext::getText)
                .collect(ImmutableList.toImmutableList());
    }

    @Override
    public UserIdentity visitUserIdentify(UserIdentifyContext ctx) {
        String user = visitIdentifierOrText(ctx.user);
        String host = null;
        if (ctx.host != null) {
            host = visitIdentifierOrText(ctx.host);
        }
        if (host == null) {
            host = "%";
        }
        boolean isDomain = ctx.LEFT_PAREN() != null;
        return new UserIdentity(user, host, isDomain);
    }

    @Override
    public LogicalPlan visitQuery(QueryContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            // TODO: need to add withQueryResultClauses and withCTE
            LogicalPlan query = plan(ctx.queryTerm());
            query = withQueryOrganization(query, ctx.queryOrganization());
            return withCte(query, ctx.cte());
        });
    }

    @Override
    public LogicalPlan visitSetOperation(SetOperationContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {

            if (ctx.UNION() != null) {
                Qualifier qualifier = getQualifier(ctx);
                List<QueryTermContext> contexts = Lists.newArrayList(ctx.right);
                QueryTermContext current = ctx.left;
                while (true) {
                    if (current instanceof SetOperationContext
                            && getQualifier((SetOperationContext) current) == qualifier
                            && ((SetOperationContext) current).UNION() != null) {
                        contexts.add(((SetOperationContext) current).right);
                        current = ((SetOperationContext) current).left;
                    } else {
                        contexts.add(current);
                        break;
                    }
                }
                Collections.reverse(contexts);
                List<LogicalPlan> logicalPlans = contexts.stream().map(this::plan).collect(Collectors.toList());
                return reduceToLogicalPlanTree(0, logicalPlans.size() - 1, logicalPlans, qualifier);
            } else {
                LogicalPlan leftQuery = plan(ctx.left);
                LogicalPlan rightQuery = plan(ctx.right);
                Qualifier qualifier = getQualifier(ctx);

                List<Plan> newChildren = ImmutableList.of(leftQuery, rightQuery);
                LogicalPlan plan;
                if (ctx.UNION() != null) {
                    plan = new LogicalUnion(qualifier, newChildren);
                } else if (ctx.EXCEPT() != null || ctx.MINUS() != null) {
                    plan = new LogicalExcept(qualifier, newChildren);
                } else if (ctx.INTERSECT() != null) {
                    plan = new LogicalIntersect(qualifier, newChildren);
                } else {
                    throw new ParseException("not support", ctx);
                }
                return plan;
            }
        });
    }

    private Qualifier getQualifier(SetOperationContext ctx) {
        if (ctx.setQuantifier() == null || ctx.setQuantifier().DISTINCT() != null) {
            return Qualifier.DISTINCT;
        } else {
            return Qualifier.ALL;
        }
    }

    private static LogicalPlan logicalPlanCombiner(LogicalPlan left, LogicalPlan right, Qualifier qualifier) {
        return new LogicalUnion(qualifier, ImmutableList.of(left, right));
    }

    /**
     * construct avl union tree
     */
    public static LogicalPlan reduceToLogicalPlanTree(int low, int high,
            List<LogicalPlan> logicalPlans, Qualifier qualifier) {
        switch (high - low) {
            case 0:
                return logicalPlans.get(low);
            case 1:
                return logicalPlanCombiner(logicalPlans.get(low), logicalPlans.get(high), qualifier);
            default:
                int mid = low + (high - low) / 2;
                return logicalPlanCombiner(
                        reduceToLogicalPlanTree(low, mid, logicalPlans, qualifier),
                        reduceToLogicalPlanTree(mid + 1, high, logicalPlans, qualifier),
                        qualifier
                );
        }
    }

    @Override
    public LogicalPlan visitSubquery(SubqueryContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> plan(ctx.query()));
    }

    @Override
    public LogicalPlan visitRegularQuerySpecification(RegularQuerySpecificationContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            SelectClauseContext selectCtx = ctx.selectClause();
            LogicalPlan selectPlan;
            LogicalPlan relation;
            if (ctx.fromClause() == null) {
                relation = new LogicalOneRowRelation(StatementScopeIdGenerator.newRelationId(),
                        ImmutableList.of(new Alias(Literal.of(0))));
            } else {
                relation = visitFromClause(ctx.fromClause());
            }
            if (ctx.intoClause() != null && !ConnectContext.get().isRunProcedure()) {
                throw new ParseException("Only procedure supports insert into variables", selectCtx);
            }
            selectPlan = withSelectQuerySpecification(
                    ctx, relation,
                    selectCtx,
                    Optional.ofNullable(ctx.whereClause()),
                    Optional.ofNullable(ctx.aggClause()),
                    Optional.ofNullable(ctx.havingClause()),
                    Optional.ofNullable(ctx.qualifyClause()));
            selectPlan = withQueryOrganization(selectPlan, ctx.queryOrganization());
            if ((selectHintMap == null) || selectHintMap.isEmpty()) {
                return selectPlan;
            }
            List<ParserRuleContext> selectHintContexts = Lists.newArrayList();
            List<ParserRuleContext> preAggOnHintContexts = Lists.newArrayList();
            for (Integer key : selectHintMap.keySet()) {
                if (key > selectCtx.getStart().getStopIndex() && key < selectCtx.getStop().getStartIndex()) {
                    selectHintContexts.add(selectHintMap.get(key));
                } else {
                    preAggOnHintContexts.add(selectHintMap.get(key));
                }
            }
            return withHints(selectPlan, selectHintContexts, preAggOnHintContexts);
        });
    }

    @Override
    public LogicalPlan visitInlineTable(InlineTableContext ctx) {
        List<RowConstructorContext> rowConstructorContexts = ctx.rowConstructor();
        ImmutableList.Builder<List<NamedExpression>> rows
                = ImmutableList.builderWithExpectedSize(rowConstructorContexts.size());
        for (RowConstructorContext rowConstructorContext : rowConstructorContexts) {
            rows.add(visitRowConstructor(rowConstructorContext));
        }
        return new UnboundInlineTable(rows.build());
    }

    /**
     * Create an aliased table reference. This is typically used in FROM clauses.
     */
    protected LogicalPlan withTableAlias(LogicalPlan plan, TableAliasContext ctx) {
        if (ctx.strictIdentifier() == null) {
            return plan;
        }
        return ParserUtils.withOrigin(ctx.strictIdentifier(), () -> {
            String alias = ctx.strictIdentifier().getText();
            if (null != ctx.identifierList()) {
                throw new ParseException("Do not implemented", ctx);
                // TODO: multi-colName
            }
            return new LogicalSubQueryAlias<>(alias, plan);
        });
    }

    @Override
    public LogicalPlan visitTableName(TableNameContext ctx) {
        List<String> nameParts = visitMultipartIdentifier(ctx.multipartIdentifier());
        List<String> partitionNames = new ArrayList<>();
        boolean isTempPart = false;
        if (ctx.specifiedPartition() != null) {
            isTempPart = ctx.specifiedPartition().TEMPORARY() != null;
            if (ctx.specifiedPartition().identifier() != null) {
                partitionNames.add(ctx.specifiedPartition().identifier().getText());
            } else {
                partitionNames.addAll(visitIdentifierList(ctx.specifiedPartition().identifierList()));
            }
        }

        Optional<String> indexName = Optional.empty();
        if (ctx.materializedViewName() != null) {
            indexName = Optional.ofNullable(ctx.materializedViewName().indexName.getText());
        }

        List<Long> tabletIdLists = new ArrayList<>();
        if (ctx.tabletList() != null) {
            ctx.tabletList().tabletIdList.stream().forEach(tabletToken -> {
                tabletIdLists.add(Long.parseLong(tabletToken.getText()));
            });
        }

        final List<String> relationHints;
        if (ctx.relationHint() != null) {
            relationHints = typedVisit(ctx.relationHint());
        } else {
            relationHints = ImmutableList.of();
        }

        TableScanParams scanParams = null;
        if (ctx.optScanParams() != null) {
            Map<String, String> map = visitPropertyItemList(ctx.optScanParams().mapParams);
            List<String> list;
            if (ctx.optScanParams().listParams != null) {
                list = visitIdentifierSeq(ctx.optScanParams().listParams);
            } else {
                list = ImmutableList.of();
            }
            scanParams = new TableScanParams(ctx.optScanParams().funcName.getText(), map, list);
        }

        TableSnapshot tableSnapshot = null;
        if (ctx.tableSnapshot() != null) {
            if (ctx.tableSnapshot().TIME() != null) {
                tableSnapshot = TableSnapshot.timeOf(stripQuotes(ctx.tableSnapshot().time.getText()));
            } else {
                tableSnapshot = TableSnapshot.versionOf(stripQuotes(ctx.tableSnapshot().version.getText()));
            }
        }

        TableSample tableSample = ctx.sample() == null ? null : (TableSample) visit(ctx.sample());
        UnboundRelation relation = new UnboundRelation(
                StatementScopeIdGenerator.newRelationId(),
                nameParts, partitionNames, isTempPart, tabletIdLists, relationHints,
                Optional.ofNullable(tableSample), indexName, scanParams, Optional.ofNullable(tableSnapshot));

        LogicalPlan checkedRelation = LogicalPlanBuilderAssistant.withCheckPolicy(relation);
        LogicalPlan plan = withTableAlias(checkedRelation, ctx.tableAlias());
        for (LateralViewContext lateralViewContext : ctx.lateralView()) {
            plan = withGenerate(plan, lateralViewContext);
        }
        return plan;
    }

    public static String stripQuotes(String str) {
        if ((str.charAt(0) == '\'' && str.charAt(str.length() - 1) == '\'')
                || (str.charAt(0) == '\"' && str.charAt(str.length() - 1) == '\"')) {
            str = str.substring(1, str.length() - 1);
        }
        return str;
    }

    @Override
    public LogicalPlan visitShowEncryptKeys(ShowEncryptKeysContext ctx) {
        String dbName = null;
        if (ctx.database != null) {
            List<String> nameParts = visitMultipartIdentifier(ctx.database);
            dbName = nameParts.get(0); // only one entry possible
        }

        String likeString = null;
        if (ctx.LIKE() != null) {
            likeString = stripQuotes(ctx.STRING_LITERAL().getText());
        }
        return new ShowEncryptKeysCommand(dbName, likeString);
    }

    @Override
    public LogicalPlan visitAliasedQuery(AliasedQueryContext ctx) {
        if (ctx.tableAlias().getText().equals("")) {
            throw new ParseException("Every derived table must have its own alias", ctx);
        }
        LogicalPlan plan = withTableAlias(visitQuery(ctx.query()), ctx.tableAlias());
        for (LateralViewContext lateralViewContext : ctx.lateralView()) {
            plan = withGenerate(plan, lateralViewContext);
        }
        return plan;
    }

    @Override
    public LogicalPlan visitTableValuedFunction(TableValuedFunctionContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            String functionName = ctx.tvfName.getText();

            Map<String, String> map = visitPropertyItemList(ctx.properties);
            LogicalPlan relation = new UnboundTVFRelation(StatementScopeIdGenerator.newRelationId(),
                    functionName, new Properties(map));
            return withTableAlias(relation, ctx.tableAlias());
        });
    }

    /**
     * Create a star (i.e. all) expression; this selects all elements (in the specified object).
     * Both un-targeted (global) and targeted aliases are supported.
     */
    @Override
    public Expression visitStar(StarContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            final QualifiedNameContext qualifiedNameContext = ctx.qualifiedName();
            List<String> target;
            if (qualifiedNameContext != null) {
                target = qualifiedNameContext.identifier()
                        .stream()
                        .map(RuleContext::getText)
                        .collect(ImmutableList.toImmutableList());
            } else {
                target = ImmutableList.of();
            }
            List<ExceptOrReplaceContext> exceptOrReplaceList = ctx.exceptOrReplace();
            if (exceptOrReplaceList != null && !exceptOrReplaceList.isEmpty()) {
                List<NamedExpression> finalExpectSlots = ImmutableList.of();
                List<NamedExpression> finalReplacedAlias = ImmutableList.of();
                for (ExceptOrReplaceContext exceptOrReplace : exceptOrReplaceList) {
                    if (exceptOrReplace instanceof ExceptContext) {
                        if (!finalExpectSlots.isEmpty()) {
                            throw new ParseException("only one except clause is supported", ctx);
                        }
                        ExceptContext exceptContext = (ExceptContext) exceptOrReplace;
                        List<NamedExpression> expectSlots = getNamedExpressions(exceptContext.namedExpressionSeq());
                        boolean allSlots = expectSlots.stream().allMatch(UnboundSlot.class::isInstance);
                        if (expectSlots.isEmpty() || !allSlots) {
                            throw new ParseException(
                                    "only column name is supported in except clause", ctx);
                        }
                        finalExpectSlots = expectSlots;
                    } else if (exceptOrReplace instanceof ReplaceContext) {
                        if (!finalReplacedAlias.isEmpty()) {
                            throw new ParseException("only one replace clause is supported", ctx);
                        }
                        ReplaceContext replaceContext = (ReplaceContext) exceptOrReplace;
                        List<NamedExpression> expectAlias = Lists.newArrayList();
                        NamedExpressionSeqContext namedExpressions = replaceContext.namedExpressionSeq();
                        for (NamedExpressionContext namedExpressionContext : namedExpressions.namedExpression()) {
                            if (namedExpressionContext.identifierOrText() == null) {
                                throw new ParseException("only alias is supported in select-replace clause", ctx);
                            }
                            expectAlias.add((NamedExpression) namedExpressionContext.accept(this));
                        }
                        if (expectAlias.isEmpty()) {
                            throw new ParseException("only alias is supported in select-replace clause", ctx);
                        }
                        finalReplacedAlias = expectAlias;
                    } else {
                        throw new ParseException(
                                "Unsupported except or replace clause: " + exceptOrReplace.getText(), ctx
                        );
                    }
                }
                return new UnboundStar(target, finalExpectSlots, finalReplacedAlias);
            } else {
                return new UnboundStar(target);
            }
        });
    }

    /**
     * Create an aliased expression if an alias is specified. Both single and multi-aliases are
     * supported.
     */
    @Override
    public NamedExpression visitNamedExpression(NamedExpressionContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            Expression expression = getExpression(ctx.expression());
            if (ctx.identifierOrText() == null) {
                if (expression instanceof NamedExpression) {
                    return (NamedExpression) expression;
                } else {
                    int start = ctx.expression().start.getStartIndex();
                    int stop = ctx.expression().stop.getStopIndex();
                    String alias = ctx.start.getInputStream()
                            .getText(new org.antlr.v4.runtime.misc.Interval(start, stop));
                    if (expression instanceof Literal) {
                        if (expression instanceof StringLikeLiteral) {
                            alias = LogicalPlanBuilderAssistant.getStringLiteralAlias((
                                    (StringLikeLiteral) expression).getStringValue());
                        }
                        return new Alias(expression, alias, true);
                    } else {
                        return new UnboundAlias(expression, alias, true);
                    }
                }
            }
            String alias = visitIdentifierOrText(ctx.identifierOrText());
            if (expression instanceof Literal) {
                return new Alias(expression, alias);
            }
            return new UnboundAlias(expression, alias);
        });
    }

    @Override
    public Expression visitSystemVariable(SystemVariableContext ctx) {
        VariableType type = null;
        if (ctx.kind == null) {
            type = VariableType.DEFAULT;
        } else if (ctx.kind.getType() == DorisParser.SESSION) {
            type = VariableType.SESSION;
        } else if (ctx.kind.getType() == DorisParser.GLOBAL) {
            type = VariableType.GLOBAL;
        }
        if (type == null) {
            throw new ParseException("Unsupported system variable: " + ctx.getText(), ctx);
        }
        return new UnboundVariable(ctx.identifier().getText(), type);
    }

    @Override
    public Expression visitUserVariable(UserVariableContext ctx) {
        return new UnboundVariable(ctx.identifierOrText().getText(), VariableType.USER);
    }

    /**
     * Create a comparison expression. This compares two expressions. The following comparison
     * operators are supported:
     * - Equal: '=' or '=='
     * - Null-safe Equal: '<=>'
     * - Not Equal: '<>' or '!='
     * - Less than: '<'
     * - Less then or Equal: '<='
     * - Greater than: '>'
     * - Greater then or Equal: '>='
     */
    @Override
    public Expression visitComparison(ComparisonContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            Expression left = getExpression(ctx.left);
            Expression right = getExpression(ctx.right);
            TerminalNode operator = (TerminalNode) ctx.comparisonOperator().getChild(0);
            switch (operator.getSymbol().getType()) {
                case DorisParser.EQ:
                    return new EqualTo(left, right);
                case DorisParser.NEQ:
                    return new Not(new EqualTo(left, right));
                case DorisParser.LT:
                    return new LessThan(left, right);
                case DorisParser.GT:
                    return new GreaterThan(left, right);
                case DorisParser.LTE:
                    return new LessThanEqual(left, right);
                case DorisParser.GTE:
                    return new GreaterThanEqual(left, right);
                case DorisParser.NSEQ:
                    return new NullSafeEqual(left, right);
                default:
                    throw new ParseException("Unsupported comparison expression: "
                            + operator.getSymbol().getText(), ctx);
            }
        });
    }

    /**
     * Create a not expression.
     * format: NOT Expression
     * for example:
     * not 1
     * not 1=1
     */
    @Override
    public Expression visitLogicalNot(LogicalNotContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> new Not(getExpression(ctx.booleanExpression())));
    }

    @Override
    public Expression visitLogicalBinary(LogicalBinaryContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            // Code block copy from Spark
            // sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/parser/AstBuilder.scala

            // Collect all similar left hand contexts.
            List<BooleanExpressionContext> contexts = Lists.newArrayList(ctx.right);
            BooleanExpressionContext current = ctx.left;
            while (true) {
                if (current instanceof LogicalBinaryContext
                        && ((LogicalBinaryContext) current).operator.getType() == ctx.operator.getType()) {
                    contexts.add(((LogicalBinaryContext) current).right);
                    current = ((LogicalBinaryContext) current).left;
                } else {
                    contexts.add(current);
                    break;
                }
            }
            // Reverse the contexts to have them in the same sequence as in the SQL statement & turn them
            // into expressions.
            Collections.reverse(contexts);
            List<Expression> expressions = contexts.stream().map(this::getExpression).collect(Collectors.toList());
            if (ctx.operator.getType() == DorisParser.AND) {
                return new And(expressions);
            } else if (ctx.operator.getType() == DorisParser.OR) {
                return new Or(expressions);
            } else {
                // Create a balanced tree.
                return reduceToExpressionTree(0, expressions.size() - 1, expressions, ctx);
            }
        });
    }

    @Override
    public Expression visitLambdaExpression(LambdaExpressionContext ctx) {
        ImmutableList<String> args = ctx.args.stream()
                .map(RuleContext::getText)
                .collect(ImmutableList.toImmutableList());
        Expression body = (Expression) visit(ctx.body);
        return new Lambda(args, body);
    }

    private Expression expressionCombiner(Expression left, Expression right, LogicalBinaryContext ctx) {
        switch (ctx.operator.getType()) {
            case DorisParser.LOGICALAND:
            case DorisParser.AND:
                return new And(left, right);
            case DorisParser.OR:
                return new Or(left, right);
            case DorisParser.XOR:
                return new Xor(left, right);
            default:
                throw new ParseException("Unsupported logical binary type: " + ctx.operator.getText(), ctx);
        }
    }

    private Expression reduceToExpressionTree(int low, int high,
            List<Expression> expressions, LogicalBinaryContext ctx) {
        switch (high - low) {
            case 0:
                return expressions.get(low);
            case 1:
                return expressionCombiner(expressions.get(low), expressions.get(high), ctx);
            default:
                int mid = low + (high - low) / 2;
                return expressionCombiner(
                        reduceToExpressionTree(low, mid, expressions, ctx),
                        reduceToExpressionTree(mid + 1, high, expressions, ctx),
                        ctx
                );
        }
    }

    /**
     * Create a predicated expression. A predicated expression is a normal expression with a
     * predicate attached to it, for example:
     * {{{
     * a + 1 IS NULL
     * }}}
     */
    @Override
    public Expression visitPredicated(PredicatedContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            Expression e = getExpression(ctx.valueExpression());
            return ctx.predicate() == null ? e : withPredicate(e, ctx.predicate());
        });
    }

    @Override
    public Expression visitArithmeticUnary(ArithmeticUnaryContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            Expression e = typedVisit(ctx.valueExpression());
            switch (ctx.operator.getType()) {
                case DorisParser.PLUS:
                    return e;
                case DorisParser.SUBTRACT:
                    IntegerLiteral zero = new IntegerLiteral(0);
                    return new Subtract(zero, e);
                case DorisParser.TILDE:
                    return new BitNot(e);
                default:
                    throw new ParseException("Unsupported arithmetic unary type: " + ctx.operator.getText(), ctx);
            }
        });
    }

    @Override
    public Expression visitArithmeticBinary(ArithmeticBinaryContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            Expression left = getExpression(ctx.left);
            Expression right = getExpression(ctx.right);

            int type = ctx.operator.getType();
            if (left instanceof Interval) {
                if (type != DorisParser.PLUS) {
                    throw new ParseException("Only supported: " + Operator.ADD, ctx);
                }
                Interval interval = (Interval) left;
                return new TimestampArithmetic(Operator.ADD, right, interval.value(), interval.timeUnit());
            }

            if (right instanceof Interval) {
                Operator op;
                if (type == DorisParser.PLUS) {
                    op = Operator.ADD;
                } else if (type == DorisParser.SUBTRACT) {
                    op = Operator.SUBTRACT;
                } else {
                    throw new ParseException("Only supported: " + Operator.ADD + " and " + Operator.SUBTRACT, ctx);
                }
                Interval interval = (Interval) right;
                return new TimestampArithmetic(op, left, interval.value(), interval.timeUnit());
            }

            return ParserUtils.withOrigin(ctx, () -> {
                switch (type) {
                    case DorisParser.ASTERISK:
                        return new Multiply(left, right);
                    case DorisParser.SLASH:
                        return new Divide(left, right);
                    case DorisParser.MOD:
                        return new Mod(left, right);
                    case DorisParser.PLUS:
                        return new Add(left, right);
                    case DorisParser.SUBTRACT:
                        return new Subtract(left, right);
                    case DorisParser.DIV:
                        return new IntegralDivide(left, right);
                    case DorisParser.HAT:
                        return new BitXor(left, right);
                    case DorisParser.PIPE:
                        return new BitOr(left, right);
                    case DorisParser.AMPERSAND:
                        return new BitAnd(left, right);
                    default:
                        throw new ParseException(
                                "Unsupported arithmetic binary type: " + ctx.operator.getText(), ctx);
                }
            });
        });
    }

    @Override
    public Expression visitCurrentDate(DorisParser.CurrentDateContext ctx) {
        return new CurrentDate();
    }

    @Override
    public Expression visitCurrentTime(DorisParser.CurrentTimeContext ctx) {
        return new CurrentTime();
    }

    @Override
    public Expression visitCurrentTimestamp(DorisParser.CurrentTimestampContext ctx) {
        return new Now();
    }

    @Override
    public Expression visitLocalTime(DorisParser.LocalTimeContext ctx) {
        return new CurrentTime();
    }

    @Override
    public Expression visitLocalTimestamp(DorisParser.LocalTimestampContext ctx) {
        return new Now();
    }

    @Override
    public Expression visitCurrentUser(DorisParser.CurrentUserContext ctx) {
        return new CurrentUser();
    }

    @Override
    public Expression visitSessionUser(DorisParser.SessionUserContext ctx) {
        return new SessionUser();
    }

    @Override
    public Expression visitDoublePipes(DorisParser.DoublePipesContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            Expression left = getExpression(ctx.left);
            Expression right = getExpression(ctx.right);
            if (SqlModeHelper.hasPipeAsConcat()) {
                return new UnboundFunction("concat", Lists.newArrayList(left, right));
            } else {
                return new Or(left, right);
            }
        });
    }

    /**
     * Create a value based [[CaseWhen]] expression. This has the following SQL form:
     * {{{
     * CASE [expression]
     * WHEN [value] THEN [expression]
     * ...
     * ELSE [expression]
     * END
     * }}}
     */
    @Override
    public Expression visitSimpleCase(DorisParser.SimpleCaseContext context) {
        Expression e = getExpression(context.value);
        List<WhenClause> whenClauses = context.whenClause().stream()
                .map(w -> new WhenClause(new EqualTo(e, getExpression(w.condition)), getExpression(w.result)))
                .collect(ImmutableList.toImmutableList());
        if (context.elseExpression == null) {
            return new CaseWhen(whenClauses);
        }
        return new CaseWhen(whenClauses, getExpression(context.elseExpression));
    }

    /**
     * Create a condition based [[CaseWhen]] expression. This has the following SQL syntax:
     * {{{
     * CASE
     * WHEN [predicate] THEN [expression]
     * ...
     * ELSE [expression]
     * END
     * }}}
     *
     * @param context the parse tree
     */
    @Override
    public Expression visitSearchedCase(DorisParser.SearchedCaseContext context) {
        List<WhenClause> whenClauses = context.whenClause().stream()
                .map(w -> new WhenClause(getExpression(w.condition), getExpression(w.result)))
                .collect(ImmutableList.toImmutableList());
        if (context.elseExpression == null) {
            return new CaseWhen(whenClauses);
        }
        return new CaseWhen(whenClauses, getExpression(context.elseExpression));
    }

    @Override
    public Expression visitCast(DorisParser.CastContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> processCast(getExpression(ctx.expression()), ctx.castDataType()));
    }

    @Override
    public UnboundFunction visitExtract(DorisParser.ExtractContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            String functionName = ctx.field.getText();
            return new UnboundFunction(functionName, false,
                    Collections.singletonList(getExpression(ctx.source)));
        });
    }

    @Override
    public Expression visitEncryptKey(DorisParser.EncryptKeyContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            String db = ctx.dbName == null ? "" : ctx.dbName.getText();
            String key = ctx.keyName.getText();
            return new EncryptKeyRef(new StringLiteral(db), new StringLiteral(key));
        });
    }

    @Override
    public Expression visitCharFunction(DorisParser.CharFunctionContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            String charSet = ctx.charSet == null ? "utf8" : ctx.charSet.getText();
            List<Expression> arguments = ImmutableList.<Expression>builder()
                    .add(new StringLiteral(charSet))
                    .addAll(visit(ctx.arguments, Expression.class))
                    .build();
            return new Char(arguments);
        });
    }

    @Override
    public Expression visitConvertCharSet(DorisParser.ConvertCharSetContext ctx) {
        return ParserUtils.withOrigin(ctx,
                () -> new ConvertTo(getExpression(ctx.argument), new StringLiteral(ctx.charSet.getText())));
    }

    @Override
    public Expression visitConvertType(DorisParser.ConvertTypeContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> processCast(getExpression(ctx.argument), ctx.castDataType()));
    }

    @Override
    public DataType visitCastDataType(CastDataTypeContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            if (ctx.dataType() != null) {
                return ((DataType) typedVisit(ctx.dataType())).conversion();
            } else if (ctx.UNSIGNED() != null) {
                return LargeIntType.UNSIGNED;
            } else {
                return BigIntType.SIGNED;
            }
        });
    }

    private Expression processCast(Expression expression, CastDataTypeContext castDataTypeContext) {
        DataType dataType = visitCastDataType(castDataTypeContext);
        Expression cast = new Cast(expression, dataType, true);
        if (dataType.isStringLikeType() && ((CharacterType) dataType).getLen() >= 0) {
            if (dataType.isVarcharType() && ((VarcharType) dataType).isWildcardVarchar()) {
                return cast;
            }
            List<Expression> args = ImmutableList.of(
                    cast,
                    new TinyIntLiteral((byte) 1),
                    Literal.of(((CharacterType) dataType).getLen())
            );
            return new UnboundFunction("substr", args);
        } else {
            return cast;
        }
    }

    @Override
    public Expression visitGroupConcat(GroupConcatContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            boolean isDistinct = ctx.DISTINCT() != null;
            List<Expression> params = Lists.newArrayList();
            params.addAll(visit(ctx.expression(), Expression.class));
            List<OrderKey> orderKeys = visit(ctx.sortItem(), OrderKey.class);
            params.addAll(orderKeys.stream().map(OrderExpression::new).collect(Collectors.toList()));
            return processUnboundFunction(ctx, null, "group_concat", isDistinct, params,
                    ctx.windowSpec(), ctx.identifier());
        });
    }

    @Override
    public Object visitTrim(TrimContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            List<Expression> params = visit(ctx.expression(), Expression.class);
            params = Lists.reverse(params);
            String name = "trim";
            if (ctx.LEADING() != null) {
                name = "ltrim";
            } else if (ctx.TRAILING() != null) {
                name = "rtrim";
            }
            return processUnboundFunction(ctx, null, name, false, params, null, null);
        });
    }

    @Override
    public Expression visitFunctionCallExpression(DorisParser.FunctionCallExpressionContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            String dbName = null;
            if (ctx.functionIdentifier().dbName != null) {
                dbName = ctx.functionIdentifier().dbName.getText();
            }
            String functionName = ctx.functionIdentifier().functionNameIdentifier().getText();
            boolean isDistinct = ctx.DISTINCT() != null;
            List<Expression> params = Lists.newArrayList();
            params.addAll(visit(ctx.funcExpression(), Expression.class));
            List<OrderKey> orderKeys = visit(ctx.sortItem(), OrderKey.class);
            params.addAll(orderKeys.stream().map(OrderExpression::new).collect(Collectors.toList()));
            return processUnboundFunction(ctx, dbName, functionName, isDistinct, params,
                    ctx.windowSpec(), ctx.identifier());
        });
    }

    private Expression processUnboundFunction(ParserRuleContext ctx, String dbName, String functionName,
            boolean isDistinct, List<Expression> params,
            WindowSpecContext windowContext, IdentifierContext hintContext) {
        List<UnboundStar> unboundStars = ExpressionUtils.collectAll(params, UnboundStar.class::isInstance);
        if (!unboundStars.isEmpty()) {
            if (dbName == null && functionName.equalsIgnoreCase("count")) {
                if (unboundStars.size() > 1) {
                    throw new ParseException(
                            "'*' can only be used once in conjunction with COUNT: " + functionName, ctx);
                }
                if (!unboundStars.get(0).getQualifier().isEmpty()) {
                    throw new ParseException("'*' can not has qualifier: " + unboundStars.size(), ctx);
                }
                if (windowContext != null) {
                    return withWindowSpec(windowContext, new Count());
                }
                return new Count();
            }
            throw new ParseException("'*' can only be used in conjunction with COUNT: " + functionName, ctx);
        } else {
            boolean isSkew = hintContext != null && hintContext.getText().equalsIgnoreCase("skew");
            UnboundFunction function = new UnboundFunction(dbName, functionName, isDistinct, params, isSkew);
            if (windowContext != null) {
                if (isDistinct
                        && !("count".equalsIgnoreCase(functionName))
                        && !("sum".equalsIgnoreCase(functionName))
                        && !("group_concat".equalsIgnoreCase(functionName))) {
                    throw new ParseException("DISTINCT not allowed in analytic function: " + functionName, ctx);
                }
                return withWindowSpec(windowContext, function);
            }
            return function;
        }
    }

    /**
     * deal with window function definition
     */
    private WindowExpression withWindowSpec(WindowSpecContext ctx, Expression function) {
        List<Expression> partitionKeyList = Lists.newArrayList();
        boolean isSkew = false;
        if (ctx.partitionClause() != null) {
            partitionKeyList = visit(ctx.partitionClause().expression(), Expression.class);
            isSkew = ctx.partitionClause().identifier() != null
                    && ctx.partitionClause().identifier().getText().equalsIgnoreCase("skew");
        }

        List<OrderExpression> orderKeyList = Lists.newArrayList();
        if (ctx.sortClause() != null) {
            orderKeyList = visit(ctx.sortClause().sortItem(), OrderKey.class).stream()
                    .map(orderKey -> new OrderExpression(orderKey))
                    .collect(Collectors.toList());
        }

        if (ctx.windowFrame() != null) {
            return new WindowExpression(function, partitionKeyList, orderKeyList, withWindowFrame(ctx.windowFrame()),
                    isSkew);
        }
        return new WindowExpression(function, partitionKeyList, orderKeyList, isSkew);
    }

    /**
     * deal with optional expressions
     */
    private <T, C> Optional<C> optionalVisit(T ctx, Supplier<C> func) {
        return Optional.ofNullable(ctx).map(a -> func.get());
    }

    /**
     * deal with window frame
     */
    private WindowFrame withWindowFrame(WindowFrameContext ctx) {
        WindowFrame.FrameUnitsType frameUnitsType = WindowFrame.FrameUnitsType.valueOf(
                ctx.frameUnits().getText().toUpperCase());
        WindowFrame.FrameBoundary leftBoundary = withFrameBound(ctx.start);
        if (ctx.end != null) {
            WindowFrame.FrameBoundary rightBoundary = withFrameBound(ctx.end);
            return new WindowFrame(frameUnitsType, leftBoundary, rightBoundary);
        }
        return new WindowFrame(frameUnitsType, leftBoundary);
    }

    private WindowFrame.FrameBoundary withFrameBound(DorisParser.FrameBoundaryContext ctx) {
        Optional<Expression> expression = Optional.empty();
        if (ctx.expression() != null) {
            expression = Optional.of(getExpression(ctx.expression()));
            // todo: use isConstant() to resolve Function in expression; currently we only
            //  support literal expression
            if (!expression.get().isLiteral()) {
                throw new ParseException("Unsupported expression in WindowFrame : " + expression, ctx);
            }
        }

        WindowFrame.FrameBoundType frameBoundType = null;
        switch (ctx.boundType.getType()) {
            case DorisParser.PRECEDING:
                if (ctx.UNBOUNDED() != null) {
                    frameBoundType = WindowFrame.FrameBoundType.UNBOUNDED_PRECEDING;
                } else {
                    frameBoundType = WindowFrame.FrameBoundType.PRECEDING;
                }
                break;
            case DorisParser.CURRENT:
                frameBoundType = WindowFrame.FrameBoundType.CURRENT_ROW;
                break;
            case DorisParser.FOLLOWING:
                if (ctx.UNBOUNDED() != null) {
                    frameBoundType = WindowFrame.FrameBoundType.UNBOUNDED_FOLLOWING;
                } else {
                    frameBoundType = WindowFrame.FrameBoundType.FOLLOWING;
                }
                break;
            default:
        }
        return new WindowFrame.FrameBoundary(expression, frameBoundType);
    }

    @Override
    public Expression visitInterval(IntervalContext ctx) {
        return new Interval(getExpression(ctx.value), visitUnitIdentifier(ctx.unit));
    }

    @Override
    public String visitUnitIdentifier(UnitIdentifierContext ctx) {
        return ctx.getText();
    }

    @Override
    public Expression visitTypeConstructor(TypeConstructorContext ctx) {
        String value = ctx.STRING_LITERAL().getText();
        value = value.substring(1, value.length() - 1);
        String type = ctx.type.getText().toUpperCase();
        switch (type) {
            case "DATE":
                try {
                    return Config.enable_date_conversion ? new DateV2Literal(value) : new DateLiteral(value);
                } catch (Exception e) {
                    return new Cast(new StringLiteral(value),
                            Config.enable_date_conversion ? DateV2Type.INSTANCE : DateType.INSTANCE);
                }
            case "TIMESTAMP":
                try {
                    return Config.enable_date_conversion ? new DateTimeV2Literal(value) : new DateTimeLiteral(value);
                } catch (Exception e) {
                    return new Cast(new StringLiteral(value),
                            Config.enable_date_conversion ? DateTimeV2Type.MAX : DateTimeType.INSTANCE);
                }
            case "DATEV2":
                try {
                    return new DateV2Literal(value);
                } catch (Exception e) {
                    return new Cast(new StringLiteral(value), DateV2Type.INSTANCE);
                }
            case "DATEV1":
                try {
                    return new DateLiteral(value);
                } catch (Exception e) {
                    return new Cast(new StringLiteral(value), DateType.INSTANCE);
                }
            default:
                throw new ParseException("Unsupported data type : " + type, ctx);
        }
    }

    @Override
    public Expression visitDereference(DereferenceContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            Expression e = getExpression(ctx.base);
            if (e instanceof UnboundSlot) {
                UnboundSlot unboundAttribute = (UnboundSlot) e;
                List<String> nameParts = Lists.newArrayList(unboundAttribute.getNameParts());
                nameParts.add(ctx.fieldName.getText());
                UnboundSlot slot = new UnboundSlot(nameParts, Optional.empty());
                return slot;
            } else {
                // todo: base is an expression, may be not a table name.
                throw new ParseException("Unsupported dereference expression: " + ctx.getText(), ctx);
            }
        });
    }

    @Override
    public Expression visitElementAt(ElementAtContext ctx) {
        return new ElementAt(typedVisit(ctx.value), typedVisit(ctx.index));
    }

    @Override
    public Expression visitArraySlice(ArraySliceContext ctx) {
        if (ctx.end != null) {
            return new ArraySlice(typedVisit(ctx.value), typedVisit(ctx.begin), typedVisit(ctx.end));
        } else {
            return new ArraySlice(typedVisit(ctx.value), typedVisit(ctx.begin));
        }
    }

    @Override
    public Expression visitColumnReference(ColumnReferenceContext ctx) {
        // todo: handle quoted and unquoted
        return new UnboundSlot(Lists.newArrayList(ctx.getText()), Optional.empty());
    }

    /**
     * Create a NULL literal expression.
     */
    @Override
    public Literal visitNullLiteral(NullLiteralContext ctx) {
        return new NullLiteral();
    }

    @Override
    public Literal visitBooleanLiteral(BooleanLiteralContext ctx) {
        Boolean b = Boolean.valueOf(ctx.getText());
        return BooleanLiteral.of(b);
    }

    @Override
    public Literal visitIntegerLiteral(IntegerLiteralContext ctx) {
        BigInteger bigInt = new BigInteger(ctx.getText());
        if (BigInteger.valueOf(bigInt.byteValue()).equals(bigInt)) {
            return new TinyIntLiteral(bigInt.byteValue());
        } else if (BigInteger.valueOf(bigInt.shortValue()).equals(bigInt)) {
            return new SmallIntLiteral(bigInt.shortValue());
        } else if (BigInteger.valueOf(bigInt.intValue()).equals(bigInt)) {
            return new IntegerLiteral(bigInt.intValue());
        } else if (BigInteger.valueOf(bigInt.longValue()).equals(bigInt)) {
            return new BigIntLiteral(bigInt.longValueExact());
        } else {
            return new LargeIntLiteral(bigInt);
        }
    }

    @Override
    public Literal visitStringLiteral(StringLiteralContext ctx) {
        String txt = ctx.STRING_LITERAL().getText();
        String s = txt.substring(1, txt.length() - 1);
        if (txt.charAt(0) == '\'') {
            // for single quote string, '' should be converted to '
            s = s.replace("''", "'");
        } else if (txt.charAt(0) == '"') {
            // for double quote string, "" should be converted to "
            s = s.replace("\"\"", "\"");
        }
        if (!SqlModeHelper.hasNoBackSlashEscapes()) {
            s = LogicalPlanBuilderAssistant.escapeBackSlash(s);
        }
        int strLength = Utils.containChinese(s) ? s.length() * StringLikeLiteral.CHINESE_CHAR_BYTE_LENGTH : s.length();
        if (strLength > ScalarType.MAX_VARCHAR_LENGTH) {
            return new StringLiteral(s);
        }
        return new VarcharLiteral(s, strLength);
    }

    @Override
    public Expression visitPlaceholder(DorisParser.PlaceholderContext ctx) {
        Placeholder parameter = new Placeholder(ConnectContext.get().getStatementContext().getNextPlaceholderId());
        tokenPosToParameters.put(ctx.start, parameter);
        return parameter;
    }

    /**
     * cast all items to same types.
     * TODO remove this function after we refactor type coercion.
     */
    private List<Literal> typeCoercionItems(List<Literal> items) {
        Array array = new Array(items.toArray(new Literal[0]));
        if (array.expectedInputTypes().isEmpty()) {
            return ImmutableList.of();
        }
        DataType dataType = array.expectedInputTypes().get(0);
        return items.stream()
                .map(item -> item.checkedCastWithFallback(dataType))
                .map(Literal.class::cast)
                .collect(ImmutableList.toImmutableList());
    }

    @Override
    public ArrayLiteral visitArrayLiteral(ArrayLiteralContext ctx) {
        List<Literal> items = ctx.items.stream().<Literal>map(this::typedVisit).collect(Collectors.toList());
        if (items.isEmpty()) {
            return new ArrayLiteral(items);
        }
        return new ArrayLiteral(typeCoercionItems(items));
    }

    @Override
    public MapLiteral visitMapLiteral(MapLiteralContext ctx) {
        List<Literal> items = ctx.items.stream().<Literal>map(this::typedVisit).collect(Collectors.toList());
        if (items.size() % 2 != 0) {
            throw new ParseException("map can't be odd parameters, need even parameters", ctx);
        }
        List<Literal> keys = Lists.newArrayList();
        List<Literal> values = Lists.newArrayList();
        for (int i = 0; i < items.size(); i++) {
            if (i % 2 == 0) {
                keys.add(items.get(i));
            } else {
                values.add(items.get(i));
            }
        }
        return new MapLiteral(typeCoercionItems(keys), typeCoercionItems(values));
    }

    @Override
    public Object visitStructLiteral(StructLiteralContext ctx) {
        List<Literal> fields = ctx.items.stream().<Literal>map(this::typedVisit).collect(Collectors.toList());
        return new StructLiteral(fields);
    }

    @Override
    public Expression visitParenthesizedExpression(ParenthesizedExpressionContext ctx) {
        return getExpression(ctx.expression());
    }

    @Override
    public List<NamedExpression> visitRowConstructor(RowConstructorContext ctx) {
        List<RowConstructorItemContext> rowConstructorItemContexts = ctx.rowConstructorItem();
        ImmutableList.Builder<NamedExpression> columns
                = ImmutableList.builderWithExpectedSize(rowConstructorItemContexts.size());
        for (RowConstructorItemContext rowConstructorItemContext : rowConstructorItemContexts) {
            columns.add(visitRowConstructorItem(rowConstructorItemContext));
        }
        return columns.build();
    }

    @Override
    public NamedExpression visitRowConstructorItem(RowConstructorItemContext ctx) {
        ConstantContext constant = ctx.constant();
        if (constant != null) {
            return new Alias((Expression) constant.accept(this));
        } else if (ctx.DEFAULT() != null) {
            return new DefaultValueSlot();
        } else {
            return visitNamedExpression(ctx.namedExpression());
        }
    }

    @Override
    public List<Expression> visitNamedExpressionSeq(NamedExpressionSeqContext namedCtx) {
        return visit(namedCtx.namedExpression(), Expression.class);
    }

    @Override
    public LogicalPlan visitRelation(RelationContext ctx) {
        return plan(ctx.relationPrimary());
    }

    @Override
    public LogicalPlan visitFromClause(FromClauseContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> visitRelations(ctx.relations()));
    }

    @Override
    public LogicalPlan visitRelations(DorisParser.RelationsContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> withRelations(null, ctx.relation()));
    }

    @Override
    public LogicalPlan visitRelationList(DorisParser.RelationListContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> withRelations(null, ctx.relations().relation()));
    }

    /* ********************************************************************************************
     * Table Identifier parsing
     * ******************************************************************************************** */

    @Override
    public List<String> visitMultipartIdentifier(MultipartIdentifierContext ctx) {
        return ctx.parts.stream()
                .map(RuleContext::getText)
                .collect(ImmutableList.toImmutableList());
    }

    /**
     * Create a Sequence of Strings for a parenthesis enclosed alias list.
     */
    @Override
    public List<String> visitIdentifierList(IdentifierListContext ctx) {
        return visitIdentifierSeq(ctx.identifierSeq());
    }

    /**
     * Create a Sequence of Strings for an identifier list.
     */
    @Override
    public List<String> visitIdentifierSeq(IdentifierSeqContext ctx) {
        return ctx.ident.stream()
                .map(RuleContext::getText)
                .collect(ImmutableList.toImmutableList());
    }

    @Override
    public EqualTo visitUpdateAssignment(UpdateAssignmentContext ctx) {
        return new EqualTo(new UnboundSlot(
                visitMultipartIdentifier(ctx.multipartIdentifier()), Optional.empty()),
                getExpression(ctx.expression()));
    }

    @Override
    public List<EqualTo> visitUpdateAssignmentSeq(UpdateAssignmentSeqContext ctx) {
        return ctx.assignments.stream()
                .map(this::visitUpdateAssignment)
                .collect(Collectors.toList());
    }

    /**
     * get OrderKey.
     *
     * @param ctx SortItemContext
     * @return SortItems
     */
    @Override
    public OrderKey visitSortItem(SortItemContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            boolean isAsc = ctx.DESC() == null;
            boolean isNullFirst = ctx.FIRST() != null || (ctx.LAST() == null && isAsc);
            Expression expression = typedVisit(ctx.expression());
            return new OrderKey(expression, isAsc, isNullFirst);
        });
    }

    @Override
    public GroupKeyWithOrder visitExpressionWithOrder(ExpressionWithOrderContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            boolean hasOrder = ctx.ASC() != null || ctx.DESC() != null;
            boolean isAsc = ctx.DESC() == null;
            Expression expression = typedVisit(ctx.expression());
            return new GroupKeyWithOrder(expression, hasOrder, isAsc);
        });
    }

    private <T> List<T> visit(List<? extends ParserRuleContext> contexts, Class<T> clazz) {
        return contexts.stream()
                .map(this::visit)
                .map(clazz::cast)
                .collect(ImmutableList.toImmutableList());
    }

    private LogicalPlan plan(ParserRuleContext tree) {
        return (LogicalPlan) tree.accept(this);
    }

    /* ********************************************************************************************
     * create table parsing
     * ******************************************************************************************** */

    @Override
    public LogicalPlan visitCreateView(CreateViewContext ctx) {
        List<String> nameParts = visitMultipartIdentifier(ctx.name);
        String comment = ctx.STRING_LITERAL() == null ? "" : LogicalPlanBuilderAssistant.escapeBackSlash(
                ctx.STRING_LITERAL().getText().substring(1, ctx.STRING_LITERAL().getText().length() - 1));
        String querySql = getOriginSql(ctx.query());
        if (ctx.REPLACE() != null && ctx.EXISTS() != null) {
            throw new AnalysisException("[OR REPLACE] and [IF NOT EXISTS] cannot used at the same time");
        }
        CreateViewInfo info = new CreateViewInfo(ctx.EXISTS() != null, ctx.REPLACE() != null,
                new TableNameInfo(nameParts),
                comment, querySql,
                ctx.cols == null ? Lists.newArrayList() : visitSimpleColumnDefs(ctx.cols));
        return new CreateViewCommand(info);
    }

    @Override
    public LogicalPlan visitCreateTable(CreateTableContext ctx) {
        String ctlName = null;
        String dbName = null;
        String tableName = null;
        List<String> nameParts = visitMultipartIdentifier(ctx.name);
        // TODO: support catalog
        if (nameParts.size() == 1) {
            // dbName should be set
            dbName = ConnectContext.get().getDatabase();
            tableName = nameParts.get(0);
        } else if (nameParts.size() == 2) {
            dbName = nameParts.get(0);
            tableName = nameParts.get(1);
        } else if (nameParts.size() == 3) {
            ctlName = nameParts.get(0);
            dbName = nameParts.get(1);
            tableName = nameParts.get(2);
        } else {
            throw new AnalysisException("nameParts in create table should be [ctl.][db.]tbl");
        }
        KeysType keysType = null;
        if (ctx.DUPLICATE() != null) {
            keysType = KeysType.DUP_KEYS;
        } else if (ctx.AGGREGATE() != null) {
            keysType = KeysType.AGG_KEYS;
        } else if (ctx.UNIQUE() != null) {
            keysType = KeysType.UNIQUE_KEYS;
        }
        // when engineName is null, get engineName from current catalog later
        String engineName = ctx.engine != null ? ctx.engine.getText().toLowerCase() : null;
        int bucketNum = FeConstants.default_bucket_num;
        if (ctx.INTEGER_VALUE() != null) {
            bucketNum = Integer.parseInt(ctx.INTEGER_VALUE().getText());
        }
        String comment = ctx.STRING_LITERAL() == null ? "" : LogicalPlanBuilderAssistant.escapeBackSlash(
                ctx.STRING_LITERAL().getText().substring(1, ctx.STRING_LITERAL().getText().length() - 1));
        DistributionDescriptor desc = null;
        if (ctx.HASH() != null) {
            desc = new DistributionDescriptor(true, ctx.autoBucket != null, bucketNum,
                    visitIdentifierList(ctx.hashKeys));
        } else if (ctx.RANDOM() != null) {
            desc = new DistributionDescriptor(false, ctx.autoBucket != null, bucketNum, null);
        }
        Map<String, String> properties = ctx.properties != null
                // NOTICE: we should not generate immutable map here, because it will be modified when analyzing.
                ? Maps.newHashMap(visitPropertyClause(ctx.properties))
                : Maps.newHashMap();
        Map<String, String> extProperties = ctx.extProperties != null
                // NOTICE: we should not generate immutable map here, because it will be modified when analyzing.
                ? Maps.newHashMap(visitPropertyClause(ctx.extProperties))
                : Maps.newHashMap();

        // solve partition by
        PartitionTableInfo partitionInfo;
        if (ctx.partition != null) {
            partitionInfo = (PartitionTableInfo) ctx.partitionTable().accept(this);
        } else {
            partitionInfo = PartitionTableInfo.EMPTY;
        }

        if (ctx.columnDefs() != null) {
            if (ctx.AS() != null) {
                throw new AnalysisException("Should not define the entire column in CTAS");
            }
            return new CreateTableCommand(Optional.empty(), new CreateTableInfo(
                    ctx.EXISTS() != null,
                    ctx.EXTERNAL() != null,
                    ctx.TEMPORARY() != null,
                    ctlName,
                    dbName,
                    tableName,
                    visitColumnDefs(ctx.columnDefs()),
                    ctx.indexDefs() != null ? visitIndexDefs(ctx.indexDefs()) : ImmutableList.of(),
                    engineName,
                    keysType,
                    ctx.keys != null ? visitIdentifierList(ctx.keys) : ImmutableList.of(),
                    comment,
                    partitionInfo,
                    desc,
                    ctx.rollupDefs() != null ? visitRollupDefs(ctx.rollupDefs()) : ImmutableList.of(),
                    properties,
                    extProperties,
                    ctx.clusterKeys != null ? visitIdentifierList(ctx.clusterKeys) : ImmutableList.of()));
        } else if (ctx.AS() != null) {
            return new CreateTableCommand(Optional.of(visitQuery(ctx.query())), new CreateTableInfo(
                    ctx.EXISTS() != null,
                    ctx.EXTERNAL() != null,
                    ctx.TEMPORARY() != null,
                    ctlName,
                    dbName,
                    tableName,
                    ctx.ctasCols != null ? visitIdentifierList(ctx.ctasCols) : null,
                    engineName,
                    keysType,
                    ctx.keys != null ? visitIdentifierList(ctx.keys) : ImmutableList.of(),
                    comment,
                    partitionInfo,
                    desc,
                    ctx.rollupDefs() != null ? visitRollupDefs(ctx.rollupDefs()) : ImmutableList.of(),
                    properties,
                    extProperties,
                    ctx.clusterKeys != null ? visitIdentifierList(ctx.clusterKeys) : ImmutableList.of()));
        } else {
            throw new AnalysisException("Should contain at least one column in a table");
        }
    }

    @Override
    public PartitionTableInfo visitPartitionTable(DorisParser.PartitionTableContext ctx) {
        boolean isAutoPartition = ctx.autoPartition != null;
        ImmutableList<Expression> partitionList = ctx.partitionList.identityOrFunction().stream()
                .map(partition -> {
                    IdentifierContext identifier = partition.identifier();
                    if (identifier != null) {
                        return new UnboundSlot(
                                Lists.newArrayList(identifier.getText()),
                                Optional.empty()
                        );
                    } else {
                        return visitFunctionCallExpression(partition.functionCallExpression());
                    }
                })
                .collect(ImmutableList.toImmutableList());
        return new PartitionTableInfo(
                isAutoPartition,
                ctx.RANGE() != null ? "RANGE" : "LIST",
                ctx.partitions != null ? visitPartitionsDef(ctx.partitions) : null,
                partitionList);
    }

    @Override
    public List<ColumnDefinition> visitColumnDefs(ColumnDefsContext ctx) {
        return ctx.cols.stream().map(this::visitColumnDef).collect(Collectors.toList());
    }

    @Override
    public ColumnDefinition visitColumnDef(ColumnDefContext ctx) {
        String colName = ctx.colName.getText();
        DataType colType = ctx.type instanceof PrimitiveDataTypeContext
                ? visitPrimitiveDataType(((PrimitiveDataTypeContext) ctx.type))
                : ctx.type instanceof ComplexDataTypeContext
                        ? visitComplexDataType((ComplexDataTypeContext) ctx.type)
                    : ctx.type instanceof VariantPredefinedFieldsContext
                            ? visitVariantPredefinedFields((VariantPredefinedFieldsContext) ctx.type)
                        : visitAggStateDataType((AggStateDataTypeContext) ctx.type);
        colType = colType.conversion();
        boolean isKey = ctx.KEY() != null;
        ColumnNullableType nullableType = ColumnNullableType.DEFAULT;
        if (ctx.NOT() != null) {
            nullableType = ColumnNullableType.NOT_NULLABLE;
        } else if (ctx.nullable != null) {
            nullableType = ColumnNullableType.NULLABLE;
        }
        String aggTypeString = ctx.aggType != null ? ctx.aggType.getText() : null;
        Optional<DefaultValue> defaultValue = Optional.empty();
        Optional<DefaultValue> onUpdateDefaultValue = Optional.empty();
        if (ctx.DEFAULT() != null) {
            if (ctx.INTEGER_VALUE() != null) {
                if (ctx.SUBTRACT() == null) {
                    defaultValue = Optional.of(new DefaultValue(ctx.INTEGER_VALUE().getText()));
                } else {
                    defaultValue = Optional.of(new DefaultValue("-" + ctx.INTEGER_VALUE().getText()));
                }
            } else if (ctx.DECIMAL_VALUE() != null) {
                if (ctx.SUBTRACT() == null) {
                    defaultValue = Optional.of(new DefaultValue(ctx.DECIMAL_VALUE().getText()));
                } else {
                    defaultValue = Optional.of(new DefaultValue("-" + ctx.DECIMAL_VALUE().getText()));
                }
            } else if (ctx.stringValue != null) {
                defaultValue = Optional.of(new DefaultValue(toStringValue(ctx.stringValue.getText())));
            } else if (ctx.nullValue != null) {
                defaultValue = Optional.of(DefaultValue.NULL_DEFAULT_VALUE);
            } else if (ctx.defaultTimestamp != null) {
                if (ctx.defaultValuePrecision == null) {
                    defaultValue = Optional.of(DefaultValue.CURRENT_TIMESTAMP_DEFAULT_VALUE);
                } else {
                    defaultValue = Optional.of(DefaultValue
                            .currentTimeStampDefaultValueWithPrecision(
                                    Long.valueOf(ctx.defaultValuePrecision.getText())));
                }
            } else if (ctx.CURRENT_DATE() != null) {
                defaultValue = Optional.of(DefaultValue.CURRENT_DATE_DEFAULT_VALUE);
            } else if (ctx.PI() != null) {
                defaultValue = Optional.of(DefaultValue.PI_DEFAULT_VALUE);
            } else if (ctx.E() != null) {
                defaultValue = Optional.of(DefaultValue.E_NUM_DEFAULT_VALUE);
            } else if (ctx.BITMAP_EMPTY() != null) {
                defaultValue = Optional.of(DefaultValue.BITMAP_EMPTY_DEFAULT_VALUE);
            }
        }
        if (ctx.UPDATE() != null) {
            if (ctx.onUpdateValuePrecision == null) {
                onUpdateDefaultValue = Optional.of(DefaultValue.CURRENT_TIMESTAMP_DEFAULT_VALUE);
            } else {
                onUpdateDefaultValue = Optional.of(DefaultValue
                        .currentTimeStampDefaultValueWithPrecision(
                                Long.valueOf(ctx.onUpdateValuePrecision.getText())));
            }
        }
        AggregateType aggType = null;
        if (aggTypeString != null) {
            try {
                aggType = AggregateType.valueOf(aggTypeString.toUpperCase());
            } catch (Exception e) {
                throw new AnalysisException(String.format("Aggregate type %s is unsupported", aggTypeString),
                        e.getCause());
            }
        }
        //comment should remove '\' and '(") at the beginning and end
        String comment = ctx.comment != null ? ctx.comment.getText().substring(1, ctx.comment.getText().length() - 1)
                .replace("\\", "") : "";
        long autoIncInitValue = -1;
        if (ctx.AUTO_INCREMENT() != null) {
            if (ctx.autoIncInitValue != null) {
                // AUTO_INCREMENT(Value) Value >= 0.
                autoIncInitValue = Long.valueOf(ctx.autoIncInitValue.getText());
                if (autoIncInitValue < 0) {
                    throw new AnalysisException("AUTO_INCREMENT start value can not be negative.");
                }
            } else {
                // AUTO_INCREMENT default 1.
                autoIncInitValue = Long.valueOf(1);
            }
        }
        Optional<GeneratedColumnDesc> desc = ctx.generatedExpr != null
                ? Optional.of(new GeneratedColumnDesc(ctx.generatedExpr.getText(), getExpression(ctx.generatedExpr)))
                : Optional.empty();
        return new ColumnDefinition(colName, colType, isKey, aggType, nullableType, autoIncInitValue, defaultValue,
                onUpdateDefaultValue, comment, desc);
    }

    @Override
    public List<IndexDefinition> visitIndexDefs(IndexDefsContext ctx) {
        return ctx.indexes.stream().map(this::visitIndexDef).collect(Collectors.toList());
    }

    @Override
    public IndexDefinition visitIndexDef(IndexDefContext ctx) {
        String indexName = ctx.indexName.getText();
        boolean ifNotExists = ctx.ifNotExists != null;
        List<String> indexCols = visitIdentifierList(ctx.cols);
        Map<String, String> properties = visitPropertyItemList(ctx.properties);
        String indexType = ctx.indexType != null ? ctx.indexType.getText().toUpperCase() : null;
        //comment should remove '\' and '(") at the beginning and end
        String comment = ctx.comment == null ? "" : LogicalPlanBuilderAssistant.escapeBackSlash(
                ctx.comment.getText().substring(1, ctx.STRING_LITERAL().getText().length() - 1));
        // change BITMAP index to INVERTED index
        if (Config.enable_create_bitmap_index_as_inverted_index
                && "BITMAP".equalsIgnoreCase(indexType)) {
            indexType = "INVERTED";
        }
        return new IndexDefinition(indexName, ifNotExists, indexCols, indexType, properties, comment);
    }

    @Override
    public List<PartitionDefinition> visitPartitionsDef(PartitionsDefContext ctx) {
        return ctx.partitions.stream()
                .map(p -> ((PartitionDefinition) visit(p))).collect(Collectors.toList());
    }

    @Override
    public PartitionDefinition visitPartitionDef(DorisParser.PartitionDefContext ctx) {
        PartitionDefinition partitionDefinition = (PartitionDefinition) visit(ctx.getChild(0));
        if (ctx.partitionProperties != null) {
            partitionDefinition.withProperties(visitPropertyItemList(ctx.partitionProperties));
        }
        return partitionDefinition;
    }

    @Override
    public PartitionDefinition visitLessThanPartitionDef(LessThanPartitionDefContext ctx) {
        String partitionName = ctx.partitionName.getText();
        if (ctx.MAXVALUE() == null) {
            List<Expression> lessThanValues = visitPartitionValueList(ctx.partitionValueList());
            return new LessThanPartition(ctx.EXISTS() != null, partitionName, lessThanValues);
        } else {
            return new LessThanPartition(ctx.EXISTS() != null, partitionName,
                    ImmutableList.of(MaxValue.INSTANCE));
        }
    }

    @Override
    public PartitionDefinition visitFixedPartitionDef(FixedPartitionDefContext ctx) {
        String partitionName = ctx.partitionName.getText();
        List<Expression> lowerBounds = visitPartitionValueList(ctx.lower);
        List<Expression> upperBounds = visitPartitionValueList(ctx.upper);
        return new FixedRangePartition(ctx.EXISTS() != null, partitionName, lowerBounds, upperBounds);
    }

    @Override
    public PartitionDefinition visitStepPartitionDef(StepPartitionDefContext ctx) {
        List<Expression> fromExpression = visitPartitionValueList(ctx.from);
        List<Expression> toExpression = visitPartitionValueList(ctx.to);
        return new StepPartition(false, null, fromExpression, toExpression,
                Long.parseLong(ctx.unitsAmount.getText()), ctx.unit != null ? ctx.unit.getText() : null);
    }

    @Override
    public PartitionDefinition visitInPartitionDef(InPartitionDefContext ctx) {
        List<List<Expression>> values;
        if (ctx.constants == null) {
            values = ctx.partitionValueLists.stream().map(this::visitPartitionValueList)
                    .collect(Collectors.toList());
        } else {
            values = visitPartitionValueList(ctx.constants).stream().map(ImmutableList::of)
                    .collect(Collectors.toList());
        }
        return new InPartition(ctx.EXISTS() != null, ctx.partitionName.getText(), values);
    }

    @Override
    public List<Expression> visitPartitionValueList(PartitionValueListContext ctx) {
        return ctx.values.stream()
                .map(this::visitPartitionValueDef)
                .collect(Collectors.toList());
    }

    @Override
    public Expression visitPartitionValueDef(PartitionValueDefContext ctx) {
        if (ctx.INTEGER_VALUE() != null) {
            if (ctx.SUBTRACT() != null) {
                return Literal.of("-" + ctx.INTEGER_VALUE().getText());
            }
            return Literal.of(ctx.INTEGER_VALUE().getText());
        } else if (ctx.STRING_LITERAL() != null) {
            return Literal.of(toStringValue(ctx.STRING_LITERAL().getText()));
        } else if (ctx.MAXVALUE() != null) {
            return MaxValue.INSTANCE;
        } else if (ctx.NULL() != null) {
            return Literal.of(null);
        }
        throw new AnalysisException("Unsupported partition value: " + ctx.getText());
    }

    @Override
    public List<RollupDefinition> visitRollupDefs(RollupDefsContext ctx) {
        return ctx.rollups.stream().map(this::visitRollupDef).collect(Collectors.toList());
    }

    @Override
    public RollupDefinition visitRollupDef(RollupDefContext ctx) {
        String rollupName = ctx.rollupName.getText();
        List<String> rollupCols = visitIdentifierList(ctx.rollupCols);
        List<String> dupKeys = ctx.dupKeys == null ? ImmutableList.of() : visitIdentifierList(ctx.dupKeys);
        Map<String, String> properties = ctx.properties == null ? Maps.newHashMap()
                : visitPropertyClause(ctx.properties);
        return new RollupDefinition(rollupName, rollupCols, dupKeys, properties);
    }

    private String toStringValue(String literal) {
        return literal.substring(1, literal.length() - 1);
    }

    /* ********************************************************************************************
     * Expression parsing
     * ******************************************************************************************** */

    /**
     * Create an expression from the given context. This method just passes the context on to the
     * visitor and only takes care of typing (We assume that the visitor returns an Expression here).
     */
    private Expression getExpression(ParserRuleContext ctx) {
        return typedVisit(ctx);
    }

    private LogicalPlan withExplain(LogicalPlan inputPlan, ExplainContext ctx) {
        if (ctx == null) {
            return inputPlan;
        }
        return ParserUtils.withOrigin(ctx, () -> {
            ExplainLevel explainLevel = ExplainLevel.NORMAL;

            if (ctx.planType() != null) {
                if (ctx.level == null || !ctx.level.getText().equalsIgnoreCase("plan")) {
                    throw new ParseException("Only explain plan can use plan type: " + ctx.planType().getText(), ctx);
                }
            }

            boolean showPlanProcess = false;
            if (ctx.level != null) {
                if (!ctx.level.getText().equalsIgnoreCase("plan")) {
                    explainLevel = ExplainLevel.valueOf(ctx.level.getText().toUpperCase(Locale.ROOT));
                } else {
                    explainLevel = parseExplainPlanType(ctx.planType());

                    if (ctx.PROCESS() != null) {
                        showPlanProcess = true;
                    }
                }
            }
            return new ExplainCommand(explainLevel, inputPlan, showPlanProcess);
        });
    }

    private LogicalPlan withOutFile(LogicalPlan plan, OutFileClauseContext ctx) {
        if (ctx == null) {
            return plan;
        }
        String format = "csv";
        if (ctx.format != null) {
            format = ctx.format.getText();
        }

        Map<String, String> properties = ImmutableMap.of();
        if (ctx.propertyClause() != null) {
            properties = visitPropertyClause(ctx.propertyClause());
        }
        Literal filePath = (Literal) visit(ctx.filePath);
        return new LogicalFileSink<>(filePath.getStringValue(), format, properties, ImmutableList.of(), plan);
    }

    private LogicalPlan withQueryOrganization(LogicalPlan inputPlan, QueryOrganizationContext ctx) {
        if (ctx == null) {
            return inputPlan;
        }
        Optional<SortClauseContext> sortClauseContext = Optional.ofNullable(ctx.sortClause());
        Optional<LimitClauseContext> limitClauseContext = Optional.ofNullable(ctx.limitClause());
        LogicalPlan sort = withSort(inputPlan, sortClauseContext);
        return withLimit(sort, limitClauseContext);
    }

    private LogicalPlan withSort(LogicalPlan input, Optional<SortClauseContext> sortCtx) {
        return input.optionalMap(sortCtx, () -> {
            List<OrderKey> orderKeys = visit(sortCtx.get().sortItem(), OrderKey.class);
            return new LogicalSort<>(orderKeys, input);
        });
    }

    private LogicalPlan withLimit(LogicalPlan input, Optional<LimitClauseContext> limitCtx) {
        return input.optionalMap(limitCtx, () -> {
            long limit = Long.parseLong(limitCtx.get().limit.getText());
            if (limit < 0) {
                throw new ParseException("Limit requires non-negative number", limitCtx.get());
            }
            long offset = 0;
            Token offsetToken = limitCtx.get().offset;
            if (offsetToken != null) {
                offset = Long.parseLong(offsetToken.getText());
            }
            return new LogicalLimit<>(limit, offset, LimitPhase.ORIGIN, input);
        });
    }

    /**
     * Add a regular (SELECT) query specification to a logical plan. The query specification
     * is the core of the logical plan, this is where sourcing (FROM clause), projection (SELECT),
     * aggregation (GROUP BY ... HAVING ...) and filtering (WHERE) takes place.
     *
     * <p>Note that query hints are ignored (both by the parser and the builder).
     */
    protected LogicalPlan withSelectQuerySpecification(
            ParserRuleContext ctx,
            LogicalPlan inputRelation,
            SelectClauseContext selectClause,
            Optional<WhereClauseContext> whereClause,
            Optional<AggClauseContext> aggClause,
            Optional<HavingClauseContext> havingClause,
            Optional<QualifyClauseContext> qualifyClause) {
        return ParserUtils.withOrigin(ctx, () -> {
            // from -> where -> group by -> having -> select
            LogicalPlan filter = withFilter(inputRelation, whereClause);
            SelectColumnClauseContext selectColumnCtx = selectClause.selectColumnClause();
            List<OrderKey> orderKeys = Lists.newArrayList();
            LogicalPlan aggregate = withAggregate(filter, selectColumnCtx, aggClause, orderKeys);
            boolean isDistinct = (selectClause.DISTINCT() != null);
            LogicalPlan selectPlan;
            if (!(aggregate instanceof Aggregate) && havingClause.isPresent()) {
                // create a project node for pattern match of ProjectToGlobalAggregate rule
                // then ProjectToGlobalAggregate rule can insert agg node as LogicalHaving node's child
                List<NamedExpression> projects = getNamedExpressions(selectColumnCtx.namedExpressionSeq());
                LogicalPlan project = new LogicalProject<>(projects, isDistinct, aggregate);
                selectPlan = new LogicalHaving<>(ExpressionUtils.extractConjunctionToSet(
                        getExpression((havingClause.get().booleanExpression()))), project);
            } else {
                LogicalPlan having = withHaving(aggregate, havingClause);
                selectPlan = withProjection(having, selectColumnCtx, aggClause, isDistinct);
            }
            // support qualify clause
            if (qualifyClause.isPresent()) {
                Expression qualifyExpr = getExpression(qualifyClause.get().booleanExpression());
                selectPlan = new LogicalQualify<>(ExpressionUtils.extractConjunctionToSet(qualifyExpr), selectPlan);
            }
            if (!orderKeys.isEmpty()) {
                selectPlan = new LogicalSort<>(orderKeys, selectPlan);
            }
            return selectPlan;
        });
    }

    /**
     * Join one more [[LogicalPlan]]s to the current logical plan.
     */
    private LogicalPlan withJoinRelations(LogicalPlan input, RelationContext ctx) {
        LogicalPlan last = input;
        for (JoinRelationContext join : ctx.joinRelation()) {
            JoinType joinType;
            if (join.joinType().CROSS() != null) {
                joinType = JoinType.CROSS_JOIN;
            } else if (join.joinType().FULL() != null) {
                joinType = JoinType.FULL_OUTER_JOIN;
            } else if (join.joinType().SEMI() != null) {
                if (join.joinType().LEFT() != null) {
                    joinType = JoinType.LEFT_SEMI_JOIN;
                } else {
                    joinType = JoinType.RIGHT_SEMI_JOIN;
                }
            } else if (join.joinType().ANTI() != null) {
                if (join.joinType().LEFT() != null) {
                    joinType = JoinType.LEFT_ANTI_JOIN;
                } else {
                    joinType = JoinType.RIGHT_ANTI_JOIN;
                }
            } else if (join.joinType().LEFT() != null) {
                joinType = JoinType.LEFT_OUTER_JOIN;
            } else if (join.joinType().RIGHT() != null) {
                joinType = JoinType.RIGHT_OUTER_JOIN;
            } else if (join.joinType().INNER() != null) {
                joinType = JoinType.INNER_JOIN;
            } else if (join.joinCriteria() != null) {
                joinType = JoinType.INNER_JOIN;
            } else {
                joinType = JoinType.CROSS_JOIN;
            }
            DistributeHint distributeHint = new DistributeHint(DistributeType.NONE);
            if (join.distributeType() != null) {
                distributeHint = visitDistributeType(join.distributeType());
            }
            // TODO: natural join, lateral join, union join
            JoinCriteriaContext joinCriteria = join.joinCriteria();
            Optional<Expression> condition = Optional.empty();
            List<Expression> ids = null;
            if (joinCriteria != null) {
                if (join.joinType().CROSS() != null) {
                    throw new ParseException("Cross join can't be used with ON clause", joinCriteria);
                }
                if (joinCriteria.booleanExpression() != null) {
                    condition = Optional.ofNullable(getExpression(joinCriteria.booleanExpression()));
                } else if (joinCriteria.USING() != null) {
                    ids = visitIdentifierList(joinCriteria.identifierList())
                            .stream().map(UnboundSlot::quoted)
                            .collect(ImmutableList.toImmutableList());
                }
            } else {
                // keep same with original planner, allow cross/inner join
                if (!joinType.isInnerOrCrossJoin()) {
                    throw new ParseException("on mustn't be empty except for cross/inner join", join);
                }
            }
            if (ids == null) {
                last = new LogicalJoin<>(joinType, ExpressionUtils.EMPTY_CONDITION,
                        condition.map(ExpressionUtils::extractConjunction)
                                .orElse(ExpressionUtils.EMPTY_CONDITION),
                        distributeHint,
                        Optional.empty(),
                        last,
                        plan(join.relationPrimary()), null);
            } else {
                last = new LogicalUsingJoin<>(joinType, last, plan(join.relationPrimary()), ids, distributeHint);

            }
            if (distributeHint.distributeType != DistributeType.NONE
                    && ConnectContext.get().getStatementContext() != null
                    && !ConnectContext.get().getStatementContext().getHints().contains(distributeHint)) {
                ConnectContext.get().getStatementContext().addHint(distributeHint);
            }
        }
        return last;
    }

    private List<List<String>> getTableList(List<MultipartIdentifierContext> ctx) {
        List<List<String>> tableList = new ArrayList<>();
        for (MultipartIdentifierContext tableCtx : ctx) {
            tableList.add(visitMultipartIdentifier(tableCtx));
        }
        return tableList;
    }

    private LogicalPlan withHints(LogicalPlan logicalPlan, List<ParserRuleContext> selectHintContexts,
            List<ParserRuleContext> preAggOnHintContexts) {
        if (selectHintContexts.isEmpty() && preAggOnHintContexts.isEmpty()) {
            return logicalPlan;
        }
        LogicalPlan newPlan = logicalPlan;
        if (!selectHintContexts.isEmpty()) {
            ImmutableList.Builder<SelectHint> hints = ImmutableList.builder();
            for (ParserRuleContext hintContext : selectHintContexts) {
                SelectHintContext selectHintContext = (SelectHintContext) hintContext;
                for (HintStatementContext hintStatement : selectHintContext.hintStatements) {
                    if (hintStatement.USE_MV() != null) {
                        hints.add(new SelectHintUseMv("USE_MV", getTableList(hintStatement.tableList), true));
                        continue;
                    } else if (hintStatement.NO_USE_MV() != null) {
                        hints.add(new SelectHintUseMv("NO_USE_MV", getTableList(hintStatement.tableList), false));
                        continue;
                    }
                    String hintName = hintStatement.hintName().getText().toLowerCase(Locale.ROOT);
                    switch (hintName) {
                        case "set_var":
                            Map<String, Optional<String>> parameters = Maps.newLinkedHashMap();
                            for (HintAssignmentContext kv : hintStatement.parameters) {
                                if (kv.key != null) {
                                    String parameterName = visitIdentifierOrText(kv.key);
                                    Optional<String> value = Optional.empty();
                                    if (kv.constantValue != null) {
                                        Literal literal = (Literal) visit(kv.constantValue);
                                        value = Optional.ofNullable(literal.toLegacyLiteral().getStringValue());
                                    } else if (kv.identifierValue != null) {
                                        // maybe we should throw exception when the identifierValue is quoted identifier
                                        value = Optional.ofNullable(kv.identifierValue.getText());
                                    }
                                    parameters.put(parameterName, value);
                                }
                            }
                            SelectHintSetVar setVar = new SelectHintSetVar(hintName, parameters);
                            setVar.setVarOnceInSql(ConnectContext.get().getStatementContext());
                            hints.add(setVar);
                            break;
                        case "leading":
                            List<String> leadingParameters = new ArrayList<>();
                            int idx = 0;
                            Map<String, DistributeHint> strToHint = new HashMap<>();
                            for (HintAssignmentContext kv : hintStatement.parameters) {
                                if (kv.key == null) {
                                    continue;
                                }
                                StringBuilder parameterName = new StringBuilder();
                                String str = visitIdentifierOrText(kv.key);
                                if (JoinDistributeType.SHUFFLE.toString().equalsIgnoreCase(str)) {
                                    parameterName.append(str).append(idx++);
                                    leadingParameters.add(parameterName.toString());
                                    DistributeHint distributeHint;
                                    if (kv.skew == null) {
                                        distributeHint = new DistributeHint(DistributeType.SHUFFLE_RIGHT);
                                    } else {
                                        List<String> identifiers = kv.skew.qualifiedName().identifier()
                                                .stream().map(RuleContext::getText)
                                                .collect(ImmutableList.toImmutableList());
                                        Expression skewExpr = new UnboundSlot(identifiers);
                                        List<Expression> skewValues = new ArrayList<>();
                                        for (ConstantContext constantContext : kv.skew.constantList().values) {
                                            skewValues.add(typedVisit(constantContext));
                                        }
                                        JoinSkewInfo skewInfo = new JoinSkewInfo(skewExpr, skewValues, false);
                                        distributeHint = new DistributeHint(DistributeType.SHUFFLE_RIGHT, skewInfo);
                                    }
                                    strToHint.put(parameterName.toString(), distributeHint);
                                } else if (JoinDistributeType.BROADCAST.toString().equalsIgnoreCase(str)) {
                                    parameterName.append(str).append(idx++);
                                    leadingParameters.add(parameterName.toString());
                                    strToHint.put(parameterName.toString(),
                                            new DistributeHint(DistributeType.BROADCAST_RIGHT));
                                } else {
                                    leadingParameters.add(str);
                                }
                            }
                            hints.add(new SelectHintLeading(hintName, leadingParameters, strToHint));
                            break;
                        case "ordered":
                            hints.add(new SelectHintOrdered(hintName));
                            break;
                        case "use_cbo_rule":
                            List<String> useRuleParameters = new ArrayList<>();
                            for (HintAssignmentContext kv : hintStatement.parameters) {
                                if (kv.key != null) {
                                    String parameterName = visitIdentifierOrText(kv.key);
                                    useRuleParameters.add(parameterName);
                                }
                            }
                            hints.add(new SelectHintUseCboRule(hintName, useRuleParameters, false));
                            break;
                        case "no_use_cbo_rule":
                            List<String> noUseRuleParameters = new ArrayList<>();
                            for (HintAssignmentContext kv : hintStatement.parameters) {
                                String parameterName = visitIdentifierOrText(kv.key);
                                if (kv.key != null) {
                                    noUseRuleParameters.add(parameterName);
                                }
                            }
                            hints.add(new SelectHintUseCboRule(hintName, noUseRuleParameters, true));
                            break;
                        default:
                            break;
                    }
                }
            }
            newPlan = new LogicalSelectHint<>(hints.build(), newPlan);
        }
        if (!preAggOnHintContexts.isEmpty()) {
            for (ParserRuleContext hintContext : preAggOnHintContexts) {
                if (hintContext instanceof SelectHintContext) {
                    SelectHintContext preAggOnHintContext = (SelectHintContext) hintContext;
                    if (preAggOnHintContext.hintStatement != null
                            && preAggOnHintContext.hintStatement.hintName() != null) {
                        String text = preAggOnHintContext.hintStatement.hintName().getText();
                        if (text.equalsIgnoreCase("PREAGGOPEN")) {
                            newPlan = new LogicalPreAggOnHint<>(newPlan);
                            break;
                        }
                    }
                }
            }
        }
        return newPlan;
    }

    @Override
    public DistributeHint visitDistributeType(DistributeTypeContext ctx) {
        String hint = ctx.identifier().getText();
        DistributeType distributeType;
        if (DistributeType.JoinDistributeType.SHUFFLE.toString().equalsIgnoreCase(hint)) {
            distributeType = DistributeType.SHUFFLE_RIGHT;
            if (ctx.skewHint() != null) {
                List<String> identifiers = ctx.skewHint().qualifiedName().identifier()
                        .stream().map(RuleContext::getText)
                        .collect(ImmutableList.toImmutableList());
                Expression skewExpr = new UnboundSlot(identifiers);
                List<Expression> skewValues = new ArrayList<>();
                for (ConstantContext constantContext : ctx.skewHint().constantList().values) {
                    skewValues.add(typedVisit(constantContext));
                }
                return new DistributeHint(distributeType, new JoinSkewInfo(skewExpr, skewValues, false));
            }
        } else if (DistributeType.JoinDistributeType.BROADCAST.toString().equalsIgnoreCase(hint)) {
            distributeType = DistributeType.BROADCAST_RIGHT;
        } else {
            throw new ParseException("Invalid join hint: " + hint, ctx);
        }
        return new DistributeHint(distributeType);
    }

    @Override
    public List<String> visitBracketRelationHint(BracketRelationHintContext ctx) {
        return ctx.identifier().stream()
                .map(RuleContext::getText)
                .collect(ImmutableList.toImmutableList());
    }

    @Override
    public Object visitCommentRelationHint(CommentRelationHintContext ctx) {
        return ctx.identifier().stream()
                .map(RuleContext::getText)
                .collect(ImmutableList.toImmutableList());
    }

    protected LogicalPlan withProjection(LogicalPlan input, SelectColumnClauseContext selectCtx,
            Optional<AggClauseContext> aggCtx, boolean isDistinct) {
        return ParserUtils.withOrigin(selectCtx, () -> {
            if (aggCtx.isPresent()) {
                if (isDistinct) {
                    return new LogicalProject<>(ImmutableList.of(new UnboundStar(ImmutableList.of())),
                            isDistinct, input);
                } else {
                    return input;
                }
            } else {
                List<NamedExpression> projects = getNamedExpressions(selectCtx.namedExpressionSeq());
                if (input instanceof OneRowRelation) {
                    if (projects.stream().anyMatch(project -> project instanceof UnboundStar)) {
                        throw new ParseException("SELECT * must have a FROM clause");
                    }
                }
                if (input instanceof LogicalOneRowRelation) {
                    return new UnboundOneRowRelation(((LogicalOneRowRelation) input).getRelationId(), projects);
                }
                return new LogicalProject<>(projects, isDistinct, input);
            }
        });
    }

    private LogicalPlan withRelations(LogicalPlan inputPlan, List<RelationContext> relations) {
        if (relations == null) {
            return inputPlan;
        }
        LogicalPlan left = inputPlan;
        for (RelationContext relation : relations) {
            // build left deep join tree
            LogicalPlan right = withJoinRelations(visitRelation(relation), relation);
            left = (left == null) ? right :
                    new LogicalJoin<>(
                            JoinType.CROSS_JOIN,
                            ExpressionUtils.EMPTY_CONDITION,
                            ExpressionUtils.EMPTY_CONDITION,
                            new DistributeHint(DistributeType.NONE),
                            Optional.empty(),
                            left,
                            right, null);
            // TODO: pivot and lateral view
        }
        return left;
    }

    private LogicalPlan withFilter(LogicalPlan input, Optional<WhereClauseContext> whereCtx) {
        return input.optionalMap(whereCtx, () ->
                new LogicalFilter<>(ExpressionUtils.extractConjunctionToSet(
                        getExpression(whereCtx.get().booleanExpression())), input));
    }

    private LogicalPlan withAggregate(LogicalPlan input, SelectColumnClauseContext selectCtx,
            Optional<AggClauseContext> aggCtx, List<OrderKey> orderKeys) {
        return input.optionalMap(aggCtx, () -> {
            GroupingElementContext groupingElementContext = aggCtx.get().groupingElement();
            List<NamedExpression> namedExpressions = getNamedExpressions(selectCtx.namedExpressionSeq());
            if (groupingElementContext.GROUPING() != null) {
                ImmutableList.Builder<List<Expression>> groupingSets = ImmutableList.builder();
                for (GroupingSetContext groupingSetContext : groupingElementContext.groupingSet()) {
                    groupingSets.add(visit(groupingSetContext.expression(), Expression.class));
                }
                return new LogicalRepeat<>(groupingSets.build(), namedExpressions, input);
            } else if (groupingElementContext.CUBE() != null) {
                List<Expression> cubeExpressions = visit(groupingElementContext.expression(), Expression.class);
                List<List<Expression>> groupingSets = ExpressionUtils.cubeToGroupingSets(cubeExpressions);
                return new LogicalRepeat<>(groupingSets, namedExpressions, input);
            } else if (groupingElementContext.ROLLUP() != null && groupingElementContext.WITH() == null) {
                List<Expression> rollupExpressions = visit(groupingElementContext.expression(), Expression.class);
                List<List<Expression>> groupingSets = ExpressionUtils.rollupToGroupingSets(rollupExpressions);
                return new LogicalRepeat<>(groupingSets, namedExpressions, input);
            } else {
                List<GroupKeyWithOrder> groupKeyWithOrders = visit(groupingElementContext.expressionWithOrder(),
                        GroupKeyWithOrder.class);
                ImmutableList<Expression> groupByExpressions = groupKeyWithOrders.stream()
                        .map(GroupKeyWithOrder::getExpr)
                        .collect(ImmutableList.toImmutableList());
                if (groupKeyWithOrders.stream().anyMatch(GroupKeyWithOrder::hasOrder)) {
                    groupKeyWithOrders.stream()
                            .map(e -> new OrderKey(e.getExpr(), e.isAsc(), e.isAsc()))
                            .forEach(orderKeys::add);
                }
                if (groupingElementContext.ROLLUP() != null) {
                    List<List<Expression>> groupingSets = ExpressionUtils.rollupToGroupingSets(groupByExpressions);
                    return new LogicalRepeat<>(groupingSets, namedExpressions, input);
                } else {
                    return new LogicalAggregate<>(groupByExpressions, namedExpressions, input);
                }
            }
        });
    }

    private LogicalPlan withHaving(LogicalPlan input, Optional<HavingClauseContext> havingCtx) {
        return input.optionalMap(havingCtx, () -> {
            if (!(input instanceof Aggregate)) {
                throw new ParseException("Having clause should be applied against an aggregation.", havingCtx.get());
            }
            return new LogicalHaving<>(ExpressionUtils.extractConjunctionToSet(
                    getExpression((havingCtx.get().booleanExpression()))), input);
        });
    }

    /**
     * match predicate type and generate different predicates.
     *
     * @param ctx PredicateContext
     * @param valueExpression valueExpression
     * @return Expression
     */
    private Expression withPredicate(Expression valueExpression, PredicateContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            Expression outExpression;
            switch (ctx.kind.getType()) {
                case DorisParser.BETWEEN:
                    Expression lower = getExpression(ctx.lower);
                    Expression upper = getExpression(ctx.upper);
                    if (lower.equals(upper)) {
                        outExpression = new EqualTo(valueExpression, lower);
                    } else {
                        outExpression = new And(
                                new GreaterThanEqual(valueExpression, getExpression(ctx.lower)),
                                new LessThanEqual(valueExpression, getExpression(ctx.upper))
                        );
                    }
                    break;
                case DorisParser.LIKE:
                    if (ctx.ESCAPE() == null) {
                        outExpression = new Like(
                                valueExpression,
                                getExpression(ctx.pattern));
                    } else {
                        outExpression = new Like(
                                valueExpression,
                                getExpression(ctx.pattern),
                                getExpression(ctx.escape));
                    }
                    break;
                case DorisParser.RLIKE:
                case DorisParser.REGEXP:
                    outExpression = new Regexp(
                            valueExpression,
                            getExpression(ctx.pattern)
                    );
                    break;
                case DorisParser.IN:
                    if (ctx.query() == null) {
                        outExpression = new InPredicate(
                                valueExpression,
                                withInList(ctx)
                        );
                    } else {
                        outExpression = new InSubquery(
                                valueExpression,
                                typedVisit(ctx.query()),
                                ctx.NOT() != null
                        );
                    }
                    break;
                case DorisParser.NULL:
                    outExpression = new IsNull(valueExpression);
                    break;
                case DorisParser.TRUE:
                    outExpression = new Cast(valueExpression,
                            BooleanType.INSTANCE, true);
                    break;
                case DorisParser.FALSE:
                    outExpression = new Not(new Cast(valueExpression,
                            BooleanType.INSTANCE, true));
                    break;
                case DorisParser.MATCH:
                case DorisParser.MATCH_ANY:
                    outExpression = new MatchAny(
                            valueExpression,
                            getExpression(ctx.pattern)
                    );
                    break;
                case DorisParser.MATCH_ALL:
                    outExpression = new MatchAll(
                            valueExpression,
                            getExpression(ctx.pattern)
                    );
                    break;
                case DorisParser.MATCH_PHRASE:
                    outExpression = new MatchPhrase(
                            valueExpression,
                            getExpression(ctx.pattern)
                    );
                    break;
                case DorisParser.MATCH_PHRASE_PREFIX:
                    outExpression = new MatchPhrasePrefix(
                            valueExpression,
                            getExpression(ctx.pattern)
                    );
                    break;
                case DorisParser.MATCH_REGEXP:
                    outExpression = new MatchRegexp(
                            valueExpression,
                            getExpression(ctx.pattern)
                    );
                    break;
                case DorisParser.MATCH_PHRASE_EDGE:
                    outExpression = new MatchPhraseEdge(
                            valueExpression,
                            getExpression(ctx.pattern)
                    );
                    break;
                default:
                    throw new ParseException("Unsupported predicate type: " + ctx.kind.getText(), ctx);
            }
            return ctx.NOT() != null ? new Not(outExpression) : outExpression;
        });
    }

    private List<NamedExpression> getNamedExpressions(NamedExpressionSeqContext namedCtx) {
        return ParserUtils.withOrigin(namedCtx, () -> visit(namedCtx.namedExpression(), NamedExpression.class));
    }

    @Override
    public Expression visitSubqueryExpression(SubqueryExpressionContext subqueryExprCtx) {
        return ParserUtils.withOrigin(subqueryExprCtx, () -> new ScalarSubquery(typedVisit(subqueryExprCtx.query())));
    }

    @Override
    public Expression visitExist(ExistContext context) {
        return ParserUtils.withOrigin(context, () -> new Exists(typedVisit(context.query()), false));
    }

    @Override
    public Expression visitIsnull(IsnullContext context) {
        return ParserUtils.withOrigin(context, () -> new IsNull(typedVisit(context.valueExpression())));
    }

    @Override
    public Expression visitIs_not_null_pred(Is_not_null_predContext context) {
        return ParserUtils.withOrigin(context, () -> new Not(new IsNull(typedVisit(context.valueExpression()))));
    }

    public List<Expression> withInList(PredicateContext ctx) {
        return ctx.expression().stream().map(this::getExpression).collect(ImmutableList.toImmutableList());
    }

    @Override
    public Literal visitDecimalLiteral(DecimalLiteralContext ctx) {
        try {
            if (Config.enable_decimal_conversion) {
                return new DecimalV3Literal(new BigDecimal(ctx.getText()));
            } else {
                return new DecimalLiteral(new BigDecimal(ctx.getText()));
            }
        } catch (Exception e) {
            return new DoubleLiteral(Double.parseDouble(ctx.getText()));
        }
    }

    private String parsePropertyKey(PropertyKeyContext item) {
        if (item.constant() != null) {
            return parseConstant(item.constant()).trim();
        }
        return item.getText().trim();
    }

    private String parsePropertyValue(PropertyValueContext item) {
        if (item.constant() != null) {
            return parseConstant(item.constant());
        }
        return item.getText();
    }

    private ExplainLevel parseExplainPlanType(PlanTypeContext planTypeContext) {
        if (planTypeContext == null || planTypeContext.ALL() != null) {
            return ExplainLevel.ALL_PLAN;
        }
        if (planTypeContext.PHYSICAL() != null || planTypeContext.OPTIMIZED() != null) {
            return ExplainLevel.OPTIMIZED_PLAN;
        }
        if (planTypeContext.REWRITTEN() != null || planTypeContext.LOGICAL() != null) {
            return ExplainLevel.REWRITTEN_PLAN;
        }
        if (planTypeContext.ANALYZED() != null) {
            return ExplainLevel.ANALYZED_PLAN;
        }
        if (planTypeContext.PARSED() != null) {
            return ExplainLevel.PARSED_PLAN;
        }
        if (planTypeContext.SHAPE() != null) {
            return ExplainLevel.SHAPE_PLAN;
        }
        if (planTypeContext.MEMO() != null) {
            return ExplainLevel.MEMO_PLAN;
        }
        if (planTypeContext.DISTRIBUTED() != null) {
            return ExplainLevel.DISTRIBUTED_PLAN;
        }
        return ExplainLevel.ALL_PLAN;
    }

    @Override
    public Pair<DataType, Boolean> visitDataTypeWithNullable(DataTypeWithNullableContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> Pair.of(typedVisit(ctx.dataType()), ctx.NOT() == null));
    }

    @Override
    public DataType visitAggStateDataType(AggStateDataTypeContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            List<Pair<DataType, Boolean>> dataTypeWithNullables = ctx.dataTypes.stream()
                    .map(this::visitDataTypeWithNullable)
                    .collect(Collectors.toList());
            List<DataType> dataTypes = dataTypeWithNullables.stream()
                    .map(dt -> dt.first)
                    .collect(ImmutableList.toImmutableList());
            List<Boolean> nullables = dataTypeWithNullables.stream()
                    .map(dt -> dt.second)
                    .collect(ImmutableList.toImmutableList());
            String functionName = ctx.functionNameIdentifier().getText();
            if (!BuiltinAggregateFunctions.INSTANCE.aggFuncNames.contains(functionName)) {
                // TODO use function binder to check function exists
                throw new ParseException("Can not found function '" + functionName + "'", ctx);
            }
            return new AggStateType(functionName, dataTypes, nullables);
        });
    }

    @Override
    public DataType visitPrimitiveDataType(PrimitiveDataTypeContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            String dataType = ctx.primitiveColType().type.getText().toLowerCase(Locale.ROOT);
            if (dataType.equalsIgnoreCase("all")) {
                throw new NotSupportedException("Disable to create table with `ALL` type columns");
            }
            List<String> l = Lists.newArrayList(dataType);
            ctx.INTEGER_VALUE().stream().map(ParseTree::getText).forEach(l::add);
            return DataType.convertPrimitiveFromStrings(l);
        });
    }

    @Override
    public DataType visitComplexDataType(ComplexDataTypeContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            switch (ctx.complex.getType()) {
                case DorisParser.ARRAY:
                    return ArrayType.of(typedVisit(ctx.dataType(0)), true);
                case DorisParser.MAP:
                    return MapType.of(typedVisit(ctx.dataType(0)), typedVisit(ctx.dataType(1)));
                case DorisParser.STRUCT:
                    return new StructType(visitComplexColTypeList(ctx.complexColTypeList()));
                default:
                    throw new AnalysisException("do not support " + ctx.complex.getText() + " type for Nereids");
            }
        });
    }

    @Override
    public DataType visitVariantPredefinedFields(VariantPredefinedFieldsContext ctx) {
        VariantTypeDefinitionsContext variantDef = ctx.complex;
        Preconditions.checkState(variantDef instanceof VariantContext,
                                        "Unsupported variant definition: " + variantDef.getText());
        VariantContext variantCtx = (VariantContext) variantDef;

        List<VariantField> fields = variantCtx.variantSubColTypeList() != null
                ? visitVariantSubColTypeList(variantCtx.variantSubColTypeList()) : Lists.newArrayList();
        Map<String, String> properties = variantCtx.properties != null
                ? Maps.newHashMap(visitPropertyClause(variantCtx.properties)) : Maps.newHashMap();

        int variantMaxSubcolumnsCount = ConnectContext.get() == null ? 0 :
                ConnectContext.get().getSessionVariable().getDefaultVariantMaxSubcolumnsCount();
        boolean enableTypedPathsToSparse = ConnectContext.get() == null ? false :
                ConnectContext.get().getSessionVariable().getDefaultEnableTypedPathsToSparse();

        try {
            variantMaxSubcolumnsCount = PropertyAnalyzer
                                        .analyzeVariantMaxSubcolumnsCount(properties, variantMaxSubcolumnsCount);
            enableTypedPathsToSparse = PropertyAnalyzer
                                        .analyzeEnableTypedPathsToSparse(properties, enableTypedPathsToSparse);
        } catch (org.apache.doris.common.AnalysisException e) {
            throw new NotSupportedException(e.getMessage());
        }

        if (!properties.isEmpty()) {
            throw new NotSupportedException("only support for "
                    + PropertyAnalyzer.PROPERTIES_VARIANT_ENABLE_TYPED_PATHS_TO_SPARSE
                    + " and " + PropertyAnalyzer.PROPERTIES_VARIANT_MAX_SUBCOLUMNS_COUNT);
        }

        return new VariantType(fields, variantMaxSubcolumnsCount, enableTypedPathsToSparse);
    }

    @Override
    public List<VariantField> visitVariantSubColTypeList(VariantSubColTypeListContext ctx) {
        return ctx.variantSubColType().stream().map(
                this::visitVariantSubColType).collect(ImmutableList.toImmutableList());
    }

    @Override
    public VariantField visitVariantSubColType(VariantSubColTypeContext ctx) {
        String comment;
        if (ctx.commentSpec() != null) {
            comment = ctx.commentSpec().STRING_LITERAL().getText();
            comment = LogicalPlanBuilderAssistant.escapeBackSlash(comment.substring(1, comment.length() - 1));
        } else {
            comment = "";
        }
        String pattern = ctx.STRING_LITERAL().getText();
        pattern = pattern.substring(1, pattern.length() - 1);
        if (ctx.variantSubColMatchType() != null) {
            return new VariantField(pattern, typedVisit(ctx.dataType()), comment,
                    ctx.variantSubColMatchType().getText());
        }
        return new VariantField(pattern, typedVisit(ctx.dataType()), comment);
    }

    @Override
    public List<StructField> visitComplexColTypeList(ComplexColTypeListContext ctx) {
        return ctx.complexColType().stream().map(this::visitComplexColType).collect(ImmutableList.toImmutableList());
    }

    @Override
    public StructField visitComplexColType(ComplexColTypeContext ctx) {
        String comment;
        if (ctx.commentSpec() != null) {
            comment = ctx.commentSpec().STRING_LITERAL().getText();
            comment = LogicalPlanBuilderAssistant.escapeBackSlash(comment.substring(1, comment.length() - 1));
        } else {
            comment = "";
        }
        return new StructField(ctx.identifier().getText(), typedVisit(ctx.dataType()), true, comment);
    }

    private String parseConstant(ConstantContext context) {
        Object constant = visit(context);
        if (constant instanceof Literal && ((Literal) constant).isStringLikeLiteral()) {
            return ((Literal) constant).getStringValue();
        }
        return context.getText();
    }

    @Override
    public Object visitCollate(CollateContext ctx) {
        return visit(ctx.primaryExpression());
    }

    @Override
    public Object visitSample(SampleContext ctx) {
        long seek = ctx.seed == null ? -1L : Long.parseLong(ctx.seed.getText());
        DorisParser.SampleMethodContext sampleContext = ctx.sampleMethod();
        if (sampleContext instanceof SampleByPercentileContext) {
            SampleByPercentileContext sampleByPercentileContext = (SampleByPercentileContext) sampleContext;
            long percent = Long.parseLong(sampleByPercentileContext.INTEGER_VALUE().getText());
            return new TableSample(percent, true, seek);
        }
        SampleByRowsContext sampleByRowsContext = (SampleByRowsContext) sampleContext;
        long rows = Long.parseLong(sampleByRowsContext.INTEGER_VALUE().getText());
        return new TableSample(rows, false, seek);
    }

    @Override
    public LogicalPlan visitShowCreateLoad(ShowCreateLoadContext ctx) {
        List<String> labelParts = visitMultipartIdentifier(ctx.label);
        String labelName = null;
        String labelDbName = null;
        if (ConnectContext.get().getDatabase().isEmpty() && labelParts.size() == 1) {
            throw new AnalysisException("Current database is not set.");
        } else if (labelParts.size() == 1) {
            labelName = labelParts.get(0);
        } else if (labelParts.size() == 2) {
            labelDbName = labelParts.get(0);
            labelName = labelParts.get(1);
        } else if (labelParts.size() == 3) {
            labelDbName = labelParts.get(1);
            labelName = labelParts.get(2);
        } else {
            throw new AnalysisException("labelParts in load should be [ctl.][db.]label");
        }
        LabelNameInfo jobLabelInfo = new LabelNameInfo(labelDbName, labelName);
        return new ShowCreateLoadCommand(jobLabelInfo);
    }

    @Override
    public Object visitCallProcedure(CallProcedureContext ctx) {
        List<String> nameParts = visitMultipartIdentifier(ctx.name);
        FuncNameInfo procedureName = new FuncNameInfo(nameParts);
        List<Expression> arguments = ctx.expression().stream()
                .<Expression>map(this::typedVisit)
                .collect(ImmutableList.toImmutableList());
        UnboundFunction unboundFunction = new UnboundFunction(procedureName.getDbName(), procedureName.getName(),
                true, arguments, false);
        return new CallCommand(unboundFunction, getOriginSql(ctx));
    }

    @Override
    public LogicalPlan visitCreateProcedure(CreateProcedureContext ctx) {
        List<String> nameParts = visitMultipartIdentifier(ctx.name);
        FuncNameInfo procedureName = new FuncNameInfo(nameParts);
        return ParserUtils.withOrigin(ctx, () -> {
            LogicalPlan createProcedurePlan;
            createProcedurePlan = new CreateProcedureCommand(procedureName, getOriginSql(ctx),
                    ctx.REPLACE() != null);
            return createProcedurePlan;
        });
    }

    @Override
    public LogicalPlan visitDropProcedure(DropProcedureContext ctx) {
        List<String> nameParts = visitMultipartIdentifier(ctx.name);
        FuncNameInfo procedureName = new FuncNameInfo(nameParts);
        return ParserUtils.withOrigin(ctx, () -> new DropProcedureCommand(procedureName, getOriginSql(ctx)));
    }

    @Override
    public LogicalPlan visitShowProcedureStatus(ShowProcedureStatusContext ctx) {
        Set<Expression> whereExpr = Collections.emptySet();
        if (ctx.whereClause() != null) {
            whereExpr = ExpressionUtils.extractConjunctionToSet(
                    getExpression(ctx.whereClause().booleanExpression()));
        }

        if (ctx.valueExpression() != null) {
            // parser allows only LIKE or WhereClause.
            // Mysql grammar: SHOW PROCEDURE STATUS [LIKE 'pattern' | WHERE expr]
            whereExpr = Sets.newHashSet(new Like(new UnboundSlot("ProcedureName"), getExpression(ctx.pattern)));
        }

        final Set<Expression> whereExprConst = whereExpr;
        return ParserUtils.withOrigin(ctx, () -> new ShowProcedureStatusCommand(whereExprConst));
    }

    @Override
    public LogicalPlan visitShowCreateProcedure(ShowCreateProcedureContext ctx) {
        List<String> nameParts = visitMultipartIdentifier(ctx.name);
        FuncNameInfo procedureName = new FuncNameInfo(nameParts);
        return ParserUtils.withOrigin(ctx, () -> new ShowCreateProcedureCommand(procedureName));
    }

    @Override
    public LogicalPlan visitCreateSqlBlockRule(CreateSqlBlockRuleContext ctx) {
        Map<String, String> properties = ctx.propertyClause() != null
                ? Maps.newHashMap(visitPropertyClause(ctx.propertyClause())) : Maps.newHashMap();
        return new CreateSqlBlockRuleCommand(stripQuotes(ctx.name.getText()), ctx.EXISTS() != null, properties);
    }

    @Override
    public LogicalPlan visitAlterSqlBlockRule(AlterSqlBlockRuleContext ctx) {
        Map<String, String> properties = ctx.propertyClause() != null
                ? Maps.newHashMap(visitPropertyClause(ctx.propertyClause())) : Maps.newHashMap();
        return new AlterSqlBlockRuleCommand(stripQuotes(ctx.name.getText()), properties);
    }

    @Override
    public LogicalPlan visitDropCatalogRecycleBin(DropCatalogRecycleBinContext ctx) {
        String idTypeStr = ctx.idType.getText().substring(1, ctx.idType.getText().length() - 1);
        IdType idType = IdType.fromString(idTypeStr);
        long id = Long.parseLong(ctx.id.getText());

        return ParserUtils.withOrigin(ctx, () -> new DropCatalogRecycleBinCommand(idType, id));
    }

    @Override
    public LogicalPlan visitSupportedUnsetStatement(SupportedUnsetStatementContext ctx) {
        if (ctx.DEFAULT() != null && ctx.STORAGE() != null && ctx.VAULT() != null) {
            return new UnsetDefaultStorageVaultCommand();
        }
        SetType statementScope = visitStatementScope(ctx.statementScope());
        if (ctx.ALL() != null) {
            return new UnsetVariableCommand(statementScope, true);
        } else if (ctx.identifier() != null) {
            return new UnsetVariableCommand(statementScope, ctx.identifier().getText());
        }
        throw new AnalysisException("Should add 'ALL' or variable name");
    }

    @Override
    public LogicalPlan visitCreateTableLike(CreateTableLikeContext ctx) {
        List<String> nameParts = visitMultipartIdentifier(ctx.name);
        List<String> existedTableNameParts = visitMultipartIdentifier(ctx.existedTable);
        ArrayList<String> rollupNames = Lists.newArrayList();
        boolean withAllRollUp = false;
        if (ctx.WITH() != null && ctx.rollupNames != null) {
            rollupNames = new ArrayList<>(visitIdentifierList(ctx.rollupNames));
        } else if (ctx.WITH() != null && ctx.rollupNames == null) {
            withAllRollUp = true;
        }
        CreateTableLikeInfo info = new CreateTableLikeInfo(ctx.EXISTS() != null,
                ctx.TEMPORARY() != null,
                new TableNameInfo(nameParts), new TableNameInfo(existedTableNameParts),
                rollupNames, withAllRollUp);
        return new CreateTableLikeCommand(info);
    }

    @Override
    public Command visitCreateUserDefineFunction(CreateUserDefineFunctionContext ctx) {
        SetType statementScope = visitStatementScope(ctx.statementScope());
        boolean ifNotExists = ctx.EXISTS() != null;
        boolean isAggFunction = ctx.AGGREGATE() != null;
        boolean isTableFunction = ctx.TABLES() != null;
        FunctionName function = visitFunctionIdentifier(ctx.functionIdentifier());
        FunctionArgTypesInfo functionArgTypesInfo;
        if (ctx.functionArguments() != null) {
            functionArgTypesInfo = visitFunctionArguments(ctx.functionArguments());
        } else {
            functionArgTypesInfo = new FunctionArgTypesInfo(new ArrayList<>(), false);
        }
        DataType returnType = typedVisit(ctx.returnType);
        returnType = returnType.conversion();
        DataType intermediateType = ctx.intermediateType != null ? typedVisit(ctx.intermediateType) : null;
        if (intermediateType != null) {
            intermediateType = intermediateType.conversion();
        }
        Map<String, String> properties = ctx.propertyClause() != null
                ? Maps.newHashMap(visitPropertyClause(ctx.propertyClause()))
                : Maps.newHashMap();
        return new CreateFunctionCommand(statementScope, ifNotExists, isAggFunction, false, isTableFunction,
                function, functionArgTypesInfo, returnType, intermediateType,
                null, null, properties);
    }

    @Override
    public Command visitCreateAliasFunction(CreateAliasFunctionContext ctx) {
        SetType statementScope = visitStatementScope(ctx.statementScope());
        boolean ifNotExists = ctx.EXISTS() != null;
        FunctionName function = visitFunctionIdentifier(ctx.functionIdentifier());
        FunctionArgTypesInfo functionArgTypesInfo;
        if (ctx.functionArguments() != null) {
            functionArgTypesInfo = visitFunctionArguments(ctx.functionArguments());
        } else {
            functionArgTypesInfo = new FunctionArgTypesInfo(new ArrayList<>(), false);
        }
        List<String> parameters = ctx.parameters != null ? visitIdentifierSeq(ctx.parameters) : new ArrayList<>();
        Expression originFunction = getExpression(ctx.expression());
        return new CreateFunctionCommand(statementScope, ifNotExists, false, true, false,
                function, functionArgTypesInfo, VarcharType.MAX_VARCHAR_TYPE, null,
                parameters, originFunction, null);
    }

    @Override
    public Command visitDropFunction(DropFunctionContext ctx) {
        SetType statementScope = visitStatementScope(ctx.statementScope());
        boolean ifExists = ctx.EXISTS() != null;
        FunctionName function = visitFunctionIdentifier(ctx.functionIdentifier());
        FunctionArgTypesInfo functionArgTypesInfo;
        if (ctx.functionArguments() != null) {
            functionArgTypesInfo = visitFunctionArguments(ctx.functionArguments());
        } else {
            functionArgTypesInfo = new FunctionArgTypesInfo(new ArrayList<>(), false);
        }
        return new DropFunctionCommand(statementScope, ifExists, function, functionArgTypesInfo);
    }

    @Override
    public FunctionArgTypesInfo visitFunctionArguments(FunctionArgumentsContext ctx) {
        boolean isVariadic = ctx.DOTDOTDOT() != null;
        List<DataType> argTypeDefs;
        if (ctx.dataTypeList() != null) {
            argTypeDefs = visitDataTypeList(ctx.dataTypeList());
        } else {
            argTypeDefs = new ArrayList<>();
        }
        return new FunctionArgTypesInfo(argTypeDefs, isVariadic);
    }

    @Override
    public FunctionName visitFunctionIdentifier(FunctionIdentifierContext ctx) {
        String functionName = ctx.functionNameIdentifier().getText();
        String dbName = ctx.dbName != null ? ctx.dbName.getText() : null;
        return new FunctionName(dbName, functionName);
    }

    @Override
    public List<DataType> visitDataTypeList(DataTypeListContext ctx) {
        List<DataType> dataTypeList = new ArrayList<>(ctx.getChildCount());
        for (DorisParser.DataTypeContext dataTypeContext : ctx.dataType()) {
            DataType dataType = typedVisit(dataTypeContext);
            dataTypeList.add(dataType.conversion());
        }
        return dataTypeList;
    }

    @Override
    public LogicalPlan visitShowAuthors(ShowAuthorsContext ctx) {
        return new ShowAuthorsCommand();
    }

    @Override
    public LogicalPlan visitShowAnalyzeTask(ShowAnalyzeTaskContext ctx) {
        long jobId = Long.parseLong(ctx.jobId.getText());
        return new ShowAnalyzeTaskCommand(jobId);
    }

    @Override
    public LogicalPlan visitShowEvents(ShowEventsContext ctx) {
        return new ShowEventsCommand();
    }

    @Override
    public LogicalPlan visitShowExport(ShowExportContext ctx) {
        String ctlName = null;
        String dbName = null;
        Expression wildWhere = null;
        List<OrderKey> orderKeys = null;
        long limit = -1L;
        if (ctx.database != null) {
            List<String> nameParts = visitMultipartIdentifier(ctx.database);
            if (nameParts.size() == 1) {
                dbName = nameParts.get(0);
            } else if (nameParts.size() == 2) {
                ctlName = nameParts.get(0);
                dbName = nameParts.get(1);
            } else {
                throw new AnalysisException("nameParts in analyze database should be [ctl.]db");
            }
        }
        if (ctx.sortClause() != null) {
            orderKeys = visit(ctx.sortClause().sortItem(), OrderKey.class);
        }
        if (ctx.wildWhere() != null) {
            wildWhere = getWildWhere(ctx.wildWhere());
        }
        if (ctx.limitClause() != null) {
            limit = ctx.limitClause().limit != null
                    ? Long.parseLong(ctx.limitClause().limit.getText())
                    : 0;
            if (limit < 0) {
                throw new ParseException("Limit requires non-negative number", ctx.limitClause());
            }
        }
        return new ShowExportCommand(ctlName, dbName, wildWhere, orderKeys, limit);
    }

    @Override
    public LogicalPlan visitShowConfig(ShowConfigContext ctx) {
        ShowConfigCommand command;
        if (ctx.type.getText().equalsIgnoreCase(NodeType.FRONTEND.name())) {
            command = new ShowConfigCommand(NodeType.FRONTEND);
        } else {
            command = new ShowConfigCommand(NodeType.BACKEND);
        }
        if (ctx.LIKE() != null && ctx.pattern != null) {
            Like like = new Like(new UnboundSlot("ProcedureName"), getExpression(ctx.pattern));
            String pattern = ((Literal) like.child(1)).getStringValue();
            command.setPattern(pattern);
        }
        if (ctx.FROM() != null && ctx.backendId != null) {
            long backendId = Long.parseLong(ctx.backendId.getText());
            command.setBackendId(backendId);
        }
        return command;
    }

    @Override
    public SetOptionsCommand visitSetOptions(SetOptionsContext ctx) {
        List<SetVarOp> setVarOpList = new ArrayList<>(1);
        for (Object child : ctx.children) {
            if (child instanceof RuleNode) {
                setVarOpList.add(typedVisit((RuleNode) child));
            }
        }
        return new SetOptionsCommand(setVarOpList);
    }

    @Override
    public SetVarOp visitSetSystemVariable(SetSystemVariableContext ctx) {
        SetType statementScope = visitStatementScope(ctx.statementScope());
        String name = stripQuotes(ctx.identifier().getText());
        Expression expression = ctx.expression() != null ? typedVisit(ctx.expression()) : null;
        return new SetSessionVarOp(statementScope, name, expression);
    }

    @Override
    public SetVarOp visitSetVariableWithType(SetVariableWithTypeContext ctx) {
        SetType statementScope = visitStatementScope(ctx.statementScope());
        String name = stripQuotes(ctx.identifier().getText());
        Expression expression = ctx.expression() != null ? typedVisit(ctx.expression()) : null;
        return new SetSessionVarOp(statementScope, name, expression);
    }

    @Override
    public SetVarOp visitSetPassword(SetPasswordContext ctx) {
        String user;
        String host;
        boolean isDomain;
        String passwordText;
        UserIdentity userIdentity = null;
        if (ctx.userIdentify() != null) {
            user = stripQuotes(ctx.userIdentify().user.getText());
            host = ctx.userIdentify().host != null ? stripQuotes(ctx.userIdentify().host.getText()) : "%";
            isDomain = ctx.userIdentify().ATSIGN() != null;
            userIdentity = new UserIdentity(user, host, isDomain);
        }
        passwordText = stripQuotes(ctx.STRING_LITERAL().getText());
        return new SetPassVarOp(userIdentity, new PassVar(passwordText, ctx.isPlain != null));
    }

    @Override
    public SetVarOp visitSetNames(SetNamesContext ctx) {
        return new SetNamesVarOp();
    }

    @Override
    public SetVarOp visitSetCharset(SetCharsetContext ctx) {
        String charset = ctx.charsetName != null ? stripQuotes(ctx.charsetName.getText()) : null;
        return new SetCharsetAndCollateVarOp(charset);
    }

    @Override
    public SetVarOp visitSetCollate(SetCollateContext ctx) {
        String charset = ctx.charsetName != null ? stripQuotes(ctx.charsetName.getText()) : null;
        String collate = ctx.collateName != null ? stripQuotes(ctx.collateName.getText()) : null;
        return new SetCharsetAndCollateVarOp(charset, collate);
    }

    @Override
    public SetVarOp visitSetLdapAdminPassword(SetLdapAdminPasswordContext ctx) {
        String passwordText = stripQuotes(ctx.STRING_LITERAL().getText());
        boolean isPlain = ctx.PASSWORD() != null;
        return new SetLdapPassVarOp(new PassVar(passwordText, isPlain));
    }

    @Override
    public SetVarOp visitSetUserVariable(SetUserVariableContext ctx) {
        String name = stripQuotes(ctx.identifier().getText());
        Expression expression = typedVisit(ctx.expression());
        return new SetUserDefinedVarOp(name, expression);
    }

    @Override
    public SetTransactionCommand visitSetTransaction(SetTransactionContext ctx) {
        return new SetTransactionCommand();
    }

    @Override
    public LogicalPlan visitShowStorageVault(ShowStorageVaultContext ctx) {
        return new ShowStorageVaultCommand();
    }

    @Override
    public SetUserPropertiesCommand visitSetUserProperties(SetUserPropertiesContext ctx) {
        String user = ctx.user != null ? stripQuotes(ctx.user.getText()) : null;
        Map<String, String> userPropertiesMap = visitPropertyItemList(ctx.propertyItemList());
        List<SetUserPropertyVarOp> setUserPropertyVarOpList = new ArrayList<>(userPropertiesMap.size());
        for (Map.Entry<String, String> entry : userPropertiesMap.entrySet()) {
            setUserPropertyVarOpList.add(new SetUserPropertyVarOp(user, entry.getKey(), entry.getValue()));
        }
        return new SetUserPropertiesCommand(user, setUserPropertyVarOpList);
    }

    @Override
    public SetDefaultStorageVaultCommand visitSetDefaultStorageVault(SetDefaultStorageVaultContext ctx) {
        return new SetDefaultStorageVaultCommand(stripQuotes(ctx.identifier().getText()));
    }

    @Override
    public Object visitRefreshCatalog(RefreshCatalogContext ctx) {
        if (ctx.name != null) {
            String catalogName = ctx.name.getText();
            Map<String, String> properties = ctx.propertyClause() != null
                    ? Maps.newHashMap(visitPropertyClause(ctx.propertyClause())) : Maps.newHashMap();
            return new RefreshCatalogCommand(catalogName, properties);
        }
        throw new AnalysisException("catalog name can not be null");
    }

    @Override
    public RefreshDatabaseCommand visitRefreshDatabase(RefreshDatabaseContext ctx) {
        Map<String, String> properties = visitPropertyClause(ctx.propertyClause()) == null ? Maps.newHashMap()
                : visitPropertyClause(ctx.propertyClause());
        List<String> parts = visitMultipartIdentifier(ctx.name);
        int size = parts.size();
        if (size == 0) {
            throw new ParseException("database name can't be empty");
        }
        String dbName = parts.get(size - 1);

        // [db].
        if (size == 1) {
            return new RefreshDatabaseCommand(dbName, properties);
        } else if (parts.size() == 2) {  // [ctl,db].
            return new RefreshDatabaseCommand(parts.get(0), dbName, properties);
        }
        throw new ParseException("Only one dot can be in the name: " + String.join(".", parts));
    }

    @Override
    public Object visitRefreshTable(RefreshTableContext ctx) {
        List<String> parts = visitMultipartIdentifier(ctx.name);
        int size = parts.size();
        if (size == 0) {
            throw new ParseException("table name can't be empty");
        } else if (size <= 3) {
            return new RefreshTableCommand(new TableNameInfo(parts));
        }
        throw new ParseException("Only one or two dot can be in the name: " + String.join(".", parts));
    }

    @Override
    public LogicalPlan visitShowCreateRepository(ShowCreateRepositoryContext ctx) {
        return new ShowCreateRepositoryCommand(ctx.identifier().getText());
    }

    public LogicalPlan visitShowLastInsert(ShowLastInsertContext ctx) {
        return new ShowLastInsertCommand();
    }

    @Override
    public LogicalPlan visitShowLoad(ShowLoadContext ctx) {
        String dbName = null;
        Expression wildWhere = null;
        List<OrderKey> orderKeys = null;
        long limit = -1L;
        long offset = 0L;
        if (ctx.database != null) {
            dbName = ctx.database.getText();
        }
        if (ctx.sortClause() != null) {
            orderKeys = visit(ctx.sortClause().sortItem(), OrderKey.class);
        }
        if (ctx.wildWhere() != null) {
            wildWhere = getWildWhere(ctx.wildWhere());
        }
        if (ctx.limitClause() != null) {
            limit = ctx.limitClause().limit != null
                    ? Long.parseLong(ctx.limitClause().limit.getText())
                    : 0;
            if (limit < 0) {
                throw new ParseException("Limit requires non-negative number", ctx.limitClause());
            }
            offset = ctx.limitClause().offset != null
                    ? Long.parseLong(ctx.limitClause().offset.getText())
                    : 0;
            if (offset < 0) {
                throw new ParseException("Offset requires non-negative number", ctx.limitClause());
            }
        }
        boolean isStreamLoad = ctx.STREAM() != null;
        return new ShowLoadCommand(wildWhere, orderKeys, limit, offset, dbName, isStreamLoad);
    }

    @Override
    public LogicalPlan visitShowLoadProfile(ShowLoadProfileContext ctx) {
        String loadIdPath = "/"; // default load id path
        if (ctx.loadIdPath != null) {
            loadIdPath = stripQuotes(ctx.loadIdPath.getText());
        }

        long limit = 20;
        if (ctx.limitClause() != null) {
            limit = Long.parseLong(ctx.limitClause().limit.getText());
            if (limit < 0) {
                throw new ParseException("Limit requires non-negative number, got " + String.valueOf(limit));
            }
        }
        return new ShowLoadProfileCommand(loadIdPath, limit);
    }

    @Override
    public LogicalPlan visitShowLoadWarings(DorisParser.ShowLoadWaringsContext ctx) {
        String dbName = null;
        Expression wildWhere = null;
        long limit = -1L;
        long offset = 0L;
        if (ctx.wildWhere() != null) {
            wildWhere = getWildWhere(ctx.wildWhere());
        }
        if (ctx.limitClause() != null) {
            limit = ctx.limitClause().limit != null
                ? Long.parseLong(ctx.limitClause().limit.getText())
                : 0;
            if (limit < 0) {
                throw new ParseException("Limit requires non-negative number", ctx.limitClause());
            }
            offset = ctx.limitClause().offset != null
                ? Long.parseLong(ctx.limitClause().offset.getText())
                : 0;
            if (offset < 0) {
                throw new ParseException("Offset requires non-negative number", ctx.limitClause());
            }
        }
        if (ctx.database != null) {
            dbName = ctx.database.getText();
        }
        String url = null;
        if (ctx.url != null) {
            url = stripQuotes(ctx.url.getText());
        }

        return new ShowLoadWarningsCommand(dbName, wildWhere, limit, offset, url);
    }

    @Override
    public LogicalPlan visitShowDataTypes(ShowDataTypesContext ctx) {
        return new ShowDataTypesCommand();
    }

    @Override
    public LogicalPlan visitShowGrants(ShowGrantsContext ctx) {
        boolean all = (ctx.ALL() != null) ? true : false;
        return new ShowGrantsCommand(null, all);
    }

    @Override
    public LogicalPlan visitAlterStoragePolicy(AlterStoragePolicyContext ctx) {
        String policyName = visitIdentifierOrText(ctx.identifierOrText());
        Map<String, String> properties = visitPropertyClause(ctx.propertyClause()) == null ? Maps.newHashMap()
                : visitPropertyClause(ctx.propertyClause());

        return new AlterStoragePolicyCommand(policyName, properties);
    }

    @Override
    public LogicalPlan visitShowGrantsForUser(ShowGrantsForUserContext ctx) {
        UserIdentity userIdent = visitUserIdentify(ctx.userIdentify());
        return new ShowGrantsCommand(userIdent, false);
    }

    @Override
    public LogicalPlan visitShowCreateUser(DorisParser.ShowCreateUserContext ctx) {
        UserIdentity userIdent = null;
        if (ctx.userIdentify() != null) {
            userIdent = visitUserIdentify(ctx.userIdentify());
        }
        return new ShowCreateUserCommand(userIdent);
    }

    @Override
    public LogicalPlan visitShowRowPolicy(ShowRowPolicyContext ctx) {
        UserIdentity user = null;
        String role = null;
        if (ctx.userIdentify() != null) {
            user = visitUserIdentify(ctx.userIdentify());
        } else if (ctx.role != null) {
            role = ctx.role.getText();
        }

        return new ShowRowPolicyCommand(user, role);
    }

    @Override
    public LogicalPlan visitShowPartitionId(ShowPartitionIdContext ctx) {
        long partitionId = -1;
        if (ctx.partitionId != null) {
            partitionId = Long.parseLong(ctx.partitionId.getText());
        }
        return new ShowPartitionIdCommand(partitionId);
    }

    @Override
    public LogicalPlan visitShowPartitions(ShowPartitionsContext ctx) {
        String ctlName = null;
        String dbName = null;
        String tableName = null;
        Expression wildWhere = null;
        List<OrderKey> orderKeys = null;
        long limit = -1L;
        long offset = 0L;

        List<String> nameParts = visitMultipartIdentifier(ctx.tableName);
        if (nameParts.size() == 1) {
            tableName = nameParts.get(0);
        } else if (nameParts.size() == 2) {
            dbName = nameParts.get(0);
            tableName = nameParts.get(1);
        } else if (nameParts.size() == 3) {
            ctlName = nameParts.get(0);
            dbName = nameParts.get(1);
            tableName = nameParts.get(2);
        } else {
            throw new AnalysisException("nameParts in create table should be [ctl.][db.]tbl");
        }

        if (ctx.sortClause() != null) {
            orderKeys = visit(ctx.sortClause().sortItem(), OrderKey.class);
        }
        if (ctx.wildWhere() != null) {
            wildWhere = getWildWhere(ctx.wildWhere());
        }
        if (ctx.limitClause() != null) {
            limit = ctx.limitClause().limit != null
                    ? Long.parseLong(ctx.limitClause().limit.getText())
                    : 0;
            if (limit < 0) {
                throw new ParseException("Limit requires non-negative number", ctx.limitClause());
            }
            offset = ctx.limitClause().offset != null
                    ? Long.parseLong(ctx.limitClause().offset.getText())
                    : 0;
            if (offset < 0) {
                throw new ParseException("Offset requires non-negative number", ctx.limitClause());
            }
        }
        boolean isTempPartition = ctx.TEMPORARY() != null;
        TableNameInfo tblNameInfo = new TableNameInfo(ctlName, dbName, tableName);
        return new ShowPartitionsCommand(tblNameInfo, wildWhere, orderKeys, limit, offset, isTempPartition);
    }

    @Override
    public AlterTableCommand visitAlterTable(AlterTableContext ctx) {
        TableNameInfo tableNameInfo = new TableNameInfo(visitMultipartIdentifier(ctx.tableName));
        List<AlterTableOp> alterTableOps = new ArrayList<>();
        for (Object child : ctx.children) {
            if (child instanceof AlterTableClauseContext) {
                alterTableOps.add(typedVisit((AlterTableClauseContext) child));
            }
        }
        return new AlterTableCommand(tableNameInfo, alterTableOps);
    }

    @Override
    public AlterTableCommand visitAlterTableAddRollup(AlterTableAddRollupContext ctx) {
        TableNameInfo tableNameInfo = new TableNameInfo(visitMultipartIdentifier(ctx.tableName));
        List<AlterTableOp> alterTableOps = new ArrayList<>();
        for (Object child : ctx.children) {
            if (child instanceof AddRollupClauseContext) {
                alterTableOps.add(typedVisit((AddRollupClauseContext) child));
            }
        }
        return new AlterTableCommand(tableNameInfo, alterTableOps);
    }

    @Override
    public AlterTableCommand visitAlterTableDropRollup(AlterTableDropRollupContext ctx) {
        TableNameInfo tableNameInfo = new TableNameInfo(visitMultipartIdentifier(ctx.tableName));
        List<AlterTableOp> alterTableOps = new ArrayList<>();
        for (Object child : ctx.children) {
            if (child instanceof DropRollupClauseContext) {
                alterTableOps.add(typedVisit((DropRollupClauseContext) child));
            }
        }
        return new AlterTableCommand(tableNameInfo, alterTableOps);
    }

    @Override
    public AlterTableCommand visitAlterTableProperties(DorisParser.AlterTablePropertiesContext ctx) {
        TableNameInfo tableNameInfo = new TableNameInfo(visitMultipartIdentifier(ctx.name));
        List<AlterTableOp> alterTableOps = new ArrayList<>();
        Map<String, String> properties = ctx.propertyItemList() != null
                ? Maps.newHashMap(visitPropertyItemList(ctx.propertyItemList()))
                : Maps.newHashMap();
        alterTableOps.add(new ModifyTablePropertiesOp(properties));
        return new AlterTableCommand(tableNameInfo, alterTableOps);
    }

    @Override
    public AlterTableOp visitAddColumnClause(AddColumnClauseContext ctx) {
        ColumnDefinition columnDefinition = visitColumnDef(ctx.columnDef());
        ColumnPosition columnPosition = null;
        if (ctx.columnPosition() != null) {
            if (ctx.columnPosition().FIRST() != null) {
                columnPosition = ColumnPosition.FIRST;
            } else {
                columnPosition = new ColumnPosition(ctx.columnPosition().position.getText());
            }
        }
        String rollupName = ctx.toRollup() != null ? ctx.toRollup().rollup.getText() : null;
        Map<String, String> properties = ctx.properties != null
                ? Maps.newHashMap(visitPropertyClause(ctx.properties))
                : Maps.newHashMap();
        return new AddColumnOp(columnDefinition, columnPosition, rollupName, properties);
    }

    @Override
    public AlterTableOp visitAddColumnsClause(AddColumnsClauseContext ctx) {
        List<ColumnDefinition> columnDefinitions = visitColumnDefs(ctx.columnDefs());
        String rollupName = ctx.toRollup() != null ? ctx.toRollup().rollup.getText() : null;
        Map<String, String> properties = ctx.properties != null
                ? Maps.newHashMap(visitPropertyClause(ctx.properties))
                : Maps.newHashMap();
        return new AddColumnsOp(columnDefinitions, rollupName, properties);
    }

    @Override
    public AlterTableOp visitDropColumnClause(DropColumnClauseContext ctx) {
        String columnName = ctx.name.getText();
        String rollupName = ctx.fromRollup() != null ? ctx.fromRollup().rollup.getText() : null;
        Map<String, String> properties = ctx.properties != null
                ? Maps.newHashMap(visitPropertyClause(ctx.properties))
                : Maps.newHashMap();
        return new DropColumnOp(columnName, rollupName, properties);
    }

    @Override
    public AlterTableOp visitModifyColumnClause(ModifyColumnClauseContext ctx) {
        ColumnDefinition columnDefinition = visitColumnDef(ctx.columnDef());
        ColumnPosition columnPosition = null;
        if (ctx.columnPosition() != null) {
            if (ctx.columnPosition().FIRST() != null) {
                columnPosition = ColumnPosition.FIRST;
            } else {
                columnPosition = new ColumnPosition(ctx.columnPosition().position.getText());
            }
        }
        String rollupName = ctx.fromRollup() != null ? ctx.fromRollup().rollup.getText() : null;
        Map<String, String> properties = ctx.properties != null
                ? Maps.newHashMap(visitPropertyClause(ctx.properties))
                : Maps.newHashMap();
        return new ModifyColumnOp(columnDefinition, columnPosition, rollupName, properties);
    }

    @Override
    public AlterTableOp visitReorderColumnsClause(ReorderColumnsClauseContext ctx) {
        List<String> columnsByPos = visitIdentifierList(ctx.identifierList());
        String rollupName = ctx.fromRollup() != null ? ctx.fromRollup().rollup.getText() : null;
        Map<String, String> properties = ctx.properties != null
                ? Maps.newHashMap(visitPropertyClause(ctx.properties))
                : Maps.newHashMap();
        return new ReorderColumnsOp(columnsByPos, rollupName, properties);
    }

    @Override
    public AlterTableOp visitAddPartitionClause(AddPartitionClauseContext ctx) {
        boolean isTempPartition = ctx.TEMPORARY() != null;
        PartitionDefinition partitionDefinition = visitPartitionDef(ctx.partitionDef());
        DistributionDescriptor desc = null;
        int bucketNum = FeConstants.default_bucket_num;
        if (ctx.INTEGER_VALUE() != null) {
            bucketNum = Integer.parseInt(ctx.INTEGER_VALUE().getText());
        }
        if (ctx.HASH() != null) {
            desc = new DistributionDescriptor(true, ctx.autoBucket != null, bucketNum,
                    visitIdentifierList(ctx.hashKeys));
        } else if (ctx.RANDOM() != null) {
            desc = new DistributionDescriptor(false, ctx.autoBucket != null, bucketNum, null);
        }
        Map<String, String> properties = ctx.properties != null
                ? Maps.newHashMap(visitPropertyClause(ctx.properties))
                : Maps.newHashMap();
        return new AddPartitionOp(partitionDefinition, desc, properties, isTempPartition);
    }

    @Override
    public AlterTableOp visitDropPartitionClause(DropPartitionClauseContext ctx) {
        boolean isTempPartition = ctx.TEMPORARY() != null;
        boolean ifExists = ctx.IF() != null;
        boolean forceDrop = ctx.FORCE() != null;
        String partitionName = ctx.partitionName.getText();
        return ctx.indexName != null
                ? new DropPartitionFromIndexOp(ifExists, partitionName, isTempPartition, forceDrop,
                ctx.indexName.getText())
                : new DropPartitionOp(ifExists, partitionName, isTempPartition, forceDrop);
    }

    @Override
    public AlterTableOp visitModifyPartitionClause(ModifyPartitionClauseContext ctx) {
        boolean isTempPartition = ctx.TEMPORARY() != null;
        Map<String, String> properties = visitPropertyItemList(ctx.partitionProperties);
        if (ctx.ASTERISK() != null) {
            return ModifyPartitionOp.createStarClause(properties, isTempPartition);
        } else {
            List<String> partitions;
            if (ctx.partitionNames != null) {
                partitions = visitIdentifierList(ctx.partitionNames);
            } else {
                partitions = new ArrayList<>();
                partitions.add(ctx.partitionName.getText());
            }
            return new ModifyPartitionOp(partitions, properties, isTempPartition);
        }
    }

    @Override
    public AlterTableOp visitReplacePartitionClause(ReplacePartitionClauseContext ctx) {
        boolean forceReplace = ctx.FORCE() != null;
        PartitionNamesInfo partitionNames = null;
        PartitionNamesInfo tempPartitionNames = null;
        if (ctx.partitions != null) {
            Pair<Boolean, List<String>> partitionSpec = visitPartitionSpec(ctx.partitions);
            partitionNames = new PartitionNamesInfo(partitionSpec.first, partitionSpec.second);
        }
        if (ctx.tempPartitions != null) {
            Pair<Boolean, List<String>> partitionSpec = visitPartitionSpec(ctx.tempPartitions);
            tempPartitionNames = new PartitionNamesInfo(partitionSpec.first, partitionSpec.second);
        }

        Map<String, String> properties = ctx.properties != null ? new HashMap<>(visitPropertyClause(ctx.properties))
                : Maps.newHashMap();
        return new ReplacePartitionOp(partitionNames, tempPartitionNames, forceReplace, properties);
    }

    @Override
    public AlterTableOp visitReplaceTableClause(ReplaceTableClauseContext ctx) {
        String tableName = ctx.name.getText();
        Map<String, String> properties = ctx.properties != null
                ? Maps.newHashMap(visitPropertyClause(ctx.properties))
                : Maps.newHashMap();
        return new ReplaceTableOp(tableName, properties, ctx.FORCE() != null);
    }

    @Override
    public AlterTableOp visitRenameClause(RenameClauseContext ctx) {
        return new RenameTableOp(ctx.newName.getText());
    }

    @Override
    public AlterTableOp visitRenameRollupClause(RenameRollupClauseContext ctx) {
        return new RenameRollupOp(ctx.name.getText(), ctx.newName.getText());
    }

    @Override
    public AlterTableOp visitRenamePartitionClause(RenamePartitionClauseContext ctx) {
        return new RenamePartitionOp(ctx.name.getText(), ctx.newName.getText());
    }

    @Override
    public AlterTableOp visitRenameColumnClause(RenameColumnClauseContext ctx) {
        return new RenameColumnOp(ctx.name.getText(), ctx.newName.getText());
    }

    @Override
    public AlterTableOp visitAddIndexClause(AddIndexClauseContext ctx) {
        IndexDefinition indexDefinition = visitIndexDef(ctx.indexDef());
        return new CreateIndexOp(null, indexDefinition, true);
    }

    @Override
    public Command visitCreateIndex(CreateIndexContext ctx) {
        String indexName = ctx.name.getText();
        boolean ifNotExists = ctx.EXISTS() != null;
        TableNameInfo tableNameInfo = new TableNameInfo(visitMultipartIdentifier(ctx.tableName));
        List<String> indexCols = visitIdentifierList(ctx.identifierList());
        Map<String, String> properties = ctx.properties != null
                ? Maps.newHashMap(visitPropertyClause(ctx.properties))
                : Maps.newHashMap();
        String indexType = null;
        if (ctx.BITMAP() != null) {
            indexType = "BITMAP";
        } else if (ctx.NGRAM_BF() != null) {
            indexType = "NGRAM_BF";
        } else if (ctx.INVERTED() != null) {
            indexType = "INVERTED";
        }
        String comment = ctx.STRING_LITERAL() == null ? "" : stripQuotes(ctx.STRING_LITERAL().getText());
        // change BITMAP index to INVERTED index
        if (Config.enable_create_bitmap_index_as_inverted_index
                && "BITMAP".equalsIgnoreCase(indexType)) {
            indexType = "INVERTED";
        }
        IndexDefinition indexDefinition = new IndexDefinition(indexName, ifNotExists, indexCols, indexType,
                properties, comment);
        List<AlterTableOp> alterTableOps = Lists.newArrayList(new CreateIndexOp(tableNameInfo,
                indexDefinition, false));
        return new AlterTableCommand(tableNameInfo, alterTableOps);
    }

    @Override
    public Command visitBuildIndex(BuildIndexContext ctx) {
        String name = ctx.name.getText();
        TableNameInfo tableName = new TableNameInfo(visitMultipartIdentifier(ctx.tableName));
        PartitionNamesInfo partitionNamesInfo = null;
        if (ctx.partitionSpec() != null) {
            Pair<Boolean, List<String>> partitionSpec = visitPartitionSpec(ctx.partitionSpec());
            partitionNamesInfo = new PartitionNamesInfo(partitionSpec.first, partitionSpec.second);
        }
        List<AlterTableOp> alterTableOps = Lists.newArrayList(new BuildIndexOp(tableName, name, partitionNamesInfo,
                false));
        return new AlterTableCommand(tableName, alterTableOps);
    }

    @Override
    public Command visitDropIndex(DropIndexContext ctx) {
        String name = ctx.name.getText();
        TableNameInfo tableName = new TableNameInfo(visitMultipartIdentifier(ctx.tableName));
        List<AlterTableOp> alterTableOps = Lists
                .newArrayList(new DropIndexOp(name, ctx.EXISTS() != null, tableName, false));
        return new AlterTableCommand(tableName, alterTableOps);
    }

    @Override
    public AlterTableOp visitDropIndexClause(DropIndexClauseContext ctx) {
        return new DropIndexOp(ctx.name.getText(), ctx.EXISTS() != null, null, true);
    }

    @Override
    public AlterTableOp visitEnableFeatureClause(EnableFeatureClauseContext ctx) {
        String featureName = stripQuotes(ctx.STRING_LITERAL().getText());
        Map<String, String> properties = ctx.properties != null
                ? Maps.newHashMap(visitPropertyClause(ctx.properties))
                : Maps.newHashMap();
        return new EnableFeatureOp(featureName, properties);
    }

    @Override
    public AlterTableOp visitModifyDistributionClause(ModifyDistributionClauseContext ctx) {
        int bucketNum = FeConstants.default_bucket_num;
        if (ctx.INTEGER_VALUE() != null) {
            bucketNum = Integer.parseInt(ctx.INTEGER_VALUE().getText());
        }
        DistributionDescriptor desc;
        if (ctx.HASH() != null) {
            desc = new DistributionDescriptor(true, ctx.AUTO() != null, bucketNum,
                    visitIdentifierList(ctx.hashKeys));
        } else if (ctx.RANDOM() != null) {
            desc = new DistributionDescriptor(false, ctx.AUTO() != null, bucketNum, null);
        } else {
            throw new ParseException("distribution can't be empty", ctx);
        }
        return new ModifyDistributionOp(desc);
    }

    @Override
    public AlterTableOp visitModifyTableCommentClause(ModifyTableCommentClauseContext ctx) {
        return new ModifyTableCommentOp(stripQuotes(ctx.STRING_LITERAL().getText()));
    }

    @Override
    public AlterTableOp visitModifyColumnCommentClause(ModifyColumnCommentClauseContext ctx) {
        String columnName = ctx.name.getText();
        String comment = stripQuotes(ctx.STRING_LITERAL().getText());
        return new ModifyColumnCommentOp(columnName, comment);
    }

    @Override
    public AlterTableOp visitModifyEngineClause(ModifyEngineClauseContext ctx) {
        String engineName = ctx.name.getText();
        Map<String, String> properties = ctx.properties != null
                ? Maps.newHashMap(visitPropertyClause(ctx.properties))
                : Maps.newHashMap();
        return new ModifyEngineOp(engineName, properties);
    }

    @Override
    public AlterTableOp visitAlterMultiPartitionClause(AlterMultiPartitionClauseContext ctx) {
        boolean isTempPartition = ctx.TEMPORARY() != null;
        List<Expression> from = visitPartitionValueList(ctx.from);
        List<Expression> to = visitPartitionValueList(ctx.to);
        int num = Integer.parseInt(ctx.INTEGER_VALUE().getText());
        String unitString = ctx.unit != null ? ctx.unit.getText() : null;
        Map<String, String> properties = ctx.properties != null
                ? Maps.newHashMap(visitPropertyClause(ctx.properties))
                : Maps.newHashMap();
        return new AlterMultiPartitionOp(from, to, num, unitString, properties, isTempPartition);
    }

    @Override
    public AlterTableOp visitAddRollupClause(DorisParser.AddRollupClauseContext ctx) {
        String rollupName = ctx.rollupName.getText();
        List<String> columnNames = visitIdentifierList(ctx.columns);
        List<String> dupKeys = ctx.dupKeys != null ? visitIdentifierList(ctx.dupKeys) : null;
        String baseRollupName = ctx.fromRollup() != null ? ctx.fromRollup().rollup.getText() : null;
        Map<String, String> properties = ctx.properties != null
                ? Maps.newHashMap(visitPropertyClause(ctx.properties))
                : Maps.newHashMap();
        return new AddRollupOp(rollupName, columnNames, dupKeys, baseRollupName, properties);
    }

    @Override
    public AlterTableOp visitDropRollupClause(DorisParser.DropRollupClauseContext ctx) {
        String rollupName = ctx.rollupName.getText();
        Map<String, String> properties = ctx.properties != null
                ? Maps.newHashMap(visitPropertyClause(ctx.properties))
                : Maps.newHashMap();
        return new DropRollupOp(rollupName, properties);
    }

    @Override
    public LogicalPlan visitShowVariables(ShowVariablesContext ctx) {
        SetType statementScope = visitStatementScope(ctx.statementScope());
        if (ctx.wildWhere() != null) {
            if (ctx.wildWhere().LIKE() != null) {
                return new ShowVariablesCommand(statementScope,
                        stripQuotes(ctx.wildWhere().STRING_LITERAL().getText()));
            } else {
                StringBuilder sb = new StringBuilder();
                sb.append("SELECT `VARIABLE_NAME` AS `Variable_name`, `VARIABLE_VALUE` AS `Value` FROM ");
                sb.append("`").append(InternalCatalog.INTERNAL_CATALOG_NAME).append("`");
                sb.append(".");
                sb.append("`").append(InfoSchemaDb.DATABASE_NAME).append("`");
                sb.append(".");
                if (statementScope == SetType.GLOBAL) {
                    sb.append("`global_variables` ");
                } else {
                    sb.append("`session_variables` ");
                }
                sb.append(getOriginSql(ctx.wildWhere()));
                return new NereidsParser().parseSingle(sb.toString());
            }
        } else {
            return new ShowVariablesCommand(statementScope, null);
        }
    }

    @Override
    public LogicalPlan visitInstallPlugin(InstallPluginContext ctx) {
        String source = visitIdentifierOrText(ctx.identifierOrText());
        Map<String, String> properties = visitPropertyClause(ctx.propertyClause()) == null ? Maps.newHashMap()
                : visitPropertyClause(ctx.propertyClause());

        return new InstallPluginCommand(source, properties);
    }

    @Override
    public LogicalPlan visitUninstallPlugin(UninstallPluginContext ctx) {
        String name = visitIdentifierOrText(ctx.identifierOrText());

        return new UninstallPluginCommand(name);
    }

    private Expression getWildWhere(DorisParser.WildWhereContext ctx) {
        if (ctx.LIKE() != null) {
            String pattern = stripQuotes(ctx.STRING_LITERAL().getText());
            return new Like(new UnboundSlot("ProcedureName"), new StringLiteral(pattern));
        } else if (ctx.WHERE() != null) {
            return getExpression(ctx.expression());
        } else {
            throw new AnalysisException("Wild where should contain like or where " + ctx.getText());
        }
    }

    @Override
    public LogicalPlan visitShowColumnStats(ShowColumnStatsContext ctx) {
        List<String> tableNameParts = visitMultipartIdentifier(ctx.tableName);
        List<String> colNames = ctx.columnList == null ? null : visitIdentifierList(ctx.columnList);
        PartitionNamesInfo partitionNames = null;
        if (ctx.partitionSpec() != null) {
            Pair<Boolean, List<String>> partitionSpec = visitPartitionSpec(ctx.partitionSpec());
            if (partitionSpec.second == null) {
                partitionNames = new PartitionNamesInfo(true); // asterisk (*) case
            } else {
                partitionNames = new PartitionNamesInfo(partitionSpec.first, partitionSpec.second);
            }
        }
        boolean isCached = ctx.CACHED() != null;

        return new ShowColumnStatsCommand(new TableNameInfo(tableNameParts), colNames, partitionNames, isCached);
    }

    @Override
    public ShowViewCommand visitShowView(ShowViewContext ctx) {
        List<String> tableNameParts = visitMultipartIdentifier(ctx.tableName);
        String databaseName = null;
        if (ctx.database != null) {
            databaseName = stripQuotes(ctx.database.getText());
        }
        return new ShowViewCommand(databaseName, new TableNameInfo(tableNameParts));
    }

    @Override
    public LogicalPlan visitAlterResource(AlterResourceContext ctx) {
        String resourceName = visitIdentifierOrText(ctx.identifierOrText());
        Map<String, String> properties = visitPropertyClause(ctx.propertyClause()) == null ? Maps.newHashMap()
                : Maps.newHashMap(visitPropertyClause(ctx.propertyClause()));

        return new AlterResourceCommand(resourceName, properties);
    }

    @Override
    public LogicalPlan visitShowBackends(ShowBackendsContext ctx) {
        return new ShowBackendsCommand();
    }

    @Override
    public LogicalPlan visitShowBackup(ShowBackupContext ctx) {
        String dbName = null;
        Expression wildWhere = null;
        if (ctx.database != null) {
            dbName = ctx.database.getText();
        }
        if (ctx.wildWhere() != null) {
            wildWhere = getWildWhere(ctx.wildWhere());
        }
        return new ShowBackupCommand(dbName, wildWhere);
    }

    @Override
    public LogicalPlan visitShowPlugins(ShowPluginsContext ctx) {
        return new ShowPluginsCommand();
    }

    @Override
    public LogicalPlan visitShowSmallFiles(ShowSmallFilesContext ctx) {
        String dbName = null;
        if (ctx.database != null) {
            List<String> nameParts = visitMultipartIdentifier(ctx.database);
            dbName = nameParts.get(0); // only one entry possible
        }
        return new ShowSmallFilesCommand(dbName);
    }

    @Override
    public LogicalPlan visitShowSnapshot(ShowSnapshotContext ctx) {
        String repoName = null;
        Expression wildWhere = null;
        if (ctx.wildWhere() != null) {
            wildWhere = getWildWhere(ctx.wildWhere());
        }
        if (ctx.repo != null) {
            repoName = ctx.repo.getText();
        }
        return new ShowSnapshotCommand(repoName, wildWhere);
    }

    @Override
    public LogicalPlan visitShowSqlBlockRule(ShowSqlBlockRuleContext ctx) {
        String ruleName = null;
        if (ctx.ruleName != null) {
            ruleName = ctx.ruleName.getText();
        }
        return new ShowSqlBlockRuleCommand(ruleName);
    }

    @Override
    public LogicalPlan visitShowTriggers(ShowTriggersContext ctx) {
        return new ShowTriggersCommand();
    }

    @Override
    public LogicalPlan visitShowTypeCast(DorisParser.ShowTypeCastContext ctx) {
        String dbName = null;
        if (ctx.database != null) {
            dbName = ctx.database.getText();
        }
        return new ShowTypeCastCommand(dbName);
    }

    @Override
    public LogicalPlan visitShowTrash(ShowTrashContext ctx) {
        if (ctx.ON() != null) {
            String backend = stripQuotes(ctx.STRING_LITERAL().getText());
            new ShowTrashCommand(backend);
        } else {
            return new ShowTrashCommand();
        }
        return new ShowTrashCommand();
    }

    @Override
    public LogicalPlan visitAdminCleanTrash(DorisParser.AdminCleanTrashContext ctx) {
        if (ctx.ON() != null) {
            List<String> backendsQuery = Lists.newArrayList();
            ctx.backends.forEach(backend -> backendsQuery.add(stripQuotes(backend.getText())));
            return new AdminCleanTrashCommand(backendsQuery);
        }
        return new AdminCleanTrashCommand();
    }

    @Override
    public LogicalPlan visitShowRepositories(ShowRepositoriesContext ctx) {
        return new ShowRepositoriesCommand();
    }

    @Override
    public LogicalPlan visitShowResources(ShowResourcesContext ctx) {
        Expression wildWhere = null;
        List<OrderKey> orderKeys = null;
        String likePattern = null;
        long limit = -1L;
        long offset = 0L;
        if (ctx.sortClause() != null) {
            orderKeys = visit(ctx.sortClause().sortItem(), OrderKey.class);
        }
        if (ctx.wildWhere() != null) {
            wildWhere = getWildWhere(ctx.wildWhere());
            if (ctx.wildWhere().LIKE() != null) {
                likePattern = stripQuotes(ctx.wildWhere().STRING_LITERAL().getText());
            } else {
                wildWhere = (Expression) ctx.wildWhere().expression().accept(this);
            }
        }
        if (ctx.limitClause() != null) {
            limit = ctx.limitClause().limit != null
                    ? Long.parseLong(ctx.limitClause().limit.getText())
                    : 0;
            if (limit < 0) {
                throw new ParseException("Limit requires non-negative number", ctx.limitClause());
            }
            offset = ctx.limitClause().offset != null
                    ? Long.parseLong(ctx.limitClause().offset.getText())
                    : 0;
            if (offset < 0) {
                throw new ParseException("Offset requires non-negative number", ctx.limitClause());
            }
        }
        return new ShowResourcesCommand(wildWhere, likePattern, orderKeys, limit, offset);
    }

    @Override
    public LogicalPlan visitShowRestore(ShowRestoreContext ctx) {
        String dbName = null;
        Expression wildWhere = null;
        if (ctx.database != null) {
            dbName = ctx.database.getText();
        }
        if (ctx.wildWhere() != null) {
            wildWhere = getWildWhere(ctx.wildWhere());
        }
        return new ShowRestoreCommand(dbName, wildWhere, ctx.BRIEF() != null);
    }

    @Override
    public LogicalPlan visitAlterDatabaseProperties(AlterDatabasePropertiesContext ctx) {
        String dbName = Optional.ofNullable(ctx.name)
                .map(ParserRuleContext::getText)
                .filter(s -> !s.isEmpty())
                .orElseThrow(() -> new ParseException("Database name is empty or cannot be an empty string"));
        Map<String, String> properties = ctx.propertyItemList() != null
                ? Maps.newHashMap(visitPropertyItemList(ctx.propertyItemList()))
                : Maps.newHashMap();

        return new AlterDatabasePropertiesCommand(dbName, properties);
    }

    @Override
    public LogicalPlan visitShowRoles(ShowRolesContext ctx) {
        return new ShowRolesCommand();
    }

    @Override
    public LogicalPlan visitShowProc(ShowProcContext ctx) {
        String path = stripQuotes(ctx.path.getText());
        return new ShowProcCommand(path);
    }

    private TableScanParams visitOptScanParamsContext(OptScanParamsContext ctx) {
        if (ctx != null) {
            Map<String, String> map = visitPropertyItemList(ctx.mapParams);
            List<String> list;
            if (ctx.listParams == null) {
                list = ImmutableList.of();
            } else {
                list = visitIdentifierSeq(ctx.listParams);
            }
            return new TableScanParams(ctx.funcName.getText(), map, list);
        }
        return null;
    }

    private TableSnapshot visitTableSnapshotContext(TableSnapshotContext ctx) {
        if (ctx != null) {
            if (ctx.TIME() != null) {
                return TableSnapshot.timeOf(stripQuotes(ctx.time.getText()));
            } else {
                return TableSnapshot.versionOf(stripQuotes(ctx.version.getText()));
            }
        }
        return null;
    }

    private List<String> visitRelationHintContext(RelationHintContext ctx) {
        final List<String> relationHints;
        if (ctx != null) {
            relationHints = typedVisit(ctx);
        } else {
            relationHints = ImmutableList.of();
        }
        return relationHints;
    }

    private PartitionNamesInfo visitSpecifiedPartitionContext(SpecifiedPartitionContext ctx) {
        if (ctx != null) {
            List<String> partitions = new ArrayList<>();
            boolean isTempPart = ctx.TEMPORARY() != null;
            if (ctx.identifier() != null) {
                partitions.add(ctx.identifier().getText());
            } else {
                partitions.addAll(visitIdentifierList(ctx.identifierList()));
            }
            return new PartitionNamesInfo(isTempPart, partitions);
        }
        return null;
    }

    private List<Long> visitTabletListContext(TabletListContext ctx) {
        List<Long> tabletIdList = new ArrayList<>();
        if (ctx != null && ctx.tabletIdList != null) {
            ctx.tabletIdList.stream().forEach(tabletToken -> {
                tabletIdList.add(Long.parseLong(tabletToken.getText()));
            });
        }
        return tabletIdList;
    }

    private TableRefInfo visitBaseTableRefContext(BaseTableRefContext ctx) {
        List<String> nameParts = visitMultipartIdentifier(ctx.multipartIdentifier());
        TableScanParams scanParams = visitOptScanParamsContext(ctx.optScanParams());
        TableSnapshot tableSnapShot = visitTableSnapshotContext(ctx.tableSnapshot());
        PartitionNamesInfo partitionNameInfo = visitSpecifiedPartitionContext(ctx.specifiedPartition());
        List<Long> tabletIdList = visitTabletListContext(ctx.tabletList());

        String tableAlias = null;
        if (ctx.tableAlias().strictIdentifier() != null) {
            tableAlias = ctx.tableAlias().strictIdentifier().getText();
        }
        TableSample tableSample = ctx.sample() == null ? null : (TableSample) visit(ctx.sample());
        List<String> hints = visitRelationHintContext(ctx.relationHint());
        return new TableRefInfo(new TableNameInfo(nameParts), scanParams, tableSnapShot, partitionNameInfo,
                tabletIdList, tableAlias, tableSample, hints);
    }

    @Override
    public LogicalPlan visitShowReplicaDistribution(ShowReplicaDistributionContext ctx) {
        TableRefInfo tableRefInfo = visitBaseTableRefContext(ctx.baseTableRef());
        return new ShowReplicaDistributionCommand(tableRefInfo);
    }

    @Override
    public LogicalPlan visitAdminShowReplicaDistribution(AdminShowReplicaDistributionContext ctx) {
        TableRefInfo tableRefInfo = visitBaseTableRefContext(ctx.baseTableRef());
        return new ShowReplicaDistributionCommand(tableRefInfo);
    }

    @Override
    public LogicalPlan visitShowCreateCatalog(ShowCreateCatalogContext ctx) {
        return new ShowCreateCatalogCommand(ctx.identifier().getText());
    }

    @Override
    public LogicalPlan visitShowCatalog(DorisParser.ShowCatalogContext ctx) {
        return new ShowCatalogCommand(ctx.identifier().getText(), null);
    }

    @Override
    public LogicalPlan visitShowCatalogs(DorisParser.ShowCatalogsContext ctx) {
        String wild = null;
        if (ctx.wildWhere() != null) {
            if (ctx.wildWhere().LIKE() != null) {
                wild = stripQuotes(ctx.wildWhere().STRING_LITERAL().getText());
            } else if (ctx.wildWhere().WHERE() != null) {
                wild = ctx.wildWhere().expression().getText();
            }
        }
        return new ShowCatalogCommand(null, wild);
    }

    @Override
    public LogicalPlan visitShowCatalogRecycleBin(ShowCatalogRecycleBinContext ctx) {
        Expression whereClause = null;
        if (ctx.WHERE() != null) {
            whereClause = getExpression(ctx.expression());
        }
        return new ShowCatalogRecycleBinCommand(whereClause);
    }

    @Override
    public LogicalPlan visitShowStorageEngines(ShowStorageEnginesContext ctx) {
        return new ShowStorageEnginesCommand();
    }

    @Override
    public LogicalPlan visitAdminRebalanceDisk(AdminRebalanceDiskContext ctx) {
        if (ctx.ON() != null) {
            List<String> backendList = Lists.newArrayList();
            ctx.backends.forEach(backend -> backendList.add(stripQuotes(backend.getText())));
            return new AdminRebalanceDiskCommand(backendList);
        }
        return new AdminRebalanceDiskCommand();
    }

    @Override
    public LogicalPlan visitAdminCancelRebalanceDisk(AdminCancelRebalanceDiskContext ctx) {
        if (ctx.ON() != null) {
            List<String> backendList = Lists.newArrayList();
            ctx.backends.forEach(backend -> backendList.add(stripQuotes(backend.getText())));
            return new AdminCancelRebalanceDiskCommand(backendList);
        }
        return new AdminCancelRebalanceDiskCommand();
    }

    @Override
    public LogicalPlan visitShowDiagnoseTablet(ShowDiagnoseTabletContext ctx) {
        long tabletId = Long.parseLong(ctx.INTEGER_VALUE().getText());
        return new ShowDiagnoseTabletCommand(tabletId);
    }

    @Override
    public LogicalPlan visitAdminDiagnoseTablet(AdminDiagnoseTabletContext ctx) {
        long tabletId = Long.parseLong(ctx.INTEGER_VALUE().getText());
        return new ShowDiagnoseTabletCommand(tabletId);
    }

    @Override
    public LogicalPlan visitShowCreateTable(ShowCreateTableContext ctx) {
        List<String> nameParts = visitMultipartIdentifier(ctx.name);
        return new ShowCreateTableCommand(new TableNameInfo(nameParts), ctx.BRIEF() != null);
    }

    @Override
    public LogicalPlan visitShowCreateView(ShowCreateViewContext ctx) {
        List<String> nameParts = visitMultipartIdentifier(ctx.name);
        return new ShowCreateViewCommand(new TableNameInfo(nameParts));
    }

    @Override
    public LogicalPlan visitShowCreateMaterializedView(ShowCreateMaterializedViewContext ctx) {
        List<String> nameParts = visitMultipartIdentifier(ctx.tableName);
        return new ShowCreateMaterializedViewCommand(stripQuotes(ctx.mvName.getText()), new TableNameInfo(nameParts));
    }

    @Override
    public LogicalPlan visitAlterWorkloadGroup(AlterWorkloadGroupContext ctx) {
        String cgName = ctx.computeGroup == null ? "" : stripQuotes(ctx.computeGroup.getText());
        Map<String, String> properties = ctx.propertyClause() != null
                ? Maps.newHashMap(visitPropertyClause(ctx.propertyClause())) : Maps.newHashMap();
        return new AlterWorkloadGroupCommand(cgName, ctx.name.getText(), properties);
    }

    @Override
    public LogicalPlan visitAlterWorkloadPolicy(AlterWorkloadPolicyContext ctx) {
        Map<String, String> properties = ctx.propertyClause() != null
                ? Maps.newHashMap(visitPropertyClause(ctx.propertyClause())) : Maps.newHashMap();
        return new AlterWorkloadPolicyCommand(ctx.name.getText(), properties);
    }

    @Override
    public LogicalPlan visitAlterRole(AlterRoleContext ctx) {
        String comment = visitCommentSpec(ctx.commentSpec());
        return new AlterRoleCommand(ctx.role.getText(), comment);
    }

    @Override
    public LogicalPlan visitShowDatabaseId(ShowDatabaseIdContext ctx) {
        long dbId = (ctx.databaseId != null) ? Long.parseLong(ctx.databaseId.getText()) : -1;
        return new ShowDatabaseIdCommand(dbId);
    }

    public LogicalPlan visitCreateRole(CreateRoleContext ctx) {
        String roleName = stripQuotes(ctx.name.getText());
        String comment = ctx.STRING_LITERAL() == null ? "" : LogicalPlanBuilderAssistant.escapeBackSlash(
                ctx.STRING_LITERAL().getText().substring(1, ctx.STRING_LITERAL().getText().length() - 1));
        return new CreateRoleCommand(ctx.EXISTS() != null, roleName, comment);
    }

    @Override
    public LogicalPlan visitCreateFile(CreateFileContext ctx) {
        String dbName = null;
        if (ctx.database != null) {
            dbName = ctx.database.getText();
        }
        Map<String, String> properties = ctx.propertyClause() != null
                ? Maps.newHashMap(visitPropertyClause(ctx.propertyClause())) : Maps.newHashMap();
        return new CreateFileCommand(stripQuotes(ctx.name.getText()), dbName, properties);
    }

    @Override
    public LogicalPlan visitShowCharset(ShowCharsetContext ctx) {
        return new ShowCharsetCommand();
    }

    @Override
    public LogicalPlan visitAdminSetTableStatus(AdminSetTableStatusContext ctx) {
        List<String> dbTblNameParts = visitMultipartIdentifier(ctx.name);
        Map<String, String> properties = ctx.propertyClause() != null
                ? Maps.newHashMap(visitPropertyClause(ctx.propertyClause())) : Maps.newHashMap();
        return new AdminSetTableStatusCommand(new TableNameInfo(dbTblNameParts), properties);
    }

    @Override
    public LogicalPlan visitAdminSetReplicaVersion(DorisParser.AdminSetReplicaVersionContext ctx) {
        return new AdminSetReplicaVersionCommand(visitPropertyItemList(ctx.propertyItemList()));
    }

    @Override
    public LogicalPlan visitAdminSetFrontendConfig(DorisParser.AdminSetFrontendConfigContext ctx) {
        Map<String, String> configs = visitPropertyItemList(ctx.propertyItemList());
        boolean applyToAll = !ctx.ALL().isEmpty();

        return new AdminSetFrontendConfigCommand(
            NodeType.FRONTEND,
            configs,
            applyToAll);
    }

    @Override
    public LogicalPlan visitAdminSetPartitionVersion(DorisParser.AdminSetPartitionVersionContext ctx) {
        List<String> parts = visitMultipartIdentifier(ctx.name);
        TableNameInfo info = new TableNameInfo(parts);
        Map<String, String> properties = ctx.properties != null
                ? Maps.newHashMap(visitPropertyClause(ctx.properties))
                : Maps.newHashMap();

        return new AdminSetPartitionVersionCommand(info, properties);
    }

    @Override
    public LogicalPlan visitShowFrontends(ShowFrontendsContext ctx) {
        String detail = (ctx.name != null) ? ctx.name.getText() : null;
        return new ShowFrontendsCommand(detail);
    }

    @Override
    public LogicalPlan visitShowFunctions(ShowFunctionsContext ctx) {
        String dbName = null;
        if (ctx.database != null) {
            List<String> nameParts = visitMultipartIdentifier(ctx.database);
            if (nameParts.size() == 1) {
                dbName = nameParts.get(0);
            } else if (nameParts.size() == 2) {
                dbName = nameParts.get(1);
            } else {
                throw new AnalysisException("nameParts in analyze database should be [ctl.]db");
            }
        }

        boolean isVerbose = ctx.FULL() != null;
        boolean isBuiltin = ctx.BUILTIN() != null;

        String wild = null;
        if (ctx.STRING_LITERAL() != null) {
            wild = stripQuotes(ctx.STRING_LITERAL().getText());
        }
        return new ShowFunctionsCommand(dbName, isBuiltin, isVerbose, wild);
    }

    @Override
    public LogicalPlan visitShowGlobalFunctions(ShowGlobalFunctionsContext ctx) {
        boolean isVerbose = ctx.FULL() != null;

        String wild = null;
        if (ctx.STRING_LITERAL() != null) {
            wild = stripQuotes(ctx.STRING_LITERAL().getText());
        }
        return new ShowFunctionsCommand(isVerbose, wild, true);
    }

    @Override
    public LogicalPlan visitShowCreateDatabase(ShowCreateDatabaseContext ctx) {
        List<String> nameParts = visitMultipartIdentifier(ctx.name);
        String databaseName = "";
        String catalogName = "";
        if (nameParts.size() == 2) {
            // The identifier is in the form "internalcatalog.databasename"
            catalogName = nameParts.get(0);
            databaseName = nameParts.get(1);
        } else if (nameParts.size() == 1) {
            // The identifier is in the form "databasename"
            databaseName = nameParts.get(0);
        }

        return new ShowCreateDatabaseCommand(new DbName(catalogName, databaseName));
    }

    @Override
    public LogicalPlan visitShowCreateFunction(ShowCreateFunctionContext ctx) {
        SetType statementScope = visitStatementScope(ctx.statementScope());
        FunctionName function = visitFunctionIdentifier(ctx.functionIdentifier());
        String dbName = null;
        FunctionArgTypesInfo functionArgTypesInfo;
        if (ctx.functionArguments() != null) {
            functionArgTypesInfo = visitFunctionArguments(ctx.functionArguments());
        } else {
            functionArgTypesInfo = new FunctionArgTypesInfo(new ArrayList<>(), false);
        }
        if (ctx.database != null) {
            List<String> nameParts = visitMultipartIdentifier(ctx.database);
            if (nameParts.size() == 1) {
                dbName = nameParts.get(0);
            } else if (nameParts.size() == 2) {
                dbName = nameParts.get(1);
            } else {
                throw new AnalysisException("nameParts in analyze database should be [ctl.]db");
            }
        }

        return new ShowCreateFunctionCommand(dbName, statementScope, function, functionArgTypesInfo);
    }

    @Override
    public LogicalPlan visitCleanAllProfile(CleanAllProfileContext ctx) {
        return new CleanAllProfileCommand();
    }

    @Override
    public Object visitCleanLabel(CleanLabelContext ctx) {
        String label = ctx.label == null ? null : ctx.label.getText();
        IdentifierContext database = ctx.database;
        return new CleanLabelCommand(stripQuotes(database.getText()), label);
    }

    @Override
    public LogicalPlan visitShowWhitelist(ShowWhitelistContext ctx) {
        return new ShowWhiteListCommand();
    }

    @Override
    public LogicalPlan visitShowUserProperties(ShowUserPropertiesContext ctx) {
        String user = ctx.user != null ? stripQuotes(ctx.user.getText()) : null;
        String pattern = null;
        if (ctx.LIKE() != null) {
            pattern = stripQuotes(ctx.STRING_LITERAL().getText());
        }
        return new ShowUserPropertyCommand(user, pattern, false);
    }

    @Override
    public LogicalPlan visitShowAllProperties(ShowAllPropertiesContext ctx) {
        String pattern = null;
        if (ctx.LIKE() != null) {
            pattern = stripQuotes(ctx.STRING_LITERAL().getText());
        }
        return new ShowUserPropertyCommand(null, pattern, true);
    }

    @Override
    public LogicalPlan visitAlterCatalogComment(AlterCatalogCommentContext ctx) {
        String catalogName = stripQuotes(ctx.name.getText());
        String comment = stripQuotes(ctx.comment.getText());
        return new AlterCatalogCommentCommand(catalogName, comment);
    }

    @Override
    public LogicalPlan visitAlterDatabaseRename(AlterDatabaseRenameContext ctx) {
        String dbName = Optional.ofNullable(ctx.name)
                .map(ParserRuleContext::getText)
                .filter(s -> !s.isEmpty())
                .orElseThrow(() -> new ParseException("Database name is empty or cannot be an empty string"));
        String newDbName = Optional.ofNullable(ctx.newName)
                .map(ParserRuleContext::getText)
                .filter(s -> !s.isEmpty())
                .orElseThrow(() -> new ParseException("New Database name is empty or cannot be an empty string"));
        return new AlterDatabaseRenameCommand(dbName, newDbName);
    }

    @Override
    public LogicalPlan visitShowDynamicPartition(ShowDynamicPartitionContext ctx) {
        String dbName = null;
        if (ctx.database != null) {
            List<String> nameParts = visitMultipartIdentifier(ctx.database);
            dbName = nameParts.get(0); // only one entry possible
        }
        return new ShowDynamicPartitionCommand(dbName);
    }

    @Override
    public LogicalPlan visitCreateCatalog(CreateCatalogContext ctx) {
        String catalogName = ctx.catalogName.getText();
        boolean ifNotExists = ctx.IF() != null;
        String resourceName = ctx.resourceName == null ? null : (ctx.resourceName.getText());
        String comment = ctx.STRING_LITERAL() == null ? null : stripQuotes(ctx.STRING_LITERAL().getText());
        Map<String, String> properties = ctx.propertyClause() != null
                ? Maps.newHashMap(visitPropertyClause(ctx.propertyClause())) : Maps.newHashMap();

        return new CreateCatalogCommand(catalogName, ifNotExists, resourceName, comment, properties);
    }

    @Override
    public LogicalPlan visitShowStages(ShowStagesContext ctx) {
        return new ShowStagesCommand();
    }

    @Override
    public LogicalPlan visitRecoverDatabase(RecoverDatabaseContext ctx) {
        String dbName = ctx.name.getText();
        long dbId = (ctx.id != null) ? Long.parseLong(ctx.id.getText()) : -1;
        String newDbName = (ctx.alias != null) ? ctx.alias.getText() : null;
        return new RecoverDatabaseCommand(dbName, dbId, newDbName);
    }

    @Override
    public LogicalPlan visitShowWarningErrors(ShowWarningErrorsContext ctx) {
        boolean isWarning = ctx.WARNINGS() != null;

        // Extract the limit value if present
        long limit = 0;
        Optional<LimitClauseContext> limitCtx = Optional.ofNullable(ctx.limitClause());
        if (ctx.limitClause() != null) {
            limit = Long.parseLong(limitCtx.get().limit.getText());
            if (limit < 0) {
                throw new ParseException("Limit requires non-negative number", limitCtx.get());
            }
        }
        return new ShowWarningErrorsCommand(isWarning, limit);
    }

    @Override
    public LogicalPlan visitAlterCatalogProperties(AlterCatalogPropertiesContext ctx) {
        String catalogName = stripQuotes(ctx.name.getText());
        Map<String, String> properties = visitPropertyItemList(ctx.propertyItemList());
        return new AlterCatalogPropertiesCommand(catalogName, properties);
    }

    @Override
    public RecoverTableCommand visitRecoverTable(RecoverTableContext ctx) {
        List<String> dbTblNameParts = visitMultipartIdentifier(ctx.name);
        String newTableName = (ctx.alias != null) ? ctx.alias.getText() : null;
        long tableId = (ctx.id != null) ? Long.parseLong(ctx.id.getText()) : -1;
        return new RecoverTableCommand(new TableNameInfo(dbTblNameParts), tableId, newTableName);
    }

    @Override
    public RecoverPartitionCommand visitRecoverPartition(RecoverPartitionContext ctx) {
        String partitionName = ctx.name.getText();
        String newPartitionName = (ctx.alias != null) ? ctx.alias.getText() : null;
        long partitionId = (ctx.id != null) ? Long.parseLong(ctx.id.getText()) : -1;
        List<String> dbTblNameParts = visitMultipartIdentifier(ctx.tableName);
        return new RecoverPartitionCommand(new TableNameInfo(dbTblNameParts),
                partitionName, partitionId, newPartitionName);
    }

    @Override

    public LogicalPlan visitShowBroker(ShowBrokerContext ctx) {
        return new ShowBrokerCommand();
    }

    @Override
    public LogicalPlan visitShowBuildIndex(ShowBuildIndexContext ctx) {
        String dbName = null;
        Expression wildWhere = null;
        List<OrderKey> orderKeys = null;
        long limit = -1L;
        long offset = 0L;
        if (ctx.database != null) {
            dbName = ctx.database.getText();
        }
        if (ctx.wildWhere() != null) {
            wildWhere = getWildWhere(ctx.wildWhere());
        }
        if (ctx.sortClause() != null) {
            orderKeys = visit(ctx.sortClause().sortItem(), OrderKey.class);
        }
        if (ctx.limitClause() != null) {
            limit = ctx.limitClause().limit != null
                    ? Long.parseLong(ctx.limitClause().limit.getText())
                    : 0;
            if (limit < 0) {
                throw new ParseException("Limit requires non-negative number", ctx.limitClause());
            }
            offset = ctx.limitClause().offset != null
                    ? Long.parseLong(ctx.limitClause().offset.getText())
                    : 0;
            if (offset < 0) {
                throw new ParseException("Offset requires non-negative number", ctx.limitClause());
            }
        }
        return new ShowBuildIndexCommand(dbName, wildWhere, orderKeys, limit, offset);
    }

    @Override
    public LogicalPlan visitDropRole(DropRoleContext ctx) {
        String roleName = stripQuotes(ctx.name.getText());
        return new DropRoleCommand(roleName, ctx.EXISTS() != null);
    }

    @Override
    public LogicalPlan visitDropTable(DropTableContext ctx) {
        String ctlName = null;
        String dbName = null;
        String tableName = null;
        List<String> nameParts = visitMultipartIdentifier(ctx.name);
        if (nameParts.size() == 1) {
            tableName = nameParts.get(0);
        } else if (nameParts.size() == 2) {
            dbName = nameParts.get(0);
            tableName = nameParts.get(1);
        } else if (nameParts.size() == 3) {
            ctlName = nameParts.get(0);
            dbName = nameParts.get(1);
            tableName = nameParts.get(2);
        } else {
            throw new AnalysisException("nameParts in create table should be [ctl.][db.]tbl");
        }

        boolean ifExists = ctx.EXISTS() != null;
        boolean forceDrop = ctx.FORCE() != null;
        TableNameInfo tblNameInfo = new TableNameInfo(ctlName, dbName, tableName);
        return new DropTableCommand(ifExists, tblNameInfo, forceDrop);
    }

    @Override
    public LogicalPlan visitDropView(DorisParser.DropViewContext ctx) {
        String ctlName = null;
        String dbName = null;
        String tableName = null;
        List<String> nameParts = visitMultipartIdentifier(ctx.name);
        if (nameParts.size() == 1) {
            tableName = nameParts.get(0);
        } else if (nameParts.size() == 2) {
            dbName = nameParts.get(0);
            tableName = nameParts.get(1);
        } else if (nameParts.size() == 3) {
            ctlName = nameParts.get(0);
            dbName = nameParts.get(1);
            tableName = nameParts.get(2);
        } else {
            throw new AnalysisException("nameParts in drop view should be [ctl.][db.]tbl");
        }

        TableNameInfo tblNameInfo = new TableNameInfo(ctlName, dbName, tableName);
        return new DropViewCommand(ctx.EXISTS() != null, tblNameInfo);
    }

    @Override
    public LogicalPlan visitDropCatalog(DropCatalogContext ctx) {
        String catalogName = stripQuotes(ctx.name.getText());
        boolean ifExists = ctx.EXISTS() != null;
        return new DropCatalogCommand(catalogName, ifExists);
    }

    @Override
    public LogicalPlan visitCreateEncryptkey(CreateEncryptkeyContext ctx) {
        List<String> nameParts = visitMultipartIdentifier(ctx.multipartIdentifier());
        return new CreateEncryptkeyCommand(new EncryptKeyName(nameParts), ctx.EXISTS() != null,
                stripQuotes(ctx.STRING_LITERAL().getText()));
    }

    @Override
    public LogicalPlan visitAlterCatalogRename(AlterCatalogRenameContext ctx) {
        String catalogName = stripQuotes(ctx.name.getText());
        String newName = stripQuotes(ctx.newName.getText());
        return new AlterCatalogRenameCommand(catalogName, newName);
    }

    @Override
    public LogicalPlan visitDropStoragePolicy(DropStoragePolicyContext ctx) {
        String policyName = ctx.name.getText();
        boolean ifExists = ctx.EXISTS() != null;
        return new DropStoragePolicyCommand(policyName, ifExists);
    }

    @Override
    public LogicalPlan visitDropEncryptkey(DropEncryptkeyContext ctx) {
        List<String> nameParts = visitMultipartIdentifier(ctx.name);
        return new DropEncryptkeyCommand(new EncryptKeyName(nameParts), ctx.EXISTS() != null);
    }

    @Override
    public LogicalPlan visitCreateWorkloadGroup(CreateWorkloadGroupContext ctx) {
        String workloadGroupName = stripQuotes(ctx.name.getText());
        String cgName = ctx.computeGroup == null ? "" : stripQuotes(ctx.computeGroup.getText());
        boolean ifNotExists = ctx.EXISTS() != null;
        Map<String, String> properties = ctx.propertyClause() != null
                ? Maps.newHashMap(visitPropertyClause(ctx.propertyClause())) : Maps.newHashMap();
        return new CreateWorkloadGroupCommand(cgName, workloadGroupName, ifNotExists, properties);
    }

    @Override
    public LogicalPlan visitShowColumns(ShowColumnsContext ctx) {
        boolean isFull = ctx.FULL() != null;
        List<String> nameParts = visitMultipartIdentifier(ctx.tableName);
        String databaseName = ctx.database != null ? ctx.database.getText() : null;
        String likePattern = null;
        Expression expr = null;
        if (ctx.wildWhere() != null) {
            if (ctx.wildWhere().LIKE() != null) {
                likePattern = stripQuotes(ctx.wildWhere().STRING_LITERAL().getText());
            } else {
                expr = (Expression) ctx.wildWhere().expression().accept(this);
            }
        }

        return new ShowColumnsCommand(isFull, new TableNameInfo(nameParts), databaseName, likePattern, expr);
    }

    @Override
    public LogicalPlan visitDropFile(DropFileContext ctx) {
        String dbName = null;
        if (ctx.database != null) {
            dbName = ctx.database.getText();
        }
        Map<String, String> properties = ctx.propertyClause() != null
                ? Maps.newHashMap(visitPropertyClause(ctx.propertyClause())) : Maps.newHashMap();
        return new DropFileCommand(stripQuotes(ctx.name.getText()), dbName, properties);
    }

    @Override
    public LogicalPlan visitDropRepository(DropRepositoryContext ctx) {
        return new DropRepositoryCommand(stripQuotes(ctx.name.getText()));
    }

    @Override
    public LogicalPlan visitDropSqlBlockRule(DropSqlBlockRuleContext ctx) {
        return new DropSqlBlockRuleCommand(visitIdentifierSeq(ctx.identifierSeq()), ctx.EXISTS() != null);
    }

    @Override
    public LogicalPlan visitDropUser(DropUserContext ctx) {
        UserIdentity userIdent = visitUserIdentify(ctx.userIdentify());
        return new DropUserCommand(userIdent, ctx.EXISTS() != null);
    }

    @Override
    public LogicalPlan visitDropWorkloadGroup(DropWorkloadGroupContext ctx) {
        String cgName = ctx.computeGroup == null ? "" : stripQuotes(ctx.computeGroup.getText());
        return new DropWorkloadGroupCommand(cgName, ctx.name.getText(), ctx.EXISTS() != null);
    }

    @Override
    public LogicalPlan visitDropWorkloadPolicy(DropWorkloadPolicyContext ctx) {
        return new DropWorkloadPolicyCommand(ctx.name.getText(), ctx.EXISTS() != null);
    }

    @Override
    public LogicalPlan visitShowTableId(ShowTableIdContext ctx) {
        long tableId = -1;
        if (ctx.tableId != null) {
            tableId = Long.parseLong(ctx.tableId.getText());
        }
        return new ShowTableIdCommand(tableId);
    }

    @Override
    public LogicalPlan visitShowProcessList(ShowProcessListContext ctx) {
        return new ShowProcessListCommand(ctx.FULL() != null);
    }

    @Override
    public LogicalPlan visitHelp(HelpContext ctx) {
        String mark = ctx.mark.getText();
        return new HelpCommand(mark);
    }

    @Override
    public LogicalPlan visitSync(SyncContext ctx) {
        return new SyncCommand();
    }

    @Override
    public LogicalPlan visitShowDelete(ShowDeleteContext ctx) {
        String dbName = null;
        if (ctx.database != null) {
            List<String> nameParts = visitMultipartIdentifier(ctx.database);
            dbName = nameParts.get(0); // only one entry possible
        }
        return new ShowDeleteCommand(dbName);
    }

    @Override
    public LogicalPlan visitShowStoragePolicy(ShowStoragePolicyContext ctx) {
        String policyName = null;
        if (ctx.identifierOrText() != null) {
            policyName = stripQuotes(ctx.identifierOrText().getText());
        }
        return new ShowStoragePolicyCommand(policyName, ctx.USING() != null);
    }

    @Override
    public LogicalPlan visitShowPrivileges(ShowPrivilegesContext ctx) {
        return new ShowPrivilegesCommand();
    }

    @Override
    public LogicalPlan visitShowTabletsBelong(ShowTabletsBelongContext ctx) {
        List<Long> tabletIdLists = new ArrayList<>();
        ctx.tabletIds.stream().forEach(tabletToken -> {
            tabletIdLists.add(Long.parseLong(tabletToken.getText()));
        });
        return new ShowTabletsBelongCommand(tabletIdLists);
    }

    @Override
    public LogicalPlan visitShowCollation(ShowCollationContext ctx) {
        String wild = null;
        if (ctx.wildWhere() != null) {
            if (ctx.wildWhere().LIKE() != null) {
                wild = stripQuotes(ctx.wildWhere().STRING_LITERAL().getText());
            } else if (ctx.wildWhere().WHERE() != null) {
                wild = ctx.wildWhere().expression().getText();
            }
        }
        return new ShowCollationCommand(wild);
    }

    @Override
    public LogicalPlan visitAdminCheckTablets(AdminCheckTabletsContext ctx) {
        List<Long> tabletIdLists = new ArrayList<>();
        if (ctx.tabletList() != null) {
            ctx.tabletList().tabletIdList.stream().forEach(tabletToken -> {
                tabletIdLists.add(Long.parseLong(tabletToken.getText()));
            });
        }
        Map<String, String> properties = ctx.properties != null
                ? Maps.newHashMap(visitPropertyClause(ctx.properties))
                : Maps.newHashMap();
        return new AdminCheckTabletsCommand(tabletIdLists, properties);
    }

    @Override
    public LogicalPlan visitShowWarningErrorCount(ShowWarningErrorCountContext ctx) {
        boolean isWarning = ctx.WARNINGS() != null;
        return new ShowWarningErrorCountCommand(isWarning);
    }

    @Override
    public LogicalPlan visitShowStatus(ShowStatusContext ctx) {
        String scope = visitStatementScope(ctx.statementScope()).name();
        return new ShowStatusCommand(scope);
    }

    @Override
    public LogicalPlan visitShowDataSkew(ShowDataSkewContext ctx) {
        TableRefInfo tableRefInfo = visitBaseTableRefContext(ctx.baseTableRef());
        return new ShowDataSkewCommand(tableRefInfo);
    }

    @Override
    public LogicalPlan visitShowData(DorisParser.ShowDataContext ctx) {
        TableNameInfo tableNameInfo = null;
        if (ctx.tableName != null) {
            tableNameInfo = new TableNameInfo(visitMultipartIdentifier(ctx.tableName));
        }
        List<OrderKey> orderKeys = null;
        if (ctx.sortClause() != null) {
            orderKeys = visit(ctx.sortClause().sortItem(), OrderKey.class);
        }
        Map<String, String> properties = ctx.propertyClause() != null
                ? Maps.newHashMap(visitPropertyClause(ctx.propertyClause())) : Maps.newHashMap();
        boolean detailed = ctx.ALL() != null;
        return new ShowDataCommand(tableNameInfo, orderKeys, properties, detailed);
    }

    @Override
    public LogicalPlan visitShowTableCreation(ShowTableCreationContext ctx) {
        String dbName = null;
        String wild = null;
        if (ctx.database != null) {
            List<String> nameParts = visitMultipartIdentifier(ctx.database);
            dbName = nameParts.get(0); // only one entry possible
        }
        if (ctx.STRING_LITERAL() != null) {
            wild = ctx.STRING_LITERAL().getText();
        }
        return new ShowTableCreationCommand(dbName, wild);
    }

    @Override
    public SetType visitStatementScope(StatementScopeContext ctx) {
        SetType statementScope = SetType.DEFAULT;
        if (ctx != null) {
            if (ctx.GLOBAL() != null) {
                statementScope = SetType.GLOBAL;
            } else if (ctx.LOCAL() != null || ctx.SESSION() != null) {
                statementScope = SetType.SESSION;
            }
        }
        return statementScope;
    }

    @Override
    public LogicalPlan visitUseCloudCluster(DorisParser.UseCloudClusterContext ctx) {
        if (ctx.catalog != null) {
            return new UseCloudClusterCommand(
                    stripQuotes(ctx.cluster.getText()),
                    stripQuotes(ctx.database.getText()),
                    stripQuotes(ctx.catalog.getText()));
        } else if (ctx.database != null) {
            return new UseCloudClusterCommand(
                    stripQuotes(ctx.cluster.getText()),
                    stripQuotes(ctx.database.getText()));
        }

        return new UseCloudClusterCommand(stripQuotes(ctx.cluster.getText()));
    }

    @Override
    public LogicalPlan visitAdminShowTabletStorageFormat(AdminShowTabletStorageFormatContext ctx) {
        return new ShowTabletStorageFormatCommand(ctx.VERBOSE() != null);
    }

    @Override
    public LogicalPlan visitShowTabletStorageFormat(ShowTabletStorageFormatContext ctx) {
        return new ShowTabletStorageFormatCommand(ctx.VERBOSE() != null);
    }

    @Override
    public LogicalPlan visitShowTabletsFromTable(DorisParser.ShowTabletsFromTableContext ctx) {
        TableNameInfo tableName = new TableNameInfo(visitMultipartIdentifier(ctx.tableName));
        PartitionNamesInfo partitionNamesInfo = null;
        if (ctx.partitionSpec() != null) {
            Pair<Boolean, List<String>> partitionSpec = visitPartitionSpec(ctx.partitionSpec());
            partitionNamesInfo = new PartitionNamesInfo(partitionSpec.first, partitionSpec.second);
        }
        List<OrderKey> orderKeys = null;
        if (ctx.sortClause() != null) {
            orderKeys = visit(ctx.sortClause().sortItem(), OrderKey.class);
        }
        long limit = 0;
        long offset = 0;
        if (ctx.limitClause() != null) {
            limit = ctx.limitClause().limit != null
                    ? Long.parseLong(ctx.limitClause().limit.getText())
                    : 0;
            if (limit < 0) {
                throw new ParseException("Limit requires non-negative number", ctx.limitClause());
            }
            offset = ctx.limitClause().offset != null
                    ? Long.parseLong(ctx.limitClause().offset.getText())
                    : 0;
            if (offset < 0) {
                throw new ParseException("Offset requires non-negative number", ctx.limitClause());
            }
        }
        if (ctx.wildWhere() != null) {
            if (ctx.wildWhere().LIKE() != null) {
                throw new ParseException("Not support like clause");
            } else {
                Expression expr = (Expression) ctx.wildWhere().expression().accept(this);
                return new ShowTabletsFromTableCommand(tableName, partitionNamesInfo, expr, orderKeys, limit, offset);
            }
        }

        return new ShowTabletsFromTableCommand(tableName, partitionNamesInfo, null, orderKeys, limit, offset);
    }

    @Override
    public LogicalPlan visitShowQueryProfile(ShowQueryProfileContext ctx) {
        String queryIdPath = "/";
        if (ctx.queryIdPath != null) {
            queryIdPath = stripQuotes(ctx.queryIdPath.getText());
        }

        long limit = 20;
        if (ctx.limitClause() != null) {
            limit = Long.parseLong(ctx.limitClause().limit.getText());
            if (limit < 0) {
                throw new ParseException("Limit requires non-negative number, got " + String.valueOf(limit));
            }
        }
        return new ShowQueryProfileCommand(queryIdPath, limit);
    }

    @Override
    public LogicalPlan visitShowQueryStats(ShowQueryStatsContext ctx) {
        String dbName = null;
        if (ctx.database != null) {
            dbName = ctx.database.getText();
        }
        TableNameInfo tableNameInfo = null;
        if (ctx.tableName != null) {
            tableNameInfo = new TableNameInfo(visitMultipartIdentifier(ctx.tableName));
        }
        boolean isAll = ctx.ALL() != null;
        boolean isVerbose = ctx.VERBOSE() != null;
        return new ShowQueryStatsCommand(dbName, tableNameInfo, isAll, isVerbose);
    }

    @Override
    public LogicalPlan visitSwitchCatalog(SwitchCatalogContext ctx) {
        if (ctx.catalog != null) {
            return new SwitchCommand(ctx.catalog.getText());
        }
        throw new ParseException("catalog name can not be null");
    }

    @Override
    public LogicalPlan visitUseDatabase(UseDatabaseContext ctx) {
        if (ctx.database == null) {
            throw new ParseException("database name can not be null");
        }
        return ctx.catalog != null ? new UseCommand(ctx.catalog.getText(), ctx.database.getText())
                : new UseCommand(ctx.database.getText());
    }

    @Override
    public LogicalPlan visitTruncateTable(DorisParser.TruncateTableContext ctx) {
        TableNameInfo tableName = new TableNameInfo(visitMultipartIdentifier(ctx.multipartIdentifier()));
        Optional<PartitionNamesInfo> partitionNamesInfo = ctx.specifiedPartition() == null
                ? Optional.empty() : Optional.of(visitSpecifiedPartitionContext(ctx.specifiedPartition()));

        return new TruncateTableCommand(
                tableName,
                partitionNamesInfo,
                ctx.FORCE() != null
        );
    }

    @Override
    public LogicalPlan visitShowConvertLsc(ShowConvertLscContext ctx) {
        if (ctx.database == null) {
            return new ShowConvertLSCCommand(null);
        }
        List<String> parts = visitMultipartIdentifier(ctx.database);
        String databaseName = parts.get(parts.size() - 1);
        if (parts.size() == 2 && !InternalCatalog.INTERNAL_CATALOG_NAME.equalsIgnoreCase(parts.get(0))) {
            throw new ParseException("The execution of this command is restricted to the internal catalog only.");
        } else if (parts.size() > 2) {
            throw new ParseException("Only one dot can be in the name: " + String.join(".", parts));
        }
        return new ShowConvertLSCCommand(databaseName);
    }

    @Override
    public LogicalPlan visitKillQuery(KillQueryContext ctx) {
        String queryId = null;
        int connectionId = -1;
        TerminalNode integerValue = ctx.INTEGER_VALUE();
        if (integerValue != null) {
            connectionId = Integer.valueOf(integerValue.getText());
        } else {
            queryId = stripQuotes(ctx.STRING_LITERAL().getText());
        }
        return new KillQueryCommand(queryId, connectionId);
    }

    @Override
    public LogicalPlan visitKillConnection(DorisParser.KillConnectionContext ctx) {
        int connectionId = Integer.parseInt(ctx.INTEGER_VALUE().getText());
        return new KillConnectionCommand(connectionId);
    }

    @Override
    public Object visitAlterDatabaseSetQuota(AlterDatabaseSetQuotaContext ctx) {
        String databaseName = Optional.ofNullable(ctx.name)
                .map(ParseTree::getText).filter(s -> !s.isEmpty())
                .orElseThrow(() -> new ParseException("database name can not be null"));
        String quota = Optional.ofNullable(ctx.quota)
                .map(ParseTree::getText)
                .orElseGet(() -> Optional.ofNullable(ctx.INTEGER_VALUE())
                        .map(TerminalNode::getText)
                        .orElse(null));
        // Determine the quota type
        QuotaType quotaType;
        if (ctx.DATA() != null) {
            quotaType = QuotaType.DATA;
        } else if (ctx.REPLICA() != null) {
            quotaType = QuotaType.REPLICA;
        } else if (ctx.TRANSACTION() != null) {
            quotaType = QuotaType.TRANSACTION;
        } else {
            quotaType = QuotaType.NONE;
        }
        return new AlterDatabaseSetQuotaCommand(databaseName, quotaType, quota);
    }

    @Override
    public LogicalPlan visitDropDatabase(DropDatabaseContext ctx) {
        boolean ifExists = ctx.EXISTS() != null;
        List<String> databaseNameParts = visitMultipartIdentifier(ctx.name);
        boolean force = ctx.FORCE() != null;
        DropDatabaseInfo databaseInfo = new DropDatabaseInfo(ifExists, databaseNameParts, force);
        return new DropDatabaseCommand(databaseInfo);
    }

    @Override
    public LogicalPlan visitAlterRepository(AlterRepositoryContext ctx) {

        Map<String, String> properties = ctx.propertyClause() != null
                ? Maps.newHashMap(visitPropertyClause(ctx.propertyClause())) : Maps.newHashMap();

        return new AlterRepositoryCommand(ctx.name.getText(), properties);
    }

    @Override
    public LogicalPlan visitShowAnalyze(ShowAnalyzeContext ctx) {
        boolean isAuto = ctx.AUTO() != null;
        List<String> tableName = ctx.tableName == null ? null : visitMultipartIdentifier(ctx.tableName);
        long jobId = ctx.jobId == null ? 0 : Long.parseLong(ctx.jobId.getText());
        String stateKey = ctx.stateKey == null ? null : stripQuotes(ctx.stateKey.getText());
        String stateValue = ctx.stateValue == null ? null : stripQuotes(ctx.stateValue.getText());
        return new ShowAnalyzeCommand(tableName, jobId, stateKey, stateValue, isAuto);
    }

    @Override
    public LogicalPlan visitShowAlterTable(ShowAlterTableContext ctx) {
        String dbName = null;
        Expression wildWhere = null;
        List<OrderKey> orderKeys = null;
        long limit = -1L;
        long offset = 0L;

        ShowAlterTableCommand.AlterType alterType;
        if (ctx.ROLLUP() != null) {
            alterType = ShowAlterTableCommand.AlterType.ROLLUP;
        } else if (ctx.MATERIALIZED() != null && ctx.VIEW() != null) {
            alterType = ShowAlterTableCommand.AlterType.MV;
        } else if (ctx.COLUMN() != null) {
            alterType = ShowAlterTableCommand.AlterType.COLUMN;
        } else {
            throw new AnalysisException("invalid ShowOpType, it must be one of 'ROLLUP',"
                    + "'MATERIALIZED VIEW' or 'COLUMN'");
        }

        if (ctx.database != null) {
            List<String> nameParts = visitMultipartIdentifier(ctx.database);
            if (nameParts.size() == 1) {
                dbName = nameParts.get(0);
            } else if (nameParts.size() == 2) {
                dbName = nameParts.get(1);
            } else {
                throw new AnalysisException("nameParts in analyze database should be [ctl.]db");
            }
        }
        if (ctx.sortClause() != null) {
            orderKeys = visit(ctx.sortClause().sortItem(), OrderKey.class);
        }
        if (ctx.wildWhere() != null) {
            wildWhere = getWildWhere(ctx.wildWhere());
        }
        if (ctx.limitClause() != null) {
            limit = ctx.limitClause().limit != null
                    ? Long.parseLong(ctx.limitClause().limit.getText())
                    : 0;
            if (limit < 0) {
                throw new ParseException("Limit requires non-negative number", ctx.limitClause());
            }
            offset = ctx.limitClause().offset != null
                    ? Long.parseLong(ctx.limitClause().offset.getText())
                    : 0;
            if (offset < 0) {
                throw new ParseException("Offset requires non-negative number", ctx.limitClause());
            }
        }
        return new ShowAlterTableCommand(dbName, wildWhere, orderKeys, limit, offset, alterType);
    }

    @Override
    public LogicalPlan visitShowOpenTables(ShowOpenTablesContext ctx) {
        String db = ctx.database != null ? visitMultipartIdentifier(ctx.database).get(0) : null;
        String likePattern = null;
        Expression expr = null;

        if (ctx.wildWhere() != null) {
            if (ctx.wildWhere().LIKE() != null) {
                likePattern = stripQuotes(ctx.wildWhere().STRING_LITERAL().getText());
            } else {
                expr = (Expression) ctx.wildWhere().expression().accept(this);
            }
        }

        return new ShowOpenTablesCommand(db, likePattern, expr);
    }

    @Override
    public LogicalPlan visitDropAllBrokerClause(DropAllBrokerClauseContext ctx) {
        String brokerName = stripQuotes(ctx.name.getText());
        AlterSystemOp alterSystemOp = new DropAllBrokerOp(brokerName);
        return new AlterSystemCommand(alterSystemOp, PlanType.ALTER_SYSTEM_DROP_ALL_BROKER);
    }

    @Override
    public LogicalPlan visitAlterSystem(DorisParser.AlterSystemContext ctx) {
        return plan(ctx.alterSystemClause());
    }

    @Override
    public LogicalPlan visitAddBrokerClause(AddBrokerClauseContext ctx) {
        String brokerName = stripQuotes(ctx.name.getText());
        List<String> hostPorts = ctx.hostPorts.stream()
                .map(e -> stripQuotes(e.getText()))
                .collect(Collectors.toList());
        AlterSystemOp alterSystemOp = new AddBrokerOp(brokerName, hostPorts);
        return new AlterSystemCommand(alterSystemOp, PlanType.ALTER_SYSTEM_ADD_BROKER);
    }

    @Override
    public LogicalPlan visitDropBrokerClause(DropBrokerClauseContext ctx) {
        String brokerName = stripQuotes(ctx.name.getText());
        List<String> hostPorts = ctx.hostPorts.stream()
                .map(e -> stripQuotes(e.getText()))
                .collect(Collectors.toList());
        AlterSystemOp alterSystemOp = new DropBrokerOp(brokerName, hostPorts);
        return new AlterSystemCommand(alterSystemOp, PlanType.ALTER_SYSTEM_DROP_BROKER);
    }

    @Override
    public LogicalPlan visitAddBackendClause(AddBackendClauseContext ctx) {
        List<String> hostPorts = ctx.hostPorts.stream()
                .map(e -> stripQuotes(e.getText()))
                .collect(Collectors.toList());
        Map<String, String> properties = visitPropertyClause(ctx.properties);
        AlterSystemOp alterSystemOp = new AddBackendOp(hostPorts, properties);
        return new AlterSystemCommand(alterSystemOp, PlanType.ALTER_SYSTEM_ADD_BACKEND);
    }

    @Override
    public LogicalPlan visitDropBackendClause(DorisParser.DropBackendClauseContext ctx) {
        List<String> hostPorts = ctx.hostPorts.stream()
                .map(e -> stripQuotes(e.getText()))
                .collect(Collectors.toList());
        boolean force = false;
        if (ctx.DROPP() != null) {
            force = true;
        }
        AlterSystemOp alterSystemOp = new DropBackendOp(hostPorts, force);
        return new AlterSystemCommand(alterSystemOp, PlanType.ALTER_SYSTEM_DROP_BACKEND);
    }

    @Override
    public LogicalPlan visitDecommissionBackendClause(DorisParser.DecommissionBackendClauseContext ctx) {
        List<String> hostPorts = ctx.hostPorts.stream()
                .map(e -> stripQuotes(e.getText()))
                .collect(Collectors.toList());
        AlterSystemOp alterSystemOp = new DecommissionBackendOp(hostPorts);
        return new AlterSystemCommand(alterSystemOp, PlanType.ALTER_SYSTEM_DECOMMISSION_BACKEND);
    }

    @Override
    public LogicalPlan visitAddFollowerClause(DorisParser.AddFollowerClauseContext ctx) {
        String hostPort = stripQuotes(ctx.hostPort.getText());
        AlterSystemOp alterSystemOp = new AddFollowerOp(hostPort);
        return new AlterSystemCommand(alterSystemOp, PlanType.ALTER_SYSTEM_ADD_FOLLOWER);
    }

    @Override
    public LogicalPlan visitDropFollowerClause(DorisParser.DropFollowerClauseContext ctx) {
        String hostPort = stripQuotes(ctx.hostPort.getText());
        AlterSystemOp alterSystemOp = new DropFollowerOp(hostPort);
        return new AlterSystemCommand(alterSystemOp, PlanType.ALTER_SYSTEM_DROP_FOLLOWER);
    }

    @Override
    public LogicalPlan visitAddObserverClause(DorisParser.AddObserverClauseContext ctx) {
        String hostPort = stripQuotes(ctx.hostPort.getText());
        AlterSystemOp alterSystemOp = new AddObserverOp(hostPort);
        return new AlterSystemCommand(alterSystemOp, PlanType.ALTER_SYSTEM_ADD_OBSERVER);
    }

    @Override
    public LogicalPlan visitDropObserverClause(DorisParser.DropObserverClauseContext ctx) {
        String hostPort = stripQuotes(ctx.hostPort.getText());
        AlterSystemOp alterSystemOp = new DropObserverOp(hostPort);
        return new AlterSystemCommand(alterSystemOp, PlanType.ALTER_SYSTEM_DROP_OBSERVER);
    }

    @Override
    public LogicalPlan visitAlterLoadErrorUrlClause(DorisParser.AlterLoadErrorUrlClauseContext ctx) {
        Map<String, String> properties = visitPropertyClause(ctx.properties);
        AlterSystemOp alterSystemOp = new AlterLoadErrorUrlOp(properties);
        return new AlterSystemCommand(alterSystemOp, PlanType.ALTER_SYSTEM_SET_LOAD_ERRORS_HU);
    }

    @Override
    public LogicalPlan visitModifyBackendClause(DorisParser.ModifyBackendClauseContext ctx) {
        List<String> hostPorts = ctx.hostPorts.stream()
                .map(e -> stripQuotes(e.getText()))
                .collect(Collectors.toList());
        Map<String, String> properties = visitPropertyItemList(ctx.propertyItemList());
        AlterSystemOp alterSystemOp = new ModifyBackendOp(hostPorts, properties);
        return new AlterSystemCommand(alterSystemOp, PlanType.ALTER_SYSTEM_MODIFY_BACKEND);
    }

    @Override
    public LogicalPlan visitModifyFrontendOrBackendHostNameClause(
            DorisParser.ModifyFrontendOrBackendHostNameClauseContext ctx) {
        String hostPort = stripQuotes(ctx.hostPort.getText());
        String hostName = stripQuotes(ctx.hostName.getText());
        AlterSystemOp alterSystemOp = null;
        if (ctx.FRONTEND() != null) {
            alterSystemOp = new ModifyFrontendOrBackendHostNameOp(hostPort, hostName, ModifyOpType.Frontend);
        } else if (ctx.BACKEND() != null) {
            alterSystemOp = new ModifyFrontendOrBackendHostNameOp(hostPort, hostName, ModifyOpType.Backend);
        }
        return new AlterSystemCommand(alterSystemOp, PlanType.ALTER_SYSTEM_MODIFY_FRONTEND_OR_BACKEND_HOSTNAME);
    }

    @Override
    public LogicalPlan visitShowQueuedAnalyzeJobs(ShowQueuedAnalyzeJobsContext ctx) {
        List<String> tableName = ctx.tableName == null ? null : visitMultipartIdentifier(ctx.tableName);
        String stateKey = ctx.stateKey == null ? null : stripQuotes(ctx.stateKey.getText());
        String stateValue = ctx.stateValue == null ? null : stripQuotes(ctx.stateValue.getText());
        return new ShowQueuedAnalyzeJobsCommand(tableName, stateKey, stateValue);
    }

    @Override
    public LogicalPlan visitShowIndexStats(DorisParser.ShowIndexStatsContext ctx) {
        TableNameInfo tableName = new TableNameInfo(visitMultipartIdentifier(ctx.tableName));
        String indexId = stripQuotes(ctx.indexId.getText());
        return new ShowIndexStatsCommand(tableName, indexId);
    }

    @Override
    public LogicalPlan visitShowIndex(DorisParser.ShowIndexContext ctx) {
        TableNameInfo tableNameInfo = new TableNameInfo(visitMultipartIdentifier(ctx.tableName));

        String ctlName = null;
        String dbName = null;
        if (ctx.database != null) {
            List<String> nameParts = visitMultipartIdentifier(ctx.database);
            if (nameParts.size() == 1) {
                dbName = nameParts.get(0);
            } else if (nameParts.size() == 2) {
                ctlName = nameParts.get(0);
                dbName = nameParts.get(1);
            } else {
                throw new AnalysisException("nameParts in analyze database should be [ctl.]db");
            }
        }

        if (dbName != null && tableNameInfo.getDb() != null && !tableNameInfo.getDb().equals(dbName)) {
            throw new ParseException("The name of two databases should be the same");
        }
        if (tableNameInfo.getDb() == null) {
            tableNameInfo.setDb(dbName);
        }
        if (tableNameInfo.getCtl() == null) {
            tableNameInfo.setCtl(ctlName);
        }
        return new ShowIndexCommand(tableNameInfo);
    }

    @Override
    public LogicalPlan visitShowTableStatus(DorisParser.ShowTableStatusContext ctx) {
        String ctlName = null;
        String dbName = null;
        if (ctx.database != null) {
            List<String> nameParts = visitMultipartIdentifier(ctx.database);
            if (nameParts.size() == 1) {
                dbName = nameParts.get(0);
            } else if (nameParts.size() == 2) {
                ctlName = nameParts.get(0);
                dbName = nameParts.get(1);
            } else {
                throw new AnalysisException("nameParts in analyze database should be [ctl.]db");
            }
        }

        if (ctx.wildWhere() != null) {
            if (ctx.wildWhere().LIKE() != null) {
                return new ShowTableStatusCommand(dbName, ctlName,
                        stripQuotes(ctx.wildWhere().STRING_LITERAL().getText()), null);
            } else {
                Expression expr = (Expression) ctx.wildWhere().expression().accept(this);
                return new ShowTableStatusCommand(dbName, ctlName, null, expr);
            }
        }
        return new ShowTableStatusCommand(dbName, ctlName);
    }

    @Override
    public LogicalPlan visitLockTables(LockTablesContext ctx) {
        List<LockTableInfo> lockTablesInfo = ctx.lockTable().stream().map(lockTableCtx -> {
            TableNameInfo tableNameInfo = new TableNameInfo(visitMultipartIdentifier(lockTableCtx.name));
            String alias = lockTableCtx.alias != null ? lockTableCtx.alias.getText() : null;

            LockTable.LockType lockType;
            if (lockTableCtx.READ() != null) {
                lockType = lockTableCtx.LOCAL() != null ? LockTable.LockType.READ_LOCAL : LockTable.LockType.READ;
            } else if (lockTableCtx.WRITE() != null) {
                lockType = lockTableCtx.LOW_PRIORITY() != null ? LockTable.LockType.LOW_PRIORITY_WRITE
                        : LockTable.LockType.WRITE;
            } else {
                throw new IllegalArgumentException("Invalid lock type in LOCK TABLES command.");
            }

            return new LockTableInfo(tableNameInfo, alias, lockType);
        }).collect(Collectors.toList());

        return new LockTablesCommand(lockTablesInfo);
    }

    @Override
    public LogicalPlan visitShowTables(DorisParser.ShowTablesContext ctx) {
        String ctlName = null;
        String dbName = null;
        if (ctx.database != null) {
            List<String> nameParts = visitMultipartIdentifier(ctx.database);
            if (nameParts.size() == 1) {
                dbName = nameParts.get(0);
            } else if (nameParts.size() == 2) {
                ctlName = nameParts.get(0);
                dbName = nameParts.get(1);
            } else {
                throw new AnalysisException("nameParts in analyze database should be [ctl.]db");
            }
        }

        boolean isVerbose = ctx.FULL() != null;

        if (ctx.wildWhere() != null) {
            if (ctx.wildWhere().LIKE() != null) {
                return new ShowTableCommand(dbName, ctlName, isVerbose,
                        stripQuotes(ctx.wildWhere().STRING_LITERAL().getText()), null, PlanType.SHOW_TABLES);
            } else {
                return new ShowTableCommand(dbName, ctlName, isVerbose, null,
                        getOriginSql(ctx.wildWhere()), PlanType.SHOW_TABLES);
            }
        }
        return new ShowTableCommand(dbName, ctlName, isVerbose, PlanType.SHOW_TABLES);
    }

    @Override
    public Plan visitUnlockTables(UnlockTablesContext ctx) {
        return new UnlockTablesCommand();
    }

    @Override
    public LogicalPlan visitCreateWorkloadPolicy(CreateWorkloadPolicyContext ctx) {
        String policyName = ctx.name.getText();
        boolean ifNotExists = ctx.IF() != null;

        List<WorkloadConditionMeta> conditions = new ArrayList<>();
        if (ctx.workloadPolicyConditions() != null) {
            for (DorisParser.WorkloadPolicyConditionContext conditionCtx :
                    ctx.workloadPolicyConditions().workloadPolicyCondition()) {
                String metricName = conditionCtx.metricName.getText();
                String operator = conditionCtx.comparisonOperator().getText();
                String value = conditionCtx.number() != null
                        ? conditionCtx.number().getText()
                        : stripQuotes(conditionCtx.STRING_LITERAL().getText());
                try {
                    WorkloadConditionMeta conditionMeta = new WorkloadConditionMeta(metricName, operator, value);
                    conditions.add(conditionMeta);
                } catch (UserException e) {
                    throw new AnalysisException(e.getMessage(), e);
                }
            }
        }

        List<WorkloadActionMeta> actions = new ArrayList<>();
        if (ctx.workloadPolicyActions() != null) {
            for (DorisParser.WorkloadPolicyActionContext actionCtx :
                    ctx.workloadPolicyActions().workloadPolicyAction()) {
                try {
                    if (actionCtx.SET_SESSION_VARIABLE() != null) {
                        actions.add(new WorkloadActionMeta("SET_SESSION_VARIABLE",
                                stripQuotes(actionCtx.STRING_LITERAL().getText())));
                    } else {
                        String identifier = actionCtx.identifier().getText();
                        String value = actionCtx.STRING_LITERAL() != null
                                ? stripQuotes(actionCtx.STRING_LITERAL().getText())
                                : null;
                        actions.add(new WorkloadActionMeta(identifier, value));
                    }
                } catch (UserException e) {
                    throw new AnalysisException(e.getMessage(), e);
                }
            }
        }

        Map<String, String> properties = ctx.propertyClause() != null
                ? Maps.newHashMap(visitPropertyClause(ctx.propertyClause()))
                : Maps.newHashMap();

        return new CreateWorkloadPolicyCommand(ifNotExists, policyName, conditions, actions, properties);
    }

    @Override
    public LogicalPlan visitShowViews(DorisParser.ShowViewsContext ctx) {
        String ctlName = null;
        String dbName = null;
        if (ctx.database != null) {
            List<String> nameParts = visitMultipartIdentifier(ctx.database);
            if (nameParts.size() == 1) {
                dbName = nameParts.get(0);
            } else if (nameParts.size() == 2) {
                ctlName = nameParts.get(0);
                dbName = nameParts.get(1);
            } else {
                throw new AnalysisException("nameParts in analyze database should be [ctl.]db");
            }
        }

        boolean isVerbose = ctx.FULL() != null;

        if (ctx.wildWhere() != null) {
            if (ctx.wildWhere().LIKE() != null) {
                return new ShowTableCommand(dbName, ctlName, isVerbose,
                        stripQuotes(ctx.wildWhere().STRING_LITERAL().getText()), null, PlanType.SHOW_VIEWS);
            } else {
                return new ShowTableCommand(dbName, ctlName, isVerbose, null,
                        getOriginSql(ctx.wildWhere()), PlanType.SHOW_VIEWS);
            }
        }
        return new ShowTableCommand(dbName, ctlName, isVerbose, PlanType.SHOW_VIEWS);
    }

    @Override
    public LogicalPlan visitShowTabletId(DorisParser.ShowTabletIdContext ctx) {
        long tabletId = Long.parseLong(ctx.tabletId.getText());
        return new ShowTabletIdCommand(tabletId);
    }

    @Override
    public LogicalPlan visitShowDatabases(DorisParser.ShowDatabasesContext ctx) {
        String ctlName = null;
        if (ctx.catalog != null) {
            ctlName = ctx.catalog.getText();
        }

        if (ctx.wildWhere() != null) {
            if (ctx.wildWhere().LIKE() != null) {
                return new ShowDatabasesCommand(ctlName,
                        stripQuotes(ctx.wildWhere().STRING_LITERAL().getText()), null);
            } else {
                Expression expr = (Expression) ctx.wildWhere().expression().accept(this);
                return new ShowDatabasesCommand(ctlName, null, expr);
            }
        }
        return new ShowDatabasesCommand(ctlName, null, null);
    }

    @Override
    public LogicalPlan visitDescribeTable(DorisParser.DescribeTableContext ctx) {
        TableNameInfo tableName = new TableNameInfo(visitMultipartIdentifier(ctx.multipartIdentifier()));
        PartitionNamesInfo partitionNames = null;
        boolean isTempPart = false;
        if (ctx.specifiedPartition() != null) {
            isTempPart = ctx.specifiedPartition().TEMPORARY() != null;
            if (ctx.specifiedPartition().identifier() != null) {
                partitionNames = new PartitionNamesInfo(isTempPart,
                        ImmutableList.of(ctx.specifiedPartition().identifier().getText()));
            } else {
                partitionNames = new PartitionNamesInfo(isTempPart,
                        visitIdentifierList(ctx.specifiedPartition().identifierList()));
            }
        }
        return new DescribeCommand(tableName, false, partitionNames);
    }

    @Override
    public LogicalPlan visitAnalyzeTable(DorisParser.AnalyzeTableContext ctx) {
        TableNameInfo tableNameInfo = new TableNameInfo(visitMultipartIdentifier(ctx.name));
        PartitionNamesInfo partitionNamesInfo = null;
        if (ctx.partitionSpec() != null) {
            Pair<Boolean, List<String>> partitionSpec = visitPartitionSpec(ctx.partitionSpec());
            partitionNamesInfo = new PartitionNamesInfo(partitionSpec.first, partitionSpec.second);
        }
        List<String> columnNames = null;
        if (ctx.columns != null) {
            columnNames = visitIdentifierList(ctx.columns);
        }
        Map<String, String> propertiesMap = new HashMap<>();
        // default values
        propertiesMap.put(AnalyzeProperties.PROPERTY_SYNC, "false");
        propertiesMap.put(AnalyzeProperties.PROPERTY_ANALYSIS_TYPE, AnalysisInfo.AnalysisType.FUNDAMENTALS.toString());
        for (DorisParser.AnalyzePropertiesContext aps : ctx.analyzeProperties()) {
            Map<String, String> map = visitAnalyzeProperties(aps);
            propertiesMap.putAll(map);
        }
        propertiesMap.putAll(visitPropertyClause(ctx.propertyClause()));
        AnalyzeProperties properties = new AnalyzeProperties(propertiesMap);
        return new AnalyzeTableCommand(tableNameInfo,
                partitionNamesInfo, columnNames, properties);
    }

    @Override
    public LogicalPlan visitAnalyzeDatabase(DorisParser.AnalyzeDatabaseContext ctx) {
        String ctlName = null;
        String dbName = null;
        List<String> nameParts = visitMultipartIdentifier(ctx.name);
        if (nameParts.size() == 1) {
            dbName = nameParts.get(0);
        } else if (nameParts.size() == 2) {
            ctlName = nameParts.get(0);
            dbName = nameParts.get(1);
        } else {
            throw new AnalysisException("nameParts in analyze database should be [ctl.]db");
        }

        Map<String, String> propertiesMap = new HashMap<>();
        // default values
        propertiesMap.put(AnalyzeProperties.PROPERTY_SYNC, "false");
        propertiesMap.put(AnalyzeProperties.PROPERTY_ANALYSIS_TYPE, AnalysisInfo.AnalysisType.FUNDAMENTALS.toString());
        for (DorisParser.AnalyzePropertiesContext aps : ctx.analyzeProperties()) {
            Map<String, String> map = visitAnalyzeProperties(aps);
            propertiesMap.putAll(map);
        }
        propertiesMap.putAll(visitPropertyClause(ctx.propertyClause()));
        AnalyzeProperties properties = new AnalyzeProperties(propertiesMap);
        return new AnalyzeDatabaseCommand(ctlName, dbName, properties);
    }

    @Override
    public Map<String, String> visitAnalyzeProperties(DorisParser.AnalyzePropertiesContext ctx) {
        Map<String, String> properties = new HashMap<>();
        if (ctx.SYNC() != null) {
            properties.put(AnalyzeProperties.PROPERTY_SYNC, "true");
        } else if (ctx.INCREMENTAL() != null) {
            properties.put(AnalyzeProperties.PROPERTY_INCREMENTAL, "true");
        } else if (ctx.FULL() != null) {
            properties.put(AnalyzeProperties.PROPERTY_FORCE_FULL, "true");
        } else if (ctx.SQL() != null) {
            properties.put(AnalyzeProperties.PROPERTY_EXTERNAL_TABLE_USE_SQL, "true");
        } else if (ctx.HISTOGRAM() != null) {
            properties.put(AnalyzeProperties.PROPERTY_ANALYSIS_TYPE, AnalysisInfo.AnalysisType.HISTOGRAM.toString());
        } else if (ctx.SAMPLE() != null) {
            if (ctx.ROWS() != null) {
                properties.put(AnalyzeProperties.PROPERTY_SAMPLE_ROWS, ctx.INTEGER_VALUE().getText());
            } else if (ctx.PERCENT() != null) {
                properties.put(AnalyzeProperties.PROPERTY_SAMPLE_PERCENT, ctx.INTEGER_VALUE().getText());
            }
        } else if (ctx.BUCKETS() != null) {
            properties.put(AnalyzeProperties.PROPERTY_NUM_BUCKETS, ctx.INTEGER_VALUE().getText());
        } else if (ctx.PERIOD() != null) {
            properties.put(AnalyzeProperties.PROPERTY_PERIOD_SECONDS, ctx.INTEGER_VALUE().getText());
        } else if (ctx.CRON() != null) {
            properties.put(AnalyzeProperties.PROPERTY_PERIOD_CRON, ctx.STRING_LITERAL().getText());
        }
        return properties;
    }

    @Override
    public LogicalPlan visitCreateDatabase(DorisParser.CreateDatabaseContext ctx) {
        Map<String, String> properties = visitPropertyClause(ctx.propertyClause());
        List<String> nameParts = visitMultipartIdentifier(ctx.multipartIdentifier());

        if (nameParts.size() > 2) {
            throw new AnalysisException("create database should be [catalog.]database");
        }

        String databaseName = "";
        String catalogName = "";
        if (nameParts.size() == 2) {
            catalogName = nameParts.get(0);
            databaseName = nameParts.get(1);
        }
        if (nameParts.size() == 1) {
            databaseName = nameParts.get(0);
        }

        return new CreateDatabaseCommand(
                ctx.IF() != null,
                new DbName(catalogName, databaseName),
                Maps.newHashMap(properties));
    }

    @Override
    public LogicalPlan visitCreateRepository(DorisParser.CreateRepositoryContext ctx) {
        String name = stripQuotes(ctx.name.getText());
        String location = stripQuotes(ctx.storageBackend().STRING_LITERAL().getText());

        String storageName = "";
        if (ctx.storageBackend().brokerName != null) {
            storageName = stripQuotes(ctx.storageBackend().brokerName.getText());
        }

        Map<String, String> properties = visitPropertyClause(ctx.storageBackend().properties);

        StorageBackend.StorageType storageType = null;
        if (ctx.storageBackend().BROKER() != null) {
            storageType = StorageBackend.StorageType.BROKER;
        } else if (ctx.storageBackend().S3() != null) {
            storageType = StorageBackend.StorageType.S3;
        } else if (ctx.storageBackend().HDFS() != null) {
            storageType = StorageBackend.StorageType.HDFS;
        } else if (ctx.storageBackend().LOCAL() != null) {
            storageType = StorageBackend.StorageType.LOCAL;
        }

        return new CreateRepositoryCommand(
                ctx.READ() != null,
                name,
                new StorageBackend(storageName, location, storageType, Maps.newHashMap(properties)));
    }

    @Override
    public LogicalPlan visitShowColumnHistogramStats(ShowColumnHistogramStatsContext ctx) {
        TableNameInfo tableNameInfo = new TableNameInfo(visitMultipartIdentifier(ctx.tableName));
        List<String> columnNames = visitIdentifierList(ctx.columnList);
        return new ShowColumnHistogramStatsCommand(tableNameInfo, columnNames);
    }

    @Override
    public LogicalPlan visitDescribeTableAll(DorisParser.DescribeTableAllContext ctx) {
        TableNameInfo tableName = new TableNameInfo(visitMultipartIdentifier(ctx.multipartIdentifier()));
        return new DescribeCommand(tableName, true, null);
    }

    @Override
    public String visitTableAlias(DorisParser.TableAliasContext ctx) {
        if (ctx.identifierList() != null) {
            throw new ParseException("Do not implemented", ctx);
        }
        return ctx.strictIdentifier() != null ? ctx.strictIdentifier().getText() : null;
    }

    @Override
    public LogicalPlan visitDescribeTableValuedFunction(DorisParser.DescribeTableValuedFunctionContext ctx) {
        String tvfName = ctx.tvfName.getText();
        String alias = visitTableAlias(ctx.tableAlias());
        Map<String, String> params = visitPropertyItemList(ctx.properties);

        TableValuedFunctionRef tableValuedFunctionRef = null;
        try {
            tableValuedFunctionRef = new TableValuedFunctionRef(tvfName, alias, params);
        } catch (org.apache.doris.common.AnalysisException e) {
            throw new AnalysisException(e.getDetailMessage());
        }
        return new DescribeCommand(tableValuedFunctionRef);
    }

    @Override
    public LogicalPlan visitCreateStage(DorisParser.CreateStageContext ctx) {
        String stageName = stripQuotes(ctx.name.getText());
        Map<String, String> properties = visitPropertyClause(ctx.properties);

        return new CreateStageCommand(
                ctx.IF() != null,
                stageName,
                properties
        );
    }

    @Override
    public LogicalPlan visitDropStage(DorisParser.DropStageContext ctx) {
        return new DropStageCommand(
                ctx.IF() != null,
                stripQuotes(ctx.name.getText()));
    }

    @Override
    public LogicalPlan visitAlterUser(DorisParser.AlterUserContext ctx) {
        boolean ifExist = ctx.EXISTS() != null;
        UserDesc userDesc = visitGrantUserIdentify(ctx.grantUserIdentify());
        PasswordOptions passwordOptions = visitPasswordOption(ctx.passwordOption());
        String comment = ctx.STRING_LITERAL() != null ? stripQuotes(ctx.STRING_LITERAL().getText()) : null;
        AlterUserInfo alterUserInfo = new AlterUserInfo(ifExist, userDesc, passwordOptions, comment);
        return new AlterUserCommand(alterUserInfo);
    }

    @Override
    public LogicalPlan visitShowTableStats(DorisParser.ShowTableStatsContext ctx) {
        if (ctx.tableId != null) {
            return new ShowTableStatsCommand(Long.parseLong(ctx.tableId.getText()));
        } else {
            TableNameInfo tableNameInfo = new TableNameInfo(visitMultipartIdentifier(ctx.tableName));

            PartitionNamesInfo partitionNamesInfo = null;
            if (ctx.partitionSpec() != null) {
                Pair<Boolean, List<String>> partitionSpec = visitPartitionSpec(ctx.partitionSpec());
                partitionNamesInfo = new PartitionNamesInfo(partitionSpec.first, partitionSpec.second);
            }

            List<String> columnNames = new ArrayList<>();
            if (ctx.columnList != null) {
                columnNames.addAll(visitIdentifierList(ctx.columnList));
            }
            return new ShowTableStatsCommand(tableNameInfo, columnNames, partitionNamesInfo);
        }
    }

    @Override
    public LogicalPlan visitDropStats(DorisParser.DropStatsContext ctx) {
        TableNameInfo tableNameInfo = new TableNameInfo(visitMultipartIdentifier(ctx.tableName));

        Set<String> columnNames = new HashSet<>();
        if (ctx.identifierList() != null) {
            columnNames.addAll(visitIdentifierList(ctx.identifierList()));
        }

        PartitionNamesInfo partitionNamesInfo = null;
        if (ctx.partitionSpec() != null) {
            Pair<Boolean, List<String>> partitionSpec = visitPartitionSpec(ctx.partitionSpec());
            partitionNamesInfo = new PartitionNamesInfo(partitionSpec.first, partitionSpec.second);
        }
        return new DropStatsCommand(tableNameInfo, columnNames, partitionNamesInfo);
    }

    @Override
    public LogicalPlan visitDropCachedStats(DorisParser.DropCachedStatsContext ctx) {
        TableNameInfo tableNameInfo = new TableNameInfo(visitMultipartIdentifier(ctx.tableName));
        return new DropCachedStatsCommand(tableNameInfo);
    }

    @Override
    public LogicalPlan visitDropExpiredStats(DorisParser.DropExpiredStatsContext ctx) {
        return new DropExpiredStatsCommand();
    }

    @Override
    public LogicalPlan visitShowClusters(ShowClustersContext ctx) {
        boolean showComputeGroups = ctx.COMPUTE() != null;
        return new ShowClustersCommand(showComputeGroups);
    }

    @Override
    public LogicalPlan visitAlterTableStats(DorisParser.AlterTableStatsContext ctx) {
        TableNameInfo tableNameInfo = new TableNameInfo(visitMultipartIdentifier(ctx.name));
        PartitionNamesInfo partitionNamesInfo = null;
        if (ctx.partitionSpec() != null) {
            Pair<Boolean, List<String>> partitionSpec = visitPartitionSpec(ctx.partitionSpec());
            partitionNamesInfo = new PartitionNamesInfo(partitionSpec.first, partitionSpec.second);
        }
        Map<String, String> properties = visitPropertyItemList(ctx.propertyItemList());
        return new AlterTableStatsCommand(tableNameInfo, partitionNamesInfo, properties);
    }

    @Override
    public LogicalPlan visitAlterColumnStats(DorisParser.AlterColumnStatsContext ctx) {
        TableNameInfo tableNameInfo = new TableNameInfo(visitMultipartIdentifier(ctx.name));
        PartitionNamesInfo partitionNamesInfo = null;
        if (ctx.partitionSpec() != null) {
            Pair<Boolean, List<String>> partitionSpec = visitPartitionSpec(ctx.partitionSpec());
            partitionNamesInfo = new PartitionNamesInfo(partitionSpec.first, partitionSpec.second);
        }

        String index = ctx.indexName != null ? ctx.indexName.getText() : null;
        String columnName = ctx.columnName.getText();
        Map<String, String> properties = visitPropertyItemList(ctx.propertyItemList());
        return new AlterColumnStatsCommand(tableNameInfo,
                partitionNamesInfo,
                index,
                columnName,
                properties);
    }

    @Override
    public List<Expression> visitColMappingList(DorisParser.ColMappingListContext ctx) {
        List<Expression> columnMappingList;
        if (ctx != null) {
            columnMappingList = new ArrayList<>();
            for (DorisParser.MappingExprContext mappingExpr : ctx.mappingSet) {
                UnboundSlot left = new UnboundSlot(mappingExpr.mappingCol.getText());
                Expression right = getExpression(mappingExpr.expression());
                EqualTo equalTo = new EqualTo(left, right);
                columnMappingList.add(equalTo);
            }
        } else {
            columnMappingList = ImmutableList.of();
        }
        return columnMappingList;
    }

    @Override
    public MysqlDataDescription visitMysqlDataDesc(DorisParser.MysqlDataDescContext ctx) {
        List<String> filePaths = new ArrayList<>();
        filePaths.add(stripQuotes(ctx.filePath.getText()));
        TableNameInfo tableNameInfo = new TableNameInfo(visitMultipartIdentifier(ctx.multipartIdentifier()));
        boolean isClientLocal = ctx.LOCAL() != null;

        List<String> partitions;
        if (ctx.partition != null) {
            partitions = visitIdentifierList(ctx.partition);
        } else {
            partitions = ImmutableList.of();
        }
        PartitionNamesInfo partitionNamesInfo = new PartitionNamesInfo(false, partitions);

        Optional<String> columnSeparator;
        Optional<String> lineDelimiter;
        if (ctx.comma != null) {
            columnSeparator = Optional.of(ctx.comma.getText().substring(1, ctx.comma.getText().length() - 1));
        } else {
            columnSeparator = Optional.empty();
        }
        if (ctx.separator != null) {
            lineDelimiter = Optional.of(ctx.separator.getText().substring(1, ctx.separator.getText().length() - 1));
        } else {
            lineDelimiter = Optional.empty();
        }

        int skipLines = ctx.skipLines() == null
                ? 0 : Integer.parseInt(ctx.skipLines().INTEGER_VALUE().getText());

        List<String> columns;
        if (ctx.columns != null) {
            columns = visitIdentifierList(ctx.columns);
        } else {
            columns = ImmutableList.of();
        }

        List<Expression> columnMappingList = visitColMappingList(ctx.colMappingList());

        Map<String, String> properties;
        if (ctx.propertyClause() != null) {
            properties = visitPropertyItemList(ctx.propertyClause().propertyItemList());
        } else {
            properties = ImmutableMap.of();
        }

        MysqlDataDescription mysqlDataDescription = new MysqlDataDescription(
                filePaths,
                tableNameInfo,
                isClientLocal,
                partitionNamesInfo,
                columnSeparator,
                lineDelimiter,
                skipLines,
                columns,
                columnMappingList,
                properties
        );
        return mysqlDataDescription;
    }

    @Override
    public LogicalPlan visitMysqlLoad(DorisParser.MysqlLoadContext ctx) {
        MysqlDataDescription mysqlDataDescription = visitMysqlDataDesc(ctx.mysqlDataDesc());
        Map<String, String> properties;
        if (ctx.properties != null) {
            properties = new HashMap<>();
            properties.putAll(visitPropertyItemList(ctx.properties));
        } else {
            properties = ImmutableMap.of();
        }
        String comment = visitCommentSpec(ctx.commentSpec());
        return new MysqlLoadCommand(mysqlDataDescription, properties, comment);
    }

    @Override
    public LogicalPlan visitCancelAlterTable(DorisParser.CancelAlterTableContext ctx) {
        TableNameInfo tableNameInfo = new TableNameInfo(visitMultipartIdentifier(ctx.tableName));

        CancelAlterTableCommand.AlterType alterType;
        if (ctx.ROLLUP() != null) {
            alterType = CancelAlterTableCommand.AlterType.ROLLUP;
        } else if (ctx.MATERIALIZED() != null && ctx.VIEW() != null) {
            alterType = CancelAlterTableCommand.AlterType.MV;
        } else if (ctx.COLUMN() != null) {
            alterType = CancelAlterTableCommand.AlterType.COLUMN;
        } else {
            throw new AnalysisException("invalid AlterOpType, it must be one of 'ROLLUP',"
                    + "'MATERIALIZED VIEW' or 'COLUMN'");
        }

        List<Long> jobIs = new ArrayList<>();
        for (Token token : ctx.jobIds) {
            jobIs.add(Long.parseLong(token.getText()));
        }
        return new CancelAlterTableCommand(tableNameInfo, alterType, jobIs);
    }

    @Override
    public LogicalPlan visitAdminSetReplicaStatus(DorisParser.AdminSetReplicaStatusContext ctx) {
        Map<String, String> properties = visitPropertyItemList(ctx.propertyItemList());
        return new AdminSetReplicaStatusCommand(properties);
    }

    @Override
    public LogicalPlan visitAdminRepairTable(DorisParser.AdminRepairTableContext ctx) {
        TableRefInfo tableRefInfo = visitBaseTableRefContext(ctx.baseTableRef());
        return new AdminRepairTableCommand(tableRefInfo);
    }

    @Override
    public LogicalPlan visitAdminCancelRepairTable(DorisParser.AdminCancelRepairTableContext ctx) {
        TableRefInfo tableRefInfo = visitBaseTableRefContext(ctx.baseTableRef());
        return new AdminCancelRepairTableCommand(tableRefInfo);
    }

    @Override
    public LogicalPlan visitAdminCopyTablet(DorisParser.AdminCopyTabletContext ctx) {
        long tabletId = Long.parseLong(ctx.tabletId.getText());
        Map<String, String> properties;
        if (ctx.propertyClause() != null) {
            properties = visitPropertyClause(ctx.propertyClause());
        } else {
            properties = ImmutableMap.of();
        }
        return new AdminCopyTabletCommand(tabletId, properties);
    }

    @Override
    public LogicalPlan visitShowCreateRoutineLoad(DorisParser.ShowCreateRoutineLoadContext ctx) {
        boolean isAll = ctx.ALL() != null;
        List<String> labelParts = visitMultipartIdentifier(ctx.label);
        String jobName;
        String dbName = null;
        if (labelParts.size() == 1) {
            jobName = labelParts.get(0);
        } else if (labelParts.size() == 2) {
            dbName = labelParts.get(0);
            jobName = labelParts.get(1);
        } else {
            throw new ParseException("only support [<db>.]<job_name>", ctx.label);
        }
        LabelNameInfo labelNameInfo = new LabelNameInfo(dbName, jobName);
        return new ShowCreateRoutineLoadCommand(labelNameInfo, isAll);
    }

    @Override
    public LogicalPlan visitPauseRoutineLoad(DorisParser.PauseRoutineLoadContext ctx) {
        List<String> labelParts = visitMultipartIdentifier(ctx.label);
        String jobName;
        String dbName = null;
        if (labelParts.size() == 1) {
            jobName = labelParts.get(0);
        } else if (labelParts.size() == 2) {
            dbName = labelParts.get(0);
            jobName = labelParts.get(1);
        } else {
            throw new ParseException("only support [<db>.]<job_name>", ctx.label);
        }
        LabelNameInfo labelNameInfo = new LabelNameInfo(dbName, jobName);
        return new PauseRoutineLoadCommand(labelNameInfo);
    }

    @Override
    public LogicalPlan visitPauseAllRoutineLoad(DorisParser.PauseAllRoutineLoadContext ctx) {
        return new PauseRoutineLoadCommand();
    }

    @Override
    public LogicalPlan visitResumeRoutineLoad(DorisParser.ResumeRoutineLoadContext ctx) {
        List<String> labelParts = visitMultipartIdentifier(ctx.label);
        String jobName;
        String dbName = null;
        if (labelParts.size() == 1) {
            jobName = labelParts.get(0);
        } else if (labelParts.size() == 2) {
            dbName = labelParts.get(0);
            jobName = labelParts.get(1);
        } else {
            throw new ParseException("only support [<db>.]<job_name>", ctx.label);
        }
        LabelNameInfo labelNameInfo = new LabelNameInfo(dbName, jobName);
        return new ResumeRoutineLoadCommand(labelNameInfo);
    }

    @Override
    public LogicalPlan visitResumeAllRoutineLoad(DorisParser.ResumeAllRoutineLoadContext ctx) {
        return new ResumeRoutineLoadCommand();
    }

    @Override
    public LogicalPlan visitStopRoutineLoad(DorisParser.StopRoutineLoadContext ctx) {
        List<String> labelParts = visitMultipartIdentifier(ctx.label);
        String jobName;
        String dbName = null;
        if (labelParts.size() == 1) {
            jobName = labelParts.get(0);
        } else if (labelParts.size() == 2) {
            dbName = labelParts.get(0);
            jobName = labelParts.get(1);
        } else {
            throw new ParseException("only support [<db>.]<job_name>", ctx.label);
        }
        LabelNameInfo labelNameInfo = new LabelNameInfo(dbName, jobName);
        return new StopRoutineLoadCommand(labelNameInfo);
    }

    public LogicalPlan visitCleanAllQueryStats(DorisParser.CleanAllQueryStatsContext ctx) {
        return new CleanQueryStatsCommand();
    }

    @Override
    public LogicalPlan visitCleanQueryStats(DorisParser.CleanQueryStatsContext ctx) {
        if (ctx.database != null) {
            return new CleanQueryStatsCommand(ctx.identifier().getText());
        } else {
            TableNameInfo tableNameInfo = new TableNameInfo(visitMultipartIdentifier(ctx.table));
            return new CleanQueryStatsCommand(tableNameInfo);
        }
    }

    @Override
    public LogicalPlan visitDropResource(DorisParser.DropResourceContext ctx) {
        boolean ifExist = ctx.EXISTS() != null;
        String resouceName = visitIdentifierOrText(ctx.identifierOrText());
        return new DropResourceCommand(ifExist, resouceName);
    }

    @Override
    public LogicalPlan visitDropRowPolicy(DorisParser.DropRowPolicyContext ctx) {
        boolean ifExist = ctx.EXISTS() != null;
        String policyName = ctx.policyName.getText();
        TableNameInfo tableNameInfo = new TableNameInfo(visitMultipartIdentifier(ctx.tableName));
        UserIdentity userIdentity = ctx.userIdentify() != null ? visitUserIdentify(ctx.userIdentify()) : null;
        String roleName = ctx.roleName != null ? ctx.roleName.getText() : null;
        return new DropRowPolicyCommand(ifExist, policyName, tableNameInfo, userIdentity, roleName);
    }

    @Override
    public LogicalPlan visitTransactionBegin(DorisParser.TransactionBeginContext ctx) {
        if (ctx.LABEL() != null) {
            return new TransactionBeginCommand(ctx.identifier().getText());
        } else {
            return new TransactionBeginCommand();
        }
    }

    @Override
    public LogicalPlan visitTranscationCommit(DorisParser.TranscationCommitContext ctx) {
        return new TransactionCommitCommand();
    }

    @Override
    public LogicalPlan visitTransactionRollback(DorisParser.TransactionRollbackContext ctx) {
        return new TransactionRollbackCommand();
    }

    @Override
    public LogicalPlan visitGrantTablePrivilege(DorisParser.GrantTablePrivilegeContext ctx) {
        List<AccessPrivilegeWithCols> accessPrivilegeWithCols = visitPrivilegeList(ctx.privilegeList());

        List<String> parts = visitMultipartIdentifierOrAsterisk(ctx.multipartIdentifierOrAsterisk());
        int size = parts.size();

        if (size < 1) {
            throw new AnalysisException("grant table privilege statement missing parameters");
        }

        TablePattern tablePattern = null;
        if (size == 1) {
            String db = parts.get(size - 1);
            tablePattern = new TablePattern(db, "");
        }

        if (size == 2) {
            String db = parts.get(size - 2);
            String tbl = parts.get(size - 1);
            tablePattern = new TablePattern(db, tbl);
        }

        if (size == 3) {
            String ctl = parts.get(size - 3);
            String db = parts.get(size - 2);
            String tbl = parts.get(size - 1);
            tablePattern = new TablePattern(ctl, db, tbl);
        }

        Optional<UserIdentity> userIdentity = Optional.empty();
        Optional<String> role = Optional.empty();

        if (ctx.ROLE() != null) {
            role = Optional.of(visitIdentifierOrText(ctx.identifierOrText()));
        } else if (ctx.userIdentify() != null) {
            userIdentity = Optional.of(visitUserIdentify(ctx.userIdentify()));
        }

        return new GrantTablePrivilegeCommand(
                accessPrivilegeWithCols,
                tablePattern,
                userIdentity,
                role);
    }

    @Override
    public LogicalPlan visitGrantResourcePrivilege(DorisParser.GrantResourcePrivilegeContext ctx) {
        List<AccessPrivilegeWithCols> accessPrivilegeWithCols = visitPrivilegeList(ctx.privilegeList());
        String name = visitIdentifierOrTextOrAsterisk(ctx.identifierOrTextOrAsterisk());

        Optional<ResourcePattern> resourcePattern = Optional.empty();
        Optional<WorkloadGroupPattern> workloadGroupPattern = Optional.empty();

        if (ctx.CLUSTER() != null || ctx.COMPUTE() != null) {
            resourcePattern = Optional.of(new ResourcePattern(name, ResourceTypeEnum.CLUSTER));
        } else if (ctx.STAGE() != null) {
            resourcePattern = Optional.of(new ResourcePattern(name, ResourceTypeEnum.STAGE));
        } else if (ctx.STORAGE() != null) {
            resourcePattern = Optional.of(new ResourcePattern(name, ResourceTypeEnum.STORAGE_VAULT));
        } else if (ctx.RESOURCE() != null) {
            resourcePattern = Optional.of(new ResourcePattern(name, ResourceTypeEnum.GENERAL));
        } else if (ctx.WORKLOAD() != null) {
            workloadGroupPattern = Optional.of(new WorkloadGroupPattern(name));
        }

        Optional<UserIdentity> userIdentity = Optional.empty();
        Optional<String> role = Optional.empty();

        if (ctx.ROLE() != null) {
            role = Optional.of(visitIdentifierOrText(ctx.identifierOrText()));
        } else if (ctx.userIdentify() != null) {
            userIdentity = Optional.of(visitUserIdentify(ctx.userIdentify()));
        }

        return new GrantResourcePrivilegeCommand(
                accessPrivilegeWithCols,
                resourcePattern,
                workloadGroupPattern,
                role,
                userIdentity);
    }

    @Override
    public LogicalPlan visitGrantRole(DorisParser.GrantRoleContext ctx) {
        UserIdentity userIdentity = visitUserIdentify(ctx.userIdentify());
        List<String> roles = ctx.roles.stream()
                .map(this::visitIdentifierOrText)
                .collect(ImmutableList.toImmutableList());

        if (roles.size() == 0) {
            throw new AnalysisException("grant role statement lack of role");
        }

        return new GrantRoleCommand(userIdentity, roles);
    }

    @Override
    public AccessPrivilegeWithCols visitPrivilege(DorisParser.PrivilegeContext ctx) {
        AccessPrivilegeWithCols accessPrivilegeWithCols;
        if (ctx.ALL() != null) {
            AccessPrivilege accessPrivilege = AccessPrivilege.ALL;
            accessPrivilegeWithCols = new AccessPrivilegeWithCols(accessPrivilege, ImmutableList.of());
        } else {
            String privilegeName = stripQuotes(ctx.name.getText());
            AccessPrivilege accessPrivilege = AccessPrivilege.fromName(privilegeName);
            List<String> columns = ctx.identifierList() == null
                    ? ImmutableList.of() : visitIdentifierList(ctx.identifierList());
            accessPrivilegeWithCols = new AccessPrivilegeWithCols(accessPrivilege, columns);
        }

        return accessPrivilegeWithCols;
    }

    @Override
    public List<AccessPrivilegeWithCols> visitPrivilegeList(DorisParser.PrivilegeListContext ctx) {
        return ctx.privilege().stream()
                .map(this::visitPrivilege)
                .collect(ImmutableList.toImmutableList());
    }

    @Override
    public LogicalPlan visitRevokeRole(DorisParser.RevokeRoleContext ctx) {
        UserIdentity userIdentity = visitUserIdentify(ctx.userIdentify());
        List<String> roles = ctx.roles.stream()
                .map(this::visitIdentifierOrText)
                .collect(ImmutableList.toImmutableList());

        return new RevokeRoleCommand(userIdentity, roles);
    }

    @Override
    public LogicalPlan visitRevokeResourcePrivilege(DorisParser.RevokeResourcePrivilegeContext ctx) {
        List<AccessPrivilegeWithCols> accessPrivilegeWithCols = visitPrivilegeList(ctx.privilegeList());

        String name = visitIdentifierOrTextOrAsterisk(ctx.identifierOrTextOrAsterisk());
        Optional<ResourcePattern> resourcePattern = Optional.empty();
        Optional<WorkloadGroupPattern> workloadGroupPattern = Optional.empty();

        if (ctx.CLUSTER() != null || ctx.COMPUTE() != null) {
            resourcePattern = Optional.of(new ResourcePattern(name, ResourceTypeEnum.CLUSTER));
        } else if (ctx.STAGE() != null) {
            resourcePattern = Optional.of(new ResourcePattern(name, ResourceTypeEnum.STAGE));
        } else if (ctx.STORAGE() != null) {
            resourcePattern = Optional.of(new ResourcePattern(name, ResourceTypeEnum.STORAGE_VAULT));
        } else if (ctx.RESOURCE() != null) {
            resourcePattern = Optional.of(new ResourcePattern(name, ResourceTypeEnum.GENERAL));
        } else if (ctx.WORKLOAD() != null) {
            workloadGroupPattern = Optional.of(new WorkloadGroupPattern(name));
        }

        Optional<UserIdentity> userIdentity = Optional.empty();
        Optional<String> role = Optional.empty();

        if (ctx.ROLE() != null) {
            role = Optional.of(visitIdentifierOrText(ctx.identifierOrText()));
        } else if (ctx.userIdentify() != null) {
            userIdentity = Optional.of(visitUserIdentify(ctx.userIdentify()));
        }

        return new RevokeResourcePrivilegeCommand(
                accessPrivilegeWithCols,
                resourcePattern,
                workloadGroupPattern,
                role,
                userIdentity);
    }

    @Override
    public LogicalPlan visitRevokeTablePrivilege(DorisParser.RevokeTablePrivilegeContext ctx) {
        List<AccessPrivilegeWithCols> accessPrivilegeWithCols = visitPrivilegeList(ctx.privilegeList());

        List<String> parts = visitMultipartIdentifierOrAsterisk(ctx.multipartIdentifierOrAsterisk());
        int size = parts.size();

        TablePattern tablePattern = null;
        if (size == 1) {
            String db = parts.get(size - 1);
            tablePattern = new TablePattern(db, "");
        }

        if (size == 2) {
            String db = parts.get(size - 2);
            String tbl = parts.get(size - 1);
            tablePattern = new TablePattern(db, tbl);
        }

        if (size == 3) {
            String ctl = parts.get(size - 3);
            String db = parts.get(size - 2);
            String tbl = parts.get(size - 1);
            tablePattern = new TablePattern(ctl, db, tbl);
        }

        Optional<UserIdentity> userIdentity = Optional.empty();
        Optional<String> role = Optional.empty();

        if (ctx.ROLE() != null) {
            role = Optional.of(visitIdentifierOrText(ctx.identifierOrText()));
        } else if (ctx.userIdentify() != null) {
            userIdentity = Optional.of(visitUserIdentify(ctx.userIdentify()));
        }

        return new RevokeTablePrivilegeCommand(
                accessPrivilegeWithCols,
                tablePattern,
                userIdentity,
                role);
    }

    public LogicalPlan visitDropAnalyzeJob(DorisParser.DropAnalyzeJobContext ctx) {
        long jobId = Long.parseLong(ctx.INTEGER_VALUE().getText());
        return new DropAnalyzeJobCommand(jobId);
    }

    @Override
    public LogicalPlan visitKillAnalyzeJob(DorisParser.KillAnalyzeJobContext ctx) {
        long jobId = Long.parseLong(ctx.jobId.getText());
        return new KillAnalyzeJobCommand(jobId);
    }

    @Override
    public LogicalPlan visitAlterRoutineLoad(DorisParser.AlterRoutineLoadContext ctx) {
        String dbName = null;
        String jobName;
        List<String> nameParts = visitMultipartIdentifier(ctx.name);
        if (nameParts.size() == 1) {
            jobName = nameParts.get(0);
        } else if (nameParts.size() == 2) {
            dbName = nameParts.get(0);
            jobName = nameParts.get(1);
        } else {
            throw new ParseException("only support [<db>.]<job_name>", ctx.name);
        }
        LabelNameInfo labelNameInfo = new LabelNameInfo(dbName, jobName);

        Map<String, String> properties = new HashMap<>();
        if (ctx.properties != null) {
            properties.putAll(visitPropertyClause(ctx.properties));
        }

        Map<String, String> dataSourceMapProperties = new HashMap<>();
        if (ctx.propertyItemList() != null) {
            dataSourceMapProperties.putAll(visitPropertyItemList(ctx.propertyItemList()));
        }

        Map<String, LoadProperty> loadPropertyMap = new HashMap<>();
        if (ctx.loadProperty() != null) {
            for (DorisParser.LoadPropertyContext oneLoadPropertyContext : ctx.loadProperty()) {
                LoadProperty loadProperty = visitLoadProperty(oneLoadPropertyContext);
                if (loadProperty == null) {
                    throw new AnalysisException("invalid clause of routine load");
                }
                if (loadPropertyMap.get(loadProperty.getClass().getName()) != null) {
                    throw new AnalysisException("repeat setting of clause load property: "
                            + loadProperty.getClass().getName());
                }
                loadPropertyMap.put(loadProperty.getClass().getName(), loadProperty);
            }
        }

        return new AlterRoutineLoadCommand(labelNameInfo, loadPropertyMap, properties, dataSourceMapProperties);
    }

    @Override
    public LogicalPlan visitAlterColocateGroup(DorisParser.AlterColocateGroupContext ctx) {
        String dbName = null;
        String group;
        List<String> nameParts = visitMultipartIdentifier(ctx.name);
        if (nameParts.size() == 1) {
            group = nameParts.get(0);
        } else if (nameParts.size() == 2) {
            dbName = nameParts.get(0);
            group = nameParts.get(1);
        } else {
            throw new ParseException("only support [<db>.]<group_name>", ctx.name);
        }
        ColocateGroupName groupName = new ColocateGroupName(dbName, group);
        Map<String, String> properties = new HashMap<>();
        properties.putAll(visitPropertyItemList(ctx.propertyItemList()));
        return new AlterColocateGroupCommand(groupName, properties);
    }

    @Override
    public LogicalPlan visitCancelBackup(DorisParser.CancelBackupContext ctx) {
        String databaseName = ctx.database != null ? ctx.database.getText() : null;
        boolean isRestore = false;
        return new CancelBackupCommand(databaseName, isRestore);
    }

    @Override
    public LogicalPlan visitCancelRestore(DorisParser.CancelRestoreContext ctx) {
        String databaseName = ctx.database != null ? ctx.database.getText() : null;
        boolean isRestore = true;
        return new CancelBackupCommand(databaseName, isRestore);
    }

    @Override
    public LogicalPlan visitCancelBuildIndex(DorisParser.CancelBuildIndexContext ctx) {
        TableNameInfo tableNameInfo = new TableNameInfo(visitMultipartIdentifier(ctx.tableName));
        List<Long> jobIs = new ArrayList<>();
        for (Token token : ctx.jobIds) {
            jobIs.add(Long.parseLong(token.getText()));
        }
        return new CancelBuildIndexCommand(tableNameInfo, jobIs);
    }

    @Override
    public PasswordOptions visitPasswordOption(DorisParser.PasswordOptionContext ctx) {
        int historyPolicy = PasswordOptions.UNSET;
        long expirePolicySecond = PasswordOptions.UNSET;
        int reusePolicy = PasswordOptions.UNSET;
        int loginAttempts = PasswordOptions.UNSET;
        long passwordLockSecond = PasswordOptions.UNSET;
        int accountUnlocked = PasswordOptions.UNSET;

        if (ctx.historyDefault != null) {
            historyPolicy = -1;
        } else if (ctx.historyValue != null) {
            historyPolicy = Integer.parseInt(ctx.historyValue.getText());
        }

        if (ctx.expireDefault != null) {
            expirePolicySecond = -1;
        } else if (ctx.expireNever != null) {
            expirePolicySecond = 0;
        } else if (ctx.expireValue != null) {
            long value = Long.parseLong(ctx.expireValue.getText());
            expirePolicySecond = ParserUtils.getSecond(value, ctx.expireTimeUnit.getText());
        }

        if (ctx.reuseValue != null) {
            reusePolicy = Integer.parseInt(ctx.reuseValue.getText());
        }

        if (ctx.attemptsValue != null) {
            loginAttempts = Integer.parseInt(ctx.attemptsValue.getText());
        }

        if (ctx.lockUnbounded != null) {
            passwordLockSecond = -1;
        } else if (ctx.lockValue != null) {
            long value = Long.parseLong(ctx.lockValue.getText());
            passwordLockSecond = ParserUtils.getSecond(value, ctx.lockTimeUint.getText());
        }

        if (ctx.ACCOUNT_LOCK() != null) {
            accountUnlocked = -1;
        } else if (ctx.ACCOUNT_UNLOCK() != null) {
            accountUnlocked = 1;
        }

        return new PasswordOptions(expirePolicySecond,
                historyPolicy,
                reusePolicy,
                loginAttempts,
                passwordLockSecond,
                accountUnlocked);
    }

    @Override
    public LogicalPlan visitCreateUser(CreateUserContext ctx) {
        String comment = visitCommentSpec(ctx.commentSpec());
        PasswordOptions passwordOptions = visitPasswordOption(ctx.passwordOption());
        UserDesc userDesc = (UserDesc) ctx.grantUserIdentify().accept(this);

        String role = null;
        if (ctx.role != null) {
            role = stripQuotes(ctx.role.getText());
        }

        CreateUserInfo userInfo = new CreateUserInfo(ctx.IF() != null,
                userDesc,
                role,
                passwordOptions,
                comment);

        return new CreateUserCommand(userInfo);
    }

    @Override
    public UserDesc visitGrantUserIdentify(DorisParser.GrantUserIdentifyContext ctx) {
        UserIdentity userIdentity = visitUserIdentify(ctx.userIdentify());
        if (ctx.IDENTIFIED() == null) {
            return new UserDesc(userIdentity);
        }
        String password = stripQuotes(ctx.STRING_LITERAL().getText());
        boolean isPlain = ctx.PASSWORD() == null;
        return new UserDesc(userIdentity, new PassVar(password, isPlain));
    }

    @Override
    public LogicalPlan visitWarmUpCluster(DorisParser.WarmUpClusterContext ctx) {
        String dstCluster = ctx.destination.getText();
        String srcCluster = null;
        if (ctx.source != null) {
            srcCluster = ctx.source.getText();
        }
        boolean isWarmUpWithTable = false;
        List<WarmUpItem> warmUpItems = new ArrayList<>();
        if (ctx.warmUpItem() != null && !ctx.warmUpItem().isEmpty()) {
            for (DorisParser.WarmUpItemContext warmUpItemContext : ctx.warmUpItem()) {
                TableNameInfo tableNameInfo = new TableNameInfo(visitMultipartIdentifier(warmUpItemContext.tableName));
                String partitionName = warmUpItemContext.partitionName != null
                        ? warmUpItemContext.partitionName.getText()
                        : "";
                WarmUpItem warmUpItem = new WarmUpItem(tableNameInfo, partitionName);
                warmUpItems.add(warmUpItem);
            }
            isWarmUpWithTable = true;
        }
        boolean isForce = false;
        if (ctx.FORCE() != null) {
            isForce = true;
        }
        ImmutableMap<String, String> properties = ImmutableMap.copyOf(visitPropertyClause(ctx.properties));
        return new WarmUpClusterCommand(warmUpItems, srcCluster, dstCluster, isForce, isWarmUpWithTable, properties);
    }

    @Override
    public LogicalPlan visitCreateResource(DorisParser.CreateResourceContext ctx) {
        String resourceName = visitIdentifierOrText(ctx.name);
        ImmutableMap<String, String> properties = ImmutableMap.copyOf(visitPropertyClause(ctx.properties));

        CreateResourceInfo createResourceInfo = new CreateResourceInfo(
                ctx.EXTERNAL() != null,
                ctx.IF() != null,
                resourceName,
                properties
        );

        return new CreateResourceCommand(createResourceInfo);
    }

    @Override
    public LogicalPlan visitCreateDictionary(CreateDictionaryContext ctx) {
        List<String> nameParts = visitMultipartIdentifier(ctx.name);
        String dbName = null;
        String dictName = null;
        if (nameParts.size() == 1) {
            dictName = nameParts.get(0);
        } else if (nameParts.size() == 2) {
            dbName = nameParts.get(0);
            dictName = nameParts.get(1);
        } else {
            throw new AnalysisException("Dictionary name should be [db.]dictionary_name");
        }

        // the source tableName parts
        String sCatalogName = null;
        String sDbName = null;
        String sTableName = null;
        List<String> sourceNames = visitMultipartIdentifier(ctx.source);
        if (sourceNames.size() == 1) {
            sTableName = sourceNames.get(0);
        } else if (sourceNames.size() == 2) {
            sDbName = sourceNames.get(0);
            sTableName = sourceNames.get(1);
        } else if (sourceNames.size() == 3) {
            sCatalogName = sourceNames.get(0);
            sDbName = sourceNames.get(1);
            sTableName = sourceNames.get(2);
        } else {
            throw new AnalysisException("nameParts in create table should be [ctl.][db.]tbl");
        }

        List<DictionaryColumnDefinition> columns = new ArrayList<>();
        for (DictionaryColumnDefContext colCtx : ctx.dictionaryColumnDefs().dictionaryColumnDef()) {
            String colName = colCtx.colName.getText();
            boolean isKey = colCtx.columnType.getType() == DorisParser.KEY;
            columns.add(new DictionaryColumnDefinition(colName, isKey));
        }

        Map<String, String> properties = ctx.properties != null ? Maps.newHashMap(visitPropertyClause(ctx.properties))
                : Maps.newHashMap();

        LayoutType layoutType;
        try {
            layoutType = LayoutType.of(ctx.layoutType.getText());
        } catch (IllegalArgumentException e) {
            throw new AnalysisException(
                    "Unknown layout type: " + ctx.layoutType.getText() + ". must be IP_TRIE or HASH_MAP");
        }

        return new CreateDictionaryCommand(ctx.EXISTS() != null, // if not exists
                dbName, dictName, sCatalogName, sDbName, sTableName, columns, properties, layoutType);
    }

    @Override
    public LogicalPlan visitCreateStorageVault(DorisParser.CreateStorageVaultContext ctx) {
        String vaultName = visitIdentifierOrText(ctx.identifierOrText());
        Map<String, String> properties = visitPropertyClause(ctx.properties);

        return new CreateStorageVaultCommand(
                ctx.IF() != null,
                vaultName,
                properties);
    }

    @Override
    public LogicalPlan visitDropDictionary(DropDictionaryContext ctx) {
        List<String> nameParts = visitMultipartIdentifier(ctx.name);
        if (nameParts.size() == 0 || nameParts.size() > 2) {
            throw new AnalysisException("Dictionary name should be [db.]dictionary_name");
        }
        String dbName;
        String dictName;
        if (nameParts.size() == 1) { // only dict name
            dbName = null;
            dictName = nameParts.get(0);
        } else {
            dbName = nameParts.get(0);
            dictName = nameParts.get(1);
        }

        return new DropDictionaryCommand(dbName, dictName, ctx.EXISTS() != null);
    }

    @Override
    public Plan visitShowDictionaries(ShowDictionariesContext ctx) {
        String wild = null;
        if (ctx.wildWhere() != null) {
            if (ctx.wildWhere().LIKE() != null) {
                // if like, it's a pattern
                wild = stripQuotes(ctx.wildWhere().STRING_LITERAL().getText());
            } else if (ctx.wildWhere().WHERE() != null) {
                // if where, it's a expression
                wild = ctx.wildWhere().expression().getText();
            }
        }
        try {
            return new ShowDictionariesCommand(wild);
        } catch (org.apache.doris.common.AnalysisException e) {
            throw new ParseException(e.getMessage());
        }
    }

    @Override
    public LogicalPlan visitShowTransaction(DorisParser.ShowTransactionContext ctx) {
        String dbName = null;
        if (ctx.database != null) {
            List<String> parts = visitMultipartIdentifier(ctx.database);
            if (parts.size() == 1) {
                dbName = parts.get(0);
            } else if (parts.size() == 2) {
                dbName = parts.get(0) + "." + parts.get(1);
            } else {
                throw new ParseException("only support [<catalog_name>.]<db_name>", ctx.database);
            }
        }

        Expression wildWhere = null;
        if (ctx.wildWhere() != null) {
            wildWhere = getWildWhere(ctx.wildWhere());
        }
        return new ShowTransactionCommand(dbName, wildWhere);
    }

    public LogicalPlan visitRefreshLdap(DorisParser.RefreshLdapContext ctx) {
        String user = "";
        if (ctx.user != null) {
            user = visitIdentifierOrText(ctx.user);
        }
        return new RefreshLdapCommand(ctx.ALL() != null, user);
    }

    @Override
    public Plan visitDescribeDictionary(DescribeDictionaryContext ctx) {
        List<String> nameParts = visitMultipartIdentifier(ctx.multipartIdentifier());
        if (nameParts.size() == 0 || nameParts.size() > 2) {
            throw new AnalysisException("Dictionary name should be [db.]dictionary_name");
        }
        String dbName;
        String dictName;
        if (nameParts.size() == 1) { // only dict name
            dbName = null;
            dictName = nameParts.get(0);
        } else {
            dbName = nameParts.get(0);
            dictName = nameParts.get(1);
        }

        return new ExplainDictionaryCommand(dbName, dictName);
    }

    @Override
    public LogicalPlan visitRefreshDictionary(RefreshDictionaryContext ctx) {
        List<String> nameParts = visitMultipartIdentifier(ctx.name);
        if (nameParts.size() == 0 || nameParts.size() > 2) {
            throw new AnalysisException("Dictionary name should be [db.]dictionary_name");
        }
        String dbName;
        String dictName;
        if (nameParts.size() == 1) { // only dict name
            dbName = null;
            dictName = nameParts.get(0);
        } else {
            dbName = nameParts.get(0);
            dictName = nameParts.get(1);
        }

        return new RefreshDictionaryCommand(dbName, dictName);
    }

    @Override
    public LogicalPlan visitBackup(DorisParser.BackupContext ctx) {
        List<String> labelParts = visitMultipartIdentifier(ctx.label);
        String snapshotName;
        String dbName = null;
        if (labelParts.size() == 1) {
            snapshotName = labelParts.get(0);
        } else if (labelParts.size() == 2) {
            dbName = labelParts.get(0);
            snapshotName = labelParts.get(1);
        } else {
            throw new ParseException("only support [<db_name>.]<snapshot_name>", ctx.label);
        }
        LabelNameInfo labelNameInfo = new LabelNameInfo(dbName, snapshotName);
        String repoName = ctx.repo.getText();
        boolean isExclude = ctx.EXCLUDE() != null;
        List<TableRefInfo> tableRefInfos = new ArrayList<>();
        for (BaseTableRefContext baseTableRefContext : ctx.baseTableRef()) {
            tableRefInfos.add(visitBaseTableRefContext(baseTableRefContext));
        }
        Map<String, String> properties = visitPropertyClause(ctx.properties);
        return new BackupCommand(labelNameInfo, repoName, tableRefInfos, properties, isExclude);
    }

    @Override
    public LogicalPlan visitRestore(DorisParser.RestoreContext ctx) {
        List<String> labelParts = visitMultipartIdentifier(ctx.label);
        String snapshotName;
        String dbName = null;
        if (labelParts.size() == 1) {
            snapshotName = labelParts.get(0);
        } else if (labelParts.size() == 2) {
            dbName = labelParts.get(0);
            snapshotName = labelParts.get(1);
        } else {
            throw new ParseException("only support [<db_name>.]<snapshot_name>", ctx.label);
        }
        LabelNameInfo labelNameInfo = new LabelNameInfo(dbName, snapshotName);
        String repoName = ctx.repo.getText();
        boolean isExclude = ctx.EXCLUDE() != null;
        List<TableRefInfo> tableRefInfos = new ArrayList<>();
        for (BaseTableRefContext baseTableRefContext : ctx.baseTableRef()) {
            tableRefInfos.add(visitBaseTableRefContext(baseTableRefContext));
        }
        Map<String, String> properties = visitPropertyClause(ctx.properties);
        return new RestoreCommand(labelNameInfo, repoName, tableRefInfos, properties, isExclude);
    }

    @Override
    public LogicalPlan visitShowRoutineLoad(DorisParser.ShowRoutineLoadContext ctx) {
        LabelNameInfo labelNameInfo = null;
        if (ctx.label != null) {
            List<String> labelParts = visitMultipartIdentifier(ctx.label);
            String jobName;
            String dbName = null;
            if (labelParts.size() == 1) {
                jobName = labelParts.get(0);
            } else if (labelParts.size() == 2) {
                dbName = labelParts.get(0);
                jobName = labelParts.get(1);
            } else {
                throw new ParseException("only support [<db>.]<job_name>", ctx.label);
            }
            labelNameInfo = new LabelNameInfo(dbName, jobName);
        }

        String pattern = null;
        if (ctx.LIKE() != null) {
            pattern = stripQuotes(ctx.STRING_LITERAL().getText());
        }

        boolean isAll = ctx.ALL() != null;
        return new ShowRoutineLoadCommand(labelNameInfo, pattern, isAll);
    }

    @Override
    public LogicalPlan visitCancelDecommisionBackend(DorisParser.CancelDecommisionBackendContext ctx) {
        List<String> hostPorts = ctx.hostPorts.stream()
                .map(e -> stripQuotes(e.getText()))
                .collect(Collectors.toList());
        return new CancelDecommissionBackendCommand(hostPorts);
    }

    @Override
    public LogicalPlan visitShowRoutineLoadTask(DorisParser.ShowRoutineLoadTaskContext ctx) {
        String dbName = null;
        if (ctx.database != null) {
            dbName = ctx.database.getText();
        }

        Expression whereClause = null;
        if (ctx.wildWhere() != null) {
            whereClause = getWildWhere(ctx.wildWhere());
        }

        return new ShowRoutineLoadTaskCommand(dbName, whereClause);
    }

    @Override
    public LogicalPlan visitAdminShowReplicaStatus(DorisParser.AdminShowReplicaStatusContext ctx) {
        Expression where = null;
        if (ctx.WHERE() != null) {
            StringLiteral left = new StringLiteral(stripQuotes(ctx.STATUS().toString()));
            StringLiteral right = new StringLiteral(stripQuotes(ctx.STRING_LITERAL().getText()));
            if (ctx.NEQ() != null) {
                where = new Not(new EqualTo(left, right));
            } else {
                where = new EqualTo(left, right);
            }
        }
        TableRefInfo tableRefInfo = visitBaseTableRefContext(ctx.baseTableRef());
        return new ShowReplicaStatusCommand(tableRefInfo, where);
    }

    @Override
    public LogicalPlan visitShowReplicaStatus(DorisParser.ShowReplicaStatusContext ctx) {
        TableRefInfo tableRefInfo = visitBaseTableRefContext(ctx.baseTableRef());
        Expression whereClause = null;
        if (ctx.whereClause() != null) {
            whereClause = getExpression(ctx.whereClause().booleanExpression());
        }
        return new ShowReplicaStatusCommand(tableRefInfo, whereClause);
    }

    @Override
    public LogicalPlan visitShowWarmUpJob(DorisParser.ShowWarmUpJobContext ctx) {
        Expression whereClause = null;
        if (ctx.wildWhere() != null) {
            whereClause = getWildWhere(ctx.wildWhere());
        }
        return new ShowWarmUpCommand(whereClause);
    }

    @Override
    public LogicalPlan visitShowWorkloadGroups(DorisParser.ShowWorkloadGroupsContext ctx) {
        String likePattern = null;
        if (ctx.LIKE() != null) {
            likePattern = stripQuotes(ctx.STRING_LITERAL().getText());
        }
        return new ShowWorkloadGroupsCommand(likePattern);
    }

    @Override
    public LogicalPlan visitShowCopy(DorisParser.ShowCopyContext ctx) {
        String dbName = null;
        if (ctx.database != null) {
            dbName = ctx.database.getText();
        }

        Expression whereClause = null;
        if (ctx.whereClause() != null) {
            whereClause = getExpression(ctx.whereClause().booleanExpression());
        }

        List<OrderKey> orderKeys = new ArrayList<>();
        if (ctx.sortClause() != null) {
            orderKeys = visit(ctx.sortClause().sortItem(), OrderKey.class);
        }

        long limit = -1L;
        long offset = 0L;
        if (ctx.limitClause() != null) {
            limit = ctx.limitClause().limit != null
                    ? Long.parseLong(ctx.limitClause().limit.getText())
                    : 0;
            if (limit < 0) {
                throw new ParseException("Limit requires non-negative number", ctx.limitClause());
            }
            offset = ctx.limitClause().offset != null
                    ? Long.parseLong(ctx.limitClause().offset.getText())
                    : 0;
            if (offset < 0) {
                throw new ParseException("Offset requires non-negative number", ctx.limitClause());
            }
        }
        return new ShowCopyCommand(dbName, orderKeys, whereClause, limit, offset);
    }

    @Override
    public LogicalPlan visitCreateIndexAnalyzer(CreateIndexAnalyzerContext ctx) {
        boolean ifNotExists = ctx.IF() != null;
        String policyName = ctx.name.getText();
        Map<String, String> properties = visitPropertyClause(ctx.properties);

        return new CreateIndexAnalyzerCommand(ifNotExists, policyName, properties);
    }

    @Override
    public LogicalPlan visitCreateIndexTokenizer(CreateIndexTokenizerContext ctx) {
        boolean ifNotExists = ctx.IF() != null;
        String policyName = ctx.name.getText();
        Map<String, String> properties = visitPropertyClause(ctx.properties);

        return new CreateIndexTokenizerCommand(ifNotExists, policyName, properties);
    }

    @Override
    public LogicalPlan visitCreateIndexTokenFilter(CreateIndexTokenFilterContext ctx) {
        boolean ifNotExists = ctx.IF() != null;
        String policyName = ctx.name.getText();
        Map<String, String> properties = visitPropertyClause(ctx.properties);

        return new CreateIndexTokenFilterCommand(ifNotExists, policyName, properties);
    }

    @Override
    public LogicalPlan visitDropIndexAnalyzer(DropIndexAnalyzerContext ctx) {
        String policyName = ctx.name.getText();
        boolean ifExists = ctx.IF() != null;

        return new DropIndexAnalyzerCommand(policyName, ifExists);
    }

    @Override
    public LogicalPlan visitDropIndexTokenizer(DropIndexTokenizerContext ctx) {
        String policyName = ctx.name.getText();
        boolean ifExists = ctx.IF() != null;

        return new DropIndexTokenizerCommand(policyName, ifExists);
    }

    @Override
    public LogicalPlan visitDropIndexTokenFilter(DropIndexTokenFilterContext ctx) {
        String policyName = ctx.name.getText();
        boolean ifExists = ctx.IF() != null;

        return new DropIndexTokenFilterCommand(policyName, ifExists);
    }

    @Override
    public LogicalPlan visitShowIndexAnalyzer(ShowIndexAnalyzerContext ctx) {
        return new ShowIndexAnalyzerCommand();
    }

    @Override
    public LogicalPlan visitShowIndexTokenizer(ShowIndexTokenizerContext ctx) {
        return new ShowIndexTokenizerCommand();
    }

    @Override
    public LogicalPlan visitShowIndexTokenFilter(ShowIndexTokenFilterContext ctx) {
        return new ShowIndexTokenFilterCommand();
    }

    @Override
    public AlterTableOp visitCreateOrReplaceBranchClauses(DorisParser.CreateOrReplaceBranchClausesContext ctx) {
        return visitCreateOrReplaceBranchClause(ctx.createOrReplaceBranchClause());
    }

    @Override
    public CreateOrReplaceBranchOp visitCreateOrReplaceBranchClause(
            DorisParser.CreateOrReplaceBranchClauseContext ctx) {
        BranchOptions branchOptions = visitBranchOptions(ctx.branchOptions());
        return new CreateOrReplaceBranchOp(
                ctx.name.getText(),
                ctx.CREATE() != null,
                ctx.REPLACE() != null,
                ctx.EXISTS() != null,
                branchOptions);
    }

    @Override
    public BranchOptions visitBranchOptions(DorisParser.BranchOptionsContext ctx) {
        if (ctx == null) {
            return BranchOptions.EMPTY;
        }

        Optional<Long> snapshotId = Optional.empty();
        if (ctx.version != null) {
            snapshotId = Optional.of(Long.parseLong(ctx.version.getText()));
        }

        Optional<Long> retainTime = Optional.empty();
        if (ctx.retainTime() != null) {
            DorisParser.TimeValueWithUnitContext time = ctx.retainTime().timeValueWithUnit();
            if (time != null) {
                retainTime = Optional.of(visitTimeValueWithUnit(time));
            }
        }

        Optional<Integer> numSnapshots = Optional.empty();
        Optional<Long> retention = Optional.empty();
        if (ctx.retentionSnapshot() != null) {
            DorisParser.RetentionSnapshotContext retentionSnapshotContext = ctx.retentionSnapshot();
            if (retentionSnapshotContext.minSnapshotsToKeep() != null) {
                numSnapshots = Optional.of(
                    Integer.parseInt(retentionSnapshotContext.minSnapshotsToKeep().value.getText()));
            }
            if (retentionSnapshotContext.timeValueWithUnit() != null) {
                retention = Optional.of(visitTimeValueWithUnit(retentionSnapshotContext.timeValueWithUnit()));
            }
        }
        return new BranchOptions(snapshotId, retainTime, numSnapshots, retention);
    }

    @Override
    public AlterTableOp visitCreateOrReplaceTagClauses(DorisParser.CreateOrReplaceTagClausesContext ctx) {
        return visitCreateOrReplaceTagClause(ctx.createOrReplaceTagClause());
    }

    @Override
    public CreateOrReplaceTagOp visitCreateOrReplaceTagClause(DorisParser.CreateOrReplaceTagClauseContext ctx) {

        TagOptions tagOptions = visitTagOptions(ctx.tagOptions());
        return new CreateOrReplaceTagOp(
                ctx.name.getText(),
                ctx.CREATE() != null,
                ctx.REPLACE() != null,
                ctx.EXISTS() != null,
                tagOptions);
    }

    @Override
    public TagOptions visitTagOptions(DorisParser.TagOptionsContext ctx) {
        if (ctx == null) {
            return TagOptions.EMPTY;
        }

        Optional<Long> snapshotId = Optional.empty();
        if (ctx.version != null) {
            snapshotId = Optional.of(Long.parseLong(ctx.version.getText()));
        }

        Optional<Long> retainTime = Optional.empty();
        if (ctx.retainTime() != null) {
            DorisParser.TimeValueWithUnitContext time = ctx.retainTime().timeValueWithUnit();
            if (time != null) {
                retainTime = Optional.of(visitTimeValueWithUnit(time));
            }
        }

        return new TagOptions(snapshotId, retainTime);
    }

    @Override
    public LogicalPlan visitShowCreateStorageVault(DorisParser.ShowCreateStorageVaultContext ctx) {
        return new ShowCreateStorageVaultCommand(stripQuotes(ctx.identifier().getText()));
    }

    @Override
    public Long visitTimeValueWithUnit(DorisParser.TimeValueWithUnitContext ctx) {
        return TimeUnit.valueOf(ctx.timeUnit.getText().toUpperCase())
            .toMillis(Long.parseLong(ctx.timeValue.getText()));
    }

    @Override
    public AlterTableOp visitDropTagClauses(DorisParser.DropTagClausesContext ctx) {
        return visitDropTagClause(ctx.dropTagClause());
    }

    @Override
    public AlterTableOp visitDropTagClause(DorisParser.DropTagClauseContext ctx) {
        return new DropTagOp(ctx.name.getText(), ctx.EXISTS() != null);
    }

    @Override
    public AlterTableOp visitDropBranchClauses(DorisParser.DropBranchClausesContext ctx) {
        return visitDropBranchClause(ctx.dropBranchClause());
    }

    @Override
    public AlterTableOp visitDropBranchClause(DorisParser.DropBranchClauseContext ctx) {
        return new DropBranchOp(ctx.name.getText(), ctx.EXISTS() != null);
    }
}
