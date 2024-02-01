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
//
// Copied from
// https://github.com/awslabs/aws-glue-data-catalog-client-for-apache-hive-metastore/blob/branch-3.4.0/
//

package com.amazonaws.glue.catalog.metastore;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.ResponseMetadata;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.BatchCreatePartitionRequest;
import com.amazonaws.services.glue.model.BatchCreatePartitionResult;
import com.amazonaws.services.glue.model.BatchDeleteConnectionRequest;
import com.amazonaws.services.glue.model.BatchDeleteConnectionResult;
import com.amazonaws.services.glue.model.BatchDeletePartitionRequest;
import com.amazonaws.services.glue.model.BatchDeletePartitionResult;
import com.amazonaws.services.glue.model.BatchDeleteTableRequest;
import com.amazonaws.services.glue.model.BatchDeleteTableResult;
import com.amazonaws.services.glue.model.BatchDeleteTableVersionRequest;
import com.amazonaws.services.glue.model.BatchDeleteTableVersionResult;
import com.amazonaws.services.glue.model.BatchGetBlueprintsRequest;
import com.amazonaws.services.glue.model.BatchGetBlueprintsResult;
import com.amazonaws.services.glue.model.BatchGetCrawlersRequest;
import com.amazonaws.services.glue.model.BatchGetCrawlersResult;
import com.amazonaws.services.glue.model.BatchGetCustomEntityTypesRequest;
import com.amazonaws.services.glue.model.BatchGetCustomEntityTypesResult;
import com.amazonaws.services.glue.model.BatchGetDataQualityResultRequest;
import com.amazonaws.services.glue.model.BatchGetDataQualityResultResult;
import com.amazonaws.services.glue.model.BatchGetDevEndpointsRequest;
import com.amazonaws.services.glue.model.BatchGetDevEndpointsResult;
import com.amazonaws.services.glue.model.BatchGetJobsRequest;
import com.amazonaws.services.glue.model.BatchGetJobsResult;
import com.amazonaws.services.glue.model.BatchGetPartitionRequest;
import com.amazonaws.services.glue.model.BatchGetPartitionResult;
import com.amazonaws.services.glue.model.BatchGetTableOptimizerRequest;
import com.amazonaws.services.glue.model.BatchGetTableOptimizerResult;
import com.amazonaws.services.glue.model.BatchGetTriggersRequest;
import com.amazonaws.services.glue.model.BatchGetTriggersResult;
import com.amazonaws.services.glue.model.BatchGetWorkflowsRequest;
import com.amazonaws.services.glue.model.BatchGetWorkflowsResult;
import com.amazonaws.services.glue.model.BatchStopJobRunRequest;
import com.amazonaws.services.glue.model.BatchStopJobRunResult;
import com.amazonaws.services.glue.model.BatchUpdatePartitionRequest;
import com.amazonaws.services.glue.model.BatchUpdatePartitionResult;
import com.amazonaws.services.glue.model.CancelDataQualityRuleRecommendationRunRequest;
import com.amazonaws.services.glue.model.CancelDataQualityRuleRecommendationRunResult;
import com.amazonaws.services.glue.model.CancelDataQualityRulesetEvaluationRunRequest;
import com.amazonaws.services.glue.model.CancelDataQualityRulesetEvaluationRunResult;
import com.amazonaws.services.glue.model.CancelMLTaskRunRequest;
import com.amazonaws.services.glue.model.CancelMLTaskRunResult;
import com.amazonaws.services.glue.model.CancelStatementRequest;
import com.amazonaws.services.glue.model.CancelStatementResult;
import com.amazonaws.services.glue.model.CheckSchemaVersionValidityRequest;
import com.amazonaws.services.glue.model.CheckSchemaVersionValidityResult;
import com.amazonaws.services.glue.model.CreateBlueprintRequest;
import com.amazonaws.services.glue.model.CreateBlueprintResult;
import com.amazonaws.services.glue.model.CreateClassifierRequest;
import com.amazonaws.services.glue.model.CreateClassifierResult;
import com.amazonaws.services.glue.model.CreateConnectionRequest;
import com.amazonaws.services.glue.model.CreateConnectionResult;
import com.amazonaws.services.glue.model.CreateCrawlerRequest;
import com.amazonaws.services.glue.model.CreateCrawlerResult;
import com.amazonaws.services.glue.model.CreateCustomEntityTypeRequest;
import com.amazonaws.services.glue.model.CreateCustomEntityTypeResult;
import com.amazonaws.services.glue.model.CreateDataQualityRulesetRequest;
import com.amazonaws.services.glue.model.CreateDataQualityRulesetResult;
import com.amazonaws.services.glue.model.CreateDatabaseRequest;
import com.amazonaws.services.glue.model.CreateDatabaseResult;
import com.amazonaws.services.glue.model.CreateDevEndpointRequest;
import com.amazonaws.services.glue.model.CreateDevEndpointResult;
import com.amazonaws.services.glue.model.CreateJobRequest;
import com.amazonaws.services.glue.model.CreateJobResult;
import com.amazonaws.services.glue.model.CreateMLTransformRequest;
import com.amazonaws.services.glue.model.CreateMLTransformResult;
import com.amazonaws.services.glue.model.CreatePartitionIndexRequest;
import com.amazonaws.services.glue.model.CreatePartitionIndexResult;
import com.amazonaws.services.glue.model.CreatePartitionRequest;
import com.amazonaws.services.glue.model.CreatePartitionResult;
import com.amazonaws.services.glue.model.CreateRegistryRequest;
import com.amazonaws.services.glue.model.CreateRegistryResult;
import com.amazonaws.services.glue.model.CreateSchemaRequest;
import com.amazonaws.services.glue.model.CreateSchemaResult;
import com.amazonaws.services.glue.model.CreateScriptRequest;
import com.amazonaws.services.glue.model.CreateScriptResult;
import com.amazonaws.services.glue.model.CreateSecurityConfigurationRequest;
import com.amazonaws.services.glue.model.CreateSecurityConfigurationResult;
import com.amazonaws.services.glue.model.CreateSessionRequest;
import com.amazonaws.services.glue.model.CreateSessionResult;
import com.amazonaws.services.glue.model.CreateTableOptimizerRequest;
import com.amazonaws.services.glue.model.CreateTableOptimizerResult;
import com.amazonaws.services.glue.model.CreateTableRequest;
import com.amazonaws.services.glue.model.CreateTableResult;
import com.amazonaws.services.glue.model.CreateTriggerRequest;
import com.amazonaws.services.glue.model.CreateTriggerResult;
import com.amazonaws.services.glue.model.CreateUserDefinedFunctionRequest;
import com.amazonaws.services.glue.model.CreateUserDefinedFunctionResult;
import com.amazonaws.services.glue.model.CreateWorkflowRequest;
import com.amazonaws.services.glue.model.CreateWorkflowResult;
import com.amazonaws.services.glue.model.DeleteBlueprintRequest;
import com.amazonaws.services.glue.model.DeleteBlueprintResult;
import com.amazonaws.services.glue.model.DeleteClassifierRequest;
import com.amazonaws.services.glue.model.DeleteClassifierResult;
import com.amazonaws.services.glue.model.DeleteColumnStatisticsForPartitionRequest;
import com.amazonaws.services.glue.model.DeleteColumnStatisticsForPartitionResult;
import com.amazonaws.services.glue.model.DeleteColumnStatisticsForTableRequest;
import com.amazonaws.services.glue.model.DeleteColumnStatisticsForTableResult;
import com.amazonaws.services.glue.model.DeleteConnectionRequest;
import com.amazonaws.services.glue.model.DeleteConnectionResult;
import com.amazonaws.services.glue.model.DeleteCrawlerRequest;
import com.amazonaws.services.glue.model.DeleteCrawlerResult;
import com.amazonaws.services.glue.model.DeleteCustomEntityTypeRequest;
import com.amazonaws.services.glue.model.DeleteCustomEntityTypeResult;
import com.amazonaws.services.glue.model.DeleteDataQualityRulesetRequest;
import com.amazonaws.services.glue.model.DeleteDataQualityRulesetResult;
import com.amazonaws.services.glue.model.DeleteDatabaseRequest;
import com.amazonaws.services.glue.model.DeleteDatabaseResult;
import com.amazonaws.services.glue.model.DeleteDevEndpointRequest;
import com.amazonaws.services.glue.model.DeleteDevEndpointResult;
import com.amazonaws.services.glue.model.DeleteJobRequest;
import com.amazonaws.services.glue.model.DeleteJobResult;
import com.amazonaws.services.glue.model.DeleteMLTransformRequest;
import com.amazonaws.services.glue.model.DeleteMLTransformResult;
import com.amazonaws.services.glue.model.DeletePartitionIndexRequest;
import com.amazonaws.services.glue.model.DeletePartitionIndexResult;
import com.amazonaws.services.glue.model.DeletePartitionRequest;
import com.amazonaws.services.glue.model.DeletePartitionResult;
import com.amazonaws.services.glue.model.DeleteRegistryRequest;
import com.amazonaws.services.glue.model.DeleteRegistryResult;
import com.amazonaws.services.glue.model.DeleteResourcePolicyRequest;
import com.amazonaws.services.glue.model.DeleteResourcePolicyResult;
import com.amazonaws.services.glue.model.DeleteSchemaRequest;
import com.amazonaws.services.glue.model.DeleteSchemaResult;
import com.amazonaws.services.glue.model.DeleteSchemaVersionsRequest;
import com.amazonaws.services.glue.model.DeleteSchemaVersionsResult;
import com.amazonaws.services.glue.model.DeleteSecurityConfigurationRequest;
import com.amazonaws.services.glue.model.DeleteSecurityConfigurationResult;
import com.amazonaws.services.glue.model.DeleteSessionRequest;
import com.amazonaws.services.glue.model.DeleteSessionResult;
import com.amazonaws.services.glue.model.DeleteTableOptimizerRequest;
import com.amazonaws.services.glue.model.DeleteTableOptimizerResult;
import com.amazonaws.services.glue.model.DeleteTableRequest;
import com.amazonaws.services.glue.model.DeleteTableResult;
import com.amazonaws.services.glue.model.DeleteTableVersionRequest;
import com.amazonaws.services.glue.model.DeleteTableVersionResult;
import com.amazonaws.services.glue.model.DeleteTriggerRequest;
import com.amazonaws.services.glue.model.DeleteTriggerResult;
import com.amazonaws.services.glue.model.DeleteUserDefinedFunctionRequest;
import com.amazonaws.services.glue.model.DeleteUserDefinedFunctionResult;
import com.amazonaws.services.glue.model.DeleteWorkflowRequest;
import com.amazonaws.services.glue.model.DeleteWorkflowResult;
import com.amazonaws.services.glue.model.GetBlueprintRequest;
import com.amazonaws.services.glue.model.GetBlueprintResult;
import com.amazonaws.services.glue.model.GetBlueprintRunRequest;
import com.amazonaws.services.glue.model.GetBlueprintRunResult;
import com.amazonaws.services.glue.model.GetBlueprintRunsRequest;
import com.amazonaws.services.glue.model.GetBlueprintRunsResult;
import com.amazonaws.services.glue.model.GetCatalogImportStatusRequest;
import com.amazonaws.services.glue.model.GetCatalogImportStatusResult;
import com.amazonaws.services.glue.model.GetClassifierRequest;
import com.amazonaws.services.glue.model.GetClassifierResult;
import com.amazonaws.services.glue.model.GetClassifiersRequest;
import com.amazonaws.services.glue.model.GetClassifiersResult;
import com.amazonaws.services.glue.model.GetColumnStatisticsTaskRunRequest;
import com.amazonaws.services.glue.model.GetColumnStatisticsTaskRunResult;
import com.amazonaws.services.glue.model.GetColumnStatisticsTaskRunsRequest;
import com.amazonaws.services.glue.model.GetColumnStatisticsTaskRunsResult;
import com.amazonaws.services.glue.model.GetConnectionRequest;
import com.amazonaws.services.glue.model.GetColumnStatisticsForPartitionResult;
import com.amazonaws.services.glue.model.GetColumnStatisticsForPartitionRequest;
import com.amazonaws.services.glue.model.GetColumnStatisticsForTableResult;
import com.amazonaws.services.glue.model.GetColumnStatisticsForTableRequest;
import com.amazonaws.services.glue.model.GetConnectionResult;
import com.amazonaws.services.glue.model.GetConnectionsRequest;
import com.amazonaws.services.glue.model.GetConnectionsResult;
import com.amazonaws.services.glue.model.GetCrawlerMetricsRequest;
import com.amazonaws.services.glue.model.GetCrawlerMetricsResult;
import com.amazonaws.services.glue.model.GetCrawlerRequest;
import com.amazonaws.services.glue.model.GetCrawlerResult;
import com.amazonaws.services.glue.model.GetCrawlersRequest;
import com.amazonaws.services.glue.model.GetCrawlersResult;
import com.amazonaws.services.glue.model.GetCustomEntityTypeRequest;
import com.amazonaws.services.glue.model.GetCustomEntityTypeResult;
import com.amazonaws.services.glue.model.GetDataCatalogEncryptionSettingsRequest;
import com.amazonaws.services.glue.model.GetDataCatalogEncryptionSettingsResult;
import com.amazonaws.services.glue.model.GetDataQualityResultRequest;
import com.amazonaws.services.glue.model.GetDataQualityResultResult;
import com.amazonaws.services.glue.model.GetDataQualityRuleRecommendationRunRequest;
import com.amazonaws.services.glue.model.GetDataQualityRuleRecommendationRunResult;
import com.amazonaws.services.glue.model.GetDataQualityRulesetEvaluationRunRequest;
import com.amazonaws.services.glue.model.GetDataQualityRulesetEvaluationRunResult;
import com.amazonaws.services.glue.model.GetDataQualityRulesetRequest;
import com.amazonaws.services.glue.model.GetDataQualityRulesetResult;
import com.amazonaws.services.glue.model.GetDatabaseRequest;
import com.amazonaws.services.glue.model.GetDatabaseResult;
import com.amazonaws.services.glue.model.GetDatabasesRequest;
import com.amazonaws.services.glue.model.GetDatabasesResult;
import com.amazonaws.services.glue.model.GetDataflowGraphRequest;
import com.amazonaws.services.glue.model.GetDataflowGraphResult;
import com.amazonaws.services.glue.model.GetDevEndpointRequest;
import com.amazonaws.services.glue.model.GetDevEndpointResult;
import com.amazonaws.services.glue.model.GetDevEndpointsRequest;
import com.amazonaws.services.glue.model.GetDevEndpointsResult;
import com.amazonaws.services.glue.model.GetJobBookmarkRequest;
import com.amazonaws.services.glue.model.GetJobBookmarkResult;
import com.amazonaws.services.glue.model.GetJobRequest;
import com.amazonaws.services.glue.model.GetJobResult;
import com.amazonaws.services.glue.model.GetJobRunRequest;
import com.amazonaws.services.glue.model.GetJobRunResult;
import com.amazonaws.services.glue.model.GetJobRunsRequest;
import com.amazonaws.services.glue.model.GetJobRunsResult;
import com.amazonaws.services.glue.model.GetJobsRequest;
import com.amazonaws.services.glue.model.GetJobsResult;
import com.amazonaws.services.glue.model.GetMLTaskRunRequest;
import com.amazonaws.services.glue.model.GetMLTaskRunResult;
import com.amazonaws.services.glue.model.GetMLTaskRunsRequest;
import com.amazonaws.services.glue.model.GetMLTaskRunsResult;
import com.amazonaws.services.glue.model.GetMLTransformRequest;
import com.amazonaws.services.glue.model.GetMLTransformResult;
import com.amazonaws.services.glue.model.GetMLTransformsRequest;
import com.amazonaws.services.glue.model.GetMLTransformsResult;
import com.amazonaws.services.glue.model.GetMappingRequest;
import com.amazonaws.services.glue.model.GetMappingResult;
import com.amazonaws.services.glue.model.GetPartitionIndexesRequest;
import com.amazonaws.services.glue.model.GetPartitionIndexesResult;
import com.amazonaws.services.glue.model.GetPartitionRequest;
import com.amazonaws.services.glue.model.GetPartitionResult;
import com.amazonaws.services.glue.model.GetPartitionsRequest;
import com.amazonaws.services.glue.model.GetPartitionsResult;
import com.amazonaws.services.glue.model.GetPlanRequest;
import com.amazonaws.services.glue.model.GetPlanResult;
import com.amazonaws.services.glue.model.GetRegistryRequest;
import com.amazonaws.services.glue.model.GetRegistryResult;
import com.amazonaws.services.glue.model.GetResourcePoliciesRequest;
import com.amazonaws.services.glue.model.GetResourcePoliciesResult;
import com.amazonaws.services.glue.model.GetResourcePolicyRequest;
import com.amazonaws.services.glue.model.GetResourcePolicyResult;
import com.amazonaws.services.glue.model.GetSchemaByDefinitionRequest;
import com.amazonaws.services.glue.model.GetSchemaByDefinitionResult;
import com.amazonaws.services.glue.model.GetSchemaRequest;
import com.amazonaws.services.glue.model.GetSchemaResult;
import com.amazonaws.services.glue.model.GetSchemaVersionRequest;
import com.amazonaws.services.glue.model.GetSchemaVersionResult;
import com.amazonaws.services.glue.model.GetSchemaVersionsDiffRequest;
import com.amazonaws.services.glue.model.GetSchemaVersionsDiffResult;
import com.amazonaws.services.glue.model.GetSecurityConfigurationRequest;
import com.amazonaws.services.glue.model.GetSecurityConfigurationResult;
import com.amazonaws.services.glue.model.GetSecurityConfigurationsRequest;
import com.amazonaws.services.glue.model.GetSecurityConfigurationsResult;
import com.amazonaws.services.glue.model.GetSessionRequest;
import com.amazonaws.services.glue.model.GetSessionResult;
import com.amazonaws.services.glue.model.GetStatementRequest;
import com.amazonaws.services.glue.model.GetStatementResult;
import com.amazonaws.services.glue.model.GetTableOptimizerRequest;
import com.amazonaws.services.glue.model.GetTableOptimizerResult;
import com.amazonaws.services.glue.model.GetTableRequest;
import com.amazonaws.services.glue.model.GetTableResult;
import com.amazonaws.services.glue.model.GetTableVersionRequest;
import com.amazonaws.services.glue.model.GetTableVersionResult;
import com.amazonaws.services.glue.model.GetTableVersionsRequest;
import com.amazonaws.services.glue.model.GetTableVersionsResult;
import com.amazonaws.services.glue.model.GetTablesRequest;
import com.amazonaws.services.glue.model.GetTablesResult;
import com.amazonaws.services.glue.model.GetTagsRequest;
import com.amazonaws.services.glue.model.GetTagsResult;
import com.amazonaws.services.glue.model.GetTriggerRequest;
import com.amazonaws.services.glue.model.GetTriggerResult;
import com.amazonaws.services.glue.model.GetTriggersRequest;
import com.amazonaws.services.glue.model.GetTriggersResult;
import com.amazonaws.services.glue.model.GetUnfilteredPartitionMetadataRequest;
import com.amazonaws.services.glue.model.GetUnfilteredPartitionMetadataResult;
import com.amazonaws.services.glue.model.GetUnfilteredPartitionsMetadataRequest;
import com.amazonaws.services.glue.model.GetUnfilteredPartitionsMetadataResult;
import com.amazonaws.services.glue.model.GetUnfilteredTableMetadataRequest;
import com.amazonaws.services.glue.model.GetUnfilteredTableMetadataResult;
import com.amazonaws.services.glue.model.GetUserDefinedFunctionRequest;
import com.amazonaws.services.glue.model.GetUserDefinedFunctionResult;
import com.amazonaws.services.glue.model.GetUserDefinedFunctionsRequest;
import com.amazonaws.services.glue.model.GetUserDefinedFunctionsResult;
import com.amazonaws.services.glue.model.GetWorkflowRequest;
import com.amazonaws.services.glue.model.GetWorkflowResult;
import com.amazonaws.services.glue.model.GetWorkflowRunPropertiesRequest;
import com.amazonaws.services.glue.model.GetWorkflowRunPropertiesResult;
import com.amazonaws.services.glue.model.GetWorkflowRunRequest;
import com.amazonaws.services.glue.model.GetWorkflowRunResult;
import com.amazonaws.services.glue.model.GetWorkflowRunsRequest;
import com.amazonaws.services.glue.model.GetWorkflowRunsResult;
import com.amazonaws.services.glue.model.ImportCatalogToGlueRequest;
import com.amazonaws.services.glue.model.ImportCatalogToGlueResult;
import com.amazonaws.services.glue.model.ListBlueprintsRequest;
import com.amazonaws.services.glue.model.ListBlueprintsResult;
import com.amazonaws.services.glue.model.ListColumnStatisticsTaskRunsRequest;
import com.amazonaws.services.glue.model.ListColumnStatisticsTaskRunsResult;
import com.amazonaws.services.glue.model.ListCrawlersRequest;
import com.amazonaws.services.glue.model.ListCrawlersResult;
import com.amazonaws.services.glue.model.ListCrawlsRequest;
import com.amazonaws.services.glue.model.ListCrawlsResult;
import com.amazonaws.services.glue.model.ListCustomEntityTypesRequest;
import com.amazonaws.services.glue.model.ListCustomEntityTypesResult;
import com.amazonaws.services.glue.model.ListDataQualityResultsRequest;
import com.amazonaws.services.glue.model.ListDataQualityResultsResult;
import com.amazonaws.services.glue.model.ListDataQualityRuleRecommendationRunsRequest;
import com.amazonaws.services.glue.model.ListDataQualityRuleRecommendationRunsResult;
import com.amazonaws.services.glue.model.ListDataQualityRulesetEvaluationRunsRequest;
import com.amazonaws.services.glue.model.ListDataQualityRulesetEvaluationRunsResult;
import com.amazonaws.services.glue.model.ListDataQualityRulesetsRequest;
import com.amazonaws.services.glue.model.ListDataQualityRulesetsResult;
import com.amazonaws.services.glue.model.ListDevEndpointsRequest;
import com.amazonaws.services.glue.model.ListDevEndpointsResult;
import com.amazonaws.services.glue.model.ListJobsRequest;
import com.amazonaws.services.glue.model.ListJobsResult;
import com.amazonaws.services.glue.model.ListMLTransformsRequest;
import com.amazonaws.services.glue.model.ListMLTransformsResult;
import com.amazonaws.services.glue.model.ListRegistriesRequest;
import com.amazonaws.services.glue.model.ListRegistriesResult;
import com.amazonaws.services.glue.model.ListSchemaVersionsRequest;
import com.amazonaws.services.glue.model.ListSchemaVersionsResult;
import com.amazonaws.services.glue.model.ListSchemasRequest;
import com.amazonaws.services.glue.model.ListSchemasResult;
import com.amazonaws.services.glue.model.ListSessionsRequest;
import com.amazonaws.services.glue.model.ListSessionsResult;
import com.amazonaws.services.glue.model.ListStatementsRequest;
import com.amazonaws.services.glue.model.ListStatementsResult;
import com.amazonaws.services.glue.model.ListTableOptimizerRunsRequest;
import com.amazonaws.services.glue.model.ListTableOptimizerRunsResult;
import com.amazonaws.services.glue.model.ListTriggersRequest;
import com.amazonaws.services.glue.model.ListTriggersResult;
import com.amazonaws.services.glue.model.ListWorkflowsRequest;
import com.amazonaws.services.glue.model.ListWorkflowsResult;
import com.amazonaws.services.glue.model.PutDataCatalogEncryptionSettingsRequest;
import com.amazonaws.services.glue.model.PutDataCatalogEncryptionSettingsResult;
import com.amazonaws.services.glue.model.PutResourcePolicyRequest;
import com.amazonaws.services.glue.model.PutResourcePolicyResult;
import com.amazonaws.services.glue.model.PutSchemaVersionMetadataRequest;
import com.amazonaws.services.glue.model.PutSchemaVersionMetadataResult;
import com.amazonaws.services.glue.model.PutWorkflowRunPropertiesRequest;
import com.amazonaws.services.glue.model.PutWorkflowRunPropertiesResult;
import com.amazonaws.services.glue.model.QuerySchemaVersionMetadataRequest;
import com.amazonaws.services.glue.model.QuerySchemaVersionMetadataResult;
import com.amazonaws.services.glue.model.RegisterSchemaVersionRequest;
import com.amazonaws.services.glue.model.RegisterSchemaVersionResult;
import com.amazonaws.services.glue.model.RemoveSchemaVersionMetadataRequest;
import com.amazonaws.services.glue.model.RemoveSchemaVersionMetadataResult;
import com.amazonaws.services.glue.model.ResetJobBookmarkRequest;
import com.amazonaws.services.glue.model.ResetJobBookmarkResult;
import com.amazonaws.services.glue.model.ResumeWorkflowRunRequest;
import com.amazonaws.services.glue.model.ResumeWorkflowRunResult;
import com.amazonaws.services.glue.model.RunStatementRequest;
import com.amazonaws.services.glue.model.RunStatementResult;
import com.amazonaws.services.glue.model.SearchTablesRequest;
import com.amazonaws.services.glue.model.SearchTablesResult;
import com.amazonaws.services.glue.model.StartBlueprintRunRequest;
import com.amazonaws.services.glue.model.StartBlueprintRunResult;
import com.amazonaws.services.glue.model.StartColumnStatisticsTaskRunRequest;
import com.amazonaws.services.glue.model.StartColumnStatisticsTaskRunResult;
import com.amazonaws.services.glue.model.StartCrawlerRequest;
import com.amazonaws.services.glue.model.StartCrawlerResult;
import com.amazonaws.services.glue.model.StartCrawlerScheduleRequest;
import com.amazonaws.services.glue.model.StartCrawlerScheduleResult;
import com.amazonaws.services.glue.model.StartDataQualityRuleRecommendationRunRequest;
import com.amazonaws.services.glue.model.StartDataQualityRuleRecommendationRunResult;
import com.amazonaws.services.glue.model.StartDataQualityRulesetEvaluationRunRequest;
import com.amazonaws.services.glue.model.StartDataQualityRulesetEvaluationRunResult;
import com.amazonaws.services.glue.model.StartExportLabelsTaskRunRequest;
import com.amazonaws.services.glue.model.StartExportLabelsTaskRunResult;
import com.amazonaws.services.glue.model.StartImportLabelsTaskRunRequest;
import com.amazonaws.services.glue.model.StartImportLabelsTaskRunResult;
import com.amazonaws.services.glue.model.StartJobRunRequest;
import com.amazonaws.services.glue.model.StartJobRunResult;
import com.amazonaws.services.glue.model.StartMLEvaluationTaskRunRequest;
import com.amazonaws.services.glue.model.StartMLEvaluationTaskRunResult;
import com.amazonaws.services.glue.model.StartMLLabelingSetGenerationTaskRunRequest;
import com.amazonaws.services.glue.model.StartMLLabelingSetGenerationTaskRunResult;
import com.amazonaws.services.glue.model.StartTriggerRequest;
import com.amazonaws.services.glue.model.StartTriggerResult;
import com.amazonaws.services.glue.model.StartWorkflowRunRequest;
import com.amazonaws.services.glue.model.StartWorkflowRunResult;
import com.amazonaws.services.glue.model.StopColumnStatisticsTaskRunRequest;
import com.amazonaws.services.glue.model.StopColumnStatisticsTaskRunResult;
import com.amazonaws.services.glue.model.StopCrawlerRequest;
import com.amazonaws.services.glue.model.StopCrawlerResult;
import com.amazonaws.services.glue.model.StopCrawlerScheduleRequest;
import com.amazonaws.services.glue.model.StopCrawlerScheduleResult;
import com.amazonaws.services.glue.model.StopSessionRequest;
import com.amazonaws.services.glue.model.StopSessionResult;
import com.amazonaws.services.glue.model.StopTriggerRequest;
import com.amazonaws.services.glue.model.StopTriggerResult;
import com.amazonaws.services.glue.model.StopWorkflowRunRequest;
import com.amazonaws.services.glue.model.StopWorkflowRunResult;
import com.amazonaws.services.glue.model.TagResourceRequest;
import com.amazonaws.services.glue.model.TagResourceResult;
import com.amazonaws.services.glue.model.UntagResourceRequest;
import com.amazonaws.services.glue.model.UntagResourceResult;
import com.amazonaws.services.glue.model.UpdateBlueprintRequest;
import com.amazonaws.services.glue.model.UpdateBlueprintResult;
import com.amazonaws.services.glue.model.UpdateClassifierRequest;
import com.amazonaws.services.glue.model.UpdateClassifierResult;
import com.amazonaws.services.glue.model.UpdateColumnStatisticsForPartitionRequest;
import com.amazonaws.services.glue.model.UpdateColumnStatisticsForPartitionResult;
import com.amazonaws.services.glue.model.UpdateColumnStatisticsForTableRequest;
import com.amazonaws.services.glue.model.UpdateColumnStatisticsForTableResult;
import com.amazonaws.services.glue.model.UpdateConnectionRequest;
import com.amazonaws.services.glue.model.UpdateConnectionResult;
import com.amazonaws.services.glue.model.UpdateCrawlerRequest;
import com.amazonaws.services.glue.model.UpdateCrawlerResult;
import com.amazonaws.services.glue.model.UpdateCrawlerScheduleRequest;
import com.amazonaws.services.glue.model.UpdateCrawlerScheduleResult;
import com.amazonaws.services.glue.model.UpdateDataQualityRulesetRequest;
import com.amazonaws.services.glue.model.UpdateDataQualityRulesetResult;
import com.amazonaws.services.glue.model.UpdateDatabaseRequest;
import com.amazonaws.services.glue.model.UpdateDatabaseResult;
import com.amazonaws.services.glue.model.UpdateDevEndpointRequest;
import com.amazonaws.services.glue.model.UpdateDevEndpointResult;
import com.amazonaws.services.glue.model.UpdateJobFromSourceControlRequest;
import com.amazonaws.services.glue.model.UpdateJobFromSourceControlResult;
import com.amazonaws.services.glue.model.UpdateJobRequest;
import com.amazonaws.services.glue.model.UpdateJobResult;
import com.amazonaws.services.glue.model.UpdateMLTransformRequest;
import com.amazonaws.services.glue.model.UpdateMLTransformResult;
import com.amazonaws.services.glue.model.UpdatePartitionRequest;
import com.amazonaws.services.glue.model.UpdatePartitionResult;
import com.amazonaws.services.glue.model.UpdateRegistryRequest;
import com.amazonaws.services.glue.model.UpdateRegistryResult;
import com.amazonaws.services.glue.model.UpdateSchemaRequest;
import com.amazonaws.services.glue.model.UpdateSchemaResult;
import com.amazonaws.services.glue.model.UpdateSourceControlFromJobRequest;
import com.amazonaws.services.glue.model.UpdateSourceControlFromJobResult;
import com.amazonaws.services.glue.model.UpdateTableOptimizerRequest;
import com.amazonaws.services.glue.model.UpdateTableOptimizerResult;
import com.amazonaws.services.glue.model.UpdateTableRequest;
import com.amazonaws.services.glue.model.UpdateTableResult;
import com.amazonaws.services.glue.model.UpdateTriggerRequest;
import com.amazonaws.services.glue.model.UpdateTriggerResult;
import com.amazonaws.services.glue.model.UpdateUserDefinedFunctionRequest;
import com.amazonaws.services.glue.model.UpdateUserDefinedFunctionResult;
import com.amazonaws.services.glue.model.UpdateWorkflowRequest;
import com.amazonaws.services.glue.model.UpdateWorkflowResult;

/**
 * Base decorator for AWSGlue interface. It doesn't decorate any functionality but just proxy all methods to
 * decoratedAwsGlue. It should be used as a parent for specific decorators where only necessary methods are overwritten
 * and decorated.
 * All @Override methods are generated by IntelliJ IDEA.
 */
public class AWSGlueDecoratorBase implements AWSGlue {



    private AWSGlue decoratedAwsGlue;

    public AWSGlueDecoratorBase(AWSGlue awsGlueToBeDecorated) {
        this.decoratedAwsGlue = awsGlueToBeDecorated;
    }

    @Override
    public BatchCreatePartitionResult batchCreatePartition(BatchCreatePartitionRequest batchCreatePartitionRequest) {
        return decoratedAwsGlue.batchCreatePartition(batchCreatePartitionRequest);
    }

    @Override
    public BatchDeleteConnectionResult batchDeleteConnection(BatchDeleteConnectionRequest batchDeleteConnectionRequest) {
        return decoratedAwsGlue.batchDeleteConnection(batchDeleteConnectionRequest);
    }

    @Override
    public BatchDeletePartitionResult batchDeletePartition(BatchDeletePartitionRequest batchDeletePartitionRequest) {
        return decoratedAwsGlue.batchDeletePartition(batchDeletePartitionRequest);
    }

    @Override
    public BatchDeleteTableResult batchDeleteTable(BatchDeleteTableRequest batchDeleteTableRequest) {
        return decoratedAwsGlue.batchDeleteTable(batchDeleteTableRequest);
    }

    @Override
    public BatchDeleteTableVersionResult batchDeleteTableVersion(BatchDeleteTableVersionRequest batchDeleteTableVersionRequest) {
        return decoratedAwsGlue.batchDeleteTableVersion(batchDeleteTableVersionRequest);
    }

    @Override
    public BatchGetCrawlersResult batchGetCrawlers(BatchGetCrawlersRequest batchGetCrawlersRequest) {
        return decoratedAwsGlue.batchGetCrawlers(batchGetCrawlersRequest);
    }

    @Override
    public BatchGetCustomEntityTypesResult batchGetCustomEntityTypes(BatchGetCustomEntityTypesRequest batchGetCustomEntityTypesRequest) {
        return decoratedAwsGlue.batchGetCustomEntityTypes(batchGetCustomEntityTypesRequest);
    }

    @Override
    public BatchGetDevEndpointsResult batchGetDevEndpoints(BatchGetDevEndpointsRequest batchGetDevEndpointsRequest) {
        return decoratedAwsGlue.batchGetDevEndpoints(batchGetDevEndpointsRequest);
    }

    @Override
    public BatchGetJobsResult batchGetJobs(BatchGetJobsRequest batchGetJobsRequest) {
        return decoratedAwsGlue.batchGetJobs(batchGetJobsRequest);
    }

    @Override
    public BatchGetPartitionResult batchGetPartition(BatchGetPartitionRequest batchGetPartitionRequest) {
        return decoratedAwsGlue.batchGetPartition(batchGetPartitionRequest);
    }

    @Override
    public BatchGetTableOptimizerResult batchGetTableOptimizer(BatchGetTableOptimizerRequest batchGetTableOptimizerRequest) {
        return null;
    }

    @Override
    public BatchGetTriggersResult batchGetTriggers(BatchGetTriggersRequest batchGetTriggersRequest) {
        return decoratedAwsGlue.batchGetTriggers(batchGetTriggersRequest);
    }

    @Override
    public BatchGetWorkflowsResult batchGetWorkflows(BatchGetWorkflowsRequest batchGetWorkflowsRequest) {
        return decoratedAwsGlue.batchGetWorkflows(batchGetWorkflowsRequest);
    }

    @Override
    public BatchStopJobRunResult batchStopJobRun(BatchStopJobRunRequest batchStopJobRunRequest) {
        return decoratedAwsGlue.batchStopJobRun(batchStopJobRunRequest);
    }

    @Override
    public BatchUpdatePartitionResult batchUpdatePartition(BatchUpdatePartitionRequest batchUpdatePartitionRequest) {
        return decoratedAwsGlue.batchUpdatePartition(batchUpdatePartitionRequest);
    }

    @Override
    public CancelMLTaskRunResult cancelMLTaskRun(CancelMLTaskRunRequest cancelMLTaskRunRequest) {
        return decoratedAwsGlue.cancelMLTaskRun(cancelMLTaskRunRequest);
    }

    @Override
    public CancelStatementResult cancelStatement(CancelStatementRequest cancelStatementRequest) {
        return decoratedAwsGlue.cancelStatement(cancelStatementRequest);
    }

    @Override
    public CheckSchemaVersionValidityResult checkSchemaVersionValidity(CheckSchemaVersionValidityRequest checkSchemaVersionValidityRequest) {
        return null;
    }

    @Override
    public CreateBlueprintResult createBlueprint(CreateBlueprintRequest createBlueprintRequest) {
        return null;
    }

    @Override
    public CreateClassifierResult createClassifier(CreateClassifierRequest createClassifierRequest) {
        return decoratedAwsGlue.createClassifier(createClassifierRequest);
    }

    @Override
    public CreateConnectionResult createConnection(CreateConnectionRequest createConnectionRequest) {
        return decoratedAwsGlue.createConnection(createConnectionRequest);
    }

    @Override
    public CreateCrawlerResult createCrawler(CreateCrawlerRequest createCrawlerRequest) {
        return decoratedAwsGlue.createCrawler(createCrawlerRequest);
    }

    @Override
    public CreateCustomEntityTypeResult createCustomEntityType(CreateCustomEntityTypeRequest createCustomEntityTypeRequest) {
        return decoratedAwsGlue.createCustomEntityType(createCustomEntityTypeRequest);
    }

    @Override
    public CreateDatabaseResult createDatabase(CreateDatabaseRequest createDatabaseRequest) {
        return decoratedAwsGlue.createDatabase(createDatabaseRequest);
    }

    @Override
    public CreateDevEndpointResult createDevEndpoint(CreateDevEndpointRequest createDevEndpointRequest) {
        return decoratedAwsGlue.createDevEndpoint(createDevEndpointRequest);
    }

    @Override
    public CreateJobResult createJob(CreateJobRequest createJobRequest) {
        return decoratedAwsGlue.createJob(createJobRequest);
    }

    @Override
    public CreateMLTransformResult createMLTransform(CreateMLTransformRequest createMLTransformRequest) {
        return decoratedAwsGlue.createMLTransform(createMLTransformRequest);
    }

    @Override
    public CreatePartitionResult createPartition(CreatePartitionRequest createPartitionRequest) {
        return decoratedAwsGlue.createPartition(createPartitionRequest);
    }

    @Override
    public CreatePartitionIndexResult createPartitionIndex(CreatePartitionIndexRequest createPartitionIndexRequest) {
        return null;
    }

    @Override
    public CreateRegistryResult createRegistry(CreateRegistryRequest createRegistryRequest) {
        return null;
    }

    @Override
    public CreateSchemaResult createSchema(CreateSchemaRequest createSchemaRequest) {
        return null;
    }

    @Override
    public CreateScriptResult createScript(CreateScriptRequest createScriptRequest) {
        return decoratedAwsGlue.createScript(createScriptRequest);
    }

    @Override
    public CreateSecurityConfigurationResult createSecurityConfiguration(CreateSecurityConfigurationRequest createSecurityConfigurationRequest) {
        return decoratedAwsGlue.createSecurityConfiguration(createSecurityConfigurationRequest);
    }

    @Override
    public CreateSessionResult createSession(CreateSessionRequest createSessionRequest) {
        return decoratedAwsGlue.createSession(createSessionRequest);
    }

    @Override
    public CreateTableResult createTable(CreateTableRequest createTableRequest) {
        return decoratedAwsGlue.createTable(createTableRequest);
    }

    @Override
    public CreateTableOptimizerResult createTableOptimizer(CreateTableOptimizerRequest createTableOptimizerRequest) {
        return null;
    }

    @Override
    public CreateTriggerResult createTrigger(CreateTriggerRequest createTriggerRequest) {
        return decoratedAwsGlue.createTrigger(createTriggerRequest);
    }

    @Override
    public CreateUserDefinedFunctionResult createUserDefinedFunction(CreateUserDefinedFunctionRequest createUserDefinedFunctionRequest) {
        return decoratedAwsGlue.createUserDefinedFunction(createUserDefinedFunctionRequest);
    }

    @Override
    public CreateWorkflowResult createWorkflow(CreateWorkflowRequest createWorkflowRequest) {
        return decoratedAwsGlue.createWorkflow(createWorkflowRequest);
    }

    @Override
    public DeleteBlueprintResult deleteBlueprint(DeleteBlueprintRequest deleteBlueprintRequest) {
        return decoratedAwsGlue.deleteBlueprint(deleteBlueprintRequest);
    }

    @Override
    public DeleteClassifierResult deleteClassifier(DeleteClassifierRequest deleteClassifierRequest) {
        return decoratedAwsGlue.deleteClassifier(deleteClassifierRequest);
    }

    @Override
    public DeleteConnectionResult deleteConnection(DeleteConnectionRequest deleteConnectionRequest) {
        return decoratedAwsGlue.deleteConnection(deleteConnectionRequest);
    }

    @Override
    public DeleteCrawlerResult deleteCrawler(DeleteCrawlerRequest deleteCrawlerRequest) {
        return decoratedAwsGlue.deleteCrawler(deleteCrawlerRequest);
    }

    @Override
    public DeleteCustomEntityTypeResult deleteCustomEntityType(DeleteCustomEntityTypeRequest deleteCustomEntityTypeRequest) {
        return decoratedAwsGlue.deleteCustomEntityType(deleteCustomEntityTypeRequest);
    }

    @Override
    public DeleteDatabaseResult deleteDatabase(DeleteDatabaseRequest deleteDatabaseRequest) {
        return decoratedAwsGlue.deleteDatabase(deleteDatabaseRequest);
    }

    @Override
    public DeleteDevEndpointResult deleteDevEndpoint(DeleteDevEndpointRequest deleteDevEndpointRequest) {
        return decoratedAwsGlue.deleteDevEndpoint(deleteDevEndpointRequest);
    }

    @Override
    public DeleteJobResult deleteJob(DeleteJobRequest deleteJobRequest) {
        return decoratedAwsGlue.deleteJob(deleteJobRequest);
    }

    @Override
    public DeleteMLTransformResult deleteMLTransform(DeleteMLTransformRequest deleteMLTransformRequest) {
        return decoratedAwsGlue.deleteMLTransform(deleteMLTransformRequest);
    }

    @Override
    public DeletePartitionResult deletePartition(DeletePartitionRequest deletePartitionRequest) {
        return decoratedAwsGlue.deletePartition(deletePartitionRequest);
    }

    @Override
    public DeletePartitionIndexResult deletePartitionIndex(DeletePartitionIndexRequest deletePartitionIndexRequest) {
        return null;
    }

    @Override
    public DeleteRegistryResult deleteRegistry(DeleteRegistryRequest deleteRegistryRequest) {
        return null;
    }

    @Override
    public DeleteResourcePolicyResult deleteResourcePolicy(DeleteResourcePolicyRequest deleteResourcePolicyRequest) {
        return decoratedAwsGlue.deleteResourcePolicy(deleteResourcePolicyRequest);
    }

    @Override
    public DeleteSchemaResult deleteSchema(DeleteSchemaRequest deleteSchemaRequest) {
        return null;
    }

    @Override
    public DeleteSchemaVersionsResult deleteSchemaVersions(DeleteSchemaVersionsRequest deleteSchemaVersionsRequest) {
        return null;
    }

    @Override
    public DeleteSecurityConfigurationResult deleteSecurityConfiguration(DeleteSecurityConfigurationRequest deleteSecurityConfigurationRequest) {
        return decoratedAwsGlue.deleteSecurityConfiguration(deleteSecurityConfigurationRequest);
    }

    @Override
    public DeleteSessionResult deleteSession(DeleteSessionRequest deleteSessionRequest) {
        return decoratedAwsGlue.deleteSession(deleteSessionRequest);
    }

    @Override
    public DeleteTableResult deleteTable(DeleteTableRequest deleteTableRequest) {
        return decoratedAwsGlue.deleteTable(deleteTableRequest);
    }

    @Override
    public DeleteTableOptimizerResult deleteTableOptimizer(DeleteTableOptimizerRequest deleteTableOptimizerRequest) {
        return null;
    }

    @Override
    public DeleteTableVersionResult deleteTableVersion(DeleteTableVersionRequest deleteTableVersionRequest) {
        return decoratedAwsGlue.deleteTableVersion(deleteTableVersionRequest);
    }

    @Override
    public DeleteTriggerResult deleteTrigger(DeleteTriggerRequest deleteTriggerRequest) {
        return decoratedAwsGlue.deleteTrigger(deleteTriggerRequest);
    }

    @Override
    public DeleteUserDefinedFunctionResult deleteUserDefinedFunction(DeleteUserDefinedFunctionRequest deleteUserDefinedFunctionRequest) {
        return decoratedAwsGlue.deleteUserDefinedFunction(deleteUserDefinedFunctionRequest);
    }

    @Override
    public DeleteWorkflowResult deleteWorkflow(DeleteWorkflowRequest deleteWorkflowRequest) {
        return decoratedAwsGlue.deleteWorkflow(deleteWorkflowRequest);
    }

    @Override
    public GetBlueprintResult getBlueprint(GetBlueprintRequest getBlueprintRequest) {
        return decoratedAwsGlue.getBlueprint(getBlueprintRequest);
    }

    @Override
    public GetBlueprintRunResult getBlueprintRun(GetBlueprintRunRequest getBlueprintRunRequest) {
        return decoratedAwsGlue.getBlueprintRun(getBlueprintRunRequest);
    }

    @Override
    public GetBlueprintRunsResult getBlueprintRuns(GetBlueprintRunsRequest getBlueprintRunsRequest) {
        return decoratedAwsGlue.getBlueprintRuns(getBlueprintRunsRequest);
    }

    @Override
    public GetCatalogImportStatusResult getCatalogImportStatus(GetCatalogImportStatusRequest getCatalogImportStatusRequest) {
        return decoratedAwsGlue.getCatalogImportStatus(getCatalogImportStatusRequest);
    }

    @Override
    public GetClassifierResult getClassifier(GetClassifierRequest getClassifierRequest) {
        return decoratedAwsGlue.getClassifier(getClassifierRequest);
    }

    @Override
    public GetClassifiersResult getClassifiers(GetClassifiersRequest getClassifiersRequest) {
        return decoratedAwsGlue.getClassifiers(getClassifiersRequest);
    }

    @Override
    public GetConnectionResult getConnection(GetConnectionRequest getConnectionRequest) {
        return decoratedAwsGlue.getConnection(getConnectionRequest);
    }

    @Override
    public GetConnectionsResult getConnections(GetConnectionsRequest getConnectionsRequest) {
        return decoratedAwsGlue.getConnections(getConnectionsRequest);
    }

    @Override
    public GetCrawlerResult getCrawler(GetCrawlerRequest getCrawlerRequest) {
        return decoratedAwsGlue.getCrawler(getCrawlerRequest);
    }

    @Override
    public GetCrawlerMetricsResult getCrawlerMetrics(GetCrawlerMetricsRequest getCrawlerMetricsRequest) {
        return decoratedAwsGlue.getCrawlerMetrics(getCrawlerMetricsRequest);
    }

    @Override
    public GetCrawlersResult getCrawlers(GetCrawlersRequest getCrawlersRequest) {
        return decoratedAwsGlue.getCrawlers(getCrawlersRequest);
    }

    @Override
    public GetCustomEntityTypeResult getCustomEntityType(GetCustomEntityTypeRequest getCustomEntityTypeRequest) {
        return decoratedAwsGlue.getCustomEntityType(getCustomEntityTypeRequest);
    }

    @Override
    public GetDataCatalogEncryptionSettingsResult getDataCatalogEncryptionSettings(GetDataCatalogEncryptionSettingsRequest getDataCatalogEncryptionSettingsRequest) {
        return decoratedAwsGlue.getDataCatalogEncryptionSettings(getDataCatalogEncryptionSettingsRequest);
    }

    @Override
    public GetDatabaseResult getDatabase(GetDatabaseRequest getDatabaseRequest) {
        return decoratedAwsGlue.getDatabase(getDatabaseRequest);
    }

    @Override
    public GetDatabasesResult getDatabases(GetDatabasesRequest getDatabasesRequest) {
        return decoratedAwsGlue.getDatabases(getDatabasesRequest);
    }

    @Override
    public GetDataflowGraphResult getDataflowGraph(GetDataflowGraphRequest getDataflowGraphRequest) {
        return decoratedAwsGlue.getDataflowGraph(getDataflowGraphRequest);
    }

    @Override
    public GetDevEndpointResult getDevEndpoint(GetDevEndpointRequest getDevEndpointRequest) {
        return decoratedAwsGlue.getDevEndpoint(getDevEndpointRequest);
    }

    @Override
    public GetDevEndpointsResult getDevEndpoints(GetDevEndpointsRequest getDevEndpointsRequest) {
        return decoratedAwsGlue.getDevEndpoints(getDevEndpointsRequest);
    }

    @Override
    public GetJobResult getJob(GetJobRequest getJobRequest) {
        return decoratedAwsGlue.getJob(getJobRequest);
    }

    @Override
    public GetJobBookmarkResult getJobBookmark(GetJobBookmarkRequest getJobBookmarkRequest) {
        return decoratedAwsGlue.getJobBookmark(getJobBookmarkRequest);
    }

    @Override
    public GetJobRunResult getJobRun(GetJobRunRequest getJobRunRequest) {
        return decoratedAwsGlue.getJobRun(getJobRunRequest);
    }

    @Override
    public GetJobRunsResult getJobRuns(GetJobRunsRequest getJobRunsRequest) {
        return decoratedAwsGlue.getJobRuns(getJobRunsRequest);
    }

    @Override
    public GetJobsResult getJobs(GetJobsRequest getJobsRequest) {
        return decoratedAwsGlue.getJobs(getJobsRequest);
    }

    @Override
    public GetMLTaskRunResult getMLTaskRun(GetMLTaskRunRequest getMLTaskRunRequest) {
        return decoratedAwsGlue.getMLTaskRun(getMLTaskRunRequest);
    }

    @Override
    public GetMLTaskRunsResult getMLTaskRuns(GetMLTaskRunsRequest getMLTaskRunsRequest) {
        return decoratedAwsGlue.getMLTaskRuns(getMLTaskRunsRequest);
    }

    @Override
    public GetMLTransformResult getMLTransform(GetMLTransformRequest getMLTransformRequest) {
        return decoratedAwsGlue.getMLTransform(getMLTransformRequest);
    }

    @Override
    public GetMLTransformsResult getMLTransforms(GetMLTransformsRequest getMLTransformsRequest) {
        return decoratedAwsGlue.getMLTransforms(getMLTransformsRequest);
    }

    @Override
    public GetMappingResult getMapping(GetMappingRequest getMappingRequest) {
        return decoratedAwsGlue.getMapping(getMappingRequest);
    }

    @Override
    public GetPartitionIndexesResult getPartitionIndexes(GetPartitionIndexesRequest getPartitionIndexesRequest) {
        return decoratedAwsGlue.getPartitionIndexes(getPartitionIndexesRequest);
    }

    @Override
    public GetPartitionResult getPartition(GetPartitionRequest getPartitionRequest) {
        return decoratedAwsGlue.getPartition(getPartitionRequest);
    }

    @Override
    public GetPartitionsResult getPartitions(GetPartitionsRequest getPartitionsRequest) {
        return decoratedAwsGlue.getPartitions(getPartitionsRequest);
    }

    @Override
    public GetPlanResult getPlan(GetPlanRequest getPlanRequest) {
        return decoratedAwsGlue.getPlan(getPlanRequest);
    }

    @Override
    public GetRegistryResult getRegistry(GetRegistryRequest getRegistryRequest) {
        return null;
    }

    @Override
    public GetResourcePolicyResult getResourcePolicy(GetResourcePolicyRequest getResourcePolicyRequest) {
        return decoratedAwsGlue.getResourcePolicy(getResourcePolicyRequest);
    }

    @Override
    public GetSchemaResult getSchema(GetSchemaRequest getSchemaRequest) {
        return null;
    }

    @Override
    public GetSchemaByDefinitionResult getSchemaByDefinition(GetSchemaByDefinitionRequest getSchemaByDefinitionRequest) {
        return null;
    }

    @Override
    public GetSchemaVersionResult getSchemaVersion(GetSchemaVersionRequest getSchemaVersionRequest) {
        return null;
    }

    @Override
    public GetSchemaVersionsDiffResult getSchemaVersionsDiff(GetSchemaVersionsDiffRequest getSchemaVersionsDiffRequest) {
        return null;
    }

    @Override
    public GetSecurityConfigurationResult getSecurityConfiguration(GetSecurityConfigurationRequest getSecurityConfigurationRequest) {
        return decoratedAwsGlue.getSecurityConfiguration(getSecurityConfigurationRequest);
    }

    @Override
    public GetSecurityConfigurationsResult getSecurityConfigurations(GetSecurityConfigurationsRequest getSecurityConfigurationsRequest) {
        return decoratedAwsGlue.getSecurityConfigurations(getSecurityConfigurationsRequest);
    }

    @Override
    public GetSessionResult getSession(GetSessionRequest getSessionRequest) {
        return decoratedAwsGlue.getSession(getSessionRequest);
    }

    @Override
    public GetStatementResult getStatement(GetStatementRequest getStatementRequest) {
        return decoratedAwsGlue.getStatement(getStatementRequest);
    }

    @Override
    public GetTableResult getTable(GetTableRequest getTableRequest) {
        return decoratedAwsGlue.getTable(getTableRequest);
    }

    @Override
    public GetTableOptimizerResult getTableOptimizer(GetTableOptimizerRequest getTableOptimizerRequest) {
        return null;
    }

    @Override
    public GetTableVersionResult getTableVersion(GetTableVersionRequest getTableVersionRequest) {
        return decoratedAwsGlue.getTableVersion(getTableVersionRequest);
    }

    @Override
    public GetTableVersionsResult getTableVersions(GetTableVersionsRequest getTableVersionsRequest) {
        return decoratedAwsGlue.getTableVersions(getTableVersionsRequest);
    }

    @Override
    public GetTablesResult getTables(GetTablesRequest getTablesRequest) {
        return decoratedAwsGlue.getTables(getTablesRequest);
    }

    @Override
    public GetTagsResult getTags(GetTagsRequest getTagsRequest) {
        return decoratedAwsGlue.getTags(getTagsRequest);
    }

    @Override
    public GetTriggerResult getTrigger(GetTriggerRequest getTriggerRequest) {
        return decoratedAwsGlue.getTrigger(getTriggerRequest);
    }

    @Override
    public GetTriggersResult getTriggers(GetTriggersRequest getTriggersRequest) {
        return decoratedAwsGlue.getTriggers(getTriggersRequest);
    }

    @Override
    public GetUnfilteredPartitionMetadataResult getUnfilteredPartitionMetadata(GetUnfilteredPartitionMetadataRequest getUnfilteredPartitionMetadataRequest) {
        return decoratedAwsGlue.getUnfilteredPartitionMetadata(getUnfilteredPartitionMetadataRequest);
    }

    @Override
    public GetUnfilteredPartitionsMetadataResult getUnfilteredPartitionsMetadata(GetUnfilteredPartitionsMetadataRequest getUnfilteredPartitionsMetadataRequest) {
        return decoratedAwsGlue.getUnfilteredPartitionsMetadata(getUnfilteredPartitionsMetadataRequest);
    }

    @Override
    public GetUnfilteredTableMetadataResult getUnfilteredTableMetadata(GetUnfilteredTableMetadataRequest getUnfilteredTableMetadataRequest) {
        return decoratedAwsGlue.getUnfilteredTableMetadata(getUnfilteredTableMetadataRequest);
    }

    @Override
    public GetUserDefinedFunctionResult getUserDefinedFunction(GetUserDefinedFunctionRequest getUserDefinedFunctionRequest) {
        return decoratedAwsGlue.getUserDefinedFunction(getUserDefinedFunctionRequest);
    }

    @Override
    public GetUserDefinedFunctionsResult getUserDefinedFunctions(GetUserDefinedFunctionsRequest getUserDefinedFunctionsRequest) {
        return decoratedAwsGlue.getUserDefinedFunctions(getUserDefinedFunctionsRequest);
    }

    @Override
    public GetWorkflowResult getWorkflow(GetWorkflowRequest getWorkflowRequest) {
        return decoratedAwsGlue.getWorkflow(getWorkflowRequest);
    }

    @Override
    public GetWorkflowRunResult getWorkflowRun(GetWorkflowRunRequest getWorkflowRunRequest) {
        return decoratedAwsGlue.getWorkflowRun(getWorkflowRunRequest);
    }

    @Override
    public GetWorkflowRunPropertiesResult getWorkflowRunProperties(GetWorkflowRunPropertiesRequest getWorkflowRunPropertiesRequest) {
        return decoratedAwsGlue.getWorkflowRunProperties(getWorkflowRunPropertiesRequest);
    }

    @Override
    public GetWorkflowRunsResult getWorkflowRuns(GetWorkflowRunsRequest getWorkflowRunsRequest) {
        return decoratedAwsGlue.getWorkflowRuns(getWorkflowRunsRequest);
    }

    @Override
    public ImportCatalogToGlueResult importCatalogToGlue(ImportCatalogToGlueRequest importCatalogToGlueRequest) {
        return decoratedAwsGlue.importCatalogToGlue(importCatalogToGlueRequest);
    }

    @Override
    public ListBlueprintsResult listBlueprints(ListBlueprintsRequest listBlueprintsRequest) {
        return decoratedAwsGlue.listBlueprints(listBlueprintsRequest);
    }

    @Override
    public ListColumnStatisticsTaskRunsResult listColumnStatisticsTaskRuns(ListColumnStatisticsTaskRunsRequest listColumnStatisticsTaskRunsRequest) {
        return null;
    }

    @Override
    public ListCrawlersResult listCrawlers(ListCrawlersRequest listCrawlersRequest) {
        return decoratedAwsGlue.listCrawlers(listCrawlersRequest);
    }

    @Override
    public ListCrawlsResult listCrawls(ListCrawlsRequest listCrawlsRequest) {
        return decoratedAwsGlue.listCrawls(listCrawlsRequest);
    }

    @Override
    public ListCustomEntityTypesResult listCustomEntityTypes(ListCustomEntityTypesRequest listCustomEntityTypesRequest) {
        return decoratedAwsGlue.listCustomEntityTypes(listCustomEntityTypesRequest);
    }

    @Override
    public ListDevEndpointsResult listDevEndpoints(ListDevEndpointsRequest listDevEndpointsRequest) {
        return decoratedAwsGlue.listDevEndpoints(listDevEndpointsRequest);
    }

    @Override
    public ListJobsResult listJobs(ListJobsRequest listJobsRequest) {
        return decoratedAwsGlue.listJobs(listJobsRequest);
    }

    @Override
    public ListMLTransformsResult listMLTransforms(ListMLTransformsRequest listMLTransformsRequest) {
        return decoratedAwsGlue.listMLTransforms(listMLTransformsRequest);
    }

    @Override
    public ListRegistriesResult listRegistries(ListRegistriesRequest listRegistriesRequest) {
        return null;
    }

    @Override
    public ListSchemaVersionsResult listSchemaVersions(ListSchemaVersionsRequest listSchemaVersionsRequest) {
        return null;
    }

    @Override
    public ListSchemasResult listSchemas(ListSchemasRequest listSchemasRequest) {
        return null;
    }

    @Override
    public ListSessionsResult listSessions(ListSessionsRequest listSessionsRequest) {
        return decoratedAwsGlue.listSessions(listSessionsRequest);
    }

    @Override
    public ListStatementsResult listStatements(ListStatementsRequest listStatementsRequest) {
        return decoratedAwsGlue.listStatements(listStatementsRequest);
    }

    @Override
    public ListTableOptimizerRunsResult listTableOptimizerRuns(ListTableOptimizerRunsRequest listTableOptimizerRunsRequest) {
        return null;
    }

    @Override
    public ListTriggersResult listTriggers(ListTriggersRequest listTriggersRequest) {
        return decoratedAwsGlue.listTriggers(listTriggersRequest);
    }

    @Override
    public ListWorkflowsResult listWorkflows(ListWorkflowsRequest listWorkflowsRequest) {
        return decoratedAwsGlue.listWorkflows(listWorkflowsRequest);
    }

    @Override
    public PutDataCatalogEncryptionSettingsResult putDataCatalogEncryptionSettings(PutDataCatalogEncryptionSettingsRequest putDataCatalogEncryptionSettingsRequest) {
        return decoratedAwsGlue.putDataCatalogEncryptionSettings(putDataCatalogEncryptionSettingsRequest);
    }

    @Override
    public PutResourcePolicyResult putResourcePolicy(PutResourcePolicyRequest putResourcePolicyRequest) {
        return decoratedAwsGlue.putResourcePolicy(putResourcePolicyRequest);
    }

    @Override
    public PutSchemaVersionMetadataResult putSchemaVersionMetadata(PutSchemaVersionMetadataRequest putSchemaVersionMetadataRequest) {
        return null;
    }

    @Override
    public PutWorkflowRunPropertiesResult putWorkflowRunProperties(PutWorkflowRunPropertiesRequest putWorkflowRunPropertiesRequest) {
        return decoratedAwsGlue.putWorkflowRunProperties(putWorkflowRunPropertiesRequest);
    }

    @Override
    public QuerySchemaVersionMetadataResult querySchemaVersionMetadata(QuerySchemaVersionMetadataRequest querySchemaVersionMetadataRequest) {
        return null;
    }

    @Override
    public RegisterSchemaVersionResult registerSchemaVersion(RegisterSchemaVersionRequest registerSchemaVersionRequest) {
        return null;
    }

    @Override
    public RemoveSchemaVersionMetadataResult removeSchemaVersionMetadata(RemoveSchemaVersionMetadataRequest removeSchemaVersionMetadataRequest) {
        return null;
    }

    @Override
    public ResetJobBookmarkResult resetJobBookmark(ResetJobBookmarkRequest resetJobBookmarkRequest) {
        return decoratedAwsGlue.resetJobBookmark(resetJobBookmarkRequest);
    }

    @Override
    public SearchTablesResult searchTables(SearchTablesRequest searchTablesRequest) {
        return decoratedAwsGlue.searchTables(searchTablesRequest);
    }

    @Override
    public StartBlueprintRunResult startBlueprintRun(StartBlueprintRunRequest startBlueprintRunRequest) {
        return decoratedAwsGlue.startBlueprintRun(startBlueprintRunRequest);
    }

    @Override
    public StartColumnStatisticsTaskRunResult startColumnStatisticsTaskRun(StartColumnStatisticsTaskRunRequest startColumnStatisticsTaskRunRequest) {
        return null;
    }

    @Override
    public StartCrawlerResult startCrawler(StartCrawlerRequest startCrawlerRequest) {
        return decoratedAwsGlue.startCrawler(startCrawlerRequest);
    }

    @Override
    public StartCrawlerScheduleResult startCrawlerSchedule(StartCrawlerScheduleRequest startCrawlerScheduleRequest) {
        return decoratedAwsGlue.startCrawlerSchedule(startCrawlerScheduleRequest);
    }

    @Override
    public StartExportLabelsTaskRunResult startExportLabelsTaskRun(StartExportLabelsTaskRunRequest startExportLabelsTaskRunRequest) {
        return decoratedAwsGlue.startExportLabelsTaskRun(startExportLabelsTaskRunRequest);
    }

    @Override
    public StartImportLabelsTaskRunResult startImportLabelsTaskRun(StartImportLabelsTaskRunRequest startImportLabelsTaskRunRequest) {
        return decoratedAwsGlue.startImportLabelsTaskRun(startImportLabelsTaskRunRequest);
    }

    @Override
    public StartJobRunResult startJobRun(StartJobRunRequest startJobRunRequest) {
        return decoratedAwsGlue.startJobRun(startJobRunRequest);
    }

    @Override
    public StartMLEvaluationTaskRunResult startMLEvaluationTaskRun(StartMLEvaluationTaskRunRequest startMLEvaluationTaskRunRequest) {
        return decoratedAwsGlue.startMLEvaluationTaskRun(startMLEvaluationTaskRunRequest);
    }

    @Override
    public StartMLLabelingSetGenerationTaskRunResult startMLLabelingSetGenerationTaskRun(StartMLLabelingSetGenerationTaskRunRequest startMLLabelingSetGenerationTaskRunRequest) {
        return decoratedAwsGlue.startMLLabelingSetGenerationTaskRun(startMLLabelingSetGenerationTaskRunRequest);
    }

    @Override
    public StartTriggerResult startTrigger(StartTriggerRequest startTriggerRequest) {
        return decoratedAwsGlue.startTrigger(startTriggerRequest);
    }

    @Override
    public StartWorkflowRunResult startWorkflowRun(StartWorkflowRunRequest startWorkflowRunRequest) {
        return decoratedAwsGlue.startWorkflowRun(startWorkflowRunRequest);
    }

    @Override
    public StopColumnStatisticsTaskRunResult stopColumnStatisticsTaskRun(StopColumnStatisticsTaskRunRequest stopColumnStatisticsTaskRunRequest) {
        return null;
    }

    @Override
    public StopCrawlerResult stopCrawler(StopCrawlerRequest stopCrawlerRequest) {
        return decoratedAwsGlue.stopCrawler(stopCrawlerRequest);
    }

    @Override
    public StopCrawlerScheduleResult stopCrawlerSchedule(StopCrawlerScheduleRequest stopCrawlerScheduleRequest) {
        return decoratedAwsGlue.stopCrawlerSchedule(stopCrawlerScheduleRequest);
    }

    @Override
    public StopSessionResult stopSession(StopSessionRequest stopSessionRequest) {
        return decoratedAwsGlue.stopSession(stopSessionRequest);
    }

    @Override
    public StopTriggerResult stopTrigger(StopTriggerRequest stopTriggerRequest) {
        return decoratedAwsGlue.stopTrigger(stopTriggerRequest);
    }

    @Override
    public StopWorkflowRunResult stopWorkflowRun(StopWorkflowRunRequest stopWorkflowRunRequest) {
        return decoratedAwsGlue.stopWorkflowRun(stopWorkflowRunRequest);
    }

    @Override
    public TagResourceResult tagResource(TagResourceRequest tagResourceRequest) {
        return decoratedAwsGlue.tagResource(tagResourceRequest);
    }

    @Override
    public UntagResourceResult untagResource(UntagResourceRequest untagResourceRequest) {
        return decoratedAwsGlue.untagResource(untagResourceRequest);
    }

    @Override
    public UpdateBlueprintResult updateBlueprint(UpdateBlueprintRequest updateBlueprintRequest) {
        return decoratedAwsGlue.updateBlueprint(updateBlueprintRequest);
    }

    @Override
    public UpdateClassifierResult updateClassifier(UpdateClassifierRequest updateClassifierRequest) {
        return decoratedAwsGlue.updateClassifier(updateClassifierRequest);
    }

    @Override
    public UpdateConnectionResult updateConnection(UpdateConnectionRequest updateConnectionRequest) {
        return decoratedAwsGlue.updateConnection(updateConnectionRequest);
    }

    @Override
    public UpdateCrawlerResult updateCrawler(UpdateCrawlerRequest updateCrawlerRequest) {
        return decoratedAwsGlue.updateCrawler(updateCrawlerRequest);
    }

    @Override
    public UpdateCrawlerScheduleResult updateCrawlerSchedule(UpdateCrawlerScheduleRequest updateCrawlerScheduleRequest) {
        return decoratedAwsGlue.updateCrawlerSchedule(updateCrawlerScheduleRequest);
    }

    @Override
    public UpdateDatabaseResult updateDatabase(UpdateDatabaseRequest updateDatabaseRequest) {
        return decoratedAwsGlue.updateDatabase(updateDatabaseRequest);
    }

    @Override
    public UpdateDevEndpointResult updateDevEndpoint(UpdateDevEndpointRequest updateDevEndpointRequest) {
        return decoratedAwsGlue.updateDevEndpoint(updateDevEndpointRequest);
    }

    @Override
    public UpdateJobResult updateJob(UpdateJobRequest updateJobRequest) {
        return decoratedAwsGlue.updateJob(updateJobRequest);
    }

    @Override
    public UpdateMLTransformResult updateMLTransform(UpdateMLTransformRequest updateMLTransformRequest) {
        return decoratedAwsGlue.updateMLTransform(updateMLTransformRequest);
    }

    @Override
    public UpdatePartitionResult updatePartition(UpdatePartitionRequest updatePartitionRequest) {
        return decoratedAwsGlue.updatePartition(updatePartitionRequest);
    }

    @Override
    public UpdateRegistryResult updateRegistry(UpdateRegistryRequest updateRegistryRequest) {
        return null;
    }

    @Override
    public UpdateSchemaResult updateSchema(UpdateSchemaRequest updateSchemaRequest) {
        return null;
    }

    @Override
    public UpdateTableResult updateTable(UpdateTableRequest updateTableRequest) {
        return decoratedAwsGlue.updateTable(updateTableRequest);
    }

    @Override
    public UpdateTableOptimizerResult updateTableOptimizer(UpdateTableOptimizerRequest updateTableOptimizerRequest) {
        return null;
    }

    @Override
    public UpdateTriggerResult updateTrigger(UpdateTriggerRequest updateTriggerRequest) {
        return decoratedAwsGlue.updateTrigger(updateTriggerRequest);
    }

    @Override
    public UpdateUserDefinedFunctionResult updateUserDefinedFunction(UpdateUserDefinedFunctionRequest updateUserDefinedFunctionRequest) {
        return decoratedAwsGlue.updateUserDefinedFunction(updateUserDefinedFunctionRequest);
    }

    @Override
    public UpdateWorkflowResult updateWorkflow(UpdateWorkflowRequest updateWorkflowRequest) {
        return decoratedAwsGlue.updateWorkflow(updateWorkflowRequest);
    }

    @Override
    public void shutdown() {
        decoratedAwsGlue.shutdown();
    }

    @Override
    public ResponseMetadata getCachedResponseMetadata(AmazonWebServiceRequest amazonWebServiceRequest) {
        return decoratedAwsGlue.getCachedResponseMetadata(amazonWebServiceRequest);
    }


    @Override
    public UpdateColumnStatisticsForTableResult updateColumnStatisticsForTable(UpdateColumnStatisticsForTableRequest updateColumnStatisticsForTableRequest) {
        return decoratedAwsGlue.updateColumnStatisticsForTable(updateColumnStatisticsForTableRequest);
    }

    @Override
    public UpdateColumnStatisticsForPartitionResult updateColumnStatisticsForPartition(UpdateColumnStatisticsForPartitionRequest updateColumnStatisticsForPartitionRequest) {
        return decoratedAwsGlue.updateColumnStatisticsForPartition(updateColumnStatisticsForPartitionRequest);
    }

    @Override
    public ResumeWorkflowRunResult resumeWorkflowRun(ResumeWorkflowRunRequest resumeWorkflowRunRequest) {
        return decoratedAwsGlue.resumeWorkflowRun(resumeWorkflowRunRequest);
    }

    @Override
    public RunStatementResult runStatement(RunStatementRequest runStatementRequest) {
        return decoratedAwsGlue.runStatement(runStatementRequest);
    }

    @Override
    public GetResourcePoliciesResult getResourcePolicies(GetResourcePoliciesRequest getResourcePoliciesRequest) {
        return decoratedAwsGlue.getResourcePolicies(getResourcePoliciesRequest);
    }

    @Override
    public GetColumnStatisticsForTableResult getColumnStatisticsForTable(GetColumnStatisticsForTableRequest getColumnStatisticsForTableRequest) {
        return decoratedAwsGlue.getColumnStatisticsForTable(getColumnStatisticsForTableRequest);
    }

    @Override
    public GetColumnStatisticsTaskRunResult getColumnStatisticsTaskRun(GetColumnStatisticsTaskRunRequest getColumnStatisticsTaskRunRequest) {
        return null;
    }

    @Override
    public GetColumnStatisticsTaskRunsResult getColumnStatisticsTaskRuns(GetColumnStatisticsTaskRunsRequest getColumnStatisticsTaskRunsRequest) {
        return null;
    }

    @Override
    public GetColumnStatisticsForPartitionResult getColumnStatisticsForPartition(GetColumnStatisticsForPartitionRequest getColumnStatisticsForPartitionRequest) {
        return decoratedAwsGlue.getColumnStatisticsForPartition(getColumnStatisticsForPartitionRequest);
    }

    @Override
    public DeleteColumnStatisticsForTableResult deleteColumnStatisticsForTable(DeleteColumnStatisticsForTableRequest deleteColumnStatisticsForTableRequest) {
        return decoratedAwsGlue.deleteColumnStatisticsForTable(deleteColumnStatisticsForTableRequest);
    }

    @Override
    public DeleteColumnStatisticsForPartitionResult deleteColumnStatisticsForPartition(DeleteColumnStatisticsForPartitionRequest deleteColumnStatisticsForPartitionRequest) {
        return decoratedAwsGlue.deleteColumnStatisticsForPartition(deleteColumnStatisticsForPartitionRequest);
    }

    @Override
    public BatchGetBlueprintsResult batchGetBlueprints(BatchGetBlueprintsRequest batchGetBlueprintsRequest) {
        return decoratedAwsGlue.batchGetBlueprints(batchGetBlueprintsRequest);
    }

    @Override
    public UpdateSourceControlFromJobResult updateSourceControlFromJob(UpdateSourceControlFromJobRequest updateSourceControlFromJobRequest) {
        return null;
    }

    @Override
    public BatchGetDataQualityResultResult batchGetDataQualityResult(BatchGetDataQualityResultRequest batchGetDataQualityResultRequest) {
        return null;
    }

    @Override
    public CancelDataQualityRuleRecommendationRunResult cancelDataQualityRuleRecommendationRun(CancelDataQualityRuleRecommendationRunRequest cancelDataQualityRuleRecommendationRunRequest) {
        return null;
    }

    @Override
    public CancelDataQualityRulesetEvaluationRunResult cancelDataQualityRulesetEvaluationRun(CancelDataQualityRulesetEvaluationRunRequest cancelDataQualityRulesetEvaluationRunRequest) {
        return null;
    }

    @Override
    public CreateDataQualityRulesetResult createDataQualityRuleset(CreateDataQualityRulesetRequest createDataQualityRulesetRequest) {
        return null;
    }

    @Override
    public DeleteDataQualityRulesetResult deleteDataQualityRuleset(DeleteDataQualityRulesetRequest deleteDataQualityRulesetRequest) {
        return null;
    }

    @Override
    public GetDataQualityResultResult getDataQualityResult(GetDataQualityResultRequest getDataQualityResultRequest) {
        return null;
    }

    @Override
    public GetDataQualityRuleRecommendationRunResult getDataQualityRuleRecommendationRun(GetDataQualityRuleRecommendationRunRequest getDataQualityRuleRecommendationRunRequest) {
        return null;
    }

    @Override
    public GetDataQualityRulesetResult getDataQualityRuleset(GetDataQualityRulesetRequest getDataQualityRulesetRequest) {
        return null;
    }

    @Override
    public GetDataQualityRulesetEvaluationRunResult getDataQualityRulesetEvaluationRun(GetDataQualityRulesetEvaluationRunRequest getDataQualityRulesetEvaluationRunRequest) {
        return null;
    }

    @Override
    public ListDataQualityResultsResult listDataQualityResults(ListDataQualityResultsRequest listDataQualityResultsRequest) {
        return null;
    }

    @Override
    public ListDataQualityRuleRecommendationRunsResult listDataQualityRuleRecommendationRuns(ListDataQualityRuleRecommendationRunsRequest listDataQualityRuleRecommendationRunsRequest) {
        return null;
    }

    @Override
    public ListDataQualityRulesetEvaluationRunsResult listDataQualityRulesetEvaluationRuns(ListDataQualityRulesetEvaluationRunsRequest listDataQualityRulesetEvaluationRunsRequest) {
        return null;
    }

    @Override
    public ListDataQualityRulesetsResult listDataQualityRulesets(ListDataQualityRulesetsRequest listDataQualityRulesetsRequest) {
        return null;
    }

    @Override
    public StartDataQualityRuleRecommendationRunResult startDataQualityRuleRecommendationRun(StartDataQualityRuleRecommendationRunRequest startDataQualityRuleRecommendationRunRequest) {
        return null;
    }

    @Override
    public StartDataQualityRulesetEvaluationRunResult startDataQualityRulesetEvaluationRun(StartDataQualityRulesetEvaluationRunRequest startDataQualityRulesetEvaluationRunRequest) {
        return null;
    }

    @Override
    public UpdateDataQualityRulesetResult updateDataQualityRuleset(UpdateDataQualityRulesetRequest updateDataQualityRulesetRequest) {
        return null;
    }

    @Override
    public UpdateJobFromSourceControlResult updateJobFromSourceControl(UpdateJobFromSourceControlRequest updateJobFromSourceControlRequest) {
        return null;
    }
}

