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

import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.BatchCreatePartitionRequest;
import com.amazonaws.services.glue.model.BatchCreatePartitionResult;
import com.amazonaws.services.glue.model.BatchDeletePartitionRequest;
import com.amazonaws.services.glue.model.BatchDeletePartitionResult;
import com.amazonaws.services.glue.model.BatchDeleteTableRequest;
import com.amazonaws.services.glue.model.BatchDeleteTableResult;
import com.amazonaws.services.glue.model.BatchGetPartitionRequest;
import com.amazonaws.services.glue.model.BatchGetPartitionResult;
import com.amazonaws.services.glue.model.CreateDatabaseRequest;
import com.amazonaws.services.glue.model.CreateDatabaseResult;
import com.amazonaws.services.glue.model.CreatePartitionRequest;
import com.amazonaws.services.glue.model.CreatePartitionResult;
import com.amazonaws.services.glue.model.CreateTableRequest;
import com.amazonaws.services.glue.model.CreateTableResult;
import com.amazonaws.services.glue.model.CreateUserDefinedFunctionRequest;
import com.amazonaws.services.glue.model.CreateUserDefinedFunctionResult;
import com.amazonaws.services.glue.model.DeleteDatabaseRequest;
import com.amazonaws.services.glue.model.DeleteDatabaseResult;
import com.amazonaws.services.glue.model.DeletePartitionRequest;
import com.amazonaws.services.glue.model.DeletePartitionResult;
import com.amazonaws.services.glue.model.DeleteTableRequest;
import com.amazonaws.services.glue.model.DeleteTableResult;
import com.amazonaws.services.glue.model.DeleteUserDefinedFunctionRequest;
import com.amazonaws.services.glue.model.DeleteUserDefinedFunctionResult;
import com.amazonaws.services.glue.model.GetDatabaseRequest;
import com.amazonaws.services.glue.model.GetDatabaseResult;
import com.amazonaws.services.glue.model.GetPartitionRequest;
import com.amazonaws.services.glue.model.GetPartitionResult;
import com.amazonaws.services.glue.model.GetPartitionsRequest;
import com.amazonaws.services.glue.model.GetPartitionsResult;
import com.amazonaws.services.glue.model.GetTableRequest;
import com.amazonaws.services.glue.model.GetTableResult;
import com.amazonaws.services.glue.model.GetTableVersionsRequest;
import com.amazonaws.services.glue.model.GetTableVersionsResult;
import com.amazonaws.services.glue.model.GetTablesRequest;
import com.amazonaws.services.glue.model.GetTablesResult;
import com.amazonaws.services.glue.model.GetUserDefinedFunctionRequest;
import com.amazonaws.services.glue.model.GetUserDefinedFunctionResult;
import com.amazonaws.services.glue.model.GetUserDefinedFunctionsRequest;
import com.amazonaws.services.glue.model.GetUserDefinedFunctionsResult;
import com.amazonaws.services.glue.model.UpdateDatabaseRequest;
import com.amazonaws.services.glue.model.UpdateDatabaseResult;
import com.amazonaws.services.glue.model.UpdatePartitionRequest;
import com.amazonaws.services.glue.model.UpdatePartitionResult;
import com.amazonaws.services.glue.model.UpdateTableRequest;
import com.amazonaws.services.glue.model.UpdateTableResult;
import com.amazonaws.services.glue.model.UpdateUserDefinedFunctionRequest;
import com.amazonaws.services.glue.model.UpdateUserDefinedFunctionResult;
import com.google.common.base.Strings;

import java.util.function.Consumer;
import java.util.function.Supplier;


public class AWSGlueMultipleCatalogDecorator extends AWSGlueDecoratorBase {

    // We're not importing this from Hive's Warehouse class as the package name is changed between Hive 1.x and Hive 3.x
    private static final String DEFAULT_DATABASE_NAME = "default";

    private String catalogSeparator;

    public AWSGlueMultipleCatalogDecorator(AWSGlue awsGlueToBeDecorated, String catalogSeparator) {
        super(awsGlueToBeDecorated);
        this.catalogSeparator = catalogSeparator;
    }

    private void configureRequest(Supplier<String> getDatabaseFunc,
            Consumer<String> setDatabaseFunc,
            Consumer<String> setCatalogFunc) {
        if (!Strings.isNullOrEmpty(this.catalogSeparator) && (getDatabaseFunc.get() != null)
                && !getDatabaseFunc.get().equals(DEFAULT_DATABASE_NAME)) {
            String databaseName = getDatabaseFunc.get();
            int idx = databaseName.indexOf(this.catalogSeparator);
            if (idx >= 0) {
                setCatalogFunc.accept(databaseName.substring(0, idx));
                setDatabaseFunc.accept(databaseName.substring(idx + this.catalogSeparator.length()));
            }
        }
    }

    @Override
    public BatchCreatePartitionResult batchCreatePartition(BatchCreatePartitionRequest batchCreatePartitionRequest) {
        configureRequest(
                batchCreatePartitionRequest::getDatabaseName,
                batchCreatePartitionRequest::setDatabaseName,
                batchCreatePartitionRequest::setCatalogId
        );
        return super.batchCreatePartition(batchCreatePartitionRequest);
    }

    @Override
    public BatchDeletePartitionResult batchDeletePartition(BatchDeletePartitionRequest batchDeletePartitionRequest) {
        configureRequest(
                batchDeletePartitionRequest::getDatabaseName,
                batchDeletePartitionRequest::setDatabaseName,
                batchDeletePartitionRequest::setCatalogId
        );
        return super.batchDeletePartition(batchDeletePartitionRequest);
    }

    @Override
    public BatchDeleteTableResult batchDeleteTable(BatchDeleteTableRequest batchDeleteTableRequest) {
        configureRequest(
                batchDeleteTableRequest::getDatabaseName,
                batchDeleteTableRequest::setDatabaseName,
                batchDeleteTableRequest::setCatalogId
        );
        return super.batchDeleteTable(batchDeleteTableRequest);
    }

    @Override
    public BatchGetPartitionResult batchGetPartition(BatchGetPartitionRequest batchGetPartitionRequest) {
        String originalDatabaseName = batchGetPartitionRequest.getDatabaseName();
        configureRequest(
                batchGetPartitionRequest::getDatabaseName,
                batchGetPartitionRequest::setDatabaseName,
                batchGetPartitionRequest::setCatalogId
        );
        BatchGetPartitionResult result = super.batchGetPartition(batchGetPartitionRequest);
        result.getPartitions().forEach(partition -> partition.setDatabaseName(originalDatabaseName));
        return result;
    }

    @Override
    public CreateDatabaseResult createDatabase(CreateDatabaseRequest createDatabaseRequest) {
        configureRequest(
                () -> createDatabaseRequest.getDatabaseInput().getName(),
                name -> createDatabaseRequest.getDatabaseInput().setName(name),
                createDatabaseRequest::setCatalogId
        );
        return super.createDatabase(createDatabaseRequest);
    }

    @Override
    public CreatePartitionResult createPartition(CreatePartitionRequest createPartitionRequest) {
        configureRequest(
                createPartitionRequest::getDatabaseName,
                createPartitionRequest::setDatabaseName,
                createPartitionRequest::setCatalogId
        );
        return super.createPartition(createPartitionRequest);
    }

    @Override
    public CreateTableResult createTable(CreateTableRequest createTableRequest) {
        configureRequest(
                createTableRequest::getDatabaseName,
                createTableRequest::setDatabaseName,
                createTableRequest::setCatalogId
        );
        return super.createTable(createTableRequest);
    }

    @Override
    public CreateUserDefinedFunctionResult createUserDefinedFunction(CreateUserDefinedFunctionRequest createUserDefinedFunctionRequest) {
        configureRequest(
                createUserDefinedFunctionRequest::getDatabaseName,
                createUserDefinedFunctionRequest::setDatabaseName,
                createUserDefinedFunctionRequest::setCatalogId
        );
        return super.createUserDefinedFunction(createUserDefinedFunctionRequest);
    }

    @Override
    public DeleteDatabaseResult deleteDatabase(DeleteDatabaseRequest deleteDatabaseRequest) {
        configureRequest(
                deleteDatabaseRequest::getName,
                deleteDatabaseRequest::setName,
                deleteDatabaseRequest::setCatalogId
        );
        return super.deleteDatabase(deleteDatabaseRequest);
    }

    @Override
    public DeletePartitionResult deletePartition(DeletePartitionRequest deletePartitionRequest) {
        configureRequest(
                deletePartitionRequest::getDatabaseName,
                deletePartitionRequest::setDatabaseName,
                deletePartitionRequest::setCatalogId
        );
        return super.deletePartition(deletePartitionRequest);
    }

    @Override
    public DeleteTableResult deleteTable(DeleteTableRequest deleteTableRequest) {
        configureRequest(
                deleteTableRequest::getDatabaseName,
                deleteTableRequest::setDatabaseName,
                deleteTableRequest::setCatalogId
        );
        return super.deleteTable(deleteTableRequest);
    }

    @Override
    public DeleteUserDefinedFunctionResult deleteUserDefinedFunction(DeleteUserDefinedFunctionRequest deleteUserDefinedFunctionRequest) {
        configureRequest(
                deleteUserDefinedFunctionRequest::getDatabaseName,
                deleteUserDefinedFunctionRequest::setDatabaseName,
                deleteUserDefinedFunctionRequest::setCatalogId
        );
        return super.deleteUserDefinedFunction(deleteUserDefinedFunctionRequest);
    }

    @Override
    public GetDatabaseResult getDatabase(GetDatabaseRequest getDatabaseRequest) {
        String originalDatabaseName = getDatabaseRequest.getName();
        configureRequest(
                getDatabaseRequest::getName,
                getDatabaseRequest::setName,
                getDatabaseRequest::setCatalogId
        );
        GetDatabaseResult result = super.getDatabase(getDatabaseRequest);
        result.getDatabase().setName(originalDatabaseName);
        return result;
    }

    @Override
    public GetPartitionResult getPartition(GetPartitionRequest getPartitionRequest) {
        String originalDatabaseName = getPartitionRequest.getDatabaseName();
        configureRequest(
                getPartitionRequest::getDatabaseName,
                getPartitionRequest::setDatabaseName,
                getPartitionRequest::setCatalogId
        );
        GetPartitionResult result = super.getPartition(getPartitionRequest);
        result.getPartition().setDatabaseName(originalDatabaseName);
        return result;
    }

    @Override
    public GetPartitionsResult getPartitions(GetPartitionsRequest getPartitionsRequest) {
        String originalDatabaseName = getPartitionsRequest.getDatabaseName();
        configureRequest(
                getPartitionsRequest::getDatabaseName,
                getPartitionsRequest::setDatabaseName,
                getPartitionsRequest::setCatalogId
        );
        GetPartitionsResult result = super.getPartitions(getPartitionsRequest);
        result.getPartitions().forEach(partition -> partition.setDatabaseName(originalDatabaseName));
        return result;
    }

    @Override
    public GetTableResult getTable(GetTableRequest getTableRequest) {
        String originalDatabaseName = getTableRequest.getDatabaseName();
        configureRequest(
                getTableRequest::getDatabaseName,
                getTableRequest::setDatabaseName,
                getTableRequest::setCatalogId
        );
        GetTableResult result = super.getTable(getTableRequest);
        result.getTable().setDatabaseName(originalDatabaseName);
        return result;
    }

    @Override
    public GetTableVersionsResult getTableVersions(GetTableVersionsRequest getTableVersionsRequest) {
        String originalDatabaseName = getTableVersionsRequest.getDatabaseName();
        configureRequest(
                getTableVersionsRequest::getDatabaseName,
                getTableVersionsRequest::setDatabaseName,
                getTableVersionsRequest::setCatalogId
        );
        GetTableVersionsResult result = super.getTableVersions(getTableVersionsRequest);
        result.getTableVersions().forEach(tableVersion -> tableVersion.getTable().setDatabaseName(originalDatabaseName));
        return result;
    }

    @Override
    public GetTablesResult getTables(GetTablesRequest getTablesRequest) {
        String originalDatabaseName = getTablesRequest.getDatabaseName();
        configureRequest(
                getTablesRequest::getDatabaseName,
                getTablesRequest::setDatabaseName,
                getTablesRequest::setCatalogId
        );
        GetTablesResult result = super.getTables(getTablesRequest);
        result.getTableList().forEach(table -> table.setDatabaseName(originalDatabaseName));
        return result;
    }

    @Override
    public GetUserDefinedFunctionResult getUserDefinedFunction(GetUserDefinedFunctionRequest getUserDefinedFunctionRequest) {
        configureRequest(
                getUserDefinedFunctionRequest::getDatabaseName,
                getUserDefinedFunctionRequest::setDatabaseName,
                getUserDefinedFunctionRequest::setCatalogId
        );
        return super.getUserDefinedFunction(getUserDefinedFunctionRequest);
    }

    @Override
    public GetUserDefinedFunctionsResult getUserDefinedFunctions(GetUserDefinedFunctionsRequest getUserDefinedFunctionsRequest) {
        configureRequest(
                getUserDefinedFunctionsRequest::getDatabaseName,
                getUserDefinedFunctionsRequest::setDatabaseName,
                getUserDefinedFunctionsRequest::setCatalogId
        );
        return super.getUserDefinedFunctions(getUserDefinedFunctionsRequest);
    }

    @Override
    public UpdateDatabaseResult updateDatabase(UpdateDatabaseRequest updateDatabaseRequest) {
        configureRequest(
                updateDatabaseRequest::getName,
                updateDatabaseRequest::setName,
                updateDatabaseRequest::setCatalogId
        );
        configureRequest(
                () -> updateDatabaseRequest.getDatabaseInput().getName(),
                name -> updateDatabaseRequest.getDatabaseInput().setName(name),
                catalogId -> {}
        );
        return super.updateDatabase(updateDatabaseRequest);
    }

    @Override
    public UpdatePartitionResult updatePartition(UpdatePartitionRequest updatePartitionRequest) {
        configureRequest(
                updatePartitionRequest::getDatabaseName,
                updatePartitionRequest::setDatabaseName,
                updatePartitionRequest::setCatalogId
        );
        return super.updatePartition(updatePartitionRequest);
    }

    @Override
    public UpdateTableResult updateTable(UpdateTableRequest updateTableRequest) {
        configureRequest(
                updateTableRequest::getDatabaseName,
                updateTableRequest::setDatabaseName,
                updateTableRequest::setCatalogId
        );
        return super.updateTable(updateTableRequest);
    }

    @Override
    public UpdateUserDefinedFunctionResult updateUserDefinedFunction(UpdateUserDefinedFunctionRequest updateUserDefinedFunctionRequest) {
        configureRequest(
                updateUserDefinedFunctionRequest::getDatabaseName,
                updateUserDefinedFunctionRequest::setDatabaseName,
                updateUserDefinedFunctionRequest::setCatalogId
        );
        return super.updateUserDefinedFunction(updateUserDefinedFunctionRequest);
    }
}
