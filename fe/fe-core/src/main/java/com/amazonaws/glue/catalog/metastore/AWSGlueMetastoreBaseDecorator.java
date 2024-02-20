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

import com.amazonaws.services.glue.model.ColumnStatistics;
import com.amazonaws.services.glue.model.ColumnStatisticsError;
import com.amazonaws.services.glue.model.Database;
import com.amazonaws.services.glue.model.DatabaseInput;
import com.amazonaws.services.glue.model.Partition;
import com.amazonaws.services.glue.model.PartitionError;
import com.amazonaws.services.glue.model.PartitionInput;
import com.amazonaws.services.glue.model.PartitionValueList;
import com.amazonaws.services.glue.model.Table;
import com.amazonaws.services.glue.model.TableInput;
import com.amazonaws.services.glue.model.UserDefinedFunction;
import com.amazonaws.services.glue.model.UserDefinedFunctionInput;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.thrift.TException;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class AWSGlueMetastoreBaseDecorator implements AWSGlueMetastore {

    private final AWSGlueMetastore awsGlueMetastore;

    public AWSGlueMetastoreBaseDecorator(AWSGlueMetastore awsGlueMetastore) {
        checkNotNull(awsGlueMetastore, "awsGlueMetastore can not be null");
        this.awsGlueMetastore = awsGlueMetastore;
    }

    @Override
    public void createDatabase(DatabaseInput databaseInput) {
        awsGlueMetastore.createDatabase(databaseInput);
    }

    @Override
    public Database getDatabase(String dbName) {
        return awsGlueMetastore.getDatabase(dbName);
    }

    @Override
    public List<Database> getAllDatabases() {
        return awsGlueMetastore.getAllDatabases();
    }

    @Override
    public void updateDatabase(String databaseName, DatabaseInput databaseInput) {
        awsGlueMetastore.updateDatabase(databaseName, databaseInput);
    }

    @Override
    public void deleteDatabase(String dbName) {
        awsGlueMetastore.deleteDatabase(dbName);
    }

    @Override
    public void createTable(String dbName, TableInput tableInput) {
        awsGlueMetastore.createTable(dbName, tableInput);
    }

    @Override
    public Table getTable(String dbName, String tableName) {
        return awsGlueMetastore.getTable(dbName, tableName);
    }

    @Override
    public List<Table> getTables(String dbname, String tablePattern) {
        return awsGlueMetastore.getTables(dbname, tablePattern);
    }

    @Override
    public void updateTable(String dbName, TableInput tableInput) {
        awsGlueMetastore.updateTable(dbName, tableInput);
    }

    @Override
    public void updateTable(String dbName, TableInput tableInput, EnvironmentContext environmentContext) {
        awsGlueMetastore.updateTable(dbName, tableInput, environmentContext);
    }

    @Override
    public void deleteTable(String dbName, String tableName) {
        awsGlueMetastore.deleteTable(dbName, tableName);
    }

    @Override
    public Partition getPartition(String dbName, String tableName, List<String> partitionValues) {
        return awsGlueMetastore.getPartition(dbName, tableName, partitionValues);
    }

    @Override
    public List<Partition> getPartitionsByNames(String dbName, String tableName, List<PartitionValueList> partitionsToGet) {
        return awsGlueMetastore.getPartitionsByNames(dbName, tableName, partitionsToGet);
    }

    @Override
    public List<Partition> getPartitions(String dbName, String tableName, String expression, long max) throws TException {
        return awsGlueMetastore.getPartitions(dbName, tableName, expression, max);
    }

    @Override
    public void updatePartition(String dbName, String tableName, List<String> partitionValues, PartitionInput partitionInput) {
        awsGlueMetastore.updatePartition(dbName, tableName, partitionValues, partitionInput);
    }

    @Override
    public void deletePartition(String dbName, String tableName, List<String> partitionValues) {
        awsGlueMetastore.deletePartition(dbName, tableName, partitionValues);
    }

    @Override
    public List<PartitionError> createPartitions(String dbName, String tableName, List<PartitionInput> partitionInputs) {
        return awsGlueMetastore.createPartitions(dbName, tableName, partitionInputs);
    }

    @Override
    public void createUserDefinedFunction(String dbName, UserDefinedFunctionInput functionInput) {
        awsGlueMetastore.createUserDefinedFunction(dbName, functionInput);
    }

    @Override
    public UserDefinedFunction getUserDefinedFunction(String dbName, String functionName) {
        return awsGlueMetastore.getUserDefinedFunction(dbName, functionName);
    }

    @Override
    public List<UserDefinedFunction> getUserDefinedFunctions(String dbName, String pattern) {
        return awsGlueMetastore.getUserDefinedFunctions(dbName, pattern);
    }

    @Override
    public List<UserDefinedFunction> getUserDefinedFunctions(String pattern) {
        return awsGlueMetastore.getUserDefinedFunctions(pattern);
    }

    @Override
    public void deleteUserDefinedFunction(String dbName, String functionName) {
        awsGlueMetastore.deleteUserDefinedFunction(dbName, functionName);
    }

    @Override
    public void updateUserDefinedFunction(String dbName, String functionName, UserDefinedFunctionInput functionInput) {
        awsGlueMetastore.updateUserDefinedFunction(dbName, functionName, functionInput);
    }

    @Override
    public void deletePartitionColumnStatistics(String dbName, String tableName, List<String> partitionValues, String colName) {
        awsGlueMetastore.deletePartitionColumnStatistics(dbName, tableName, partitionValues, colName);
    }

    @Override
    public void deleteTableColumnStatistics(String dbName, String tableName, String colName) {
        awsGlueMetastore.deleteTableColumnStatistics(dbName, tableName, colName);
    }

    @Override
    public Map<String, List<ColumnStatistics>> getPartitionColumnStatistics(String dbName, String tableName, List<String> partitionValues, List<String> columnNames) {
        return awsGlueMetastore.getPartitionColumnStatistics(dbName, tableName, partitionValues, columnNames);
    }

    @Override
    public List<ColumnStatistics> getTableColumnStatistics(String dbName, String tableName, List<String> colNames) {
        return awsGlueMetastore.getTableColumnStatistics(dbName, tableName, colNames);
    }

    @Override
    public List<ColumnStatisticsError> updatePartitionColumnStatistics(String dbName, String tableName, List<String> partitionValues, List<ColumnStatistics> columnStatistics) {
        return awsGlueMetastore.updatePartitionColumnStatistics(dbName, tableName, partitionValues, columnStatistics);
    }

    @Override
    public List<ColumnStatisticsError> updateTableColumnStatistics(String dbName, String tableName, List<ColumnStatistics> columnStatistics) {
        return awsGlueMetastore.updateTableColumnStatistics(dbName, tableName, columnStatistics);
    }

}
