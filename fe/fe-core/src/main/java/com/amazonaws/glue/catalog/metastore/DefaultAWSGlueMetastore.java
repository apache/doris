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

import com.amazonaws.AmazonServiceException;
import com.amazonaws.glue.catalog.converters.PartitionNameParser;
import com.amazonaws.glue.catalog.util.MetastoreClientUtils;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.BatchCreatePartitionRequest;
import com.amazonaws.services.glue.model.BatchGetPartitionRequest;
import com.amazonaws.services.glue.model.BatchGetPartitionResult;
import com.amazonaws.services.glue.model.ColumnStatistics;
import com.amazonaws.services.glue.model.ColumnStatisticsError;
import com.amazonaws.services.glue.model.CreateDatabaseRequest;
import com.amazonaws.services.glue.model.CreateTableRequest;
import com.amazonaws.services.glue.model.CreateUserDefinedFunctionRequest;
import com.amazonaws.services.glue.model.Database;
import com.amazonaws.services.glue.model.DatabaseInput;
import com.amazonaws.services.glue.model.DeleteColumnStatisticsForPartitionRequest;
import com.amazonaws.services.glue.model.DeleteColumnStatisticsForTableRequest;
import com.amazonaws.services.glue.model.DeleteDatabaseRequest;
import com.amazonaws.services.glue.model.DeletePartitionRequest;
import com.amazonaws.services.glue.model.DeleteTableRequest;
import com.amazonaws.services.glue.model.DeleteUserDefinedFunctionRequest;
import com.amazonaws.services.glue.model.GetColumnStatisticsForPartitionRequest;
import com.amazonaws.services.glue.model.GetColumnStatisticsForPartitionResult;
import com.amazonaws.services.glue.model.GetColumnStatisticsForTableRequest;
import com.amazonaws.services.glue.model.GetColumnStatisticsForTableResult;
import com.amazonaws.services.glue.model.GetDatabaseRequest;
import com.amazonaws.services.glue.model.GetDatabaseResult;
import com.amazonaws.services.glue.model.GetDatabasesRequest;
import com.amazonaws.services.glue.model.GetDatabasesResult;
import com.amazonaws.services.glue.model.GetPartitionRequest;
import com.amazonaws.services.glue.model.GetPartitionsRequest;
import com.amazonaws.services.glue.model.GetPartitionsResult;
import com.amazonaws.services.glue.model.GetTableRequest;
import com.amazonaws.services.glue.model.GetTableResult;
import com.amazonaws.services.glue.model.GetTablesRequest;
import com.amazonaws.services.glue.model.GetTablesResult;
import com.amazonaws.services.glue.model.GetUserDefinedFunctionRequest;
import com.amazonaws.services.glue.model.GetUserDefinedFunctionsRequest;
import com.amazonaws.services.glue.model.GetUserDefinedFunctionsResult;
import com.amazonaws.services.glue.model.Partition;
import com.amazonaws.services.glue.model.PartitionError;
import com.amazonaws.services.glue.model.PartitionInput;
import com.amazonaws.services.glue.model.PartitionValueList;
import com.amazonaws.services.glue.model.Segment;
import com.amazonaws.services.glue.model.Table;
import com.amazonaws.services.glue.model.TableInput;
import com.amazonaws.services.glue.model.UpdateColumnStatisticsForPartitionRequest;
import com.amazonaws.services.glue.model.UpdateColumnStatisticsForPartitionResult;
import com.amazonaws.services.glue.model.UpdateColumnStatisticsForTableRequest;
import com.amazonaws.services.glue.model.UpdateColumnStatisticsForTableResult;
import com.amazonaws.services.glue.model.UpdateDatabaseRequest;
import com.amazonaws.services.glue.model.UpdatePartitionRequest;
import com.amazonaws.services.glue.model.UpdateTableRequest;
import com.amazonaws.services.glue.model.UpdateUserDefinedFunctionRequest;
import com.amazonaws.services.glue.model.UserDefinedFunction;
import com.amazonaws.services.glue.model.UserDefinedFunctionInput;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class DefaultAWSGlueMetastore implements AWSGlueMetastore {

    public static final int BATCH_GET_PARTITIONS_MAX_REQUEST_SIZE = 1000;
    /**
     * Based on the maxResults parameter at https://docs.aws.amazon.com/glue/latest/webapi/API_GetPartitions.html
     */
    public static final int GET_PARTITIONS_MAX_SIZE = 1000;
    /**
     * Maximum number of Glue Segments. A segment defines a non-overlapping region of a table's partitions,
     * allowing multiple requests to be executed in parallel.
     */
    public static final int DEFAULT_NUM_PARTITION_SEGMENTS = 5;
    /**
     * Currently the upper limit allowed by Glue is 10.
     * https://docs.aws.amazon.com/glue/latest/webapi/API_Segment.html
     */
    public static final int MAX_NUM_PARTITION_SEGMENTS = 10;
    public static final String NUM_PARTITION_SEGMENTS_CONF = "aws.glue.partition.num.segments";
    public static final String CUSTOM_EXECUTOR_FACTORY_CONF = "hive.metastore.executorservice.factory.class";

    /**
     * Based on the ColumnNames parameter at https://docs.aws.amazon.com/glue/latest/webapi/API_GetColumnStatisticsForPartition.html
     */
    public static final int GET_COLUMNS_STAT_MAX_SIZE = 100;
    public static final int UPDATE_COLUMNS_STAT_MAX_SIZE = 25;

    /**
     * To be used with UpdateTable
     */
    public static final String SKIP_AWS_GLUE_ARCHIVE = "skipAWSGlueArchive";

    private static final int NUM_EXECUTOR_THREADS = 5;
    static final String GLUE_METASTORE_DELEGATE_THREADPOOL_NAME_FORMAT = "glue-metastore-delegate-%d";
    private static final ExecutorService GLUE_METASTORE_DELEGATE_THREAD_POOL = Executors.newFixedThreadPool(
            NUM_EXECUTOR_THREADS,
            new ThreadFactoryBuilder()
                    .setNameFormat(GLUE_METASTORE_DELEGATE_THREADPOOL_NAME_FORMAT)
                    .setDaemon(true).build()
    );

    private final Configuration conf;
    private final AWSGlue glueClient;
    private final String catalogId;
    private final ExecutorService executorService;
    private final int numPartitionSegments;

    protected ExecutorService getExecutorService(Configuration conf) {
        Class<? extends ExecutorServiceFactory> executorFactoryClass = conf
                .getClass(CUSTOM_EXECUTOR_FACTORY_CONF,
                        DefaultExecutorServiceFactory.class).asSubclass(
                        ExecutorServiceFactory.class);
        ExecutorServiceFactory factory = ReflectionUtils.newInstance(
                executorFactoryClass, conf);
        return factory.getExecutorService(conf);
    }

    public DefaultAWSGlueMetastore(Configuration conf, AWSGlue glueClient) {
        checkNotNull(conf, "Hive Config cannot be null");
        checkNotNull(glueClient, "glueClient cannot be null");
        this.numPartitionSegments = conf.getInt(NUM_PARTITION_SEGMENTS_CONF, DEFAULT_NUM_PARTITION_SEGMENTS);
        checkArgument(numPartitionSegments <= MAX_NUM_PARTITION_SEGMENTS,
                String.format("Hive Config [%s] can't exceed %d", NUM_PARTITION_SEGMENTS_CONF, MAX_NUM_PARTITION_SEGMENTS));
        this.conf = conf;
        this.glueClient = glueClient;
        this.catalogId = MetastoreClientUtils.getCatalogId(conf);
        this.executorService = getExecutorService(conf);
    }

    // ======================= Database =======================

    @Override
    public void createDatabase(DatabaseInput databaseInput) {
        CreateDatabaseRequest createDatabaseRequest = new CreateDatabaseRequest().withDatabaseInput(databaseInput)
                .withCatalogId(catalogId);
        glueClient.createDatabase(createDatabaseRequest);
    }

    @Override
    public Database getDatabase(String dbName) {
        GetDatabaseRequest getDatabaseRequest = new GetDatabaseRequest().withCatalogId(catalogId).withName(dbName);
        GetDatabaseResult result = glueClient.getDatabase(getDatabaseRequest);
        return result.getDatabase();
    }

    @Override
    public List<Database> getAllDatabases() {
        List<Database> ret = Lists.newArrayList();
        String nextToken = null;
        do {
            GetDatabasesRequest getDatabasesRequest = new GetDatabasesRequest().withNextToken(nextToken).withCatalogId(
                    catalogId);
            GetDatabasesResult result = glueClient.getDatabases(getDatabasesRequest);
            nextToken = result.getNextToken();
            ret.addAll(result.getDatabaseList());
        } while (nextToken != null);
        return ret;
    }

    @Override
    public void updateDatabase(String databaseName, DatabaseInput databaseInput) {
        UpdateDatabaseRequest updateDatabaseRequest = new UpdateDatabaseRequest().withName(databaseName)
                .withDatabaseInput(databaseInput).withCatalogId(catalogId);
        glueClient.updateDatabase(updateDatabaseRequest);
    }

    @Override
    public void deleteDatabase(String dbName) {
        DeleteDatabaseRequest deleteDatabaseRequest = new DeleteDatabaseRequest().withName(dbName).withCatalogId(
                catalogId);
        glueClient.deleteDatabase(deleteDatabaseRequest);
    }

    // ======================== Table ========================

    @Override
    public void createTable(String dbName, TableInput tableInput) {
        CreateTableRequest createTableRequest = new CreateTableRequest().withTableInput(tableInput)
                .withDatabaseName(dbName).withCatalogId(catalogId);
        glueClient.createTable(createTableRequest);
    }

    @Override
    public Table getTable(String dbName, String tableName) {
        GetTableRequest getTableRequest = new GetTableRequest().withDatabaseName(dbName).withName(tableName)
                .withCatalogId(catalogId);
        GetTableResult result = glueClient.getTable(getTableRequest);
        return result.getTable();
    }

    @Override
    public List<Table> getTables(String dbname, String tablePattern) {
        List<Table> ret = new ArrayList<>();
        String nextToken = null;
        do {
            GetTablesRequest getTablesRequest = new GetTablesRequest().withDatabaseName(dbname)
                    .withExpression(tablePattern).withNextToken(nextToken).withCatalogId(catalogId);
            GetTablesResult result = glueClient.getTables(getTablesRequest);
            ret.addAll(result.getTableList());
            nextToken = result.getNextToken();
        } while (nextToken != null);
        return ret;
    }

    @Override
    public void updateTable(String dbName, TableInput tableInput) {
        UpdateTableRequest updateTableRequest = new UpdateTableRequest().withDatabaseName(dbName)
                .withTableInput(tableInput).withCatalogId(catalogId);
        glueClient.updateTable(updateTableRequest);
    }

    @Override
    public void updateTable(String dbName, TableInput tableInput, EnvironmentContext environmentContext) {
        UpdateTableRequest updateTableRequest = new UpdateTableRequest().withDatabaseName(dbName)
                .withTableInput(tableInput).withCatalogId(catalogId).withSkipArchive(skipArchive(environmentContext));
        glueClient.updateTable(updateTableRequest);
    }

    private boolean skipArchive(EnvironmentContext environmentContext) {
        return environmentContext != null &&
                environmentContext.isSetProperties() &&
                StatsSetupConst.TRUE.equals(environmentContext.getProperties().get(SKIP_AWS_GLUE_ARCHIVE));
    }

    @Override
    public void deleteTable(String dbName, String tableName) {
        DeleteTableRequest deleteTableRequest = new DeleteTableRequest().withDatabaseName(dbName).withName(tableName)
                .withCatalogId(catalogId);
        glueClient.deleteTable(deleteTableRequest);
    }

    // =========================== Partition ===========================

    @Override
    public Partition getPartition(String dbName, String tableName, List<String> partitionValues) {
        GetPartitionRequest request = new GetPartitionRequest()
                .withDatabaseName(dbName)
                .withTableName(tableName)
                .withPartitionValues(partitionValues)
                .withCatalogId(catalogId);
        return glueClient.getPartition(request).getPartition();
    }

    @Override
    public List<Partition> getPartitionsByNames(String dbName, String tableName,
            List<PartitionValueList> partitionsToGet) {

        List<List<PartitionValueList>> batchedPartitionsToGet = Lists.partition(partitionsToGet,
                BATCH_GET_PARTITIONS_MAX_REQUEST_SIZE);
        List<Future<BatchGetPartitionResult>> batchGetPartitionFutures = Lists.newArrayList();

        for (List<PartitionValueList> batch : batchedPartitionsToGet) {
            final BatchGetPartitionRequest request = new BatchGetPartitionRequest()
                    .withDatabaseName(dbName)
                    .withTableName(tableName)
                    .withPartitionsToGet(batch)
                    .withCatalogId(catalogId);
            batchGetPartitionFutures.add(this.executorService.submit(new Callable<BatchGetPartitionResult>() {
                @Override
                public BatchGetPartitionResult call() throws Exception {
                    return glueClient.batchGetPartition(request);
                }
            }));
        }

        List<Partition> result = Lists.newArrayList();
        try {
            for (Future<BatchGetPartitionResult> future : batchGetPartitionFutures) {
                result.addAll(future.get().getPartitions());
            }
        } catch (ExecutionException e) {
            Throwables.propagateIfInstanceOf(e.getCause(), AmazonServiceException.class);
            Throwables.propagate(e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return result;
    }

    @Override
    public List<Partition> getPartitions(String dbName, String tableName, String expression,
            long max) throws TException {
        if (max == 0) {
            return Collections.emptyList();
        }
        if (max < 0 || max > GET_PARTITIONS_MAX_SIZE) {
            return getPartitionsParallel(dbName, tableName, expression, max);
        } else {
            // We don't need to get too many partitions, so just do it serially.
            return getCatalogPartitions(dbName, tableName, expression, max, null);
        }
    }

    private List<Partition> getPartitionsParallel(
            final String databaseName,
            final String tableName,
            final String expression,
            final long max) throws TException {
        // Prepare the segments
        List<Segment> segments = Lists.newArrayList();
        for (int i = 0; i < numPartitionSegments; i++) {
            segments.add(new Segment()
                    .withSegmentNumber(i)
                    .withTotalSegments(numPartitionSegments));
        }
        // Submit Glue API calls in parallel using the thread pool.
        // We could convert this into a parallelStream after upgrading to JDK 8 compiler base.
        List<Future<List<Partition>>> futures = Lists.newArrayList();
        for (final Segment segment : segments) {
            futures.add(this.executorService.submit(new Callable<List<Partition>>() {
                @Override
                public List<Partition> call() throws Exception {
                    return getCatalogPartitions(databaseName, tableName, expression, max, segment);
                }
            }));
        }

        // Get the results
        List<Partition> partitions = Lists.newArrayList();
        try {
            for (Future<List<Partition>> future : futures) {
                List<Partition> segmentPartitions = future.get();
                if (partitions.size() + segmentPartitions.size() >= max && max > 0) {
                    // Extract the required number of partitions from the segment and we're done.
                    long remaining = max - partitions.size();
                    partitions.addAll(segmentPartitions.subList(0, (int) remaining));
                    break;
                } else {
                    partitions.addAll(segmentPartitions);
                }
            }
        } catch (ExecutionException e) {
            Throwables.propagateIfInstanceOf(e.getCause(), AmazonServiceException.class);
            Throwables.propagate(e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return partitions;
    }


    private List<Partition> getCatalogPartitions(String databaseName, String tableName, String expression,
            long max, Segment segment) {
        List<Partition> partitions = Lists.newArrayList();
        String nextToken = null;
        do {
            GetPartitionsRequest request = new GetPartitionsRequest()
                    .withDatabaseName(databaseName)
                    .withTableName(tableName)
                    .withExpression(expression)
                    .withNextToken(nextToken)
                    .withCatalogId(catalogId)
                    .withSegment(segment);
            GetPartitionsResult res = glueClient.getPartitions(request);
            List<Partition> list = res.getPartitions();
            if ((partitions.size() + list.size()) >= max && max > 0) {
                long remaining = max - partitions.size();
                partitions.addAll(list.subList(0, (int) remaining));
                break;
            }
            partitions.addAll(list);
            nextToken = res.getNextToken();
        } while (nextToken != null);
        return partitions;
    }

    @Override
    public void updatePartition(String dbName, String tableName, List<String> partitionValues,
            PartitionInput partitionInput) {
        UpdatePartitionRequest updatePartitionRequest = new UpdatePartitionRequest().withDatabaseName(dbName)
                .withTableName(tableName).withPartitionValueList(partitionValues)
                .withPartitionInput(partitionInput).withCatalogId(catalogId);
        glueClient.updatePartition(updatePartitionRequest);
    }

    @Override
    public void deletePartition(String dbName, String tableName, List<String> partitionValues) {
        DeletePartitionRequest request = new DeletePartitionRequest()
                .withDatabaseName(dbName)
                .withTableName(tableName)
                .withPartitionValues(partitionValues)
                .withCatalogId(catalogId);
        glueClient.deletePartition(request);
    }

    @Override
    public List<PartitionError> createPartitions(String dbName, String tableName,
            List<PartitionInput> partitionInputs) {
        BatchCreatePartitionRequest request =
                new BatchCreatePartitionRequest().withDatabaseName(dbName)
                        .withTableName(tableName).withCatalogId(catalogId)
                        .withPartitionInputList(partitionInputs);
        return glueClient.batchCreatePartition(request).getErrors();
    }

    // ====================== User Defined Function ======================

    @Override
    public void createUserDefinedFunction(String dbName, UserDefinedFunctionInput functionInput) {
        CreateUserDefinedFunctionRequest createUserDefinedFunctionRequest = new CreateUserDefinedFunctionRequest()
                .withDatabaseName(dbName).withFunctionInput(functionInput).withCatalogId(catalogId);
        glueClient.createUserDefinedFunction(createUserDefinedFunctionRequest);
    }

    @Override
    public UserDefinedFunction getUserDefinedFunction(String dbName, String functionName) {
        GetUserDefinedFunctionRequest getUserDefinedFunctionRequest = new GetUserDefinedFunctionRequest()
                .withDatabaseName(dbName).withFunctionName(functionName).withCatalogId(catalogId);
        return glueClient.getUserDefinedFunction(getUserDefinedFunctionRequest).getUserDefinedFunction();
    }

    @Override
    public List<UserDefinedFunction> getUserDefinedFunctions(String dbName, String pattern) {
        List<UserDefinedFunction> ret = Lists.newArrayList();
        String nextToken = null;
        do {
            GetUserDefinedFunctionsRequest getUserDefinedFunctionsRequest = new GetUserDefinedFunctionsRequest()
                    .withDatabaseName(dbName).withPattern(pattern).withNextToken(nextToken).withCatalogId(catalogId);
            GetUserDefinedFunctionsResult result = glueClient.getUserDefinedFunctions(getUserDefinedFunctionsRequest);
            nextToken = result.getNextToken();
            ret.addAll(result.getUserDefinedFunctions());
        } while (nextToken != null);
        return ret;
    }

    @Override
    public List<UserDefinedFunction> getUserDefinedFunctions(String pattern) {
        List<UserDefinedFunction> ret = Lists.newArrayList();
        String nextToken = null;
        do {
            GetUserDefinedFunctionsRequest getUserDefinedFunctionsRequest = new GetUserDefinedFunctionsRequest()
                    .withPattern(pattern).withNextToken(nextToken).withCatalogId(catalogId);
            GetUserDefinedFunctionsResult result = glueClient.getUserDefinedFunctions(getUserDefinedFunctionsRequest);
            nextToken = result.getNextToken();
            ret.addAll(result.getUserDefinedFunctions());
        } while (nextToken != null);
        return ret;
    }

    @Override
    public void deleteUserDefinedFunction(String dbName, String functionName) {
        DeleteUserDefinedFunctionRequest deleteUserDefinedFunctionRequest = new DeleteUserDefinedFunctionRequest()
                .withDatabaseName(dbName).withFunctionName(functionName).withCatalogId(catalogId);
        glueClient.deleteUserDefinedFunction(deleteUserDefinedFunctionRequest);
    }

    @Override
    public void updateUserDefinedFunction(String dbName, String functionName, UserDefinedFunctionInput functionInput) {
        UpdateUserDefinedFunctionRequest updateUserDefinedFunctionRequest = new UpdateUserDefinedFunctionRequest()
                .withDatabaseName(dbName).withFunctionName(functionName).withFunctionInput(functionInput)
                .withCatalogId(catalogId);
        glueClient.updateUserDefinedFunction(updateUserDefinedFunctionRequest);
    }

    @Override
    public void deletePartitionColumnStatistics(String dbName, String tableName, List<String> partitionValues, String colName) {
        DeleteColumnStatisticsForPartitionRequest request = new DeleteColumnStatisticsForPartitionRequest()
                .withCatalogId(catalogId)
                .withDatabaseName(dbName)
                .withTableName(tableName)
                .withPartitionValues(partitionValues)
                .withColumnName(colName);
        glueClient.deleteColumnStatisticsForPartition(request);
    }

    @Override
    public void deleteTableColumnStatistics(String dbName, String tableName, String colName) {
        DeleteColumnStatisticsForTableRequest request = new DeleteColumnStatisticsForTableRequest()
                .withCatalogId(catalogId)
                .withDatabaseName(dbName)
                .withTableName(tableName)
                .withColumnName(colName);
        glueClient.deleteColumnStatisticsForTable(request);
    }

    @Override
    public Map<String, List<ColumnStatistics>> getPartitionColumnStatistics(String dbName, String tableName, List<String> partitionValues, List<String> columnNames) {
        Map<String, List<ColumnStatistics>> partitionStatistics = new HashMap<>();
        List<List<String>> pagedColNames = Lists.partition(columnNames, GET_COLUMNS_STAT_MAX_SIZE);
        List<String> partValues;
        for (String partName : partitionValues) {
            partValues = PartitionNameParser.getPartitionValuesFromName(partName);
            List<Future<GetColumnStatisticsForPartitionResult>> pagedResult = new ArrayList<>();
            for (List<String> cols : pagedColNames) {
                GetColumnStatisticsForPartitionRequest request = new GetColumnStatisticsForPartitionRequest()
                        .withCatalogId(catalogId)
                        .withDatabaseName(dbName)
                        .withTableName(tableName)
                        .withPartitionValues(partValues)
                        .withColumnNames(cols);
                pagedResult.add(GLUE_METASTORE_DELEGATE_THREAD_POOL.submit(new Callable<GetColumnStatisticsForPartitionResult>() {
                    @Override
                    public GetColumnStatisticsForPartitionResult call() throws Exception {
                        return glueClient.getColumnStatisticsForPartition(request);
                    }
                }));
            }

            List<ColumnStatistics> result = new ArrayList<>();
            for (Future<GetColumnStatisticsForPartitionResult> page : pagedResult) {
                try {
                    result.addAll(page.get().getColumnStatisticsList());
                } catch (ExecutionException e) {
                    Throwables.propagateIfInstanceOf(e.getCause(), AmazonServiceException.class);
                    Throwables.propagate(e.getCause());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            partitionStatistics.put(partName, result);
        }
        return partitionStatistics;
    }

    @Override
    public List<ColumnStatistics> getTableColumnStatistics(String dbName, String tableName, List<String> colNames) {
        List<List<String>> pagedColNames = Lists.partition(colNames, GET_COLUMNS_STAT_MAX_SIZE);
        List<Future<GetColumnStatisticsForTableResult>> pagedResult = new ArrayList<>();

        for (List<String> cols : pagedColNames) {
            GetColumnStatisticsForTableRequest request = new GetColumnStatisticsForTableRequest()
                    .withCatalogId(catalogId)
                    .withDatabaseName(dbName)
                    .withTableName(tableName)
                    .withColumnNames(cols);
            pagedResult.add(GLUE_METASTORE_DELEGATE_THREAD_POOL.submit(new Callable<GetColumnStatisticsForTableResult>() {
                @Override
                public GetColumnStatisticsForTableResult call() throws Exception {
                    return glueClient.getColumnStatisticsForTable(request);
                }
            }));
        }
        List<ColumnStatistics> results = new ArrayList<>();

        for (Future<GetColumnStatisticsForTableResult> page : pagedResult) {
            try {
                results.addAll(page.get().getColumnStatisticsList());
            } catch (ExecutionException e) {
                Throwables.propagateIfInstanceOf(e.getCause(), AmazonServiceException.class);
                Throwables.propagate(e.getCause());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        return results;
    }

    @Override
    public List<ColumnStatisticsError> updatePartitionColumnStatistics(
            String dbName,
            String tableName,
            List<String> partitionValues,
            List<ColumnStatistics> columnStatistics) {

        List<List<ColumnStatistics>> statisticsListPaged = Lists.partition(columnStatistics, UPDATE_COLUMNS_STAT_MAX_SIZE);
        List<Future<UpdateColumnStatisticsForPartitionResult>> pagedResult = new ArrayList<>();
        for (List<ColumnStatistics> statList : statisticsListPaged) {
            UpdateColumnStatisticsForPartitionRequest request = new UpdateColumnStatisticsForPartitionRequest()
                    .withCatalogId(catalogId)
                    .withDatabaseName(dbName)
                    .withTableName(tableName)
                    .withPartitionValues(partitionValues)
                    .withColumnStatisticsList(statList);
            pagedResult.add(GLUE_METASTORE_DELEGATE_THREAD_POOL.submit(new Callable<UpdateColumnStatisticsForPartitionResult>() {
                @Override
                public UpdateColumnStatisticsForPartitionResult call() throws Exception {
                    return glueClient.updateColumnStatisticsForPartition(request);
                }
            }));
        }
        // Waiting for calls to finish. Will fail the call if one of the future task fails
        List<ColumnStatisticsError> columnStatisticsErrors = new ArrayList<>();
        try {
            for (Future<UpdateColumnStatisticsForPartitionResult> page : pagedResult) {
                Optional.ofNullable(page.get().getErrors()).ifPresent(error -> columnStatisticsErrors.addAll(error));
            }
        } catch (ExecutionException e) {
            Throwables.propagateIfInstanceOf(e.getCause(), AmazonServiceException.class);
            Throwables.propagate(e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return columnStatisticsErrors;
    }

    @Override
    public List<ColumnStatisticsError> updateTableColumnStatistics(
            String dbName,
            String tableName,
            List<ColumnStatistics> columnStatistics) {

        List<List<ColumnStatistics>> statisticsListPaged = Lists.partition(columnStatistics, UPDATE_COLUMNS_STAT_MAX_SIZE);
        List<Future<UpdateColumnStatisticsForTableResult>> pagedResult = new ArrayList<>();
        for (List<ColumnStatistics> statList : statisticsListPaged) {
            UpdateColumnStatisticsForTableRequest request = new UpdateColumnStatisticsForTableRequest()
                    .withCatalogId(catalogId)
                    .withDatabaseName(dbName)
                    .withTableName(tableName)
                    .withColumnStatisticsList(statList);
            pagedResult.add(GLUE_METASTORE_DELEGATE_THREAD_POOL.submit(new Callable<UpdateColumnStatisticsForTableResult>() {
                @Override
                public UpdateColumnStatisticsForTableResult call() throws Exception {
                    return glueClient.updateColumnStatisticsForTable(request);
                }
            }));
        }

        // Waiting for calls to finish. Will fail the call if one of the future task fails
        List<ColumnStatisticsError> columnStatisticsErrors = new ArrayList<>();
        try {
            for (Future<UpdateColumnStatisticsForTableResult> page : pagedResult) {
                Optional.ofNullable(page.get().getErrors()).ifPresent(error -> columnStatisticsErrors.addAll(error));
            }
        } catch (ExecutionException e) {
            Throwables.propagateIfInstanceOf(e.getCause(), AmazonServiceException.class);
            Throwables.propagate(e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return columnStatisticsErrors;
    }
}
