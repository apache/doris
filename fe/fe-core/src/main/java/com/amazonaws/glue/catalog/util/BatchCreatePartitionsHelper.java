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

package com.amazonaws.glue.catalog.util;

import com.amazonaws.glue.catalog.converters.CatalogToHiveConverter;
import com.amazonaws.glue.catalog.converters.CatalogToHiveConverterFactory;
import com.amazonaws.glue.catalog.converters.GlueInputConverter;
import com.amazonaws.glue.catalog.metastore.AWSGlueMetastore;
import static com.amazonaws.glue.catalog.util.PartitionUtils.isInvalidUserInputException;
import com.amazonaws.services.glue.model.EntityNotFoundException;
import com.amazonaws.services.glue.model.Partition;
import com.amazonaws.services.glue.model.PartitionError;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.log4j.Logger;
import shade.doris.hive.org.apache.thrift.TException;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public final class BatchCreatePartitionsHelper {

  private static final Logger logger = Logger.getLogger(BatchCreatePartitionsHelper.class);

  private final AWSGlueMetastore glueClient;
  private final String databaseName;
  private final String tableName;
  private final List<Partition> partitions;
  private final boolean ifNotExists;
  private Map<PartitionKey, Partition> partitionMap;
  private List<Partition> partitionsFailed;
  private TException firstTException;
  private String catalogId;
  private CatalogToHiveConverter catalogToHiveConverter;

  public BatchCreatePartitionsHelper(AWSGlueMetastore glueClient, String databaseName, String tableName, String catalogId,
          List<Partition> partitions, boolean ifNotExists) {
    this.glueClient = glueClient;
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.catalogId = catalogId;
    this.partitions = partitions;
    this.ifNotExists = ifNotExists;
    catalogToHiveConverter = CatalogToHiveConverterFactory.getCatalogToHiveConverter();
  }

  public BatchCreatePartitionsHelper createPartitions() {
    partitionMap = PartitionUtils.buildPartitionMap(partitions);
    partitionsFailed = Lists.newArrayList();

    try {
      List<PartitionError> result =
              glueClient.createPartitions(databaseName, tableName,
                      GlueInputConverter.convertToPartitionInputs(partitionMap.values()));
      processResult(result);
    } catch (Exception e) {
      logger.error("Exception thrown while creating partitions in DataCatalog: ", e);
      firstTException = catalogToHiveConverter.wrapInHiveException(e);
      if (isInvalidUserInputException(e)) {
        setAllFailed();
      } else {
        checkIfPartitionsCreated();
      }
    }
    return this;
  }

  private void setAllFailed() {
    partitionsFailed = partitions;
    partitionMap.clear();
  }

  private void processResult(List<PartitionError> partitionErrors) {
    if (partitionErrors == null || partitionErrors.isEmpty()) {
      return;
    }

    logger.error(String.format("BatchCreatePartitions failed to create %d out of %d partitions. \n",
            partitionErrors.size(), partitionMap.size()));

    for (PartitionError partitionError : partitionErrors) {
      Partition partitionFailed = partitionMap.remove(new PartitionKey(partitionError.getPartitionValues()));

      TException exception = catalogToHiveConverter.errorDetailToHiveException(partitionError.getErrorDetail());
      if (ifNotExists && exception instanceof AlreadyExistsException) {
        // AlreadyExistsException is allowed, so we shouldn't add the partition to partitionsFailed list
        continue;
      }
      logger.error(exception);
      if (firstTException == null) {
        firstTException = exception;
      }
      partitionsFailed.add(partitionFailed);
    }
  }

  private void checkIfPartitionsCreated() {
    for (Partition partition : partitions) {
      if (!partitionExists(partition)) {
        partitionsFailed.add(partition);
        partitionMap.remove(new PartitionKey(partition));
      }
    }
  }

  private boolean partitionExists(Partition partition) {
    try {
      Partition partitionReturned = glueClient.getPartition(databaseName, tableName, partition.getValues());
      return partitionReturned != null; //probably always true here
    } catch (EntityNotFoundException e) {
      // here we assume namespace and table exist. It is assured by calling "isInvalidUserInputException" method above
      return false;
    } catch (Exception e) {
      logger.error(String.format("Get partition request %s failed. ", StringUtils.join(partition.getValues(), "/")), e);
      // partition status unknown, we assume that the partition was not created
      return false;
    }
  }

  public TException getFirstTException() {
    return firstTException;
  }

  public Collection<Partition> getPartitionsCreated() {
    return partitionMap.values();
  }

  public List<Partition> getPartitionsFailed() {
    return partitionsFailed;
  }

}
