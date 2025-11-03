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

package com.amazonaws.glue.catalog.converters;

import com.amazonaws.services.glue.model.DatabaseInput;
import com.amazonaws.services.glue.model.PartitionInput;
import com.amazonaws.services.glue.model.TableInput;
import com.amazonaws.services.glue.model.UserDefinedFunctionInput;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * This class provides methods to convert Hive/Catalog objects to Input objects used
 * for Glue API parameters
 */
public final class GlueInputConverter {

  public static DatabaseInput convertToDatabaseInput(Database hiveDatabase) {
    return convertToDatabaseInput(HiveToCatalogConverter.convertDatabase(hiveDatabase));
  }

  public static DatabaseInput convertToDatabaseInput(com.amazonaws.services.glue.model.Database database) {
    DatabaseInput input = new DatabaseInput();

    input.setName(database.getName());
    input.setDescription(database.getDescription());
    input.setLocationUri(database.getLocationUri());
    input.setParameters(database.getParameters());

    return input;
  }

  public static TableInput convertToTableInput(Table hiveTable) {
    return convertToTableInput(HiveToCatalogConverter.convertTable(hiveTable));
  }

  public static TableInput convertToTableInput(com.amazonaws.services.glue.model.Table table) {
    TableInput tableInput = new TableInput();

    tableInput.setRetention(table.getRetention());
    tableInput.setPartitionKeys(table.getPartitionKeys());
    tableInput.setTableType(table.getTableType());
    tableInput.setName(table.getName());
    tableInput.setOwner(table.getOwner());
    tableInput.setLastAccessTime(table.getLastAccessTime());
    tableInput.setStorageDescriptor(table.getStorageDescriptor());
    tableInput.setParameters(table.getParameters());
    tableInput.setViewExpandedText(table.getViewExpandedText());
    tableInput.setViewOriginalText(table.getViewOriginalText());

    return tableInput;
  }

  public static PartitionInput convertToPartitionInput(Partition src) {
    return convertToPartitionInput(HiveToCatalogConverter.convertPartition(src));
  }

  public static PartitionInput convertToPartitionInput(com.amazonaws.services.glue.model.Partition src) {
    PartitionInput partitionInput = new PartitionInput();

    partitionInput.setLastAccessTime(src.getLastAccessTime());
    partitionInput.setParameters(src.getParameters());
    partitionInput.setStorageDescriptor(src.getStorageDescriptor());
    partitionInput.setValues(src.getValues());

    return partitionInput;
  }

  public static List<PartitionInput> convertToPartitionInputs(Collection<com.amazonaws.services.glue.model.Partition> parts) {
    List<PartitionInput> inputList = new ArrayList<>();

    for (com.amazonaws.services.glue.model.Partition part : parts) {
      inputList.add(convertToPartitionInput(part));
    }
    return inputList;
  }

  public static UserDefinedFunctionInput convertToUserDefinedFunctionInput(Function hiveFunction) {
    UserDefinedFunctionInput functionInput = new UserDefinedFunctionInput();

    functionInput.setClassName(hiveFunction.getClassName());
    functionInput.setFunctionName(hiveFunction.getFunctionName());
    functionInput.setOwnerName(hiveFunction.getOwnerName());
    if(hiveFunction.getOwnerType() != null) {
      functionInput.setOwnerType(hiveFunction.getOwnerType().name());
    }
    functionInput.setResourceUris(HiveToCatalogConverter.covertResourceUriList(hiveFunction.getResourceUris()));
    return functionInput;
  }

}
