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

import com.amazonaws.services.glue.model.ColumnStatistics;
import com.amazonaws.services.glue.model.ErrorDetail;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import shade.doris.hive.org.apache.thrift.TException;

import java.util.List;

public interface CatalogToHiveConverter {

  TException wrapInHiveException(Throwable e);

  TException errorDetailToHiveException(ErrorDetail errorDetail);

  Database convertDatabase(com.amazonaws.services.glue.model.Database catalogDatabase);

  List<FieldSchema> convertFieldSchemaList(List<com.amazonaws.services.glue.model.Column> catalogFieldSchemaList);

  Table convertTable(com.amazonaws.services.glue.model.Table catalogTable, String dbname);

  TableMeta convertTableMeta(com.amazonaws.services.glue.model.Table catalogTable, String dbName);

  Partition convertPartition(com.amazonaws.services.glue.model.Partition src);

  List<Partition> convertPartitions(List<com.amazonaws.services.glue.model.Partition> src);

  Function convertFunction(String dbName, com.amazonaws.services.glue.model.UserDefinedFunction catalogFunction);

  List<ColumnStatisticsObj> convertColumnStatisticsList(List<ColumnStatistics> catatlogColumnStatisticsList);
}
