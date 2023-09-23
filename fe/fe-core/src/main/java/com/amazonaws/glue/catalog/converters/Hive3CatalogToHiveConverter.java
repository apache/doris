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

import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_CATALOG_NAME;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;

public class Hive3CatalogToHiveConverter extends BaseCatalogToHiveConverter {

  @Override
  public Database convertDatabase(com.amazonaws.services.glue.model.Database catalogDatabase) {
    Database hiveDatabase = super.convertDatabase(catalogDatabase);
    hiveDatabase.setCatalogName(DEFAULT_CATALOG_NAME);
    return hiveDatabase;
  }

  @Override
  public Table convertTable(com.amazonaws.services.glue.model.Table catalogTable, String dbname) {
    Table hiveTable = super.convertTable(catalogTable, dbname);
    hiveTable.setCatName(DEFAULT_CATALOG_NAME);
    return hiveTable;
  }

  @Override
  public TableMeta convertTableMeta(com.amazonaws.services.glue.model.Table catalogTable, String dbName) {
    TableMeta tableMeta = super.convertTableMeta(catalogTable, dbName);
    tableMeta.setCatName(DEFAULT_CATALOG_NAME);
    return tableMeta;
  }

  @Override
  public Partition convertPartition(com.amazonaws.services.glue.model.Partition src) {
    Partition hivePartition = super.convertPartition(src);
    hivePartition.setCatName(DEFAULT_CATALOG_NAME);
    return hivePartition;
  }

  @Override
  public Function convertFunction(String dbName, com.amazonaws.services.glue.model.UserDefinedFunction catalogFunction) {
    Function hiveFunction = super.convertFunction(dbName, catalogFunction);
    if (hiveFunction == null) {
      return null;
    }
    hiveFunction.setCatName(DEFAULT_CATALOG_NAME);
    return hiveFunction;
  }
}
