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

import com.amazonaws.glue.catalog.metastore.GlueMetastoreClientDelegate;
import static com.google.common.base.Preconditions.checkNotNull;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import static org.apache.hadoop.hive.metastore.TableType.EXTERNAL_TABLE;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.Map;

public final class MetastoreClientUtils {

  // private static final AwsGlueHiveShims hiveShims = ShimsLoader.getHiveShims();

  private MetastoreClientUtils() {
    // static util class should not be instantiated
  }

  /**
   * @return boolean
   *     true -> if directory was able to be created.
   *     false -> if directory already exists.
   * @throws MetaException if directory could not be created.
   */
  public static boolean makeDirs(Warehouse wh, Path path) throws MetaException {
    checkNotNull(wh, "Warehouse cannot be null");
    checkNotNull(path, "Path cannot be null");

    boolean madeDir = false;
    if (!wh.isDir(path)) {
      // if (!hiveShims.mkdirs(wh, path)) {
      throw new MetaException("Unable to create path: " + path);
      // }
      // madeDir = true;
    }
    return madeDir;
  }

  /**
   * Taken from HiveMetaStore#create_table_core
   * https://github.com/apache/hive/blob/rel/release-2.3.0/metastore/src/java/org/apache/hadoop/hive/metastore/HiveMetaStore.java#L1370-L1383
   */
  public static void validateTableObject(Table table, Configuration conf) throws InvalidObjectException {
    checkNotNull(table, "table cannot be null");
    checkNotNull(table.getSd(), "Table#StorageDescriptor cannot be null");

    // if (!hiveShims.validateTableName(table.getTableName(), conf)) {
    //   throw new InvalidObjectException(table.getTableName() + " is not a valid object name");
    // }
    // String validate = hiveShims.validateTblColumns(table.getSd().getCols());
    // if (validate != null) {
    //   throw new InvalidObjectException("Invalid column " + validate);
    // }

    // if (table.getPartitionKeys() != null) {
    //   validate = hiveShims.validateTblColumns(table.getPartitionKeys());
    //   if (validate != null) {
    //     throw new InvalidObjectException("Invalid partition column " + validate);
    //   }
    // }
  }

  /**
   * Should be used when getting table from Glue that may have been created by
   * users manually or through Crawlers. Validates that table contains properties required by Hive/Spark.
   * @param table
   */
  public static void validateGlueTable(com.amazonaws.services.glue.model.Table table) {
    checkNotNull(table, "table cannot be null");

    for (HiveTableValidator validator : HiveTableValidator.values()) {
      validator.validate(table);
    }
  }

  public static <K, V> Map<K, V> deepCopyMap(Map<K, V> originalMap) {
    Map<K, V> deepCopy = Maps.newHashMap();
    if (originalMap == null) {
      return deepCopy;
    }

    for (Map.Entry<K, V> entry : originalMap.entrySet()) {
      deepCopy.put(entry.getKey(), entry.getValue());
    }
    return deepCopy;
  }

  /**
   * Mimics MetaStoreUtils.isExternalTable
   * Additional logic: check Table#getTableType to see if isExternalTable
   */
  public static boolean isExternalTable(org.apache.hadoop.hive.metastore.api.Table table) {
    if (table == null) {
      return false;
    }

    Map<String, String> params = table.getParameters();
    String paramsExternalStr = params == null ? null : params.get("EXTERNAL");
    if (paramsExternalStr != null) {
      return "TRUE".equalsIgnoreCase(paramsExternalStr);
    }

    return table.getTableType() != null && EXTERNAL_TABLE.name().equalsIgnoreCase(table.getTableType());
  }

  public static String getCatalogId(Configuration conf) {
    if (StringUtils.isNotEmpty(conf.get(GlueMetastoreClientDelegate.CATALOG_ID_CONF))) {
      return conf.get(GlueMetastoreClientDelegate.CATALOG_ID_CONF);
    }
    // This case defaults to using the caller's account Id as Catalog Id.
    return null;
  }

}
