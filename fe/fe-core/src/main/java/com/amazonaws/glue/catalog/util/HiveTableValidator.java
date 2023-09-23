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

import com.amazonaws.services.glue.model.InvalidInputException;
import com.amazonaws.services.glue.model.Table;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import org.apache.hadoop.hive.metastore.TableType;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_STORAGE;

public enum HiveTableValidator {

  REQUIRED_PROPERTIES_VALIDATOR {
    public void validate(Table table) {
      String missingProperty = null;

      if(notApplicableTableType(table)) {
        return;
      }

      if (table.getTableType() == null) {
        missingProperty = "TableType";
      } else if (table.getStorageDescriptor() == null) {
        missingProperty = "StorageDescriptor";
      } else if (table.getStorageDescriptor().getInputFormat() == null) {
        missingProperty = "StorageDescriptor#InputFormat";
      } else if (table.getStorageDescriptor().getOutputFormat() == null) {
        missingProperty = "StorageDescriptor#OutputFormat";
      } else if (table.getStorageDescriptor().getSerdeInfo() == null) {
        missingProperty = "StorageDescriptor#SerdeInfo";
      } else if (table.getStorageDescriptor().getSerdeInfo().getSerializationLibrary() == null) {
        missingProperty = "StorageDescriptor#SerdeInfo#SerializationLibrary";
      }

      if (missingProperty != null) {
        throw new InvalidInputException(String.format("%s cannot be null for table: %s", missingProperty, table.getName()));
      }
    }
  };

  public abstract void validate(Table table);

  private static boolean notApplicableTableType(Table table) {
    if (isNotManagedOrExternalTable(table) ||
            isStorageHandlerType(table)) {
      return true;
    }
    return false;
  }

  private static boolean isNotManagedOrExternalTable(Table table) {
    if (table.getTableType() != null &&
            TableType.valueOf(table.getTableType()) != TableType.MANAGED_TABLE &&
            TableType.valueOf(table.getTableType()) != TableType.EXTERNAL_TABLE) {
      return true;
    }
    return false;
  }

  private static boolean isStorageHandlerType(Table table) {
    if (table.getParameters() != null && table.getParameters().containsKey(META_TABLE_STORAGE) &&
            isNotEmpty(table.getParameters().get(META_TABLE_STORAGE))) {
      return true;
    }
    return false;
  }
}
