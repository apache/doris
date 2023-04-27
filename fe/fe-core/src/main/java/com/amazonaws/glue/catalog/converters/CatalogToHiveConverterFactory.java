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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hive.common.util.HiveVersionInfo;

public class CatalogToHiveConverterFactory {

  private static final String HIVE_3_VERSION = "3.";

  private static CatalogToHiveConverter catalogToHiveConverter;

  public static CatalogToHiveConverter getCatalogToHiveConverter() {
    if (catalogToHiveConverter == null) {
      catalogToHiveConverter = loadConverter();
    }
    return catalogToHiveConverter;
  }

  private static CatalogToHiveConverter loadConverter() {
    String hiveVersion = HiveVersionInfo.getShortVersion();

    if (hiveVersion.startsWith(HIVE_3_VERSION)) {
      return new Hive3CatalogToHiveConverter();
    } else {
      return new BaseCatalogToHiveConverter();
    }
  }

  @VisibleForTesting
  static void clearConverter() {
    catalogToHiveConverter = null;
  }
}
