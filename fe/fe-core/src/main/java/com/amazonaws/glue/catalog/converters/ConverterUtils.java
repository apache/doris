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

import com.amazonaws.services.glue.model.Table;
import com.google.gson.Gson;

import java.util.Date;
import java.util.concurrent.TimeUnit;

public class ConverterUtils {

  private static final Gson gson = new Gson();

  public static String catalogTableToString(final Table table) {
    return gson.toJson(table);
  }

  public static Table stringToCatalogTable(final String input) {
    return gson.fromJson(input, Table.class);
  }

  public static org.apache.hadoop.hive.metastore.api.Date dateToHiveDate(Date date) {
    return new org.apache.hadoop.hive.metastore.api.Date(TimeUnit.MILLISECONDS.toDays(date.getTime()));
  }

  public static Date hiveDatetoDate(org.apache.hadoop.hive.metastore.api.Date hiveDate) {
    return new Date(TimeUnit.DAYS.toMillis(hiveDate.getDaysSinceEpoch()));
  }
}
