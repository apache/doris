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

package org.apache.doris.connector.hive;

/**
 * The detected format type of a Hive-registered table.
 *
 * <p>Tables registered in HMS may actually be backed by different engines.
 * This enum distinguishes them. The Hive connector serves {@link #HIVE}
 * tables directly and delegates the other supported formats to their sibling plugins.</p>
 */
public enum HiveTableType {

    /** Standard Hive table (ORC, Parquet, Text, etc.) */
    HIVE,

    /** Hudi table registered in HMS (detected by input format or flink.connector=hudi) */
    HUDI,

    /** Iceberg table registered in HMS (detected by table_type=ICEBERG parameter) */
    ICEBERG,

    /** Delta Lake table registered in HMS (spark.sql.sources.provider=delta). */
    DELTA,

    /** Cannot determine the table format */
    UNKNOWN;
}
