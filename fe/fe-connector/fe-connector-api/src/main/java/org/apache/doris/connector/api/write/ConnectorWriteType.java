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

package org.apache.doris.connector.api.write;

/**
 * Identifies the type of a write operation, which determines how BE
 * processes the data sink.
 *
 * <p>Each type maps to a specific Thrift data sink variant in the
 * execution layer. Connectors choose the appropriate type based on
 * their write mechanism:</p>
 * <ul>
 *   <li>{@link #FILE_WRITE} — write data files to external storage (Hive, Iceberg, Paimon, Hudi)</li>
 *   <li>{@link #JDBC_WRITE} — execute INSERT statements via JDBC</li>
 *   <li>{@link #REMOTE_OLAP_WRITE} — stream load to remote Doris cluster</li>
 *   <li>{@link #CUSTOM} — connector-specific write mechanism</li>
 * </ul>
 */
public enum ConnectorWriteType {

    /** File-based write: produce data files (Parquet, ORC, etc.) in external storage. */
    FILE_WRITE,

    /** JDBC write: execute INSERT/UPSERT statements through JDBC connection. */
    JDBC_WRITE,

    /** Remote OLAP write: stream load to another Doris cluster. */
    REMOTE_OLAP_WRITE,

    /** Custom write: all configuration carried in properties map. */
    CUSTOM
}
