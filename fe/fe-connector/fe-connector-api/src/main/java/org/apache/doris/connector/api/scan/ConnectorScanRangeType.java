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

package org.apache.doris.connector.api.scan;

/**
 * Identifies the type of a {@link ConnectorScanRange}, which determines
 * how BE processes the scan range.
 *
 * <p>Each type maps to a specific Thrift scan range variant in the
 * execution layer. Connectors choose the appropriate type based on
 * their data access pattern:</p>
 * <ul>
 *   <li>{@link #FILE_SCAN} — for file-based connectors (Hive, Iceberg, Paimon, Hudi, Elasticsearch)</li>
 *   <li>{@link #JDBC_SCAN} — for JDBC connectors</li>
 *   <li>{@link #REMOTE_OLAP_SCAN} — for remote Doris/OLAP federation</li>
 *   <li>{@link #CUSTOM} — for connectors with custom scan patterns</li>
 * </ul>
 */
public enum ConnectorScanRangeType {

    /** File-based scan: path + offset + length. Used by Hive, Iceberg, Paimon, Hudi. */
    FILE_SCAN,

    /** JDBC scan: connection info + SQL query. */
    JDBC_SCAN,

    /** Remote OLAP scan: tablet info for Doris federation. */
    REMOTE_OLAP_SCAN,

    /** Custom scan: all information carried in properties map. */
    CUSTOM
}
