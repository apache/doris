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
 * Engine-neutral category for a connector's SPECIAL (non-data-file) columns, returned by
 * {@link ConnectorScanPlanProvider#classifyColumn(String)}.
 *
 * <p>This lets a connector tell the generic scan node how to classify the synthetic / generated
 * columns it owns (e.g. iceberg's hidden row-id and v3 row-lineage columns) WITHOUT the generic node
 * importing any connector-specific code. The generic node maps these to its internal BE column
 * categories; {@link #DEFAULT} means "not a connector special column" — the node falls through to its
 * own partition-key / regular classification.</p>
 */
public enum ConnectorColumnCategory {

    /** Not a connector special column: let the generic node classify it (partition key / regular). */
    DEFAULT,

    /** Synthesized column: never present in the data file, materialized by the connector reader. */
    SYNTHESIZED,

    /** Generated column: read from the file when present, otherwise backfilled by the connector reader. */
    GENERATED
}
