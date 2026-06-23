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

package org.apache.doris.connector.api.handle;

/**
 * The kind of DML write a {@link ConnectorWriteHandle} carries.
 *
 * <p>This is the <em>operation</em> axis (what the statement does), distinct from the sink
 * <em>mechanism</em> that the removed {@code ConnectorWriteType} used to encode. A single
 * {@code planWrite} reads it to pick the connector's Thrift sink dialect (e.g. iceberg's
 * {@code TIcebergTableSink} vs {@code TIcebergDeleteSink} vs {@code TIcebergMergeSink}), and the
 * iceberg transaction reads it to pick the SDK operation (AppendFiles / ReplacePartitions /
 * OverwriteFiles / RowDelta). The default on {@link ConnectorWriteHandle#getWriteOperation()} is
 * {@link #INSERT}, so connectors that only do plain appends (jdbc / maxcompute) need not declare it.</p>
 */
public enum WriteOperation {
    /** Plain INSERT (append rows). */
    INSERT,
    /** INSERT OVERWRITE (truncate-and-insert, whole-table / dynamic / static partition). */
    OVERWRITE,
    /** DELETE rows matching a predicate. */
    DELETE,
    /** UPDATE rows (delete + re-insert under merge-on-read). */
    UPDATE,
    /** MERGE INTO (matched/not-matched clauses; insert + delete). */
    MERGE
}
