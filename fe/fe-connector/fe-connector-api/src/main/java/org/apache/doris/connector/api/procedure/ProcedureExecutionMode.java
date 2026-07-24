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

package org.apache.doris.connector.api.procedure;

/**
 * How the engine must drive a connector table procedure ({@code ALTER TABLE t EXECUTE proc(...)}).
 *
 * <p>This is the procedure-routing analogue of Trino's {@code TableProcedureExecutionMode}
 * ({@code coordinatorOnly()} vs {@code distributedWithFilteringAndRepartitioning()}). The engine reads
 * {@link ConnectorProcedureOps#getExecutionMode(String)} to decide whether a procedure can run as a single
 * synchronous in-FE call or needs distributed orchestration — WITHOUT hard-coding a procedure name in a
 * general engine class.</p>
 */
public enum ProcedureExecutionMode {

    /**
     * Coordinator-local: the procedure body is a single synchronous call that the engine dispatches through
     * {@link ConnectorProcedureOps#execute} and wraps into one result set. This is the default for every
     * procedure (e.g. the eight pure-SDK iceberg snapshot/maintenance procedures).
     */
    SINGLE_CALL,

    /**
     * Distributed: the procedure rewrites data and must be orchestrated by the engine as a distributed
     * read-write job (e.g. iceberg {@code rewrite_data_files}: N per-group INSERT-SELECT writes under one
     * shared connector transaction). The connector contributes only planning and commit through narrow SPI;
     * the distributed execution loop stays in the engine. Such procedures are NOT routed through
     * {@link ConnectorProcedureOps#execute} (whose single-result contract cannot express them).
     */
    DISTRIBUTED
}
