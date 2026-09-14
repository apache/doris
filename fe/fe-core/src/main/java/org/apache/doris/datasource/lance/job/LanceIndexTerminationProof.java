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

package org.apache.doris.datasource.lance.job;

/**
 * Evidence that releases a possible-live worker slot. Termination proof
 * releases only that slot: it neither changes an UNKNOWN outcome nor releases
 * the same-name fence. Deadlines bound wait/runtime but never prove
 * termination.
 */
public enum LanceIndexTerminationProof {
    /** No proof; the worker may still be running. */
    NONE,
    /** The supervisor reaped the exact matching child process. */
    CHILD_REAPED,
    /** The recorded BE process epoch no longer exists (the BE process was replaced). */
    BE_PROCESS_EPOCH_GONE
}
