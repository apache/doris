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

package org.apache.doris.nereids.spm;

/**
 * Storage scope of a baseline. GLOBAL rows are persisted in the shared internal table. SESSION rows belong to a
 * connection and can be copied into the execution context of a forwarded request.
 *
 * GLOBAL ids are allocated below SESSION_ID_BASE, and SESSION ids at or above that threshold. Session counters are
 * local to each FE, so the same numeric SESSION id can exist in different connection stores.
 */
public enum BaselineScope {
    /** Global baseline (persisted; the default). */
    GLOBAL("GLOBAL"),
    /** Session-local baseline (in-memory only). */
    SESSION("SESSION");

    /** First SESSION id, separating the positive GLOBAL and SESSION allocation ranges. */
    public static final long SESSION_ID_BASE = 1L << 62;

    private final String name;

    BaselineScope(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }

    /**
     * Routes an id by the SESSION_ID_BASE threshold. Values below the threshold map to GLOBAL; this method does
     * not validate whether an id was allocated.
     */
    public static BaselineScope ofId(long id) {
        return id >= SESSION_ID_BASE ? SESSION : GLOBAL;
    }

}
