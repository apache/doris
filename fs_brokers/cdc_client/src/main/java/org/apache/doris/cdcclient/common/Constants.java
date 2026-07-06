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

package org.apache.doris.cdcclient.common;

public class Constants {
    public static final String DORIS_DELETE_SIGN = "__DORIS_DELETE_SIGN__";
    public static final long POLL_SPLIT_RECORDS_TIMEOUTS = 15000L;

    // Debezium default properties
    public static final long DEBEZIUM_HEARTBEAT_INTERVAL_MS = 3000L;

    public static final String DORIS_TARGET_DB = "doris_target_db";

    // Background cleanup tick: idle-reader release + retrying deferred slot drops.
    public static final long BACKGROUND_CLEANUP_INTERVAL_MS = 15_000L;
    // Idle from-to reader cleanup: release (keep slot) when idle past MULTIPLIER * max_interval.
    public static final int IDLE_READER_TIMEOUT_MULTIPLIER = 10;
    // Floor the idle timeout: PG reader rebuild is costly, absorb heartbeat jitter at small
    // interval.
    public static final long IDLE_READER_MIN_TIMEOUT_MS = 90_000L;

    // Retry dropping a slot still held by a dead BE until it frees (wal_sender_timeout) or this
    // elapses.
    public static final long SLOT_DROP_RETRY_WINDOW_MS = 300_000L;
}
