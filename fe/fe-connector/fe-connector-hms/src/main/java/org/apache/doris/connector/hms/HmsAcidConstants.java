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

package org.apache.doris.connector.hms;

/**
 * Hadoop-configuration keys that carry the ACID snapshot (valid transaction list + valid write-id
 * list) from FE to BE for transactional Hive reads.
 *
 * <p>These are a fixed Hive/BE contract: the same literal strings fe-core's {@code AcidUtil}
 * produces (via {@link HmsClient#getValidWriteIds}) and consumes. Defined once here so the write-id
 * producer ({@link ThriftHmsClient}) and the future read-side ACID descent share one source of
 * truth.</p>
 */
public final class HmsAcidConstants {

    /** Serialized {@code ValidTxnList}. */
    public static final String VALID_TXNS_KEY = "hive.txn.valid.txns";

    /** Serialized {@code ValidWriteIdList}. */
    public static final String VALID_WRITEIDS_KEY = "hive.txn.valid.writeids";

    private HmsAcidConstants() {
    }
}
