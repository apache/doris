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

package org.apache.doris.transaction;

import org.apache.doris.common.UserException;

public interface Transaction {

    void commit() throws UserException;

    void rollback();

    /**
     * Receives one serialized commit fragment produced by BE after writing a
     * data fragment. Implementations deserialize their connector-specific Thrift
     * payload and accumulate it for {@link #commit()}.
     *
     * <p>Default is a no-op for transactions that do not collect BE commit data.</p>
     *
     * @param commitFragment the serialized connector-specific commit payload
     */
    default void addCommitData(byte[] commitFragment) {
        // no-op: write transactions override this
    }

    /** Returns the number of rows affected by the write(s) in this transaction. */
    default long getUpdateCnt() {
        return 0;
    }
}
