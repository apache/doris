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

package org.apache.doris.connector.fake;

import org.apache.doris.connector.api.handle.ConnectorTransaction;
import org.apache.doris.connector.api.handle.RewriteCapableTransaction;
import org.apache.doris.connector.api.handle.WriteBlockAllocatingConnectorTransaction;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Verifies the default (read-only) behavior of the write-SPI surface on
 * {@link ConnectorTransaction}. A connector that does not participate in writes leaves the generic
 * defaults (addCommitData no-op, getUpdateCnt 0, profileLabel EXTERNAL) and carries NONE of the narrow
 * source-specific capabilities ({@link WriteBlockAllocatingConnectorTransaction} /
 * {@link RewriteCapableTransaction}).
 */
public class ConnectorTransactionDefaultsTest {

    /** Minimal read-only transaction: overrides only the abstract methods. */
    private static final class ReadOnlyTransaction implements ConnectorTransaction {
        @Override
        public long getTransactionId() {
            return 1L;
        }

        @Override
        public void commit() {
        }

        @Override
        public void rollback() {
        }

        @Override
        public void close() {
        }
    }

    @Test
    void addCommitDataDefaultIsNoOp() {
        // A read-only connector must silently ignore commit fragments, not throw.
        new ReadOnlyTransaction().addCommitData(new byte[] {1, 2, 3});
    }

    @Test
    void readOnlyTransactionCarriesNoSourceSpecificCapability() {
        // Source-specific capabilities are narrow opt-in interfaces, NOT default methods on the shared
        // contract, so a read-only connector transaction is neither of them (the engine's instanceof gates
        // reject it instead of it inheriting a throwing default).
        ConnectorTransaction txn = new ReadOnlyTransaction();
        Assertions.assertFalse(txn instanceof WriteBlockAllocatingConnectorTransaction);
        Assertions.assertFalse(txn instanceof RewriteCapableTransaction);
    }

    @Test
    void getUpdateCntDefaultsZero() {
        Assertions.assertEquals(0L, new ReadOnlyTransaction().getUpdateCnt());
    }

    @Test
    void profileLabelDefaultsToExternal() {
        Assertions.assertEquals("EXTERNAL", new ReadOnlyTransaction().profileLabel());
    }
}
