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

import org.apache.doris.connector.api.pushdown.ConnectorPredicate;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Pins the contract of the degenerate {@link NoOpConnectorTransaction} used by connectors whose
 * writes are auto-committed by BE (e.g. jdbc).
 *
 * <p><b>WHY this matters:</b> {@link NoOpConnectorTransaction#getUpdateCnt()} must return
 * {@code -1}, NOT the {@link ConnectorTransaction} default of {@code 0}. The insert executor
 * backfills {@code loadedRows} from {@code getUpdateCnt()} only when it is {@code >= 0}; a
 * {@code 0} here would clobber the coordinator's row count and report "affected rows: 0" for
 * every jdbc INSERT. {@code -1} ("no count from the transaction") is the agreed sentinel and is
 * deliberately distinct from a genuine zero-row write.</p>
 */
public class NoOpConnectorTransactionTest {

    @Test
    public void getUpdateCntReturnsMinusOneSentinelNotZeroDefault() {
        NoOpConnectorTransaction txn = new NoOpConnectorTransaction(123L, "JDBC");
        Assertions.assertEquals(-1L, txn.getUpdateCnt(),
                "no-op transaction must report -1 (no count) so the executor keeps the coordinator "
                        + "row counter rather than overwriting affected-rows with 0");
    }

    @Test
    public void carriesIdAndProfileLabel() {
        NoOpConnectorTransaction txn = new NoOpConnectorTransaction(456L, "JDBC");
        Assertions.assertEquals(456L, txn.getTransactionId());
        Assertions.assertEquals("JDBC", txn.profileLabel());
    }

    @Test
    public void commitRollbackCloseAreNoOps() {
        NoOpConnectorTransaction txn = new NoOpConnectorTransaction(789L, "JDBC");
        // Auto-committed by BE; FE-side lifecycle must do nothing and never throw.
        Assertions.assertDoesNotThrow(() -> {
            txn.commit();
            txn.rollback();
            txn.close();
        });
    }

    @Test
    public void applyWriteConstraintIsNoOpByDefault() {
        NoOpConnectorTransaction txn = new NoOpConnectorTransaction(321L, "JDBC");
        // O5-2 default: a connector that does no optimistic conflict detection ignores the write constraint
        // (and tolerates a null), so jdbc/maxcompute/es/trino are unaffected by the new SPI method.
        Assertions.assertDoesNotThrow(() -> {
            txn.applyWriteConstraint(null);
            txn.applyWriteConstraint(new ConnectorPredicate(null));
        });
    }
}
