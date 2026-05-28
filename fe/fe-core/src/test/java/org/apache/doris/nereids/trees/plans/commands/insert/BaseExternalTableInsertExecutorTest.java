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

package org.apache.doris.nereids.trees.plans.commands.insert;

import org.apache.doris.qe.SessionVariable;
import org.apache.doris.transaction.TransactionStatus;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for external table DML status selection.
 */
public class BaseExternalTableInsertExecutorTest {

    @Test
    public void testGetExternalTableDmlReturnStatus() {
        SessionVariable sessionVariable = new SessionVariable();
        Assertions.assertEquals(TransactionStatus.VISIBLE,
                BaseExternalTableInsertExecutor.getExternalTableDmlReturnStatus(sessionVariable));

        // External table DML can still report COMMITTED when the session variable requests it.
        sessionVariable.setExternalTableDmlReturnStatus(SessionVariable.EXTERNAL_TABLE_DML_RETURN_STATUS_COMMITTED);
        Assertions.assertEquals(TransactionStatus.COMMITTED,
                BaseExternalTableInsertExecutor.getExternalTableDmlReturnStatus(sessionVariable));
    }
}
