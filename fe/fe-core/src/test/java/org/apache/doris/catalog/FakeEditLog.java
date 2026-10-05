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

package org.apache.doris.catalog;

import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.OperationType;
import org.apache.doris.transaction.TransactionState;

import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

public class FakeEditLog implements AutoCloseable {

    private Map<Long, TransactionState> allTransactionState = new HashMap<>();
    private MockedConstruction<EditLog> mockedConstruction;

    public FakeEditLog() {
        mockedConstruction = Mockito.mockConstruction(EditLog.class,
            (mock, context) -> {
                Mockito.when(mock.getNumEditStreams()).thenReturn(1);
                Mockito.doAnswer(inv -> {
                    TransactionState ts = inv.getArgument(0);
                    System.out.println("insert transaction manager is called");
                    allTransactionState.put(ts.getTransactionId(), ts);
                    return null;
                }).when(mock).logInsertTransactionState(Mockito.any(TransactionState.class));
                Mockito.doAnswer(inv -> {
                    short op = inv.getArgument(0);
                    Writable writable = inv.getArgument(1);
                    if (writable instanceof TransactionState) {
                        TransactionState ts = (TransactionState) writable;
                        if (op == OperationType.OP_UPSERT_TRANSACTION_STATE) {
                            allTransactionState.put(ts.getTransactionId(), ts);
                        } else if (op == OperationType.OP_DELETE_TRANSACTION_STATE) {
                            allTransactionState.remove(ts.getTransactionId());
                        }
                    }
                    return null;
                }).when(mock).submitEdit(Mockito.anyShort(), Mockito.any(Writable.class));
            });
    }

    @Override
    public void close() {
        if (mockedConstruction != null) {
            mockedConstruction.close();
            mockedConstruction = null;
        }
    }

    public TransactionState getTransaction(long transactionId) {
        return allTransactionState.get(transactionId);
    }
}
