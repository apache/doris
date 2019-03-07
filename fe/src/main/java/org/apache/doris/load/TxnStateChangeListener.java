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

package org.apache.doris.load;

import org.apache.doris.transaction.AbortTransactionException;
import org.apache.doris.transaction.TransactionException;
import org.apache.doris.transaction.TransactionState;

public interface TxnStateChangeListener {

    void beforeCommitted(TransactionState txnState) throws TransactionException;

    /**
     * update catalog of job which has related txn after transaction has been committed
     *
     * @param txnState
     */
    void onCommitted(TransactionState txnState) throws TransactionException;

    /**
     * this interface is executed before txn aborted, you can check if txn could be abort in this stage
     *
     * @param txnState
     * @param txnStatusChangeReason maybe null
     * @throws AbortTransactionException if transaction could not be abort or there are some exception before aborted,
     *                                   it will throw this exception
     */
    void beforeAborted(TransactionState txnState, String txnStatusChangeReason)
            throws AbortTransactionException;

    /**
     * this interface is executed when transaction has been aborted
     *
     * @param txnState
     * @param txnStatusChangeReason maybe null
     */
    void onAborted(TransactionState txnState, String txnStatusChangeReason);
}
