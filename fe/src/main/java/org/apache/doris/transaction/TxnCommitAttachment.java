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

import org.apache.doris.common.io.Writable;
import org.apache.doris.load.routineload.RLTaskTxnCommitAttachment;
import org.apache.doris.thrift.TTxnCommitAttachment;

import java.io.DataInput;
import java.io.IOException;

public abstract class TxnCommitAttachment implements Writable {

    public static TxnCommitAttachment readTxnCommitAttachment(DataInput in,
                                                              TransactionState.LoadJobSourceType sourceType)
            throws IOException {
        switch (sourceType) {
            case ROUTINE_LOAD_TASK:
                RLTaskTxnCommitAttachment RLTaskTxnCommitAttachment = new RLTaskTxnCommitAttachment();
                RLTaskTxnCommitAttachment.readFields(in);
                return RLTaskTxnCommitAttachment;
            default:
                return null;
        }
    }

    public static TxnCommitAttachment fromThrift(TTxnCommitAttachment txnCommitAttachment) {
        if (txnCommitAttachment != null) {
            switch (txnCommitAttachment.txnSourceType) {
                case ROUTINE_LOAD_TASK:
                    return new RLTaskTxnCommitAttachment(txnCommitAttachment.getRlTaskTxnCommitAttachment());
                default:
                    return null;
            }
        } else {
            return null;
        }
    }
}
