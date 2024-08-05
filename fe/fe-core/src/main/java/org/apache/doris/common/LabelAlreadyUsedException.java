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

package org.apache.doris.common;

import org.apache.doris.transaction.TransactionState;

import com.google.common.base.Preconditions;

public class LabelAlreadyUsedException extends DdlException {

    private static final long serialVersionUID = -6798925248765094813L;

    // status of existing load job
    // RUNNING or FINISHED
    private String jobStatus;

    public LabelAlreadyUsedException(String label) {
        this(label, true);
    }

    public LabelAlreadyUsedException(String msg, boolean isLabel) {
        super(isLabel ? "Label [" + msg + "] has already been used." : msg);
    }

    public LabelAlreadyUsedException(TransactionState txn) {
        super("Label [" + txn.getLabel() + "] has already been used, relate to txn [" + txn.getTransactionId() + "]");
        switch (txn.getTransactionStatus()) {
            case UNKNOWN:
            case PREPARE:
                jobStatus = "RUNNING";
                break;
            case PRECOMMITTED:
                jobStatus = "PRECOMMITTED";
                break;
            case COMMITTED:
            case VISIBLE:
                jobStatus = "FINISHED";
                break;
            default:
                Preconditions.checkState(false, txn.getTransactionStatus());
                break;
        }
    }

    public LabelAlreadyUsedException(String label, String subLabel) {
        super("Sub label [" + subLabel + "] has already been used.");
    }

    public String getJobStatus() {
        return jobStatus;
    }
}
