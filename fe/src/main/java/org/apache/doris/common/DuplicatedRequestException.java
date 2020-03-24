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

/*
 * This exception throws when the request from Backend is duplicated.
 * It is currently used for mini load and stream load's begin txn requests.
 * Because the request may be a retry request, so that we should throw this exception
 * and return the 'already-begun' txn id.
 */
public class DuplicatedRequestException extends DdlException {

    private String duplicatedRequestId;
    // save exist txn id
    private long txnId;

    public DuplicatedRequestException(String duplicatedRequestId, long txnId, String msg) {
        super(msg);
        this.duplicatedRequestId = duplicatedRequestId;
        this.txnId = txnId;
    }

    public long getTxnId() {
        return txnId;
    }

    public String getDuplicatedRequestId() {
        return duplicatedRequestId;
    }
}
