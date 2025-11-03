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

import org.apache.doris.proto.Types;
import org.apache.doris.proto.Types.PStatus;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;

import org.apache.logging.log4j.message.ParameterizedMessage;

public class Status {
    public static final Status OK = new Status();
    public static final Status CANCELLED = new Status(TStatusCode.CANCELLED, "Cancelled");
    public static final Status TIMEOUT = new Status(TStatusCode.TIMEOUT, "Timeout");

    private TStatusCode  errorCode; // anything other than OK
    private String errorMsg;

    public Status() {
        this.errorCode = TStatusCode.OK;
        this.errorMsg = null;
    }

    public Status(Status status) {
        this(status.getErrorCode(), status.getErrorMsg());
    }

    public Status(TStatusCode code, final String errorMsg) {
        this.errorCode = code;
        this.errorMsg = errorMsg;
    }

    public Status(TStatusCode code, final String errorMsg, final Object...params) {
        this.errorCode = code;
        this.errorMsg = ParameterizedMessage.format(errorMsg, params);
    }

    public Status(final TStatus status) {
        this.errorCode = status.status_code;
        if (status.isSetErrorMsgs()) {
            this.errorMsg = status.error_msgs.get(0);
        }
    }

    public Status(final PStatus status) {
        TStatusCode mappingCode = TStatusCode.findByValue(status.getStatusCode());
        // Not all pstatus code could be mapped to TStatusCode, see BE status.h file
        // For those not mapped, set it to internal error.
        if (mappingCode == null) {
            this.errorCode = TStatusCode.INTERNAL_ERROR;
        } else {
            this.errorCode = mappingCode;
        }
        if (!status.getErrorMsgsList().isEmpty()) {
            this.errorMsg = status.getErrorMsgs(0);
        }
    }

    // TODO add a unit test to ensure all TStatusCode is subset of PStatus error code.
    public PStatus toPStatus() {
        return PStatus.newBuilder().setStatusCode(errorCode.getValue())
                .addErrorMsgs(errorMsg).build();
    }

    public void updateStatus(TStatusCode code, String errorMessage) {
        this.errorCode = code;
        this.errorMsg = errorMessage;
    }

    public boolean ok() {
        return this.errorCode == TStatusCode.OK;
    }

    public boolean isCancelled() {
        return this.errorCode == TStatusCode.CANCELLED;
    }

    public boolean isRpcError() {
        return this.errorCode == TStatusCode.THRIFT_RPC_ERROR;
    }

    public TStatusCode getErrorCode() {
        return errorCode;
    }

    public String getErrorMsg() {
        return errorMsg;
    }

    // This logic is a temp logic.
    // It is just for compatible upgrade from old version BE. When using old
    // BE and new FE, old BE need cancel reason.
    // Should remove this logic in the future.
    public Types.PPlanFragmentCancelReason getPCancelReason() {
        switch (errorCode) {
            case LIMIT_REACH: {
                // Only limit reach is useless.
                // BE will not send message to fe when meet this error code.
                return Types.PPlanFragmentCancelReason.LIMIT_REACH;
            } case TIMEOUT: {
                return Types.PPlanFragmentCancelReason.TIMEOUT;
            } case MEM_ALLOC_FAILED: {
                return Types.PPlanFragmentCancelReason.MEMORY_LIMIT_EXCEED;
            } default: {
                return Types.PPlanFragmentCancelReason.INTERNAL_ERROR;
            }
        }
    }

    public void rewriteErrorMsg() {
        if (ok()) {
            return;
        }

        switch (errorCode) {
            case CANCELLED: {
                this.errorMsg = "Cancelled";
                break;
            } case ANALYSIS_ERROR: {
                this.errorMsg = "Analysis_error";
                break;
            } case NOT_IMPLEMENTED_ERROR: {
                this.errorMsg = "Not_implemented_error";
                break;
            } case RUNTIME_ERROR: {
                this.errorMsg = "Runtime_error";
                break;
            } case MEM_LIMIT_EXCEEDED: {
                this.errorMsg = "Mem_limit_error";
                break;
            } case INTERNAL_ERROR: {
                this.errorMsg = "Internal_error";
                break;
            } case THRIFT_RPC_ERROR: {
                this.errorMsg = "Thrift_rpc_error";
                break;
            } case TIMEOUT: {
                this.errorMsg = "Timeout";
                break;
            } default: {
                this.errorMsg = "Unknown_error";
                break;
            }
        }
    }

    @Override
    public String toString() {
        return "Status [errorCode=" + errorCode + ", errorMsg=" + errorMsg + "]";
    }
}
