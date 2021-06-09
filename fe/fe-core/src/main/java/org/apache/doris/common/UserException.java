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

import com.google.common.base.Strings;

/**
 * Thrown for internal server errors.
 */
public class UserException extends Exception {
    private InternalErrorCode errorCode;
    public UserException(String msg, Throwable cause) {
        super(Strings.nullToEmpty(msg), cause);
        errorCode = InternalErrorCode.INTERNAL_ERR;
    }

    public UserException(Throwable cause) {
        super(cause);
        errorCode = InternalErrorCode.INTERNAL_ERR;
    }

    public UserException(String msg, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(Strings.nullToEmpty(msg), cause, enableSuppression, writableStackTrace);
        errorCode = InternalErrorCode.INTERNAL_ERR;
    }

    public UserException(String msg) {
        super(Strings.nullToEmpty(msg));
        errorCode = InternalErrorCode.INTERNAL_ERR;
    }

    public UserException(InternalErrorCode errCode, String msg) {
        super(Strings.nullToEmpty(msg));
        this.errorCode = errCode;
    }

    public InternalErrorCode getErrorCode() {
        return errorCode;
    }

    @Override
    public String getMessage() {
        return errorCode + ", detailMessage = " + super.getMessage();
    }
}
