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

package org.apache.doris.protocol;

/**
 * Authentication result returned by protocol handlers.
 *
 * <p>This class encapsulates the result of authentication operations,
 * providing a protocol-agnostic way to communicate authentication status.
 */
public class AuthenticationResult {

    private final boolean success;
    private final String userName;
    private final String errorMessage;
    private final int errorCode;

    private AuthenticationResult(boolean success, String userName, String errorMessage, int errorCode) {
        this.success = success;
        this.userName = userName;
        this.errorMessage = errorMessage;
        this.errorCode = errorCode;
    }

    /**
     * Creates a successful authentication result.
     *
     * @param userName authenticated user name
     * @return success result
     */
    public static AuthenticationResult success(String userName) {
        return new AuthenticationResult(true, userName, null, 0);
    }

    /**
     * Creates a failed authentication result.
     *
     * @param errorMessage error message
     * @return failure result
     */
    public static AuthenticationResult failure(String errorMessage) {
        return new AuthenticationResult(false, null, errorMessage, ProtocolException.ERROR_AUTH);
    }

    /**
     * Creates a failed authentication result with error code.
     *
     * @param errorCode    error code
     * @param errorMessage error message
     * @return failure result
     */
    public static AuthenticationResult failure(int errorCode, String errorMessage) {
        return new AuthenticationResult(false, null, errorMessage, errorCode);
    }

    /**
     * Checks if authentication was successful.
     *
     * @return true if successful
     */
    public boolean isSuccess() {
        return success;
    }

    /**
     * Gets the authenticated user name.
     *
     * @return user name, or null if authentication failed
     */
    public String getUserName() {
        return userName;
    }

    /**
     * Gets the error message.
     *
     * @return error message, or null if successful
     */
    public String getErrorMessage() {
        return errorMessage;
    }

    /**
     * Gets the error code.
     *
     * @return error code, or 0 if successful
     */
    public int getErrorCode() {
        return errorCode;
    }

    @Override
    public String toString() {
        if (success) {
            return "AuthenticationResult{success=true, user='" + userName + "'}";
        } else {
            return "AuthenticationResult{success=false, error='" + errorMessage + "', code=" + errorCode + "}";
        }
    }
}
