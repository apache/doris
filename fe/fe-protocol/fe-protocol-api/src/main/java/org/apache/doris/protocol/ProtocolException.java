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
 * Exception thrown by protocol handlers.
 *
 * <p>This exception is used to signal protocol-level errors such as:
 * <ul>
 *   <li>Configuration errors</li>
 *   <li>Initialization failures</li>
 *   <li>Connection errors</li>
 *   <li>Protocol negotiation failures</li>
 * </ul>
 *
 * @since 2.0.0
 */
public class ProtocolException extends Exception {

    private static final long serialVersionUID = 1L;

    /**
     * Error code for general protocol errors
     */
    public static final int ERROR_GENERAL = 1000;

    /**
     * Error code for configuration errors
     */
    public static final int ERROR_CONFIG = 1001;

    /**
     * Error code for initialization errors
     */
    public static final int ERROR_INIT = 1002;

    /**
     * Error code for connection errors
     */
    public static final int ERROR_CONNECTION = 1003;

    /**
     * Error code for authentication errors
     */
    public static final int ERROR_AUTH = 1004;

    private final int errorCode;

    /**
     * Creates a protocol exception with message.
     *
     * @param message error message
     */
    public ProtocolException(String message) {
        super(message);
        this.errorCode = ERROR_GENERAL;
    }

    /**
     * Creates a protocol exception with message and cause.
     *
     * @param message error message
     * @param cause   underlying cause
     */
    public ProtocolException(String message, Throwable cause) {
        super(message, cause);
        this.errorCode = ERROR_GENERAL;
    }

    /**
     * Creates a protocol exception with error code and message.
     *
     * @param errorCode protocol error code
     * @param message   error message
     */
    public ProtocolException(int errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    /**
     * Creates a protocol exception with error code, message and cause.
     *
     * @param errorCode protocol error code
     * @param message   error message
     * @param cause     underlying cause
     */
    public ProtocolException(int errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    /**
     * Gets the error code.
     *
     * @return error code
     */
    public int getErrorCode() {
        return errorCode;
    }

    /**
     * Creates a configuration error.
     *
     * @param message error message
     * @return protocol exception
     */
    public static ProtocolException configError(String message) {
        return new ProtocolException(ERROR_CONFIG, "Configuration error: " + message);
    }

    /**
     * Creates an initialization error.
     *
     * @param message error message
     * @return protocol exception
     */
    public static ProtocolException initError(String message) {
        return new ProtocolException(ERROR_INIT, "Initialization error: " + message);
    }

    /**
     * Creates an initialization error with cause.
     *
     * @param message error message
     * @param cause   underlying cause
     * @return protocol exception
     */
    public static ProtocolException initError(String message, Throwable cause) {
        return new ProtocolException(ERROR_INIT, "Initialization error: " + message, cause);
    }

    /**
     * Creates a connection error.
     *
     * @param message error message
     * @return protocol exception
     */
    public static ProtocolException connectionError(String message) {
        return new ProtocolException(ERROR_CONNECTION, "Connection error: " + message);
    }

    /**
     * Creates an authentication error.
     *
     * @param message error message
     * @return protocol exception
     */
    public static ProtocolException authError(String message) {
        return new ProtocolException(ERROR_AUTH, "Authentication error: " + message);
    }
}
