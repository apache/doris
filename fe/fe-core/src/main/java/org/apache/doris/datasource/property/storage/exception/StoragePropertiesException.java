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

/**
 * Custom exception class for handling storage property-related errors.
 * This exception class extends RuntimeException and is used to handle errors related
 * to storage properties at runtime. It provides two constructors: one that accepts only
 * an error message, and another that also accepts a cause exception.
 */

package org.apache.doris.datasource.property.storage.exception;

public class StoragePropertiesException extends RuntimeException {

    /**
     * Constructor that initializes the exception with an error message.
     *
     * @param message The error message describing the reason for the exception.
     */
    public StoragePropertiesException(String message) {
        super(message);
    }

    /**
     * Constructor that initializes the exception with a message and a cause.
     *
     * @param message The error message describing the reason for the exception.
     * @param cause   The underlying cause of the exception, typically another Throwable.
     */
    public StoragePropertiesException(String message, Throwable cause) {
        super(message, cause);
    }
}
