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

package org.apache.doris.authentication;

/**
 * Failure category for authentication results.
 */
public enum AuthenticationFailureType {
    USER_NOT_FOUND(true),
    BAD_CREDENTIAL(false),
    SOURCE_UNAVAILABLE(true),
    ACCESS_DENIED(false),
    MISCONFIGURED(false),
    INTERNAL_ERROR(false);

    private final boolean shouldContinueInChain;

    AuthenticationFailureType(boolean shouldContinueInChain) {
        this.shouldContinueInChain = shouldContinueInChain;
    }

    public boolean shouldContinueInChain() {
        return shouldContinueInChain;
    }
}
