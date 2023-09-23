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

package org.apache.doris.nereids.util;

import org.junit.jupiter.api.Assertions;

import java.util.function.Function;

/**
 * Helper to check exception message.
 */
public class ExceptionChecker {
    private final Throwable exception;

    public ExceptionChecker(Throwable exception) {
        this.exception = exception;
    }

    public ExceptionChecker assertMessageEquals(String message) {
        Assertions.assertEquals(message, exception.getMessage());
        return this;
    }

    public ExceptionChecker assertMessageContains(String context) {
        Assertions.assertTrue(exception.getMessage().contains(context), exception.getMessage());
        return this;
    }

    public ExceptionChecker assertWith(Function<Throwable, Boolean> asserter) {
        Assertions.assertTrue(asserter.apply(exception));
        return this;
    }
}
