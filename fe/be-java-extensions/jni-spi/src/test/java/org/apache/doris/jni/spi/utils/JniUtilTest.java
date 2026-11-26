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

package org.apache.doris.jni.spi.utils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;

class JniUtilTest {
    @Test
    void preservesOrdinaryExceptionSummaries() {
        RuntimeException cause = new RuntimeException("root");
        Assertions.assertEquals("Exception: context | CAUSED BY: RuntimeException: root",
                JniUtil.throwableToString(new Exception("context", cause)));
    }

    @Test
    void terminatesOnOrdinaryCauseCycles() {
        Exception first = new Exception("one");
        Exception second = new Exception("two", first);
        first.initCause(second);
        Assertions.assertTimeoutPreemptively(Duration.ofSeconds(2), () -> Assertions.assertEquals(
                "Exception: one | CAUSED BY: Exception: two", JniUtil.throwableToString(first)));
    }
}
