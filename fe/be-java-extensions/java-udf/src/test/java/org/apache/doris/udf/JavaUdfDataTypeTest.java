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

package org.apache.doris.udf;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

public class JavaUdfDataTypeTest {
    /**
     * TIMESTAMP_NS carries nanoseconds, and only {@code java.time.LocalDateTime} can hold them: the
     * joda types are millisecond-precise, so a function declared over them must not be offered the
     * nanosecond type, while it keeps DATETIMEV2 as before.
     */
    @Test
    public void onlyJavaTimeLocalDateTimeSupportsTimestampNs() {
        Assertions.assertTrue(JavaUdfDataType.getCandidateTypes(LocalDateTime.class)
                .contains(JavaUdfDataType.TIMESTAMP_NS));
        Assertions.assertFalse(JavaUdfDataType.getCandidateTypes(org.joda.time.LocalDateTime.class)
                .contains(JavaUdfDataType.TIMESTAMP_NS));
        Assertions.assertFalse(JavaUdfDataType.getCandidateTypes(org.joda.time.DateTime.class)
                .contains(JavaUdfDataType.TIMESTAMP_NS));
        Assertions.assertTrue(JavaUdfDataType.getCandidateTypes(org.joda.time.LocalDateTime.class)
                .contains(JavaUdfDataType.DATETIMEV2));
    }
}
