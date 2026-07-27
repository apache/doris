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

package org.apache.doris.common.jni.vec;

import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDateTime;

public class VectorColumnTest {

    @Test
    public void testToEpochNanosBoundaries() {
        Assert.assertEquals(Long.MIN_VALUE, VectorColumn.toEpochNanos(
                LocalDateTime.parse("1677-09-21T00:12:43.145224192")));
        Assert.assertEquals(-1, VectorColumn.toEpochNanos(
                LocalDateTime.parse("1969-12-31T23:59:59.999999999")));
        Assert.assertEquals(0, VectorColumn.toEpochNanos(
                LocalDateTime.parse("1970-01-01T00:00:00")));
        Assert.assertEquals(Long.MAX_VALUE, VectorColumn.toEpochNanos(
                LocalDateTime.parse("2262-04-11T23:47:16.854775807")));
    }
}
