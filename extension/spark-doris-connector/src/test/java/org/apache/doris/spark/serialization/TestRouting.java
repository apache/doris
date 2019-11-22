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

package org.apache.doris.spark.serialization;

import static org.hamcrest.core.StringStartsWith.startsWith;

import org.apache.doris.spark.exception.IllegalArgumentException;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;


public class TestRouting {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testRouting() throws Exception {
        Routing r1 = new Routing("10.11.12.13:1234");
        Assert.assertEquals("10.11.12.13", r1.getHost());
        Assert.assertEquals(1234, r1.getPort());

        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(startsWith("argument "));
        new Routing("10.11.12.13:wxyz");

        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(startsWith("Parse "));
        new Routing("10.11.12.13");
    }
}
