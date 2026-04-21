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

package org.apache.doris.cluster;

import org.junit.Assert;
import org.junit.Test;

public class ClusterGuardExceptionTest {

    @Test
    public void testMessageConstructor() {
        ClusterGuardException ex = new ClusterGuardException("policy violated");
        Assert.assertEquals("policy violated", ex.getMessage());
        Assert.assertNull(ex.getCause());
    }

    @Test
    public void testMessageAndCauseConstructor() {
        RuntimeException cause = new RuntimeException("root cause");
        ClusterGuardException ex = new ClusterGuardException("wrapped", cause);
        Assert.assertEquals("wrapped", ex.getMessage());
        Assert.assertSame(cause, ex.getCause());
    }

    @Test
    public void testIsCheckedException() {
        // ClusterGuardException must be a checked exception (extends Exception, not RuntimeException).
        // Cast to Object first so the compiler does not reject the instanceof check as always-false.
        Object ex = new ClusterGuardException("test");
        Assert.assertTrue(ex instanceof Exception);
        Assert.assertFalse(ex instanceof RuntimeException);
    }
}
