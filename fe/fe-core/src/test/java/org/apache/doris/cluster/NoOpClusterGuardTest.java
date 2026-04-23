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

public class NoOpClusterGuardTest {

    @Test
    public void testOnStartupDoesNotThrow() throws ClusterGuardException {
        NoOpClusterGuard.INSTANCE.onStartup("/some/home/dir");
        // no exception expected
    }

    @Test
    public void testCheckTimeValidityDoesNotThrow() throws ClusterGuardException {
        NoOpClusterGuard.INSTANCE.checkTimeValidity();
        // always valid in open-source edition
    }

    @Test
    public void testCheckNodeLimitDoesNotThrow() throws ClusterGuardException {
        // should pass for any node count
        NoOpClusterGuard.INSTANCE.checkNodeLimit(0);
        NoOpClusterGuard.INSTANCE.checkNodeLimit(1);
        NoOpClusterGuard.INSTANCE.checkNodeLimit(Integer.MAX_VALUE);
    }

    @Test
    public void testGetGuardInfoReturnsEmptyJson() {
        String info = NoOpClusterGuard.INSTANCE.getGuardInfo();
        Assert.assertEquals("{}", info);
    }

    @Test
    public void testSingletonIdentity() {
        Assert.assertSame(NoOpClusterGuard.INSTANCE, NoOpClusterGuard.INSTANCE);
    }
}
