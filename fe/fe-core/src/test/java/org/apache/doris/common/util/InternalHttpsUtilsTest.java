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

package org.apache.doris.common.util;

import org.junit.Assert;
import org.junit.Test;

import javax.net.ssl.SSLContext;

public class InternalHttpsUtilsTest {

    /**
     * Verifies that getSslContext() returns the same SSLContext instance on every call —
     * i.e. the double-checked locking cache works correctly and buildSslContext() is not
     * invoked on subsequent calls.
     *
     * SSLContext.getInstance("TLS").init(null, null, null) is valid without a truststore,
     * so the test does not require a truststore file on disk.
     */
    @Test
    public void testGetSslContextReturnsCachedInstance() throws Exception {
        SSLContext previous = InternalHttpsUtils.cachedSslContext;

        SSLContext injected = SSLContext.getInstance("TLS");
        injected.init(null, null, null);
        InternalHttpsUtils.cachedSslContext = injected;

        try {
            SSLContext first = InternalHttpsUtils.getSslContext();
            SSLContext second = InternalHttpsUtils.getSslContext();

            Assert.assertSame("getSslContext() must return the injected cached instance", injected, first);
            Assert.assertSame("getSslContext() must return the same instance on every call", first, second);
        } finally {
            InternalHttpsUtils.cachedSslContext = previous;
        }
    }
}
