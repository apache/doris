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

package org.apache.doris.datasource.jdbc.client;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;

public class JdbcClickHouseClientTest {

    @Test
    public void testIsNewClickHouseDriver() {
        try {
            Method method = JdbcClickHouseClient.class.getDeclaredMethod("isNewClickHouseDriver", String.class);
            method.setAccessible(true);

            // Valid test cases
            Assert.assertTrue((boolean) method.invoke(null, "0.5.0")); // Major version 0, Minor version 5
            Assert.assertTrue((boolean) method.invoke(null, "1.0.0")); // Major version 1
            Assert.assertTrue((boolean) method.invoke(null, "0.6.3 (revision: a6a8a22)")); // Major version 0, Minor version 6
            Assert.assertFalse((boolean) method.invoke(null, "0.4.2 (revision: 1513b27)")); // Major version 0, Minor version 4

            // Invalid version formats
            try {
                method.invoke(null, "invalid.version"); // Invalid version format
                Assert.fail("Expected JdbcClientException for invalid version 'invalid.version'");
            } catch (Exception e) {
                Assert.assertTrue(e.getCause() instanceof JdbcClientException);
                Assert.assertTrue(e.getCause().getMessage().contains("Invalid clickhouse driver version format"));
            }

            try {
                method.invoke(null, ""); // Empty version
                Assert.fail("Expected JdbcClientException for empty version");
            } catch (Exception e) {
                Assert.assertTrue(e.getCause() instanceof JdbcClientException);
                Assert.assertTrue(e.getCause().getMessage().contains("Invalid clickhouse driver version format"));
            }

            try {
                method.invoke(null, (Object) null); // Null version
                Assert.fail("Expected JdbcClientException for null version");
            } catch (Exception e) {
                Assert.assertTrue(e.getCause() instanceof JdbcClientException);
                Assert.assertTrue(e.getCause().getMessage().contains("Driver version cannot be null"));
            }
        } catch (Exception e) {
            Assert.fail("Exception occurred while testing isNewClickHouseDriver: " + e.getMessage());
        }
    }
}
