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

package org.apache.doris.connector.jdbc;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Tests for the mandatory, non-configurable create-time driver_url security rule
 * in {@link JdbcDorisConnector#checkDriverUrlSecurityRule(String)}.
 */
public class JdbcDriverUrlSecurityRuleTest {

    // ---- rejected ----

    @Test
    public void testBareNameTraversalRejected() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> JdbcDorisConnector.checkDriverUrlSecurityRule("../evil.jar"));
    }

    @Test
    public void testBareNameWithDirectoryRejected() {
        // A scheme-less driver_url must be a plain file name; any '/' fails the charset check.
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> JdbcDorisConnector.checkDriverUrlSecurityRule("sub/dir/driver.jar"));
    }

    @Test
    public void testBareNameSpecialCharsRejected() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> JdbcDorisConnector.checkDriverUrlSecurityRule("driver.jar; rm -rf /"));
    }

    @Test
    public void testFileUrlTraversalRejected() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> JdbcDorisConnector.checkDriverUrlSecurityRule(
                        "file:///opt/doris/plugins/jdbc_drivers/../../etc/evil.jar"));
    }

    @Test
    public void testHttpUrlTraversalRejected() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> JdbcDorisConnector.checkDriverUrlSecurityRule("http://host/a/../b.jar"));
    }

    @Test
    public void testEncodedTraversalRejected() {
        // %2e%2e decodes to "..", which must be caught on the decoded path.
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> JdbcDorisConnector.checkDriverUrlSecurityRule(
                        "file:///opt/doris/plugins/jdbc_drivers/%2e%2e/%2e%2e/etc/evil.jar"));
    }

    // ---- allowed ----

    @Test
    public void testPlainJarNameAllowed() {
        Assertions.assertDoesNotThrow(
                () -> JdbcDorisConnector.checkDriverUrlSecurityRule("mysql-connector-j-8.4.0.jar"));
        Assertions.assertDoesNotThrow(
                () -> JdbcDorisConnector.checkDriverUrlSecurityRule("postgresql-42.5.0.jar"));
    }

    @Test
    public void testNormalHttpsUrlAllowed() {
        Assertions.assertDoesNotThrow(() -> JdbcDorisConnector.checkDriverUrlSecurityRule(
                "https://bucket.s3.amazonaws.com/regression/jdbc_driver/mysql-connector-j-8.4.0.jar"));
    }

    @Test
    public void testNormalFileUrlAllowed() {
        Assertions.assertDoesNotThrow(() -> JdbcDorisConnector.checkDriverUrlSecurityRule(
                "file:///opt/doris/plugins/jdbc_drivers/mysql-connector-j-8.4.0.jar"));
    }
}
