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

package org.apache.doris.regression

import org.junit.jupiter.api.Test

import java.sql.Connection
import java.sql.Driver
import java.sql.DriverManager

import static org.junit.jupiter.api.Assertions.*

class ConfigRootPasswordTest {
    @Test
    void rootConnectionsOnlyReusePasswordsConfiguredForRoot() {
        def calls = []
        boolean closed = false
        Connection connection = [close: { closed = true }] as Connection
        Driver driver = [connect: { String url, Properties properties ->
            if (url != 'jdbc:root-credentials:test') return null
            calls.add(new HashMap(properties))
            return connection
        }] as Driver
        DriverManager.registerDriver(driver)
        try {
            def cases = [
                    ['root', 'fake password \u5bc6\u78bc', 'fake password \u5bc6\u78bc'],
                    ['root', '', ''], ['root', null, ''], ['root', ' ', ' '],
                    ['test_user', 'other-user-password', ''], ['test_user', '', ''],
                    [null, 'other-user-password', '']
            ]
            cases.each { user, password, expected ->
                Config config = new Config(jdbcUrl: 'jdbc:root-credentials:test',
                        jdbcUser: user, jdbcPassword: password)
                assertEquals(expected, config.getRootPassword())
                closed = false
                config.getRootConnection().withCloseable { assertSame(connection, it) }
                assertTrue(closed)
                assertEquals('root', calls.last().user)
                assertEquals(expected, calls.last().password)
            }
        } finally {
            DriverManager.deregisterDriver(driver)
        }
    }
}
