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

package org.apache.doris.regression.suite

import org.junit.jupiter.api.Test

import java.nio.file.Files

import static org.junit.jupiter.api.Assertions.assertTrue

class PrivateRpcTlsPlatformTest {
    @Test
    void unsupportedHostReturnsBeforeStartingTheLinuxFixture() {
        String previousOs = System.getProperty("os.name")
        String previousHome = System.getProperty("DORIS_HOME")
        def home = Files.createTempDirectory("private-rpc-platform-").toFile()
        def messages = []
        try {
            System.setProperty("os.name", "Mac OS X")
            System.setProperty("DORIS_HOME", home.absolutePath)
            def binding = new Binding([
                suite: { String name, String group, Closure body -> body.call() },
                context: [file: new File("../suites/rpc_p0/test_remote_doris_private_rpc_tls.groovy")],
                logger: new Expando(info: { String message -> messages.add(message) })
            ])
            // Fail at the process boundary without launching Linux daemons on the simulated host.
            String source = new File("../suites/rpc_p0/test_remote_doris_private_rpc_tls.groovy").text
            new GroovyShell(getClass().classLoader, binding).evaluate(source + '''
                class ProcessBuilder {
                    ProcessBuilder(Object... arguments) {
                        throw new AssertionError("Unsupported host attempted to launch a Linux fixture")
                    }
                }
            ''')
            assertTrue(messages.any { it.contains("Linux") })
        } finally {
            System.setProperty("os.name", previousOs)
            if (previousHome == null) {
                System.clearProperty("DORIS_HOME")
            } else {
                System.setProperty("DORIS_HOME", previousHome)
            }
            home.deleteDir()
        }
    }
}
