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

package org.apache.doris.regression.logger

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.spi.LoggingEvent
import java.nio.charset.StandardCharsets
import org.apache.doris.regression.util.SecretRedactor
import org.apache.doris.regression.Config
import org.apache.doris.regression.suite.ScriptContext
import org.apache.doris.regression.suite.SuiteCluster
import org.apache.doris.regression.suite.SuiteContext
import org.apache.doris.regression.util.TeamcityUtils
import org.junit.jupiter.api.Test
import static org.junit.jupiter.api.Assertions.*

class SecretRedactingEncoderTest {
    @Test
    void actualLogbackOutputRedactsSqlAndExceptionWithoutChangingTheEvent() {
        String token = "logEncoderSessionToken0001+/="
        String sql = '"s3.session_token"="' + token + '"'
        SecretRedactor.register([token])
        def context = new LoggerContext()
        def encoder = new SecretRedactingEncoder()
        encoder.context = context
        encoder.pattern = "%msg%n%ex"
        encoder.charset = StandardCharsets.UTF_8
        encoder.start()
        try {
            def event = new LoggingEvent("test", context.getLogger("test"), Level.ERROR, sql,
                    new IllegalStateException("InvalidAccessKeyId " + token), null)
            String encoded = new String(encoder.encode(event), StandardCharsets.UTF_8)
            assertFalse(encoded.contains(token))
            assertTrue(encoded.contains("InvalidAccessKeyId"))
            assertTrue(encoded.contains("IllegalStateException"))
            assertEquals(sql, event.formattedMessage)
            assertTrue(event.throwableProxy.message.contains(token))
        } finally {
            encoder.stop()
            context.stop()
        }
    }
    @Test
    void teamcityFailureRemainsAFailureAndRedactsMessageAndDetails() {
        String token = "teamcitySessionToken0002+/="
        SecretRedactor.register([token])
        def config = new Config()
        config.suitePath = new File("regression-test/suites").absolutePath
        config.dataPath = new File("regression-test/data").absolutePath
        config.realDataPath = new File("regression-test/real-data").absolutePath
        config.defaultDb = "regression_test"
        def file = new File(config.suitePath, "credential_redaction_test.groovy")
        def script = new ScriptContext(file, null, null, config, [], { true })
        def cluster = new SuiteCluster("credential_redaction_test", config)
        def suite = new SuiteContext(file, "credential_redaction_test", "p0", script, cluster, null, null, config)
        def captured = new ByteArrayOutputStream()
        def original = System.out
        try {
            System.setOut(new PrintStream(captured, true, "UTF-8"))
            TeamcityUtils.testFailed(suite, "AccessDenied " + token, "request failed\n" + token)
        } finally {
            System.setOut(original)
        }
        String output = captured.toString("UTF-8")
        assertTrue(output.contains("##teamcity[testFailed"))
        assertTrue(output.contains("AccessDenied"))
        assertTrue(output.contains("|n"))
        assertFalse(output.contains(token))
        assertTrue(output.contains("|[REDACTED|]"))
    }

}
