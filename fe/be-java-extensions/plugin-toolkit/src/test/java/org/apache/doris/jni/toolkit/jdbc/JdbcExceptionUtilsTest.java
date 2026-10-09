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

package org.apache.doris.jni.toolkit.jdbc;

import org.apache.doris.jni.spi.utils.JniUtil;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.time.Duration;

class JdbcExceptionUtilsTest {
    @Test
    void reportsRemoteCodesSeparatelyFromTheOperationContext() {
        SQLException cause = new SQLException("bad syntax", "42000", 1064);
        JdbcIOException error = new JdbcIOException("scan open failed", cause);
        Assertions.assertEquals("scan open failed: [remote_sqlstate=42000, "
                + "remote_vendor_error_code=1064, message=bad syntax]", error.getMessage());
        Assertions.assertSame(cause, error.getCause());
        Assertions.assertEquals("JdbcIOException: " + error.getMessage(), JniUtil.throwableToString(error));
    }

    @Test
    void followsBothChainsAndKeepsDifferentCodesWithTheSameMessage() {
        SQLException first = new SQLException("denied", "42000", 1142);
        SQLException second = new SQLException("denied", "28000", 1045);
        first.setNextException(second);
        SQLException duplicate = new SQLException("denied", "42000", 1142);
        second.initCause(duplicate);
        String result = JdbcExceptionUtils.format("schema", new RuntimeException("denied", first));
        Assertions.assertEquals("schema: [remote_sqlstate=42000, remote_vendor_error_code=1142, message=denied]"
                + " | [remote_sqlstate=28000, remote_vendor_error_code=1045, message=denied]", result);
    }

    @Test
    void handlesNullEmptyAndBlankSqlStateAndKeepsZeroVendorCode() {
        for (String state : new String[] {null, "", "  "}) {
            String result = JdbcExceptionUtils.format("read", new SQLException(null, state, 0));
            Assertions.assertEquals("read: [remote_sqlstate=unknown, remote_vendor_error_code=0, message=]", result);
        }
    }

    @Test
    void terminatesOnCauseAndNextExceptionCycles() {
        SQLException first = new SQLException("one", "08001", 1);
        SQLException second = new SQLException("two", "08006", 2);
        first.setNextException(second);
        second.setNextException(first);
        RuntimeException wrapper = new RuntimeException("pool failed", first);
        first.initCause(wrapper);
        Assertions.assertTimeoutPreemptively(Duration.ofSeconds(2), () -> {
            String result = JdbcExceptionUtils.format("connect", wrapper);
            Assertions.assertEquals("connect: pool failed | [remote_sqlstate=08001, "
                    + "remote_vendor_error_code=1, message=one] | [remote_sqlstate=08006, "
                    + "remote_vendor_error_code=2, message=two]", result);
            Assertions.assertTrue(JniUtil.throwableToString(new JdbcIOException("connect", wrapper))
                    .contains("remote_sqlstate=08006"));
        });
    }

    @Test
    void absentCauseRetainsContextWithoutRemoteCodes() {
        Assertions.assertEquals("open", JdbcExceptionUtils.format("open", null));
        Assertions.assertEquals("open", JdbcExceptionUtils.appendSqlDiagnostics("open", null));
        Assertions.assertEquals("", JdbcExceptionUtils.format("", null));
    }

    @Test
    void doesNotInventRemoteCodesForNonSqlExceptions() {
        RuntimeException cause = new RuntimeException("driver missing");
        Assertions.assertEquals("open: driver missing", JdbcExceptionUtils.format("open", cause));
        Assertions.assertEquals("open", JdbcExceptionUtils.appendSqlDiagnostics("open", cause));
    }

    @Test
    void doesNotAppendDiagnosticsAlreadyPresentInTheContext() {
        SQLException cause = new SQLException("duplicate key", "23000", 1062);
        String once = JdbcExceptionUtils.format("write", cause);
        Assertions.assertEquals(once, JdbcExceptionUtils.appendSqlDiagnostics(once, cause));
    }

    @Test
    void redactsSecretsFromBothJniSummaryAndStackTraceWithoutChangingTheCause() {
        String password = "a$special\\password";
        String url = "jdbc:mysql://user:other-secret@localhost/db?password=url-secret";
        SQLException cause = new SQLException("password=label-secret; " + url + " raw=" + password, "28000", 1045);
        JdbcIOException error = new JdbcIOException("connection failed", cause, password, url);
        for (String rendered : new String[] {JniUtil.throwableToString(error), JniUtil.throwableToStackTrace(error)}) {
            for (String secret : new String[] {password, "other-secret", "url-secret", "label-secret", url}) {
                Assertions.assertFalse(rendered.contains(secret), rendered);
            }
            Assertions.assertTrue(rendered.contains("remote_sqlstate=28000"), rendered);
        }
        Assertions.assertSame(cause, error.getCause());
        Assertions.assertTrue(cause.getMessage().contains(password));
    }

    @Test
    void doesNotDuplicateDiagnosticsFromAnAlreadyFormattedCause() {
        SQLException sql = new SQLException("denied", "42000", 1142);
        RuntimeException connection = new RuntimeException(JdbcExceptionUtils.format("connect", sql), sql);
        Assertions.assertEquals("schema: connect: [remote_sqlstate=42000, "
                + "remote_vendor_error_code=1142, message=denied]",
                JdbcExceptionUtils.format("schema", connection));
    }

    @Test
    void shortNumericPasswordsDoNotCorruptRemoteCodes() {
        for (String password : new String[] {"42", "0", "1064"}) {
            SQLException cause = new SQLException("raw=" + password, "42000", 1064);
            String once = JdbcExceptionUtils.format("schema", cause, password);
            Assertions.assertTrue(once.contains("remote_sqlstate=42000, remote_vendor_error_code=1064"));
            Assertions.assertTrue(once.contains("message=raw=***"));
            Assertions.assertEquals(once, JdbcExceptionUtils.appendSqlDiagnostics(once, cause, password));
            Assertions.assertTrue(JniUtil.throwableToStackTrace(new JdbcIOException("schema", cause, password))
                    .contains("remote_sqlstate=42000, remote_vendor_error_code=1064"));
        }
    }

    @Test
    void redactsUnknownJdbcUrlsAndQuotedCredentialProperties() {
        Assertions.assertEquals("[redacted JDBC URL] password=***; user=*** token=***",
                JdbcExceptionUtils.redact("jdbc:postgresql://host/db?password=p password='two words'; user=alice token=xyz"));
    }

    @Test
    void overlappingPasswordsCannotDisableStructuralRedaction() {
        String url = "jdbc:oracle:thin:alice/url-secret@host:1521:db";
        for (String password : new String[] {"jdbc", "oracle", "password", "*", "redacted"}) {
            String text = url + " password=another-secret raw=" + password;
            String safe = JdbcExceptionUtils.redact(text, password, url);
            Assertions.assertFalse(safe.contains("url-secret"), safe);
            Assertions.assertFalse(safe.contains("another-secret"), safe);
            Assertions.assertEquals(safe, JdbcExceptionUtils.redact(safe, password, url));
        }
        Assertions.assertEquals("\"password\":***", JdbcExceptionUtils.redact("\"password\":\"another-secret\""));
    }

    @Test
    void maskCharactersDoNotDuplicateDiagnosticsWhenRewrapped() {
        for (String password : new String[] {"*", "**", "***", "a***b", "]", "[", "message", ",", "="}) {
            SQLException cause = new SQLException("raw=" + password, "42000", 1142);
            String once = JdbcExceptionUtils.format("connect", cause, password);
            RuntimeException wrapper = new RuntimeException(once, cause);
            String twice = JdbcExceptionUtils.format("schema", wrapper, password);
            Assertions.assertEquals("schema: " + once, twice);
            Assertions.assertEquals(once, JdbcExceptionUtils.appendSqlDiagnostics(once, cause, password));
        }
    }

    @Test
    void credentialLabelsAndDiagnosticBracketsStayStableWhenRewrapped() {
        for (String message : new String[] {"password=remote-secret", "password='remote-secret'",
                "password=abc,def", "password=abc]def"}) {
            SQLException cause = new SQLException(message, "28000", 1045);
            String once = JdbcExceptionUtils.format("connect", cause, "abc,def", "abc]def");
            Assertions.assertEquals("schema: " + once, JdbcExceptionUtils.format("schema",
                    new RuntimeException(once, cause), "abc,def", "abc]def"));
            Assertions.assertFalse(once.contains("remote-secret"));
            Assertions.assertFalse(once.contains("def"));
        }
    }

    @Test
    void nestedBracketsAndUrlsStayStableAcrossRepeatedFormatting() {
        for (String password : new String[] {"redacted", "]", "[", "\\", "0"}) {
            SQLException first = new SQLException("connect jdbc:mysql://host/db [unbalanced] ] | raw=" + password,
                    "28000", 1045);
            first.setNextException(new SQLException("other [message] \\ path", "08001", 0));
            String once = JdbcExceptionUtils.format("connect", first, password);
            Assertions.assertEquals(once, JdbcExceptionUtils.redact(once, password));
            Assertions.assertEquals("schema: " + once, JdbcExceptionUtils.format("schema",
                    new RuntimeException(once, first), password));
            Assertions.assertEquals(once, JdbcExceptionUtils.appendSqlDiagnostics(once, first, password));
            Assertions.assertTrue(once.contains("[redacted JDBC URL]"), once);
        }
    }

    @Test
    void longMessagesDoNotConsumeStackDuringRedactionAndRewrapping() {
        String message = String.join("", java.util.Collections.nCopies(20000, "text ] \\ [redacted JDBC URL] "));
        SQLException cause = new SQLException(message, "42000", 1064);
        String once = JdbcExceptionUtils.format("connect", cause, "redacted");
        Assertions.assertEquals(once, JdbcExceptionUtils.redact(once, "redacted"));
        Assertions.assertEquals("schema: " + once, JdbcExceptionUtils.format("schema",
                new RuntimeException(once, cause), "redacted"));
        Assertions.assertTrue(JdbcExceptionUtils.stackTrace(new JdbcIOException("connect", cause, "redacted"),
                "redacted").contains(once));
    }

    @Test
    void escapedQuotedCredentialsAndUrlValuesAreRedactedStably() {
        for (String text : new String[] {"password=\"first\\\"remaining-secret\"", "password='first\\'remaining-secret'",
                "password=jdbc:mysql://host/db?password=url-secret", "password=[redacted JDBC URL]"}) {
            String safe = JdbcExceptionUtils.redact(text);
            Assertions.assertFalse(safe.contains("remaining-secret"), safe);
            Assertions.assertFalse(safe.contains("url-secret"), safe);
            Assertions.assertEquals(safe, JdbcExceptionUtils.redact(safe));
            SQLException cause = new SQLException(text, "28000", 1045);
            String once = JdbcExceptionUtils.format("connect", cause);
            Assertions.assertEquals("schema: " + once, JdbcExceptionUtils.format("schema",
                    new RuntimeException(once, cause)));
        }
    }
}
