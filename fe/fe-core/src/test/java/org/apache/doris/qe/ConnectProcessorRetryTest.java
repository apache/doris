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

package org.apache.doris.qe;

import org.apache.doris.analysis.StatementBase;
import org.apache.doris.common.ConnectionException;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.List;

public class ConnectProcessorRetryTest extends TestWithFeService {

    @Mock
    private SessionVariable mockSessionVariable;

    private TestableConnectProcessor testProcessor;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        testProcessor = new TestableConnectProcessor(connectContext);
    }

    // parseWithFallback tests
    @Test
    public void testParseWithFallbackRetryEnabledCorrectConvertedStmt() throws ConnectionException {
        Mockito.when(mockSessionVariable.isRetryOriginSqlOnConvertFail()).thenReturn(true);

        List<StatementBase> result = testProcessor.parseWithFallback(
                "SELECT 1",
                "SELECT 1",
                mockSessionVariable
        );

        Assertions.assertNotNull(result, "Should return parsed statements for correct convertedStmt");
        Assertions.assertFalse(result.isEmpty(), "Should return non-empty list for correct convertedStmt");
    }

    @Test
    public void testParseWithFallbackRetryEnabledIncorrectConvertedStmtCorrectOriginStmt() throws ConnectionException {
        Mockito.when(mockSessionVariable.isRetryOriginSqlOnConvertFail()).thenReturn(true);

        List<StatementBase> result = testProcessor.parseWithFallback(
                "SELECT 1",
                "INVALID SQL SYNTAX;;;",
                mockSessionVariable
        );

        Assertions.assertNotNull(result, "Should return parsed statements from origin SQL when convertedStmt fails");
        Assertions.assertFalse(result.isEmpty(), "Should return non-empty list from origin SQL");
    }

    @Test
    public void testParseWithFallbackRetryDisabledCorrectConvertedStmt() throws ConnectionException {
        Mockito.when(mockSessionVariable.isRetryOriginSqlOnConvertFail()).thenReturn(false);

        List<StatementBase> result = testProcessor.parseWithFallback(
                "SELECT 1",
                "SELECT 1",
                mockSessionVariable
        );

        Assertions.assertNotNull(result, "Should return parsed statements for correct convertedStmt");
        Assertions.assertFalse(result.isEmpty(), "Should return non-empty list for correct convertedStmt");
    }

    @Test
    public void testParseWithFallbackRetryDisabledIncorrectConvertedStmtCorrectOriginStmt() throws ConnectionException {
        Mockito.when(mockSessionVariable.isRetryOriginSqlOnConvertFail()).thenReturn(false);

        List<StatementBase> result = testProcessor.parseWithFallback(
                "SELECT 1",
                "INVALID SQL SYNTAX;;;",
                mockSessionVariable
        );

        Assertions.assertNull(result, "Should return null when retry is disabled and convertedStmt fails");
    }

    // tryRetryOriginalSql tests
    @Test
    public void testTryRetryOriginalSqlRetryEnabledCorrectOriginStmt() {
        Mockito.when(mockSessionVariable.isRetryOriginSqlOnConvertFail()).thenReturn(true);

        List<StatementBase> result = testProcessor.tryRetryOriginalSql(
                "SELECT 1",
                "DIFFERENT CONVERTED SQL",
                mockSessionVariable
        );

        Assertions.assertNotNull(result, "Should return parsed statements for correct origin SQL");
        Assertions.assertFalse(result.isEmpty(), "Should return non-empty list for correct origin SQL");
    }

    @Test
    public void testTryRetryOriginalSqlRetryEnabledIncorrectOriginStmt() {
        Mockito.when(mockSessionVariable.isRetryOriginSqlOnConvertFail()).thenReturn(true);

        List<StatementBase> result = testProcessor.tryRetryOriginalSql(
                "INVALID SQL SYNTAX;;;",
                "DIFFERENT CONVERTED SQL",
                mockSessionVariable
        );

        Assertions.assertNull(result, "Should return null when origin SQL parsing fails");
    }

    @Test
    public void testTryRetryOriginalSqlRetryDisabledCorrectOriginStmt() {
        Mockito.when(mockSessionVariable.isRetryOriginSqlOnConvertFail()).thenReturn(false);

        List<StatementBase> result = testProcessor.tryRetryOriginalSql(
                "SELECT 1",
                "DIFFERENT CONVERTED SQL",
                mockSessionVariable
        );

        Assertions.assertNull(result, "Should return null when retry is disabled");
    }

    @Test
    public void testTryRetryOriginalSqlRetryDisabledIncorrectOriginStmt() {
        Mockito.when(mockSessionVariable.isRetryOriginSqlOnConvertFail()).thenReturn(false);

        List<StatementBase> result = testProcessor.tryRetryOriginalSql(
                "INVALID SQL SYNTAX;;;",
                "DIFFERENT CONVERTED SQL",
                mockSessionVariable
        );

        Assertions.assertNull(result, "Should return null when retry is disabled");
    }

    /**
     * Testable concrete implementation of ConnectProcessor for testing purposes
     */
    private static class TestableConnectProcessor extends ConnectProcessor {

        public TestableConnectProcessor(ConnectContext context) {
            super(context);
        }

        @Override
        public void processOnce() {
            // Test implementation - do nothing
        }

        // Expose protected methods for testing
        public List<StatementBase> parseWithFallback(String originStmt, String convertedStmt,
                SessionVariable sessionVariable) throws ConnectionException {
            return super.parseWithFallback(originStmt, convertedStmt, sessionVariable);
        }

        public List<StatementBase> tryRetryOriginalSql(String originStmt, String convertedStmt,
                SessionVariable sessionVariable) {
            return super.tryRetryOriginalSql(originStmt, convertedStmt, sessionVariable);
        }
    }
}
