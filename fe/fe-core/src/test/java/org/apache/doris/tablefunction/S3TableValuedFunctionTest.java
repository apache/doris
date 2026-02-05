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

package org.apache.doris.tablefunction;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeConstants;

import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class S3TableValuedFunctionTest {

    private Map<String, String> baseS3Props() {
        Map<String, String> props = new HashMap<>();
        props.put("uri", "s3://test-bucket/data/file.parquet");
        props.put("s3.endpoint", "https://s3.us-east-1.amazonaws.com");
        props.put("s3.region", "us-east-1");
        props.put("format", "parquet");
        return props;
    }

    @Test
    public void testAnonymousFallbackOn403NoCredentials() throws Exception {
        boolean origUnitTest = FeConstants.runningUnitTest;
        FeConstants.runningUnitTest = false;
        try {
            AtomicInteger parseFileCallCount = new AtomicInteger(0);
            new MockUp<ExternalFileTableValuedFunction>() {
                @Mock
                protected void parseFile() throws AnalysisException {
                    int count = parseFileCallCount.incrementAndGet();
                    if (count == 1) {
                        throw new AnalysisException(
                                "parse file failed, err: Status Code: 403, AWS Error Code: AccessDenied");
                    }
                    // Second call (anonymous retry) succeeds
                }
            };

            S3TableValuedFunction tvf = new S3TableValuedFunction(baseS3Props());

            // parseFile should have been called twice (original + retry)
            Assert.assertEquals(2, parseFileCallCount.get());

            // Verify processedParams was updated with ANONYMOUS provider
            Assert.assertEquals("ANONYMOUS",
                    tvf.getBrokerDesc().getProperties().get("s3.credentials_provider_type"));

            // Verify backendConnectProperties has ANONYMOUS provider type
            Assert.assertEquals("ANONYMOUS",
                    tvf.getBackendConnectProperties().get("AWS_CREDENTIALS_PROVIDER_TYPE"));
        } finally {
            FeConstants.runningUnitTest = origUnitTest;
        }
    }

    @Test
    public void testNoFallbackWhenExplicitCredentials() {
        boolean origUnitTest = FeConstants.runningUnitTest;
        FeConstants.runningUnitTest = false;
        try {
            new MockUp<ExternalFileTableValuedFunction>() {
                @Mock
                protected void parseFile() throws AnalysisException {
                    throw new AnalysisException(
                            "parse file failed, err: Status Code: 403, AWS Error Code: AccessDenied");
                }
            };

            Map<String, String> props = baseS3Props();
            props.put("s3.access_key", "AKIAIOSFODNN7EXAMPLE");
            props.put("s3.secret_key", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");

            try {
                new S3TableValuedFunction(props);
                Assert.fail("Should have thrown AnalysisException");
            } catch (AnalysisException e) {
                Assert.assertTrue(e.getMessage().contains("Status Code: 403"));
            }
        } finally {
            FeConstants.runningUnitTest = origUnitTest;
        }
    }

    @Test
    public void testNoFallbackWhenExplicitRoleArn() {
        boolean origUnitTest = FeConstants.runningUnitTest;
        FeConstants.runningUnitTest = false;
        try {
            new MockUp<ExternalFileTableValuedFunction>() {
                @Mock
                protected void parseFile() throws AnalysisException {
                    throw new AnalysisException(
                            "parse file failed, err: Status Code: 403, AWS Error Code: AccessDenied");
                }
            };

            Map<String, String> props = baseS3Props();
            props.put("s3.role_arn", "arn:aws:iam::123456789012:role/MyRole");

            try {
                new S3TableValuedFunction(props);
                Assert.fail("Should have thrown an exception");
            } catch (AnalysisException e) {
                // Expected: 403 error propagated without retry because role_arn is set
                Assert.assertTrue(e.getMessage().contains("Status Code: 403"));
            }
        } finally {
            FeConstants.runningUnitTest = origUnitTest;
        }
    }

    @Test
    public void testOriginalErrorThrownWhenBothAttemptsFail() {
        boolean origUnitTest = FeConstants.runningUnitTest;
        FeConstants.runningUnitTest = false;
        try {
            AtomicInteger parseFileCallCount = new AtomicInteger(0);
            new MockUp<ExternalFileTableValuedFunction>() {
                @Mock
                protected void parseFile() throws AnalysisException {
                    int count = parseFileCallCount.incrementAndGet();
                    if (count == 1) {
                        throw new AnalysisException(
                                "parse file failed, err: Status Code: 403, Original error");
                    }
                    // Second call also fails (bucket isn't actually public)
                    throw new AnalysisException(
                            "parse file failed, err: Status Code: 403, Anonymous also denied");
                }
            };

            try {
                new S3TableValuedFunction(baseS3Props());
                Assert.fail("Should have thrown AnalysisException");
            } catch (AnalysisException e) {
                // Should throw the ORIGINAL error, not the retry error
                Assert.assertTrue(e.getMessage().contains("Original error"));
                Assert.assertFalse(e.getMessage().contains("Anonymous also denied"));
            }

            Assert.assertEquals(2, parseFileCallCount.get());
        } finally {
            FeConstants.runningUnitTest = origUnitTest;
        }
    }

    @Test
    public void testNoFallbackOnNon403Error() {
        boolean origUnitTest = FeConstants.runningUnitTest;
        FeConstants.runningUnitTest = false;
        try {
            AtomicInteger parseFileCallCount = new AtomicInteger(0);
            new MockUp<ExternalFileTableValuedFunction>() {
                @Mock
                protected void parseFile() throws AnalysisException {
                    parseFileCallCount.incrementAndGet();
                    throw new AnalysisException("parse file failed, err: Status Code: 404, Not Found");
                }
            };

            try {
                new S3TableValuedFunction(baseS3Props());
                Assert.fail("Should have thrown AnalysisException");
            } catch (AnalysisException e) {
                Assert.assertTrue(e.getMessage().contains("Status Code: 404"));
            }

            // parseFile should have been called only once (no retry for non-403)
            Assert.assertEquals(1, parseFileCallCount.get());
        } finally {
            FeConstants.runningUnitTest = origUnitTest;
        }
    }
}
