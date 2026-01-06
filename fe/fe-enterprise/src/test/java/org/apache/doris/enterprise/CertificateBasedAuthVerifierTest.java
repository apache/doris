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

package org.apache.doris.enterprise;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.common.Config;
import org.apache.doris.mysql.authenticate.CertificateAuthVerifier.VerificationResult;

import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class CertificateBasedAuthVerifierTest {

    private CertificateBasedAuthVerifier verifier;

    @Before
    public void setUp() {
        verifier = new CertificateBasedAuthVerifier();
    }

    @Test
    public void testIsEnabled() {
        Assert.assertTrue(verifier.isEnabled());
    }

    @Test
    public void testShouldSkipPasswordVerification() {
        // Test with config = false
        Config.tls_cert_based_auth_ignore_password = false;
        Assert.assertFalse(verifier.shouldSkipPasswordVerification());

        // Test with config = true
        Config.tls_cert_based_auth_ignore_password = true;
        Assert.assertTrue(verifier.shouldSkipPasswordVerification());

        // Reset
        Config.tls_cert_based_auth_ignore_password = false;
    }

    @Test
    public void testVerifyNullUserIdentity() {
        VerificationResult result = verifier.verify(null, null);
        Assert.assertFalse(result.isSuccess());
        Assert.assertTrue(result.getErrorMessage().contains("UserIdentity is null"));
    }

    @Test
    public void testVerifyNoTlsRequirements() {
        UserIdentity userIdentity = new UserIdentity("testuser", "%");
        userIdentity.setIsAnalyzed();
        // No TLS requirements set

        VerificationResult result = verifier.verify(userIdentity, null);
        Assert.assertTrue(result.isSuccess());
    }

    @Test
    public void testVerifyWithSanRequirementButNoCert() {
        UserIdentity userIdentity = new UserIdentity("testuser", "%");
        userIdentity.setIsAnalyzed();
        userIdentity.setSan("alice@example.com");

        VerificationResult result = verifier.verify(userIdentity, null);
        Assert.assertFalse(result.isSuccess());
        Assert.assertTrue(result.getErrorMessage().contains("no client certificate was provided"));
    }

    @Test
    public void testVerifyWithSanMatch(@Mocked X509Certificate mockCert) throws Exception {
        UserIdentity userIdentity = new UserIdentity("testuser", "%");
        userIdentity.setIsAnalyzed();
        userIdentity.setSan("alice@example.com");

        // Create mock SANs
        Collection<List<?>> sans = new ArrayList<>();
        List<Object> emailSan = new ArrayList<>();
        emailSan.add(Integer.valueOf(1)); // RFC822 Name (email)
        emailSan.add("alice@example.com");
        sans.add(emailSan);

        new Expectations() {
            {
                mockCert.getSubjectAlternativeNames();
                result = sans;
            }
        };

        VerificationResult result = verifier.verify(userIdentity, mockCert);
        Assert.assertTrue(result.isSuccess());
    }

    @Test
    public void testVerifyWithSanMismatch(@Mocked X509Certificate mockCert) throws Exception {
        UserIdentity userIdentity = new UserIdentity("testuser", "%");
        userIdentity.setIsAnalyzed();
        userIdentity.setSan("alice@example.com");

        // Create mock SANs with different email
        Collection<List<?>> sans = new ArrayList<>();
        List<Object> emailSan = new ArrayList<>();
        emailSan.add(Integer.valueOf(1)); // RFC822 Name (email)
        emailSan.add("bob@example.com");
        sans.add(emailSan);

        new Expectations() {
            {
                mockCert.getSubjectAlternativeNames();
                result = sans;
            }
        };

        VerificationResult result = verifier.verify(userIdentity, mockCert);
        Assert.assertFalse(result.isSuccess());
        Assert.assertTrue(result.getErrorMessage().contains("was not found in certificate SANs"));
    }

    @Test
    public void testVerifyWithEmptySans(@Mocked X509Certificate mockCert) throws Exception {
        UserIdentity userIdentity = new UserIdentity("testuser", "%");
        userIdentity.setIsAnalyzed();
        userIdentity.setSan("alice@example.com");

        new Expectations() {
            {
                mockCert.getSubjectAlternativeNames();
                result = null;
            }
        };

        VerificationResult result = verifier.verify(userIdentity, mockCert);
        Assert.assertFalse(result.isSuccess());
        Assert.assertTrue(result.getErrorMessage().contains("has no Subject Alternative Names"));
    }

    @Test
    public void testVerifyWithDnsNameSan(@Mocked X509Certificate mockCert) throws Exception {
        UserIdentity userIdentity = new UserIdentity("testuser", "%");
        userIdentity.setIsAnalyzed();
        userIdentity.setSan("server.example.com");

        // Create mock SANs with DNS name
        Collection<List<?>> sans = new ArrayList<>();
        List<Object> dnsSan = new ArrayList<>();
        dnsSan.add(Integer.valueOf(2)); // DNS Name
        dnsSan.add("server.example.com");
        sans.add(dnsSan);

        new Expectations() {
            {
                mockCert.getSubjectAlternativeNames();
                result = sans;
            }
        };

        VerificationResult result = verifier.verify(userIdentity, mockCert);
        Assert.assertTrue(result.isSuccess());
    }

    @Test
    public void testVerifyWithUriSan(@Mocked X509Certificate mockCert) throws Exception {
        UserIdentity userIdentity = new UserIdentity("testuser", "%");
        userIdentity.setIsAnalyzed();
        userIdentity.setSan("spiffe://example.com/workload");

        // Create mock SANs with URI
        Collection<List<?>> sans = new ArrayList<>();
        List<Object> uriSan = new ArrayList<>();
        uriSan.add(Integer.valueOf(6)); // URI
        uriSan.add("spiffe://example.com/workload");
        sans.add(uriSan);

        new Expectations() {
            {
                mockCert.getSubjectAlternativeNames();
                result = sans;
            }
        };

        VerificationResult result = verifier.verify(userIdentity, mockCert);
        Assert.assertTrue(result.isSuccess());
    }

    @Test
    public void testVerifyWithMultipleSans(@Mocked X509Certificate mockCert) throws Exception {
        UserIdentity userIdentity = new UserIdentity("testuser", "%");
        userIdentity.setIsAnalyzed();
        userIdentity.setSan("alice@example.com");

        // Create mock SANs with multiple entries
        Collection<List<?>> sans = new ArrayList<>();

        List<Object> dnsSan = new ArrayList<>();
        dnsSan.add(Integer.valueOf(2)); // DNS Name
        dnsSan.add("server.example.com");
        sans.add(dnsSan);

        List<Object> emailSan = new ArrayList<>();
        emailSan.add(Integer.valueOf(1)); // RFC822 Name (email)
        emailSan.add("alice@example.com");
        sans.add(emailSan);

        new Expectations() {
            {
                mockCert.getSubjectAlternativeNames();
                result = sans;
            }
        };

        VerificationResult result = verifier.verify(userIdentity, mockCert);
        Assert.assertTrue(result.isSuccess());
    }
}
