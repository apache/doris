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

import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import javax.security.auth.x500.X500Principal;

public class TlsCertificateUtilsTest {

    @Test
    public void testExtractSubjectAlternativeNamesNull() {
        String san = TlsCertificateUtils.extractSubjectAlternativeNames(null);
        Assert.assertEquals("", san);
    }

    @Test
    public void testExtractSubjectAlternativeNamesEmpty(@Mocked X509Certificate mockCert) throws Exception {
        new Expectations() {
            {
                mockCert.getSubjectAlternativeNames();
                result = null;
            }
        };

        String san = TlsCertificateUtils.extractSubjectAlternativeNames(mockCert);
        Assert.assertEquals("", san);
    }

    @Test
    public void testExtractSingleEmailSan(@Mocked X509Certificate mockCert) throws Exception {
        Collection<List<?>> sanCollection = new ArrayList<>();

        List<Object> emailSan = new ArrayList<>();
        emailSan.add(Integer.valueOf(1)); // RFC822 Name (email)
        emailSan.add("alice@example.com");
        sanCollection.add(emailSan);

        new Expectations() {
            {
                mockCert.getSubjectAlternativeNames();
                result = sanCollection;
            }
        };

        String san = TlsCertificateUtils.extractSubjectAlternativeNames(mockCert);
        Assert.assertEquals("email:alice@example.com", san);
    }

    @Test
    public void testExtractSingleDnsSan(@Mocked X509Certificate mockCert) throws Exception {
        Collection<List<?>> sanCollection = new ArrayList<>();

        List<Object> dnsSan = new ArrayList<>();
        dnsSan.add(Integer.valueOf(2)); // DNS Name
        dnsSan.add("server.example.com");
        sanCollection.add(dnsSan);

        new Expectations() {
            {
                mockCert.getSubjectAlternativeNames();
                result = sanCollection;
            }
        };

        String san = TlsCertificateUtils.extractSubjectAlternativeNames(mockCert);
        Assert.assertEquals("DNS:server.example.com", san);
    }

    @Test
    public void testExtractSingleUriSan(@Mocked X509Certificate mockCert) throws Exception {
        Collection<List<?>> sanCollection = new ArrayList<>();

        List<Object> uriSan = new ArrayList<>();
        uriSan.add(Integer.valueOf(6)); // URI
        uriSan.add("spiffe://example.com/workload");
        sanCollection.add(uriSan);

        new Expectations() {
            {
                mockCert.getSubjectAlternativeNames();
                result = sanCollection;
            }
        };

        String san = TlsCertificateUtils.extractSubjectAlternativeNames(mockCert);
        Assert.assertEquals("URI:spiffe://example.com/workload", san);
    }

    @Test
    public void testExtractSingleIpSan(@Mocked X509Certificate mockCert) throws Exception {
        Collection<List<?>> sanCollection = new ArrayList<>();

        List<Object> ipSan = new ArrayList<>();
        ipSan.add(Integer.valueOf(7)); // IP Address
        ipSan.add("192.168.1.1");
        sanCollection.add(ipSan);

        new Expectations() {
            {
                mockCert.getSubjectAlternativeNames();
                result = sanCollection;
            }
        };

        String san = TlsCertificateUtils.extractSubjectAlternativeNames(mockCert);
        Assert.assertEquals("IP Address:192.168.1.1", san);
    }

    @Test
    public void testExtractMultipleSansPreservesOrder(@Mocked X509Certificate mockCert) throws Exception {
        // Test that order is preserved: email -> DNS -> URI
        Collection<List<?>> sanCollection = new ArrayList<>();

        List<Object> emailSan = new ArrayList<>();
        emailSan.add(Integer.valueOf(1));
        emailSan.add("test@example.com");
        sanCollection.add(emailSan);

        List<Object> dnsSan = new ArrayList<>();
        dnsSan.add(Integer.valueOf(2));
        dnsSan.add("testclient.example.com");
        sanCollection.add(dnsSan);

        List<Object> uriSan = new ArrayList<>();
        uriSan.add(Integer.valueOf(6));
        uriSan.add("spiffe://example.com/testclient");
        sanCollection.add(uriSan);

        new Expectations() {
            {
                mockCert.getSubjectAlternativeNames();
                result = sanCollection;
            }
        };

        String san = TlsCertificateUtils.extractSubjectAlternativeNames(mockCert);
        Assert.assertEquals(
                "email:test@example.com, DNS:testclient.example.com, URI:spiffe://example.com/testclient",
                san);
    }

    @Test
    public void testExtractMultipleSansDifferentOrder(@Mocked X509Certificate mockCert) throws Exception {
        // Test that order is preserved when order is: DNS -> URI -> email
        Collection<List<?>> sanCollection = new ArrayList<>();

        List<Object> dnsSan = new ArrayList<>();
        dnsSan.add(Integer.valueOf(2));
        dnsSan.add("first-dns.example.com");
        sanCollection.add(dnsSan);

        List<Object> uriSan = new ArrayList<>();
        uriSan.add(Integer.valueOf(6));
        uriSan.add("spiffe://second-uri.example.com");
        sanCollection.add(uriSan);

        List<Object> emailSan = new ArrayList<>();
        emailSan.add(Integer.valueOf(1));
        emailSan.add("third-email@example.com");
        sanCollection.add(emailSan);

        new Expectations() {
            {
                mockCert.getSubjectAlternativeNames();
                result = sanCollection;
            }
        };

        String san = TlsCertificateUtils.extractSubjectAlternativeNames(mockCert);
        Assert.assertEquals(
                "DNS:first-dns.example.com, URI:spiffe://second-uri.example.com, email:third-email@example.com",
                san);
    }

    @Test
    public void testExtractIpAddressFromBytes(@Mocked X509Certificate mockCert) throws Exception {
        Collection<List<?>> sanCollection = new ArrayList<>();

        // IPv4 as byte array
        List<Object> ipSan = new ArrayList<>();
        ipSan.add(Integer.valueOf(7)); // IP Address
        ipSan.add(new byte[] {(byte) 192, (byte) 168, (byte) 1, (byte) 100});
        sanCollection.add(ipSan);

        new Expectations() {
            {
                mockCert.getSubjectAlternativeNames();
                result = sanCollection;
            }
        };

        String san = TlsCertificateUtils.extractSubjectAlternativeNames(mockCert);
        Assert.assertEquals("IP Address:192.168.1.100", san);
    }

    @Test
    public void testExtractMixedSansWithIpBytes(@Mocked X509Certificate mockCert) throws Exception {
        Collection<List<?>> sanCollection = new ArrayList<>();

        List<Object> dnsSan = new ArrayList<>();
        dnsSan.add(Integer.valueOf(2));
        dnsSan.add("server.example.com");
        sanCollection.add(dnsSan);

        // IPv4 as byte array
        List<Object> ipSan = new ArrayList<>();
        ipSan.add(Integer.valueOf(7));
        ipSan.add(new byte[] {(byte) 10, (byte) 0, (byte) 0, (byte) 1});
        sanCollection.add(ipSan);

        new Expectations() {
            {
                mockCert.getSubjectAlternativeNames();
                result = sanCollection;
            }
        };

        String san = TlsCertificateUtils.extractSubjectAlternativeNames(mockCert);
        Assert.assertEquals("DNS:server.example.com, IP Address:10.0.0.1", san);
    }

    @Test
    public void testUnsupportedSanTypeIsSkipped(@Mocked X509Certificate mockCert) throws Exception {
        Collection<List<?>> sanCollection = new ArrayList<>();

        // otherName (type 0) - not supported
        List<Object> otherSan = new ArrayList<>();
        otherSan.add(Integer.valueOf(0));
        otherSan.add(new byte[] {0x01, 0x02, 0x03});
        sanCollection.add(otherSan);

        // DNS - supported
        List<Object> dnsSan = new ArrayList<>();
        dnsSan.add(Integer.valueOf(2));
        dnsSan.add("server.example.com");
        sanCollection.add(dnsSan);

        // directoryName (type 4) - not supported
        List<Object> dirSan = new ArrayList<>();
        dirSan.add(Integer.valueOf(4));
        dirSan.add("CN=Test");
        sanCollection.add(dirSan);

        new Expectations() {
            {
                mockCert.getSubjectAlternativeNames();
                result = sanCollection;
            }
        };

        String san = TlsCertificateUtils.extractSubjectAlternativeNames(mockCert);
        // Only DNS should be in the result
        Assert.assertEquals("DNS:server.example.com", san);
    }

    @Test
    public void testCertificateParsingException(@Mocked X509Certificate mockCert) throws Exception {
        new Expectations() {
            {
                mockCert.getSubjectAlternativeNames();
                result = new CertificateParsingException("Test exception");
            }
        };

        // Should return empty string on exception, not throw
        String san = TlsCertificateUtils.extractSubjectAlternativeNames(mockCert);
        Assert.assertEquals("", san);
    }

    @Test
    public void testGetSubjectDN(@Mocked X509Certificate mockCert,
            @Mocked X500Principal mockPrincipal) {
        new Expectations() {
            {
                mockCert.getSubjectX500Principal();
                result = mockPrincipal;
                mockPrincipal.getName();
                result = "CN=testuser,O=TestOrg";
            }
        };

        String subjectDN = TlsCertificateUtils.getSubjectDN(mockCert);
        Assert.assertEquals("CN=testuser,O=TestOrg", subjectDN);
    }

    @Test
    public void testGetIssuerDN(@Mocked X509Certificate mockCert,
            @Mocked X500Principal mockPrincipal) {
        new Expectations() {
            {
                mockCert.getIssuerX500Principal();
                result = mockPrincipal;
                mockPrincipal.getName();
                result = "CN=TestCA,O=TestOrg";
            }
        };

        String issuerDN = TlsCertificateUtils.getIssuerDN(mockCert);
        Assert.assertEquals("CN=TestCA,O=TestOrg", issuerDN);
    }

    @Test
    public void testGetSubjectDNNull() {
        Assert.assertNull(TlsCertificateUtils.getSubjectDN(null));
    }

    @Test
    public void testGetIssuerDNNull() {
        Assert.assertNull(TlsCertificateUtils.getIssuerDN(null));
    }

    @Test
    public void testNullSanListEntry(@Mocked X509Certificate mockCert) throws Exception {
        Collection<List<?>> sanCollection = new ArrayList<>();

        // Add a null entry
        sanCollection.add(null);

        // Add a valid DNS entry
        List<Object> dnsSan = new ArrayList<>();
        dnsSan.add(Integer.valueOf(2));
        dnsSan.add("server.example.com");
        sanCollection.add(dnsSan);

        new Expectations() {
            {
                mockCert.getSubjectAlternativeNames();
                result = sanCollection;
            }
        };

        String san = TlsCertificateUtils.extractSubjectAlternativeNames(mockCert);
        Assert.assertEquals("DNS:server.example.com", san);
    }

    @Test
    public void testEmptySanListEntry(@Mocked X509Certificate mockCert) throws Exception {
        Collection<List<?>> sanCollection = new ArrayList<>();

        // Add an entry with less than 2 elements
        List<Object> incompleteSan = new ArrayList<>();
        incompleteSan.add(Integer.valueOf(2));
        sanCollection.add(incompleteSan);

        // Add a valid DNS entry
        List<Object> dnsSan = new ArrayList<>();
        dnsSan.add(Integer.valueOf(2));
        dnsSan.add("server.example.com");
        sanCollection.add(dnsSan);

        new Expectations() {
            {
                mockCert.getSubjectAlternativeNames();
                result = sanCollection;
            }
        };

        String san = TlsCertificateUtils.extractSubjectAlternativeNames(mockCert);
        Assert.assertEquals("DNS:server.example.com", san);
    }
}
