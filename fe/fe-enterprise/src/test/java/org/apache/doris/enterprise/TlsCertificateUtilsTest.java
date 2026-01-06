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

import java.net.InetAddress;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import javax.security.auth.x500.X500Principal;

public class TlsCertificateUtilsTest {

    @Test
    public void testExtractSubjectAlternativeNamesNull() {
        Set<String> sans = TlsCertificateUtils.extractSubjectAlternativeNames(null);
        Assert.assertTrue(sans.isEmpty());
    }

    @Test
    public void testExtractSubjectAlternativeNamesEmpty(@Mocked X509Certificate mockCert) throws Exception {
        new Expectations() {
            {
                mockCert.getSubjectAlternativeNames();
                result = null;
            }
        };

        Set<String> sans = TlsCertificateUtils.extractSubjectAlternativeNames(mockCert);
        Assert.assertTrue(sans.isEmpty());
    }

    @Test
    public void testExtractDnsNames(@Mocked X509Certificate mockCert) throws Exception {
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

        Set<String> dnsNames = TlsCertificateUtils.extractDnsNames(mockCert);
        Assert.assertEquals(1, dnsNames.size());
        Assert.assertTrue(dnsNames.contains("server.example.com"));
    }

    @Test
    public void testExtractEmailAddresses(@Mocked X509Certificate mockCert) throws Exception {
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

        Set<String> emails = TlsCertificateUtils.extractEmailAddresses(mockCert);
        Assert.assertEquals(1, emails.size());
        Assert.assertTrue(emails.contains("alice@example.com"));
    }

    @Test
    public void testExtractUris(@Mocked X509Certificate mockCert) throws Exception {
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

        Set<String> uris = TlsCertificateUtils.extractUris(mockCert);
        Assert.assertEquals(1, uris.size());
        Assert.assertTrue(uris.contains("spiffe://example.com/workload"));
    }

    @Test
    public void testExtractIpAddresses(@Mocked X509Certificate mockCert) throws Exception {
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

        Set<String> ips = TlsCertificateUtils.extractIpAddresses(mockCert);
        Assert.assertEquals(1, ips.size());
        Assert.assertTrue(ips.contains("192.168.1.1"));
    }

    @Test
    public void testExtractAllSanTypes(@Mocked X509Certificate mockCert) throws Exception {
        Collection<List<?>> sanCollection = new ArrayList<>();

        // Add DNS name
        List<Object> dnsSan = new ArrayList<>();
        dnsSan.add(Integer.valueOf(2));
        dnsSan.add("server.example.com");
        sanCollection.add(dnsSan);

        // Add email
        List<Object> emailSan = new ArrayList<>();
        emailSan.add(Integer.valueOf(1));
        emailSan.add("alice@example.com");
        sanCollection.add(emailSan);

        // Add URI
        List<Object> uriSan = new ArrayList<>();
        uriSan.add(Integer.valueOf(6));
        uriSan.add("spiffe://example.com/workload");
        sanCollection.add(uriSan);

        // Add IP
        List<Object> ipSan = new ArrayList<>();
        ipSan.add(Integer.valueOf(7));
        ipSan.add("10.0.0.1");
        sanCollection.add(ipSan);

        new Expectations() {
            {
                mockCert.getSubjectAlternativeNames();
                result = sanCollection;
            }
        };

        Set<String> allSans = TlsCertificateUtils.extractSubjectAlternativeNames(mockCert);
        Assert.assertEquals(4, allSans.size());
        Assert.assertTrue(allSans.contains("server.example.com"));
        Assert.assertTrue(allSans.contains("alice@example.com"));
        Assert.assertTrue(allSans.contains("spiffe://example.com/workload"));
        Assert.assertTrue(allSans.contains("10.0.0.1"));
    }

    @Test
    public void testHasSan(@Mocked X509Certificate mockCert) throws Exception {
        Collection<List<?>> sanCollection = new ArrayList<>();

        List<Object> emailSan = new ArrayList<>();
        emailSan.add(Integer.valueOf(1));
        emailSan.add("alice@example.com");
        sanCollection.add(emailSan);

        new Expectations() {
            {
                mockCert.getSubjectAlternativeNames();
                result = sanCollection;
            }
        };

        Assert.assertTrue(TlsCertificateUtils.hasSan(mockCert, "alice@example.com"));
        Assert.assertFalse(TlsCertificateUtils.hasSan(mockCert, "bob@example.com"));
    }

    @Test
    public void testHasSanNullCert() {
        Assert.assertFalse(TlsCertificateUtils.hasSan(null, "alice@example.com"));
    }

    @Test
    public void testHasSanNullValue(@Mocked X509Certificate mockCert) {
        Assert.assertFalse(TlsCertificateUtils.hasSan(mockCert, null));
        Assert.assertFalse(TlsCertificateUtils.hasSan(mockCert, ""));
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

        Set<String> ips = TlsCertificateUtils.extractIpAddresses(mockCert);
        Assert.assertEquals(1, ips.size());
        Assert.assertTrue(ips.contains("192.168.1.100"));
    }

    @Test
    public void testExtractIpv6AddressNormalization(@Mocked X509Certificate mockCert) throws Exception {
        Collection<List<?>> sanCollection = new ArrayList<>();

        // Canonical compressed form as returned by InetAddress.getHostAddress()
        String canonicalIpv6 = InetAddress.getByName("2001:db8::1").getHostAddress();
        byte[] ipv6Bytes = InetAddress.getByName("2001:db8::1").getAddress();

        // Add IPv6 as byte array (will be normalized via InetAddress.getByAddress)
        List<Object> ipv6BytesSan = new ArrayList<>();
        ipv6BytesSan.add(Integer.valueOf(7));
        ipv6BytesSan.add(ipv6Bytes);
        sanCollection.add(ipv6BytesSan);

        // Add IPv6 as zero-padded string (will be normalized via InetAddress.getByName)
        String zeroPaddedIpv6 = "2001:0db8:0000:0000:0000:0000:0000:0001";
        List<Object> ipv6StringSan = new ArrayList<>();
        ipv6StringSan.add(Integer.valueOf(7));
        ipv6StringSan.add(zeroPaddedIpv6);
        sanCollection.add(ipv6StringSan);

        new Expectations() {
            {
                mockCert.getSubjectAlternativeNames();
                result = sanCollection;
                minTimes = 1;
            }
        };

        Set<String> ips = TlsCertificateUtils.extractIpAddresses(mockCert);
        // Both byte[] and string input should produce the canonical form
        Assert.assertTrue("Should contain canonical IPv6: " + canonicalIpv6, ips.contains(canonicalIpv6));
        // The original zero-padded string should also be preserved
        Assert.assertTrue("Should contain original zero-padded: " + zeroPaddedIpv6, ips.contains(zeroPaddedIpv6));

        // hasSan should match using canonical form
        Assert.assertTrue(TlsCertificateUtils.hasSan(mockCert, canonicalIpv6));
    }

    @Test
    public void testCertificateParsingException(@Mocked X509Certificate mockCert) throws Exception {
        new Expectations() {
            {
                mockCert.getSubjectAlternativeNames();
                result = new CertificateParsingException("Test exception");
            }
        };

        // Should return empty set on exception, not throw
        Set<String> sans = TlsCertificateUtils.extractSubjectAlternativeNames(mockCert);
        Assert.assertTrue(sans.isEmpty());
    }
}
