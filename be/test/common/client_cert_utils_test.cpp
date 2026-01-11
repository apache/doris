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

#include "common/client_cert_utils.h"

#include <gtest/gtest.h>
#include <openssl/bio.h>
#include <openssl/pem.h>
#include <openssl/x509.h>

#include <memory>
#include <string>

namespace doris {

// Test certificate WITH SAN (Subject Alternative Name)
// Contains: email:test@example.com, DNS:testclient.example.com,
//           URI:spiffe://example.com/testclient, IP Address:192.168.1.100
static const std::string kTestCertWithSan = R"(-----BEGIN CERTIFICATE-----
MIIDuTCCAqGgAwIBAgIJAOo/EwpOibq3MA0GCSqGSIb3DQEBCwUAMGwxCzAJBgNV
BAYTAkNOMRAwDgYDVQQIDAdCZWlqaW5nMRAwDgYDVQQHDAdCZWlqaW5nMRAwDgYD
VQQKDAdUZXN0T3JnMREwDwYDVQQLDAhUZXN0VW5pdDEUMBIGA1UEAwwLVGVzdCBD
bGllbnQwHhcNMjYwMTA4MDYyODAzWhcNMjcwMTA4MDYyODAzWjBsMQswCQYDVQQG
EwJDTjEQMA4GA1UECAwHQmVpamluZzEQMA4GA1UEBwwHQmVpamluZzEQMA4GA1UE
CgwHVGVzdE9yZzERMA8GA1UECwwIVGVzdFVuaXQxFDASBgNVBAMMC1Rlc3QgQ2xp
ZW50MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA2CRMIw7qwdy/lfap
9qfBjOok4nQlSWPzaoMf0wQk8CUlQQRj88ZBhnbVkL732n4RNW/8Sk+vDO31JHgj
jvIIbYiDCxeE878bF5upgXcoWDs36itcDSHTq+Ln9MLZfTe+3UfKjPAFZ7STrU0w
YYNWjbrQzhSKymNoChe7+sQESl7AOypPU9S7xvBwWMO1Sf4F7KMbe8r9b4HmwTPI
02V7/6MPnU0BcSw7d3mQEVlelEPDSJHXeLfD9aV+2VQ+7j3kNimkD29QGajTupc3
4B+8xf4NE/kAmSO2XQGr8X7fxrtm9Xc/HlsrJwSpbVJbotMA/SVFN7cL8uDe0pz6
ADhVfwIDAQABo14wXDBaBgNVHREEUzBRgRB0ZXN0QGV4YW1wbGUuY29tghZ0ZXN0
Y2xpZW50LmV4YW1wbGUuY29thh9zcGlmZmU6Ly9leGFtcGxlLmNvbS90ZXN0Y2xp
ZW50hwTAqAFkMA0GCSqGSIb3DQEBCwUAA4IBAQAW1UMTC82DxwHMssRiv/UfGViC
ouBG3qriLMZyYFf6oDhgpRNLPDISDYMbnyLpw1L1r5LmRGvCJdmxipF6IkT+7rkO
xOG9/8HvRvZv+CSc/FNMBCDlik3EeITmRwRoWnFxv4CzAQx96HhyLiFk/8mcCVyp
fZt2hoE7Hb8YE3BNZwdI+d8qALY5lhPhkTbNX/dG4tU/I336gyXmeub2EZ8iCD35
qK3NfWiDfpoQMGyU3yt/gYVXTVS12sCL0rht23mx7yPWUCO/ZPpYyTcT6CBg3Qa3
c9jndkTMuRQDT2XcfKIJUw9qQbGelgDEB3pOFlGPKdVW7m1h5r8kd1LZsmXo
-----END CERTIFICATE-----)";

// Test certificate WITHOUT SAN
static const std::string kTestCertNoSan = R"(-----BEGIN CERTIFICATE-----
MIIDWDCCAkACCQCtyy2bOzWjnzANBgkqhkiG9w0BAQsFADBuMQswCQYDVQQGEwJD
TjEQMA4GA1UECAwHQmVpamluZzEQMA4GA1UEBwwHQmVpamluZzEQMA4GA1UECgwH
VGVzdE9yZzERMA8GA1UECwwIVGVzdFVuaXQxFjAUBgNVBAMMDU5vIFNBTiBDbGll
bnQwHhcNMjYwMTA4MDYyODAzWhcNMjcwMTA4MDYyODAzWjBuMQswCQYDVQQGEwJD
TjEQMA4GA1UECAwHQmVpamluZzEQMA4GA1UEBwwHQmVpamluZzEQMA4GA1UECgwH
VGVzdE9yZzERMA8GA1UECwwIVGVzdFVuaXQxFjAUBgNVBAMMDU5vIFNBTiBDbGll
bnQwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQCjQdgqgCP1SDIDgLou
YDFKiD/PbrBK0z7eIbQQIasWdDftmmySU+5uQ39xMJbteAO8SxIgH/CEvseIBhFG
hTuz/6SKpRLJHRfJXGzOMSE6PgIoN4yua1VgSWcwKP5psZq7A0fD0g0oO6i0k9bp
8uxLkoOkdYTA6XU0A2m02kHBxu3g6lzull4tntUfVaaNAIRuyexOvicckJy5fhMA
zYlQdUaMoRKqox7Ew2PzruT5HDh5+TdfOQ6yCUBHFqyCZP460dzF0JW5e80CSDbW
dFi/aNnLCLDb1EnDaa7V+dEM1J81hER5pie+Jovm7nPjTS4sq4NnI3xggwOf2car
IbP5AgMBAAEwDQYJKoZIhvcNAQELBQADggEBAFDvRxX/qiwdogDhCaLsKN6At8GP
OrtoFhsdZvBRkR6Wk66vv4HUKPRyedX++vgxAlKQFHJs/151wu4F7tSEV2g9sRB9
IejIm2O5OE+EC5m1cJtMeTFZUI7pamPfTDb59PjC29GPBTMiU5SUGKOfNIiy+szp
aieiJdH3lRlDCok+r7zwKmQJs9sOvh7XLdiy6aAHuGbFapUtzIdHlZvD1OWv1TDG
Pj1LdSMPVR5NXURHE7JASp/zBkNkqWbq7auWD7h89jnWQeydj6GNgMjLt0lMk1KE
oRJjI+F2QSsGo1y9LhX2Rd5BGkQ2wyIN0azC6j7YZba1yadiMjwIp7DFmWg=
-----END CERTIFICATE-----)";

// Expected SAN string from the test certificate (matches OpenSSL X509V3_EXT_print output)
static const std::string kExpectedSan =
        "email:test@example.com, DNS:testclient.example.com, "
        "URI:spiffe://example.com/testclient, IP Address:192.168.1.100";

class ClientCertUtilsTest : public ::testing::Test {
protected:
    // Load X509 certificate from PEM string
    X509* load_cert_from_string(const std::string& pem) {
        BIO* bio = BIO_new_mem_buf(pem.c_str(), static_cast<int>(pem.size()));
        if (!bio) {
            return nullptr;
        }
        X509* cert = PEM_read_bio_X509(bio, nullptr, nullptr, nullptr);
        BIO_free(bio);
        return cert;
    }
};

// Test 1: Extract SAN with multiple types (email, DNS, URI, IP)
TEST_F(ClientCertUtilsTest, ExtractSanWithMultipleTypes) {
    std::unique_ptr<X509, decltype(&X509_free)> cert(load_cert_from_string(kTestCertWithSan),
                                                     X509_free);
    ASSERT_NE(cert, nullptr) << "Failed to load test certificate with SAN";

    std::string san = extract_san(cert.get());

    // Verify format matches OpenSSL X509V3_EXT_print output
    EXPECT_FALSE(san.empty());
    EXPECT_EQ(san, kExpectedSan) << "SAN string should exactly match expected format";

    // Verify individual components
    EXPECT_NE(san.find("email:test@example.com"), std::string::npos);
    EXPECT_NE(san.find("DNS:testclient.example.com"), std::string::npos);
    EXPECT_NE(san.find("URI:spiffe://example.com/testclient"), std::string::npos);
    EXPECT_NE(san.find("IP Address:192.168.1.100"), std::string::npos);
    // Verify separator format (", " - comma followed by space)
    EXPECT_NE(san.find(", "), std::string::npos);
}

// Test 2: Extract SAN from nullptr certificate returns empty string
TEST_F(ClientCertUtilsTest, ExtractSanNullCert) {
    std::string san = extract_san(nullptr);
    EXPECT_TRUE(san.empty());
}

// Test 3: Certificate without SAN extension returns empty string
TEST_F(ClientCertUtilsTest, ExtractSanNoSanExtension) {
    std::unique_ptr<X509, decltype(&X509_free)> cert(load_cert_from_string(kTestCertNoSan),
                                                     X509_free);
    ASSERT_NE(cert, nullptr) << "Failed to load test certificate without SAN";

    std::string san = extract_san(cert.get());
    EXPECT_TRUE(san.empty()) << "Certificate without SAN should return empty string";
}

// Test 4: Convert certificate to PEM format
TEST_F(ClientCertUtilsTest, X509ToPem) {
    std::unique_ptr<X509, decltype(&X509_free)> cert(load_cert_from_string(kTestCertWithSan),
                                                     X509_free);
    ASSERT_NE(cert, nullptr);

    std::string pem = x509_to_pem(cert.get());

    EXPECT_FALSE(pem.empty());
    EXPECT_NE(pem.find("-----BEGIN CERTIFICATE-----"), std::string::npos);
    EXPECT_NE(pem.find("-----END CERTIFICATE-----"), std::string::npos);
}

// Test 5: Convert nullptr certificate to PEM returns empty string
TEST_F(ClientCertUtilsTest, X509ToPemNullCert) {
    std::string pem = x509_to_pem(nullptr);
    EXPECT_TRUE(pem.empty());
}

// Test 6: Convert X509 name to string
TEST_F(ClientCertUtilsTest, X509NameToString) {
    std::unique_ptr<X509, decltype(&X509_free)> cert(load_cert_from_string(kTestCertWithSan),
                                                     X509_free);
    ASSERT_NE(cert, nullptr);

    std::string subject = x509_name_to_string(X509_get_subject_name(cert.get()));
    std::string issuer = x509_name_to_string(X509_get_issuer_name(cert.get()));

    EXPECT_FALSE(subject.empty());
    EXPECT_FALSE(issuer.empty());
    // Verify contains common fields
    EXPECT_NE(subject.find("CN"), std::string::npos) << "Subject should contain CN";
    EXPECT_NE(subject.find("Test Client"), std::string::npos);
}

// Test 7: Convert nullptr X509 name returns empty string
TEST_F(ClientCertUtilsTest, X509NameToStringNull) {
    std::string name = x509_name_to_string(nullptr);
    EXPECT_TRUE(name.empty());
}

// Test 8: Extract certificate validity period
TEST_F(ClientCertUtilsTest, ExtractValidity) {
    std::unique_ptr<X509, decltype(&X509_free)> cert(load_cert_from_string(kTestCertWithSan),
                                                     X509_free);
    ASSERT_NE(cert, nullptr);

    std::string not_before = extract_validity_not_before(cert.get());
    std::string not_after = extract_validity_not_after(cert.get());

    EXPECT_FALSE(not_before.empty()) << "Not before should not be empty";
    EXPECT_FALSE(not_after.empty()) << "Not after should not be empty";
    // ASN1_TIME_print typically outputs format like "Jan  8 06:28:03 2026 GMT"
}

// Test 9: Extract validity from nullptr certificate
TEST_F(ClientCertUtilsTest, ExtractValidityNullCert) {
    std::string not_before = extract_validity_not_before(nullptr);
    std::string not_after = extract_validity_not_after(nullptr);

    EXPECT_TRUE(not_before.empty());
    EXPECT_TRUE(not_after.empty());
}

// Test 10: ClientCertInfo::is_empty() behavior
TEST_F(ClientCertUtilsTest, ClientCertInfoIsEmpty) {
    // Empty info
    ClientCertInfo empty_info;
    EXPECT_TRUE(empty_info.is_empty()) << "Default ClientCertInfo should be empty";

    // Info with only SAN
    ClientCertInfo info_with_san;
    info_with_san.san = "DNS:example.com";
    EXPECT_FALSE(info_with_san.is_empty()) << "ClientCertInfo with SAN should not be empty";

    // Info with only subject
    ClientCertInfo info_with_subject;
    info_with_subject.subject = "CN=Test";
    EXPECT_FALSE(info_with_subject.is_empty()) << "ClientCertInfo with subject should not be empty";

    // Info with both SAN and subject
    ClientCertInfo info_with_both;
    info_with_both.san = "DNS:example.com";
    info_with_both.subject = "CN=Test";
    EXPECT_FALSE(info_with_both.is_empty());

    // Info with other fields but empty SAN and subject
    ClientCertInfo info_other_fields;
    info_other_fields.cert_pem = "some pem";
    info_other_fields.issuer = "CN=Issuer";
    EXPECT_TRUE(info_other_fields.is_empty())
            << "ClientCertInfo with only other fields should be considered empty";
}

// Test 11: extract_client_cert_info with nullptr SSL
TEST_F(ClientCertUtilsTest, ExtractClientCertInfoNullSSL) {
    ClientCertInfo info = extract_client_cert_info(nullptr);

    EXPECT_TRUE(info.is_empty());
    EXPECT_TRUE(info.cert_pem.empty());
    EXPECT_TRUE(info.san.empty());
    EXPECT_TRUE(info.subject.empty());
    EXPECT_TRUE(info.issuer.empty());
    EXPECT_TRUE(info.cipher.empty());
    EXPECT_TRUE(info.validity_not_before.empty());
    EXPECT_TRUE(info.validity_not_after.empty());
}

// Test 12: SAN format compatibility with Java FE TlsCertificateUtils
// This test ensures the C++ extract_san output matches Java's extractSubjectAlternativeNames
TEST_F(ClientCertUtilsTest, SanFormatCompatibilityWithJava) {
    std::unique_ptr<X509, decltype(&X509_free)> cert(load_cert_from_string(kTestCertWithSan),
                                                     X509_free);
    ASSERT_NE(cert, nullptr);

    std::string san = extract_san(cert.get());

    // Java TlsCertificateUtils uses these prefixes:
    // - SAN_TYPE_RFC822_NAME (1): "email:"
    // - SAN_TYPE_DNS_NAME (2): "DNS:"
    // - SAN_TYPE_URI (6): "URI:"
    // - SAN_TYPE_IP_ADDRESS (7): "IP Address:"
    // And joins with ", " (comma + space)

    // Verify prefixes match Java implementation
    if (san.find("email:") != std::string::npos) {
        // email prefix should be lowercase
        EXPECT_EQ(san.find("EMAIL:"), std::string::npos) << "email prefix should be lowercase";
    }
    if (san.find("DNS:") != std::string::npos) {
        // DNS prefix should be uppercase
        EXPECT_EQ(san.find("dns:"), std::string::npos) << "DNS prefix should be uppercase";
    }
    if (san.find("URI:") != std::string::npos) {
        // URI prefix should be uppercase
        EXPECT_EQ(san.find("uri:"), std::string::npos) << "URI prefix should be uppercase";
    }
    if (san.find("IP Address:") != std::string::npos) {
        // IP Address prefix should have space
        EXPECT_EQ(san.find("IPAddress:"), std::string::npos) << "IP Address should have space";
        EXPECT_EQ(san.find("IP:"), std::string::npos) << "Should use 'IP Address:' not 'IP:'";
    }
}

// Test 13: Round-trip PEM conversion
TEST_F(ClientCertUtilsTest, PemRoundTrip) {
    std::unique_ptr<X509, decltype(&X509_free)> cert1(load_cert_from_string(kTestCertWithSan),
                                                      X509_free);
    ASSERT_NE(cert1, nullptr);

    // Convert to PEM
    std::string pem = x509_to_pem(cert1.get());
    ASSERT_FALSE(pem.empty());

    // Load from PEM again
    std::unique_ptr<X509, decltype(&X509_free)> cert2(load_cert_from_string(pem), X509_free);
    ASSERT_NE(cert2, nullptr);

    // Compare SAN
    std::string san1 = extract_san(cert1.get());
    std::string san2 = extract_san(cert2.get());
    EXPECT_EQ(san1, san2) << "SAN should be identical after PEM round-trip";
}

} // namespace doris
