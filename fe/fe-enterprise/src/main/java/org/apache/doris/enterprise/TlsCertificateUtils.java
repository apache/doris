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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Utility class for extracting information from X509 certificates,
 * particularly Subject Alternative Names (SANs).
 */
public class TlsCertificateUtils {
    private static final Logger LOG = LogManager.getLogger(TlsCertificateUtils.class);

    // SAN type constants as per RFC 5280
    private static final int SAN_TYPE_RFC822_NAME = 1;  // Email
    private static final int SAN_TYPE_DNS_NAME = 2;
    private static final int SAN_TYPE_URI = 6;
    private static final int SAN_TYPE_IP_ADDRESS = 7;

    private TlsCertificateUtils() {
        // Utility class
    }

    /**
     * Extracts all Subject Alternative Names (SANs) from an X509 certificate
     * as a single formatted string matching OpenSSL X509V3_EXT_print output.
     *
     * <p>The output format is: "email:xxx, DNS:xxx, URI:xxx, IP Address:xxx"
     * with entries separated by ", " (comma and space).
     *
     * <p>The order of SANs in the output matches the order in which they appear
     * in the certificate, which is determined by the order they were specified
     * when the certificate was generated.
     *
     * @param cert the X509 certificate
     * @return the formatted SAN string, or empty string if no SANs found or on error
     */
    public static String extractSubjectAlternativeNames(X509Certificate cert) {
        if (cert == null) {
            return "";
        }

        List<String> sanParts = new ArrayList<>();
        try {
            Collection<List<?>> sanCollection = cert.getSubjectAlternativeNames();
            if (sanCollection == null) {
                return "";
            }

            // Iterate in certificate storage order (preserved by Java API)
            for (List<?> san : sanCollection) {
                if (san == null || san.size() < 2) {
                    continue;
                }

                Integer type = (Integer) san.get(0);
                Object value = san.get(1);
                String prefix = getTypePrefix(type);

                if (prefix != null && value != null) {
                    String strValue = formatSanValue(type, value);
                    if (strValue != null && !strValue.isEmpty()) {
                        sanParts.add(prefix + strValue);
                    }
                }
            }
        } catch (CertificateParsingException e) {
            LOG.warn("Failed to parse Subject Alternative Names from certificate: {}", e.getMessage());
            return "";
        }

        return String.join(", ", sanParts);
    }

    /**
     * Gets the type prefix for a SAN type, matching OpenSSL output format.
     *
     * @param type the SAN type code (RFC 5280)
     * @return the prefix string, or null if the type is not supported
     */
    private static String getTypePrefix(int type) {
        switch (type) {
            case SAN_TYPE_RFC822_NAME:
                return "email:";
            case SAN_TYPE_DNS_NAME:
                return "DNS:";
            case SAN_TYPE_URI:
                return "URI:";
            case SAN_TYPE_IP_ADDRESS:
                return "IP Address:";
            default:
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Skipping unsupported SAN type: {}", type);
                }
                return null;
        }
    }

    /**
     * Formats a SAN value to string.
     *
     * @param type  the SAN type code
     * @param value the SAN value (String or byte[] for IP addresses)
     * @return the formatted string value, or null if invalid
     */
    private static String formatSanValue(int type, Object value) {
        if (type == SAN_TYPE_IP_ADDRESS) {
            if (value instanceof String) {
                return (String) value;
            } else if (value instanceof byte[]) {
                return bytesToIpAddress((byte[]) value);
            }
            return null;
        } else if (value instanceof String) {
            return (String) value;
        }
        return null;
    }

    /**
     * Converts a byte array representing an IP address to its string representation.
     * Handles both IPv4 (4 bytes) and IPv6 (16 bytes) addresses.
     *
     * @param bytes the IP address as a byte array
     * @return the string representation of the IP address, or null if invalid
     */
    private static String bytesToIpAddress(byte[] bytes) {
        if (bytes == null) {
            return null;
        }

        try {
            return InetAddress.getByAddress(bytes).getHostAddress();
        } catch (UnknownHostException e) {
            LOG.warn("Invalid IP address byte array length: {}", bytes.length);
            return null;
        }
    }

    /**
     * Gets the Subject DN (Distinguished Name) from the certificate.
     *
     * @param cert the X509 certificate
     * @return the subject DN as a string, or null if not available
     */
    public static String getSubjectDN(X509Certificate cert) {
        if (cert == null || cert.getSubjectX500Principal() == null) {
            return null;
        }
        return cert.getSubjectX500Principal().getName();
    }

    /**
     * Gets the Issuer DN (Distinguished Name) from the certificate.
     *
     * @param cert the X509 certificate
     * @return the issuer DN as a string, or null if not available
     */
    public static String getIssuerDN(X509Certificate cert) {
        if (cert == null || cert.getIssuerX500Principal() == null) {
            return null;
        }
        return cert.getIssuerX500Principal().getName();
    }
}
