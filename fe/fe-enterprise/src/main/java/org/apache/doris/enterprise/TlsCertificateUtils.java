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
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Utility class for extracting information from X509 certificates,
 * particularly Subject Alternative Names (SANs).
 */
public class TlsCertificateUtils {
    private static final Logger LOG = LogManager.getLogger(TlsCertificateUtils.class);

    // SAN type constants as per RFC 5280
    private static final int SAN_TYPE_OTHER_NAME = 0;
    private static final int SAN_TYPE_RFC822_NAME = 1;  // Email
    private static final int SAN_TYPE_DNS_NAME = 2;
    private static final int SAN_TYPE_X400_ADDRESS = 3;
    private static final int SAN_TYPE_DIRECTORY_NAME = 4;
    private static final int SAN_TYPE_EDI_PARTY_NAME = 5;
    private static final int SAN_TYPE_URI = 6;
    private static final int SAN_TYPE_IP_ADDRESS = 7;
    private static final int SAN_TYPE_REGISTERED_ID = 8;

    private TlsCertificateUtils() {
        // Utility class
    }

    /**
     * Extracts all Subject Alternative Names (SANs) from an X509 certificate.
     * Supports DNS names, email addresses (RFC822), URIs, and IP addresses.
     *
     * @param cert the X509 certificate
     * @return a set of all SAN values (as strings), empty set if none found or on error
     */
    public static Set<String> extractSubjectAlternativeNames(X509Certificate cert) {
        if (cert == null) {
            return Collections.emptySet();
        }

        Set<String> sans = new HashSet<>();
        try {
            Collection<List<?>> sanCollection = cert.getSubjectAlternativeNames();
            if (sanCollection == null) {
                return Collections.emptySet();
            }

            for (List<?> san : sanCollection) {
                if (san == null || san.size() < 2) {
                    continue;
                }

                Integer type = (Integer) san.get(0);
                Object value = san.get(1);

                switch (type) {
                    case SAN_TYPE_DNS_NAME:
                    case SAN_TYPE_RFC822_NAME:
                    case SAN_TYPE_URI:
                        if (value instanceof String) {
                            sans.add((String) value);
                        }
                        break;
                    case SAN_TYPE_IP_ADDRESS:
                        // IP address may be returned as String or byte array
                        if (value instanceof String) {
                            addNormalizedIp(sans, (String) value);
                        } else if (value instanceof byte[]) {
                            String ipStr = bytesToIpAddress((byte[]) value);
                            if (ipStr != null) {
                                sans.add(ipStr);
                            }
                        }
                        break;
                    default:
                        // Skip other types (otherName, directoryName, etc.)
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Skipping SAN type {}: {}", type, value);
                        }
                        break;
                }
            }
        } catch (CertificateParsingException e) {
            LOG.warn("Failed to parse Subject Alternative Names from certificate: {}", e.getMessage());
        }

        return sans;
    }

    /**
     * Extracts DNS names from the Subject Alternative Names of an X509 certificate.
     *
     * @param cert the X509 certificate
     * @return a set of DNS name SAN values, empty set if none found
     */
    public static Set<String> extractDnsNames(X509Certificate cert) {
        return extractSansByType(cert, SAN_TYPE_DNS_NAME);
    }

    /**
     * Extracts email addresses (RFC822 names) from the Subject Alternative Names.
     *
     * @param cert the X509 certificate
     * @return a set of email SAN values, empty set if none found
     */
    public static Set<String> extractEmailAddresses(X509Certificate cert) {
        return extractSansByType(cert, SAN_TYPE_RFC822_NAME);
    }

    /**
     * Extracts URIs from the Subject Alternative Names.
     *
     * @param cert the X509 certificate
     * @return a set of URI SAN values, empty set if none found
     */
    public static Set<String> extractUris(X509Certificate cert) {
        return extractSansByType(cert, SAN_TYPE_URI);
    }

    /**
     * Extracts IP addresses from the Subject Alternative Names.
     *
     * @param cert the X509 certificate
     * @return a set of IP address SAN values, empty set if none found
     */
    public static Set<String> extractIpAddresses(X509Certificate cert) {
        return extractSansByType(cert, SAN_TYPE_IP_ADDRESS);
    }

    private static Set<String> extractSansByType(X509Certificate cert, int targetType) {
        if (cert == null) {
            return Collections.emptySet();
        }

        Set<String> result = new HashSet<>();
        try {
            Collection<List<?>> sanCollection = cert.getSubjectAlternativeNames();
            if (sanCollection == null) {
                return Collections.emptySet();
            }

            for (List<?> san : sanCollection) {
                if (san == null || san.size() < 2) {
                    continue;
                }

                Integer type = (Integer) san.get(0);
                if (type != targetType) {
                    continue;
                }

                Object value = san.get(1);
                if (targetType == SAN_TYPE_IP_ADDRESS) {
                    if (value instanceof byte[]) {
                        String ipStr = bytesToIpAddress((byte[]) value);
                        if (ipStr != null) {
                            result.add(ipStr);
                        }
                    } else if (value instanceof String) {
                        addNormalizedIp(result, (String) value);
                    }
                } else if (value instanceof String) {
                    result.add((String) value);
                }
            }
        } catch (CertificateParsingException e) {
            LOG.warn("Failed to parse SANs of type {} from certificate: {}", targetType, e.getMessage());
        }

        return result;
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

    private static void addNormalizedIp(Set<String> target, String ipValue) {
        if (ipValue == null || ipValue.isEmpty()) {
            return;
        }
        String normalized = normalizeIpString(ipValue);
        if (normalized != null && !normalized.isEmpty()) {
            target.add(normalized);
            if (!normalized.equals(ipValue)) {
                target.add(ipValue);
            }
        }
    }

    private static String normalizeIpString(String ipValue) {
        if (ipValue == null || ipValue.isEmpty()) {
            return ipValue;
        }
        try {
            return InetAddress.getByName(ipValue).getHostAddress();
        } catch (UnknownHostException e) {
            LOG.warn("Invalid IP address string in SAN: {}", ipValue);
            return ipValue;
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


    /**
     * Checks if a given SAN value exists in the certificate's Subject Alternative Names.
     *
     * @param cert     the X509 certificate
     * @param sanValue the SAN value to check
     * @return true if the SAN value is found, false otherwise
     */
    public static boolean hasSan(X509Certificate cert, String sanValue) {
        if (cert == null || sanValue == null || sanValue.isEmpty()) {
            return false;
        }
        Set<String> sans = extractSubjectAlternativeNames(cert);
        return sans.contains(sanValue);
    }
}
