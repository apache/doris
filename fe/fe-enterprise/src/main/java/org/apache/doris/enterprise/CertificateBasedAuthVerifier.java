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
import org.apache.doris.mysql.authenticate.CertificateAuthVerifier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.security.cert.X509Certificate;

/**
 * Enterprise implementation of CertificateAuthVerifier.
 * Provides actual TLS certificate verification against user's requirements.
 * Currently supports SAN (Subject Alternative Name) exact matching.
 */
public class CertificateBasedAuthVerifier implements CertificateAuthVerifier {
    private static final Logger LOG = LogManager.getLogger(CertificateBasedAuthVerifier.class);

    @Override
    public VerificationResult verify(UserIdentity userIdentity, X509Certificate clientCert) {
        if (userIdentity == null) {
            return VerificationResult.failure("UserIdentity is null");
        }

        // Check if user has any TLS requirements
        if (!userIdentity.hasTlsRequirements()) {
            // No TLS requirements - verification passes by default
            return VerificationResult.success();
        }

        // User has TLS requirements but no certificate provided
        if (clientCert == null) {
            String errorMsg = String.format(
                    "User %s requires TLS certificate authentication but no client certificate was provided",
                    userIdentity);
            LOG.warn(errorMsg);
            return VerificationResult.failure(errorMsg);
        }

        // Verify SAN if required
        String requiredSan = userIdentity.getSan();
        if (requiredSan != null && !requiredSan.isEmpty()) {
            VerificationResult sanResult = verifySan(userIdentity, clientCert, requiredSan);
            if (!sanResult.isSuccess()) {
                return sanResult;
            }
        }

        // TODO: Verify ISSUER if required (future enhancement)
        String requiredIssuer = userIdentity.getIssuer();
        if (requiredIssuer != null && !requiredIssuer.isEmpty()) {
            LOG.debug("ISSUER verification is not yet implemented, skipping for user {}", userIdentity);
            // Future: verify issuer DN matches
        }

        // TODO: Verify SUBJECT if required (future enhancement)
        String requiredSubject = userIdentity.getSubject();
        if (requiredSubject != null && !requiredSubject.isEmpty()) {
            LOG.debug("SUBJECT verification is not yet implemented, skipping for user {}", userIdentity);
            // Future: verify subject DN matches
        }

        // TODO: Verify CIPHER if required (future enhancement)
        String requiredCipher = userIdentity.getCipher();
        if (requiredCipher != null && !requiredCipher.isEmpty()) {
            LOG.debug("CIPHER verification is not yet implemented, skipping for user {}", userIdentity);
            // Future: verify cipher suite matches
        }

        LOG.info("TLS certificate verification succeeded for user {}", userIdentity);
        return VerificationResult.success();
    }

    /**
     * Verifies the user's TLS requirements using pre-extracted certificate information.
     * This method is used for Stream Load authentication where BE extracts certificate
     * info and sends it via Thrift (TCertBasedAuth).
     *
     * @param userIdentity The user identity containing TLS requirements.
     * @param certSan      The pre-extracted SAN string from the certificate (may be null or empty).
     * @param certSubject  The pre-extracted Subject DN (for future use, may be null).
     * @param certIssuer   The pre-extracted Issuer DN (for future use, may be null).
     * @param certCipher   The cipher suite used (for future use, may be null).
     * @return VerificationResult indicating success or failure with an error message.
     */
    public VerificationResult verifyWithExtractedCertInfo(UserIdentity userIdentity,
            String certSan, String certSubject, String certIssuer, String certCipher) {
        if (userIdentity == null) {
            return VerificationResult.failure("UserIdentity is null");
        }

        // Check if user has any TLS requirements
        if (!userIdentity.hasTlsRequirements()) {
            // No TLS requirements - verification passes by default
            return VerificationResult.success();
        }

        // Verify SAN if required
        String requiredSan = userIdentity.getSan();
        if (requiredSan != null && !requiredSan.isEmpty()) {
            VerificationResult sanResult = verifySanString(userIdentity, certSan, requiredSan);
            if (!sanResult.isSuccess()) {
                return sanResult;
            }
        }

        // TODO: Verify ISSUER if required (future enhancement)
        String requiredIssuer = userIdentity.getIssuer();
        if (requiredIssuer != null && !requiredIssuer.isEmpty()) {
            LOG.debug("ISSUER verification is not yet implemented, skipping for user {}", userIdentity);
            // Future: verify certIssuer matches requiredIssuer
        }

        // TODO: Verify SUBJECT if required (future enhancement)
        String requiredSubject = userIdentity.getSubject();
        if (requiredSubject != null && !requiredSubject.isEmpty()) {
            LOG.debug("SUBJECT verification is not yet implemented, skipping for user {}", userIdentity);
            // Future: verify certSubject matches requiredSubject
        }

        // TODO: Verify CIPHER if required (future enhancement)
        String requiredCipher = userIdentity.getCipher();
        if (requiredCipher != null && !requiredCipher.isEmpty()) {
            LOG.debug("CIPHER verification is not yet implemented, skipping for user {}", userIdentity);
            // Future: verify certCipher matches requiredCipher
        }

        LOG.info("TLS certificate verification (from extracted info) succeeded for user {}", userIdentity);
        return VerificationResult.success();
    }

    /**
     * Verifies that the certificate's SAN string exactly matches the required SAN.
     * This is a full string comparison - the entire SAN extension content must match.
     */
    private VerificationResult verifySan(UserIdentity userIdentity, X509Certificate clientCert, String requiredSan) {
        String certSanString = TlsCertificateUtils.extractSubjectAlternativeNames(clientCert);
        return verifySanString(userIdentity, certSanString, requiredSan);
    }

    /**
     * Verifies that the provided SAN string exactly matches the required SAN.
     * This is a full string comparison - the entire SAN string must match.
     *
     * @param userIdentity The user identity (for error messages).
     * @param certSanString The SAN string from the certificate.
     * @param requiredSan The required SAN string from user configuration.
     * @return VerificationResult indicating success or failure.
     */
    private VerificationResult verifySanString(UserIdentity userIdentity, String certSanString, String requiredSan) {
        if (certSanString == null || certSanString.isEmpty()) {
            String errorMsg = String.format(
                    "User %s requires SAN '%s' but the client certificate has no Subject Alternative Names",
                    userIdentity, requiredSan);
            LOG.warn(errorMsg);
            return VerificationResult.failure(errorMsg);
        }

        // Exact string match
        if (!certSanString.equals(requiredSan)) {
            String errorMsg = String.format(
                    "User %s requires SAN '%s' but certificate SAN is '%s'",
                    userIdentity, requiredSan, certSanString);
            LOG.warn(errorMsg);
            return VerificationResult.failure(errorMsg);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("SAN verification succeeded for user {}: required='{}', cert SAN='{}'",
                    userIdentity, requiredSan, certSanString);
        }
        return VerificationResult.success();
    }

    @Override
    public boolean shouldSkipPasswordVerification() {
        return Config.tls_cert_based_auth_ignore_password;
    }

    @Override
    public boolean isEnabled() {
        return true;
    }
}
