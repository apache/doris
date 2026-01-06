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

package org.apache.doris.mysql.authenticate;

import org.apache.doris.analysis.UserIdentity;

import java.security.cert.X509Certificate;

/**
 * Interface for TLS certificate-based authentication verification.
 * This is designed as a pluggable SPI interface where the open-source version
 * provides a no-op implementation and the enterprise version provides the actual
 * certificate verification logic.
 */
public interface CertificateAuthVerifier {

    /**
     * Verification result containing the outcome and optional error message.
     */
    class VerificationResult {
        private final boolean success;
        private final String errorMessage;

        private VerificationResult(boolean success, String errorMessage) {
            this.success = success;
            this.errorMessage = errorMessage;
        }

        public static VerificationResult success() {
            return new VerificationResult(true, null);
        }

        public static VerificationResult failure(String errorMessage) {
            return new VerificationResult(false, errorMessage);
        }

        public boolean isSuccess() {
            return success;
        }

        public String getErrorMessage() {
            return errorMessage;
        }
    }

    /**
     * Verifies the client certificate against the user's TLS requirements.
     * This method is called when a user has configured TLS requirements (e.g., REQUIRE SAN).
     *
     * @param userIdentity The user identity containing TLS requirements (SAN, issuer, subject, cipher).
     * @param clientCert   The client's X509 certificate from the TLS handshake.
     *                     May be null if no certificate was provided.
     * @return VerificationResult indicating success or failure with an error message.
     */
    VerificationResult verify(UserIdentity userIdentity, X509Certificate clientCert);

    /**
     * Determines whether password verification should be skipped after successful
     * certificate verification. This is controlled by the configuration
     * `tls_cert_based_auth_ignore_password`.
     *
     * @return true if password verification should be skipped when certificate
     *         verification succeeds, false otherwise.
     */
    boolean shouldSkipPasswordVerification();

    /**
     * Checks if this verifier is enabled (i.e., not a no-op implementation).
     * The open-source no-op verifier returns false, while the enterprise
     * implementation returns true.
     *
     * @return true if actual certificate verification is enabled, false for no-op.
     */
    boolean isEnabled();
}
