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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.security.cert.X509Certificate;
import java.util.ServiceLoader;

/**
 * Factory for creating CertificateAuthVerifier instances.
 * Uses ServiceLoader to load the implementation from enterprise module if available,
 * otherwise falls back to the NoOp implementation.
 */
public class CertificateAuthVerifierFactory {
    private static final Logger LOG = LogManager.getLogger(CertificateAuthVerifierFactory.class);

    private static volatile CertificateAuthVerifier instance;

    private CertificateAuthVerifierFactory() {
    }

    /**
     * Gets the singleton CertificateAuthVerifier instance.
     * Uses ServiceLoader to load the enterprise implementation if available,
     * otherwise returns a NoOp implementation.
     *
     * @return the CertificateAuthVerifier instance
     */
    public static CertificateAuthVerifier getInstance() {
        if (instance == null) {
            synchronized (CertificateAuthVerifierFactory.class) {
                if (instance == null) {
                    instance = loadVerifier();
                }
            }
        }
        return instance;
    }

    private static CertificateAuthVerifier loadVerifier() {
        ServiceLoader<CertificateAuthVerifier> loader = ServiceLoader.load(CertificateAuthVerifier.class);
        for (CertificateAuthVerifier verifier : loader) {
            if (verifier.isEnabled()) {
                LOG.info("Loaded CertificateAuthVerifier: {}", verifier.getClass().getName());
                return verifier;
            }
        }
        LOG.info("No enterprise CertificateAuthVerifier found, using NoOp implementation");
        return new NoOpCertificateAuthVerifier();
    }

    /**
     * Resets the singleton instance. This is mainly for testing purposes.
     */
    public static void reset() {
        synchronized (CertificateAuthVerifierFactory.class) {
            instance = null;
        }
    }

    /**
     * NoOp implementation of CertificateAuthVerifier.
     * This is used in the open-source version where certificate-based authentication
     * is not supported. It always allows verification to pass (returns success)
     * but indicates that certificate verification is not actually enabled.
     */
    public static class NoOpCertificateAuthVerifier implements CertificateAuthVerifier {

        @Override
        public VerificationResult verify(UserIdentity userIdentity, X509Certificate clientCert) {
            // NoOp implementation always succeeds - actual verification is done
            // by the enterprise implementation
            return VerificationResult.success();
        }

        @Override
        public boolean shouldSkipPasswordVerification() {
            // NoOp always requires password verification
            return false;
        }

        @Override
        public boolean isEnabled() {
            // NoOp is not a real implementation
            return false;
        }
    }
}
