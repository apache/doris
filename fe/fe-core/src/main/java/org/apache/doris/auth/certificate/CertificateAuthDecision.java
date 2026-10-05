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

package org.apache.doris.auth.certificate;

import org.apache.doris.analysis.UserIdentity;

public final class CertificateAuthDecision {
    public enum Outcome {
        NOT_APPLICABLE,
        REJECT,
        VERIFIED
    }

    private final Outcome outcome;
    private final UserIdentity userIdentity;
    private final boolean skipPassword;
    private final String errorMessage;

    private CertificateAuthDecision(Outcome outcome, UserIdentity userIdentity,
            boolean skipPassword, String errorMessage) {
        this.outcome = outcome;
        this.userIdentity = userIdentity;
        this.skipPassword = skipPassword;
        this.errorMessage = errorMessage;
    }

    public static CertificateAuthDecision notApplicable() {
        return new CertificateAuthDecision(Outcome.NOT_APPLICABLE, null, false, null);
    }

    public static CertificateAuthDecision reject(String errorMessage) {
        return new CertificateAuthDecision(Outcome.REJECT, null, false, errorMessage);
    }

    public static CertificateAuthDecision verified(UserIdentity userIdentity, boolean skipPassword) {
        return new CertificateAuthDecision(Outcome.VERIFIED, userIdentity, skipPassword, null);
    }

    public Outcome getOutcome() {
        return outcome;
    }

    public boolean isVerified() {
        return outcome == Outcome.VERIFIED;
    }

    public boolean isReject() {
        return outcome == Outcome.REJECT;
    }

    public boolean isNotApplicable() {
        return outcome == Outcome.NOT_APPLICABLE;
    }

    public UserIdentity getUserIdentity() {
        return userIdentity;
    }

    public boolean isSkipPassword() {
        return skipPassword;
    }

    public boolean shouldSkipPasswordVerification() {
        return isVerified() && skipPassword;
    }

    public String getErrorMessage() {
        return errorMessage;
    }
}
