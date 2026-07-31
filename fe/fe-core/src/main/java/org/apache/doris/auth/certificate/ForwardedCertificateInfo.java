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

import java.util.Collections;
import java.util.List;

public final class ForwardedCertificateInfo {
    private final String san;
    private final String subject;
    private final String issuer;
    private final String cipher;
    private final String certPem;
    private final String validityNotBefore;
    private final String validityNotAfter;

    public ForwardedCertificateInfo(String san, String subject, String issuer, String cipher,
            String certPem, String validityNotBefore, String validityNotAfter) {
        this.san = san;
        this.subject = subject;
        this.issuer = issuer;
        this.cipher = cipher;
        this.certPem = certPem;
        this.validityNotBefore = validityNotBefore;
        this.validityNotAfter = validityNotAfter;
    }

    public String getSan() {
        return san;
    }

    public List<String> getSanEntries() {
        if (san == null || san.trim().isEmpty()) {
            return Collections.emptyList();
        }
        return SanEntryCodec.parseAndNormalize(san);
    }

    public String getSubject() {
        return subject;
    }

    public String getIssuer() {
        return issuer;
    }

    public String getCipher() {
        return cipher;
    }

    public String getCertPem() {
        return certPem;
    }

    public String getValidityNotBefore() {
        return validityNotBefore;
    }

    public String getValidityNotAfter() {
        return validityNotAfter;
    }
}
