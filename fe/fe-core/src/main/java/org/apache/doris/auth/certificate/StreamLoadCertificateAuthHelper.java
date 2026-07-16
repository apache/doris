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

import org.apache.doris.thrift.TCertBasedAuth;

public final class StreamLoadCertificateAuthHelper {
    private StreamLoadCertificateAuthHelper() {
    }

    public static CertificateAuthDecision authenticateForwarded(
            CertificateRuntimeAuthService runtimeAuthService,
            String userName,
            String remoteIp,
            ForwardedCertificateInfo certInfo) {
        if (runtimeAuthService == null) {
            return CertificateAuthDecision.notApplicable();
        }
        return runtimeAuthService.authenticateForwarded(userName, remoteIp, certInfo);
    }

    public static ForwardedCertificateInfo fromThrift(TCertBasedAuth certInfo) {
        if (certInfo == null) {
            return null;
        }
        return new ForwardedCertificateInfo(
                certInfo.isSetSan() ? certInfo.getSan() : null,
                certInfo.isSetSubject() ? certInfo.getSubject() : null,
                certInfo.isSetIssuer() ? certInfo.getIssuer() : null,
                certInfo.isSetCipher() ? certInfo.getCipher() : null,
                certInfo.isSetCertPem() ? certInfo.getCertPem() : null,
                certInfo.isSetValidityNotBefore() ? certInfo.getValidityNotBefore() : null,
                certInfo.isSetValidityNotAfter() ? certInfo.getValidityNotAfter() : null);
    }
}
