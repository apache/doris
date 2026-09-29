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

package org.apache.doris.filesystem.auth;

import java.util.Optional;

/** Immutable authentication choice shared by GCS clients, cache identity and protocol builders. */
public final class GcsAuth {
    public enum Mode {
        HMAC, ADC, COMPUTE_ENGINE, ANONYMOUS
    }

    private final Mode mode;
    private final GcpCredential nativeCredential;

    GcsAuth(Mode mode, String impersonationServiceAccount) {
        this.mode = mode;
        this.nativeCredential = mode == Mode.ADC || mode == Mode.COMPUTE_ENGINE
                ? new GcpCredential(mode == Mode.ADC ? GcpCredentialProviderType.DEFAULT
                        : GcpCredentialProviderType.COMPUTE_ENGINE, impersonationServiceAccount)
                : null;
    }

    public Mode getMode() {
        return mode;
    }

    public boolean isAnonymous() {
        return mode == Mode.ANONYMOUS;
    }

    public Optional<GcpCredential> getNativeCredential() {
        return Optional.ofNullable(nativeCredential);
    }
}
