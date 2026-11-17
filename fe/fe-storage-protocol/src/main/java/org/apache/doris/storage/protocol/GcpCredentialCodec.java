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

package org.apache.doris.storage.protocol;

import org.apache.doris.cloud.proto.Cloud.GcpCredentialPB;
import org.apache.doris.cloud.proto.Cloud.ObjectStoreCredentialPB;
import org.apache.doris.filesystem.auth.GcpCredentialSpec;
import org.apache.doris.thrift.TCredential;
import org.apache.doris.thrift.TGcpCredential;
import org.apache.doris.thrift.TGcpCredentialProviderType;

/** GCP wire-schema mapping only; property validation and credential loading stay in the GCS plugin. */
final class GcpCredentialCodec implements ObjectStorageCredentialCodec<GcpCredentialSpec> {
    @Override
    public Class<GcpCredentialSpec> credentialType() {
        return GcpCredentialSpec.class;
    }

    @Override
    public ObjectStoreCredentialPB toCloud(GcpCredentialSpec spec) {
        GcpCredentialPB.Builder credential = GcpCredentialPB.newBuilder();
        spec.getProviderType().ifPresent(type -> {
            GcpCredentialPB.CredentialProviderType mapped = switch (type) {
                case DEFAULT -> GcpCredentialPB.CredentialProviderType.DEFAULT;
                case COMPUTE_ENGINE -> GcpCredentialPB.CredentialProviderType.COMPUTE_ENGINE;
                default -> throw new IllegalArgumentException("Unsupported GCP identity source: " + type);
            };
            credential.setCredentialProviderType(mapped);
        });
        spec.getImpersonationServiceAccount().ifPresent(credential::setImpersonationServiceAccount);
        return ObjectStoreCredentialPB.newBuilder().setGcpCredential(credential).build();
    }

    @Override
    public TCredential toThrift(GcpCredentialSpec spec) {
        TGcpCredential credential = new TGcpCredential();
        spec.getProviderType().ifPresent(type -> {
            TGcpCredentialProviderType mapped = switch (type) {
                case DEFAULT -> TGcpCredentialProviderType.DEFAULT;
                case COMPUTE_ENGINE -> TGcpCredentialProviderType.COMPUTE_ENGINE;
                default -> throw new IllegalArgumentException("Unsupported GCP identity source: " + type);
            };
            credential.setCredentialProviderType(mapped);
        });
        spec.getImpersonationServiceAccount().ifPresent(credential::setImpersonationServiceAccount);
        return new TCredential().setGcpCredential(credential);
    }

    @Override
    public GcpCredentialSpec fromCloud(ObjectStoreCredentialPB envelope) {
        if (!envelope.hasGcpCredential()) {
            throw new IllegalArgumentException("Missing GCP credentials in the Cloud credential envelope");
        }
        GcpCredentialPB credential = envelope.getGcpCredential();
        // Proto2 retains unknown enum values as unknown fields. Treating that as absence would
        // make a newer identity source silently fall back to ADC when properties are restored.
        if (credential.getUnknownFields().hasField(GcpCredentialPB.CREDENTIAL_PROVIDER_TYPE_FIELD_NUMBER)) {
            throw new IllegalArgumentException("Unsupported GCP identity source in Cloud credentials");
        }
        GcpCredentialSpec.ProviderType type = null;
        if (credential.hasCredentialProviderType()) {
            type = switch (credential.getCredentialProviderType()) {
                case DEFAULT -> GcpCredentialSpec.ProviderType.DEFAULT;
                case COMPUTE_ENGINE -> GcpCredentialSpec.ProviderType.COMPUTE_ENGINE;
                default -> throw new IllegalArgumentException("Unsupported GCP identity source in Cloud credentials");
            };
        }
        return new GcpCredentialSpec(type,
                credential.hasImpersonationServiceAccount() ? credential.getImpersonationServiceAccount() : null);
    }
}
