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

package org.apache.doris.filesystem.gcs;

import org.apache.doris.cloud.proto.Cloud.GcpCredentialPB;
import org.apache.doris.cloud.proto.Cloud.ObjectStoreCredentialPB;
import org.apache.doris.filesystem.spi.ObjectStorageCredentialCodec;
import org.apache.doris.thrift.TCredential;
import org.apache.doris.thrift.TGcpCredential;
import org.apache.doris.thrift.TGcpCredentialProviderType;

import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/** Native GCP wire encoding belongs to the GCS plugin, alongside its authentication rules. */
public final class GcsCredentialCodec implements ObjectStorageCredentialCodec {
    @Override
    public String provider() {
        return "GCP";
    }

    @Override
    public byte[] toCloudCredential(Map<String, String> fields) {
        GcpCredentialPB.Builder credential = GcpCredentialPB.newBuilder();
        if (fields.containsKey("credential_provider_type")) {
            credential.setCredentialProviderType(GcpCredentialPB.CredentialProviderType.valueOf(
                    fields.get("credential_provider_type")));
        }
        if (fields.containsKey("impersonation_service_account")) {
            credential.setImpersonationServiceAccount(fields.get("impersonation_service_account"));
        }
        return ObjectStoreCredentialPB.newBuilder().setGcpCredential(credential).build().toByteArray();
    }

    @Override
    public byte[] toThriftCredential(Map<String, String> fields) throws IOException {
        TGcpCredential credential = new TGcpCredential();
        if (fields.containsKey("credential_provider_type")) {
            credential.setCredentialProviderType(TGcpCredentialProviderType.valueOf(
                    fields.get("credential_provider_type")));
        }
        if (fields.containsKey("impersonation_service_account")) {
            credential.setImpersonationServiceAccount(fields.get("impersonation_service_account"));
        }
        try {
            return new TSerializer(new TBinaryProtocol.Factory())
                    .serialize(new TCredential().setGcpCredential(credential));
        } catch (TException e) {
            throw new IOException("Cannot encode GCS credentials for BE", e);
        }
    }

    @Override
    public Map<String, String> toProperties(byte[] cloudCredential) throws IOException {
        ObjectStoreCredentialPB envelope = ObjectStoreCredentialPB.parseFrom(cloudCredential);
        if (!envelope.hasGcpCredential()) {
            throw new IOException("Missing GCP credentials in the Cloud credential envelope");
        }
        GcpCredentialPB credential = envelope.getGcpCredential();
        Map<String, String> fields = new HashMap<>();
        fields.put("credential_provider_type", credential.getCredentialProviderType().name());
        if (credential.hasImpersonationServiceAccount()) {
            fields.put("impersonation_service_account", credential.getImpersonationServiceAccount());
        }
        return new GcsFileSystemProvider().credentialToProperties(fields);
    }
}
