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
import org.apache.doris.filesystem.auth.ObjectStorageAuthentication;
import org.apache.doris.filesystem.spi.ObjectStorageCredentialCodec;
import org.apache.doris.thrift.TCredential;
import org.apache.doris.thrift.TGcpCredential;
import org.apache.doris.thrift.TGcpCredentialProviderType;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;

class GcsCredentialCodecTest {
    private final GcsFileSystemProvider provider = new GcsFileSystemProvider();
    private final ObjectStorageCredentialCodec codec = provider.credentialCodec().orElseThrow();

    @Test
    void nativeCredentialsRoundTripThroughBothWireFormats() throws Exception {
        for (String source : new String[] {"DEFAULT", "COMPUTE_ENGINE"}) {
            Map<String, String> properties = Map.of(
                    "provider", "GCP",
                    "gs.credential_provider_type", source,
                    "gs.impersonation_service_account", "target@test.iam.gserviceaccount.com");
            ObjectStorageAuthentication auth = provider.resolveAuthentication(properties).orElseThrow();
            Assertions.assertEquals(auth.getProvider(), codec.provider());
            byte[] cloudBytes = codec.toCloudCredential(auth.getCredential());
            GcpCredentialPB cloud = ObjectStoreCredentialPB.parseFrom(cloudBytes).getGcpCredential();
            TGcpCredential thrift = decodeThrift(auth.getCredential());
            Assertions.assertEquals(GcpCredentialPB.CredentialProviderType.valueOf(source),
                    cloud.getCredentialProviderType());
            Assertions.assertEquals(TGcpCredentialProviderType.valueOf(source), thrift.getCredentialProviderType());
            Assertions.assertEquals("target@test.iam.gserviceaccount.com", cloud.getImpersonationServiceAccount());
            Assertions.assertEquals(cloud.getImpersonationServiceAccount(), thrift.getImpersonationServiceAccount());
            Assertions.assertEquals(Map.of("gs.credential_provider_type", source,
                    "gs.impersonation_service_account", "target@test.iam.gserviceaccount.com"),
                    codec.toProperties(cloudBytes));
        }
    }

    @Test
    void partialAlterPreservesExplicitClearWithoutAddingProviderType() throws Exception {
        ObjectStorageAuthentication auth = provider.resolveAuthentication(Map.of(
                "provider", "GCP", "gs.impersonation_service_account", "")).orElseThrow();
        GcpCredentialPB cloud = ObjectStoreCredentialPB.parseFrom(
                codec.toCloudCredential(auth.getCredentialUpdates())).getGcpCredential();
        TGcpCredential thrift = decodeThrift(auth.getCredentialUpdates());
        Assertions.assertFalse(cloud.hasCredentialProviderType());
        Assertions.assertFalse(thrift.isSetCredentialProviderType());
        Assertions.assertTrue(cloud.hasImpersonationServiceAccount());
        Assertions.assertTrue(thrift.isSetImpersonationServiceAccount());
        Assertions.assertEquals("", cloud.getImpersonationServiceAccount());
        Assertions.assertEquals("", thrift.getImpersonationServiceAccount());
    }

    @Test
    void omittedImpersonationStaysAbsent() throws Exception {
        Map<String, String> fields = Map.of("credential_provider_type", "DEFAULT");
        byte[] cloudBytes = codec.toCloudCredential(fields);
        Assertions.assertFalse(ObjectStoreCredentialPB.parseFrom(cloudBytes)
                .getGcpCredential().hasImpersonationServiceAccount());
        Assertions.assertFalse(decodeThrift(fields).isSetImpersonationServiceAccount());
        Assertions.assertEquals(Map.of("gs.credential_provider_type", "DEFAULT"), codec.toProperties(cloudBytes));
    }

    @Test
    void invalidEnvelopeFailsInsteadOfSelectingDefaultCredentials() {
        Assertions.assertThrows(IOException.class,
                () -> codec.toProperties(ObjectStoreCredentialPB.getDefaultInstance().toByteArray()));
        Assertions.assertThrows(IOException.class, () -> codec.toProperties(new byte[] {(byte) 0xff}));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> codec.toCloudCredential(Map.of("credential_provider_type", "UNKNOWN")));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> codec.toThriftCredential(Map.of("credential_provider_type", "UNKNOWN")));
    }

    @Test
    void hmacAndAnonymousAuthenticationDoNotProduceNativeCredentials() {
        Assertions.assertFalse(provider.resolveAuthentication(Map.of("provider", "GCP",
                "s3.access_key", "ak", "s3.secret_key", "sk")).orElseThrow().isNative());
        Assertions.assertFalse(provider.resolveAuthentication(Map.of("provider", "GCP",
                "gs.credential_provider_type", "ANONYMOUS")).orElseThrow().isNative());
    }

    private TGcpCredential decodeThrift(Map<String, String> fields) throws Exception {
        TCredential envelope = new TCredential();
        new TDeserializer(new TBinaryProtocol.Factory()).deserialize(envelope, codec.toThriftCredential(fields));
        Assertions.assertTrue(envelope.isSetGcpCredential());
        return envelope.getGcpCredential();
    }
}
