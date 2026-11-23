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
import org.apache.doris.filesystem.auth.NativeObjectStorageCredential;
import org.apache.doris.thrift.TGcpCredential;
import org.apache.doris.thrift.TGcpCredentialProviderType;

import com.google.protobuf.UnknownFieldSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ObjectStorageCredentialTranslatorTest {
    @Test
    void bothWireFormatsPreserveAllSupportedIdentitySources() {
        for (GcpCredentialSpec.ProviderType type : GcpCredentialSpec.ProviderType.values()) {
            GcpCredentialSpec spec = new GcpCredentialSpec(type, "target@test.iam.gserviceaccount.com");
            ObjectStoreCredentialPB cloud = ObjectStorageCredentialTranslator.toCloud(spec);
            TGcpCredential thrift = ObjectStorageCredentialTranslator.toThrift(spec).getGcpCredential();
            Assertions.assertEquals(GcpCredentialPB.CredentialProviderType.valueOf(type.name()),
                    cloud.getGcpCredential().getCredentialProviderType());
            Assertions.assertEquals(TGcpCredentialProviderType.valueOf(type.name()), thrift.getCredentialProviderType());
            Assertions.assertEquals("target@test.iam.gserviceaccount.com", thrift.getImpersonationServiceAccount());
            Assertions.assertEquals(thrift.getImpersonationServiceAccount(),
                    cloud.getGcpCredential().getImpersonationServiceAccount());
            Assertions.assertEquals(spec, ObjectStorageCredentialTranslator.fromCloud("gcp", cloud));
        }
    }

    @Test
    void partialAlterPreservesExplicitClearWithoutAddingSource() {
        GcpCredentialSpec patch = new GcpCredentialSpec(null, "");
        ObjectStoreCredentialPB cloud = ObjectStorageCredentialTranslator.toCloud(patch);
        TGcpCredential thrift = ObjectStorageCredentialTranslator.toThrift(patch).getGcpCredential();
        Assertions.assertFalse(cloud.getGcpCredential().hasCredentialProviderType());
        Assertions.assertFalse(thrift.isSetCredentialProviderType());
        Assertions.assertTrue(cloud.getGcpCredential().hasImpersonationServiceAccount());
        Assertions.assertTrue(thrift.isSetImpersonationServiceAccount());
        Assertions.assertEquals("", thrift.getImpersonationServiceAccount());
        Assertions.assertEquals("", cloud.getGcpCredential().getImpersonationServiceAccount());
        Assertions.assertEquals(patch, ObjectStorageCredentialTranslator.fromCloud("GCP", cloud));
    }

    @Test
    void absentFieldsStayAbsentInBothDirections() {
        for (GcpCredentialSpec spec : new GcpCredentialSpec[] {
                new GcpCredentialSpec(GcpCredentialSpec.ProviderType.DEFAULT, null),
                new GcpCredentialSpec(null, null)}) {
            ObjectStoreCredentialPB cloud = ObjectStorageCredentialTranslator.toCloud(spec);
            TGcpCredential thrift = ObjectStorageCredentialTranslator.toThrift(spec).getGcpCredential();
            Assertions.assertFalse(cloud.getGcpCredential().hasImpersonationServiceAccount());
            Assertions.assertFalse(thrift.isSetImpersonationServiceAccount());
            Assertions.assertEquals(spec, ObjectStorageCredentialTranslator.fromCloud("GCP", cloud));
            Assertions.assertEquals(spec.getProviderType().isPresent(), thrift.isSetCredentialProviderType());
        }
    }

    @Test
    void missingEnvelopeDoesNotSilentlySelectDefaultCredentials() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> ObjectStorageCredentialTranslator.fromCloud(
                "GCP", ObjectStoreCredentialPB.getDefaultInstance()));
    }

    @Test
    void unknownFutureSourceDoesNotTurnIntoApplicationDefaultCredentials() throws Exception {
        GcpCredentialPB unknown = GcpCredentialPB.newBuilder().setUnknownFields(UnknownFieldSet.newBuilder()
                .addField(GcpCredentialPB.CREDENTIAL_PROVIDER_TYPE_FIELD_NUMBER,
                        UnknownFieldSet.Field.newBuilder().addVarint(100).build()).build()).build();
        ObjectStoreCredentialPB envelope = ObjectStoreCredentialPB.parseFrom(
                ObjectStoreCredentialPB.newBuilder().setGcpCredential(unknown).build().toByteArray());
        Assertions.assertFalse(envelope.getGcpCredential().hasCredentialProviderType());
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ObjectStorageCredentialTranslator.fromCloud("GCP", envelope));
    }

    @Test
    void unsupportedProvidersAndMismatchedTypesFailExplicitly() {
        for (String provider : new String[] {"OTHER", "GCP"}) {
            NativeObjectStorageCredential credential = new NativeObjectStorageCredential() {
                @Override
                public String getProvider() {
                    return provider;
                }

                @Override
                public boolean isEmpty() {
                    return false;
                }
            };
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> ObjectStorageCredentialTranslator.toCloud(credential));
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> ObjectStorageCredentialTranslator.toThrift(credential));
        }
        Assertions.assertThrows(IllegalArgumentException.class, () -> ObjectStorageCredentialTranslator.fromCloud(
                "OTHER", ObjectStoreCredentialPB.getDefaultInstance()));
    }
}
