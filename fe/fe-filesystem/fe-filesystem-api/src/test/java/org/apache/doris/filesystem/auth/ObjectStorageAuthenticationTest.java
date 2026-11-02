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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ObjectStorageAuthenticationTest {
    @Test
    void absentAndClearedFieldsAreDifferentValues() {
        GcpCredentialSpec absent = new GcpCredentialSpec(null, null);
        GcpCredentialSpec cleared = new GcpCredentialSpec(null, "");
        Assertions.assertTrue(absent.isEmpty());
        Assertions.assertFalse(cleared.isEmpty());
        Assertions.assertNotEquals(absent, cleared);
        Assertions.assertTrue(absent.getImpersonationServiceAccount().isEmpty());
        Assertions.assertEquals("", cleared.getImpersonationServiceAccount().orElseThrow());
        Assertions.assertEquals(cleared, new GcpCredentialSpec(null, ""));
        Assertions.assertEquals(cleared.hashCode(), new GcpCredentialSpec(null, "").hashCode());
    }

    @Test
    void resolvedIdentityAndPartialUpdateHaveSeparatePresence() {
        GcpCredentialSpec resolved = new GcpCredentialSpec(GcpCredentialSpec.ProviderType.DEFAULT, null);
        ObjectStorageAuthentication initial = ObjectStorageAuthentication.nativeAuthentication(
                resolved, new GcpCredentialSpec(null, null));
        Assertions.assertTrue(initial.isNative());
        Assertions.assertFalse(initial.isAnonymous());
        Assertions.assertEquals("GCP", initial.getProvider());
        Assertions.assertEquals(resolved, initial.getCredential().orElseThrow());
        Assertions.assertTrue(initial.getCredentialUpdates().isEmpty());
        ObjectStorageAuthentication alter = ObjectStorageAuthentication.nativeAuthentication(
                resolved, new GcpCredentialSpec(null, ""));
        Assertions.assertEquals(new GcpCredentialSpec(null, ""), alter.getCredentialUpdates().orElseThrow());
    }

    @Test
    void invalidNativeAuthenticationCannotBeConstructed() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> ObjectStorageAuthentication.nativeAuthentication(
                new GcpCredentialSpec(null, null), new GcpCredentialSpec(null, null)));
        NativeObjectStorageCredential foreign = new NativeObjectStorageCredential() {
            @Override
            public String getProvider() {
                return "OTHER";
            }

            @Override
            public boolean isEmpty() {
                return false;
            }
        };
        Assertions.assertThrows(IllegalArgumentException.class, () -> ObjectStorageAuthentication.nativeAuthentication(
                new GcpCredentialSpec(GcpCredentialSpec.ProviderType.DEFAULT, null), foreign));
    }

    @Test
    void nonNativeModesCannotContainNativeFields() {
        ObjectStorageAuthentication hmac = ObjectStorageAuthentication.hmac("GCP");
        ObjectStorageAuthentication anonymous = ObjectStorageAuthentication.anonymous("GCP");
        Assertions.assertFalse(hmac.isAnonymous());
        Assertions.assertTrue(anonymous.isAnonymous());
        for (ObjectStorageAuthentication auth : new ObjectStorageAuthentication[] {hmac, anonymous}) {
            Assertions.assertFalse(auth.isNative());
            Assertions.assertTrue(auth.getCredential().isEmpty());
            Assertions.assertTrue(auth.getCredentialUpdates().isEmpty());
        }
    }
}
