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

import java.util.HashMap;
import java.util.Map;

public class GcsAuthResolverTest {
    @Test
    public void testDefaultAndExplicitModes() {
        Map<String, String> props = new HashMap<>();
        Assertions.assertFalse(GcsAuthResolver.resolve(props).isPresent());
        props.put("provider", "GCP");
        Assertions.assertEquals(GcsAuth.Mode.ADC, GcsAuthResolver.resolve(props).get().getMode());
        for (String provider : new String[] {"DEFAULT", "COMPUTE_ENGINE", "ANONYMOUS"}) {
            props.put(GcpCredential.CREDENTIAL_PROVIDER_TYPE, provider);
            GcsAuth auth = GcsAuthResolver.resolve(props).get();
            Assertions.assertEquals(provider.equals("DEFAULT") ? GcsAuth.Mode.ADC : GcsAuth.Mode.valueOf(provider),
                    auth.getMode());
            Assertions.assertEquals(!provider.equals("ANONYMOUS"), auth.getNativeCredential().isPresent());
        }
    }

    @Test
    public void testImpersonationAndFactoryUseResolvedCredentials() {
        Map<String, String> props = new HashMap<>();
        props.put(GcpCredential.IMPERSONATION_SERVICE_ACCOUNT, "target@test.iam.gserviceaccount.com");
        GcsAuth auth = GcsAuthResolver.resolve(props).get();
        Assertions.assertEquals(GcsAuth.Mode.ADC, auth.getMode());
        GcpCredential credential = auth.getNativeCredential().orElseThrow();
        Assertions.assertEquals(auth.getNativeCredential().get().getCredentialProviderType(),
                credential.getCredentialProviderType());
        Assertions.assertEquals("target@test.iam.gserviceaccount.com", credential.getImpersonationServiceAccount());
    }

    @Test
    public void testHmacAliasesDoNotAcquireAdc() {
        for (String[] keys : new String[][] {{"gs.access_key", "gs.secret_key"},
                {"s3.access_key", "s3.secret_key"}, {"AWS_ACCESS_KEY", "AWS_SECRET_KEY"},
                {"ACCESS_KEY", "SECRET_KEY"}}) {
            Map<String, String> props = new HashMap<>();
            props.put("provider", "GCP");
            props.put(keys[0], "ak");
            props.put(keys[1], "sk");
            Assertions.assertEquals(GcsAuth.Mode.HMAC, GcsAuthResolver.resolve(props).get().getMode());
            Assertions.assertFalse(GcsAuthResolver.resolve(props).get().getNativeCredential().isPresent());
            // Vault ALTER may supply just one HMAC field; it must not inject native credentials.
            props.remove(keys[1]);
            Assertions.assertEquals(GcsAuth.Mode.HMAC, GcsAuthResolver.resolve(props).get().getMode());
        }
    }

    @Test
    public void testLegacyS3HmacRequiresGcsSelector() {
        Map<String, String> props = new HashMap<>();
        props.put("provider", "S3");
        props.put("s3.access_key", "ak");
        props.put("s3.secret_key", "sk");
        Assertions.assertFalse(GcsAuthResolver.resolve(props).isPresent());
        props.put("s3.endpoint", "storage.googleapis.com");
        Assertions.assertEquals(GcsAuth.Mode.HMAC, GcsAuthResolver.resolve(props).get().getMode());
        for (String provider : new String[] {"AZURE", "OSS"}) {
            props.put("provider", provider);
            Assertions.assertFalse(GcsAuthResolver.resolve(props).isPresent());
        }
        props.put("provider", "S3");
        props.put(GcpCredential.CREDENTIAL_PROVIDER_TYPE, "DEFAULT");
        Assertions.assertThrows(IllegalArgumentException.class, () -> GcsAuthResolver.resolve(props));
    }

    @Test
    public void testExplicitS3OmittedAuthenticationPreservesDefaultChain() {
        Map<String, String> props = new HashMap<>();
        props.put("provider", "s3");
        Assertions.assertFalse(GcsAuthResolver.resolve(props).isPresent());
        props.put("s3.endpoint", "https://storage.googleapis.com");
        Assertions.assertFalse(GcsAuthResolver.resolve(props).isPresent());

        props.put("s3.credentials_provider_type", "DEFAULT");
        Assertions.assertFalse(GcsAuthResolver.resolve(props).isPresent());
        props.put("s3.credentials_provider_type", "ANONYMOUS");
        Assertions.assertEquals(GcsAuth.Mode.ANONYMOUS, GcsAuthResolver.resolve(props).get().getMode());
        props.remove("s3.credentials_provider_type");
        props.put("provider", "GCP");
        Assertions.assertEquals(GcsAuth.Mode.ADC, GcsAuthResolver.resolve(props).get().getMode());
        props.remove("provider");
        Assertions.assertEquals(GcsAuth.Mode.ADC, GcsAuthResolver.resolve(props).get().getMode());
    }

    @Test
    public void testOtherProvidersAndConflictingNativeProperties() {
        for (String provider : new String[] {"S3", "AZURE", "OSS"}) {
            Map<String, String> props = new HashMap<>();
            props.put("provider", provider);
            props.put("s3.endpoint", "storage.googleapis.com");
            Assertions.assertFalse(GcsAuthResolver.resolve(props).isPresent());
            props.put(GcpCredential.CREDENTIAL_PROVIDER_TYPE, "DEFAULT");
            Assertions.assertThrows(IllegalArgumentException.class, () -> GcsAuthResolver.resolve(props));
        }
        Map<String, String> props = new HashMap<>();
        props.put(GcpCredential.CREDENTIAL_PROVIDER_TYPE, "");
        Assertions.assertThrows(IllegalArgumentException.class, () -> GcsAuthResolver.resolve(props));
    }
}
