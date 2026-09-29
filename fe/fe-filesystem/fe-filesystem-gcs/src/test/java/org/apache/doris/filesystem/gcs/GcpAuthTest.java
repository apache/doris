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

import org.apache.doris.filesystem.auth.GcpCredential;
import org.apache.doris.filesystem.auth.GcpCredentialProviderType;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.ComputeEngineCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ImpersonatedCredentials;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;

import java.net.URI;
import java.util.Collections;
import java.util.Date;

public class GcpAuthTest {
    @Test
    public void testComputeEngineCredentialsUseMetadataProviderDirectly() throws Exception {
        GoogleCredentials credentials = GcpAuth.createCredentials(
                new GcpCredential(GcpCredentialProviderType.COMPUTE_ENGINE, ""));

        Assertions.assertInstanceOf(ComputeEngineCredentials.class, credentials);
    }

    @Test
    public void testComputeEngineImpersonationWrapsMetadataCredentials() throws Exception {
        GoogleCredentials credentials = GcpAuth.createCredentials(
                new GcpCredential(GcpCredentialProviderType.COMPUTE_ENGINE,
                        "target@my-project.iam.gserviceaccount.com"));

        Assertions.assertInstanceOf(ImpersonatedCredentials.class, credentials);
    }

    @Test
    public void testBearerTokenInterceptorUsesCredentialRequestMetadata() {
        GoogleCredentials credentials = GoogleCredentials.create(
                new AccessToken("metadata-token", new Date(System.currentTimeMillis() + 3_600_000)));
        ExecutionInterceptor interceptor = GcpAuth.bearerTokenInterceptor(credentials);
        Context.ModifyHttpRequest context = Mockito.mock(Context.ModifyHttpRequest.class);
        SdkHttpRequest request = SdkHttpRequest.builder()
                .uri(URI.create("https://storage.googleapis.com/bucket/key"))
                .method(SdkHttpMethod.GET)
                .build();
        Mockito.when(context.httpRequest()).thenReturn(request);

        SdkHttpRequest modified = interceptor.modifyHttpRequest(context, new ExecutionAttributes());

        Assertions.assertEquals("Bearer metadata-token",
                modified.firstMatchingHeader("Authorization").orElse(null));
    }

    @Test
    public void testBearerTokenInterceptorRejectsMissingAuthorizationMetadata() throws Exception {
        GoogleCredentials credentials = Mockito.mock(GoogleCredentials.class);
        Mockito.when(credentials.getRequestMetadata()).thenReturn(Collections.emptyMap());
        ExecutionInterceptor interceptor = GcpAuth.bearerTokenInterceptor(credentials);
        Context.ModifyHttpRequest context = Mockito.mock(Context.ModifyHttpRequest.class);
        SdkHttpRequest request = SdkHttpRequest.builder()
                .uri(URI.create("https://storage.googleapis.com/bucket/key"))
                .method(SdkHttpMethod.GET)
                .build();
        Mockito.when(context.httpRequest()).thenReturn(request);

        RuntimeException exception = Assertions.assertThrows(RuntimeException.class,
                () -> interceptor.modifyHttpRequest(context, new ExecutionAttributes()));
        Assertions.assertTrue(exception.getMessage().contains("Failed to obtain a GCP access token"));
    }
}
