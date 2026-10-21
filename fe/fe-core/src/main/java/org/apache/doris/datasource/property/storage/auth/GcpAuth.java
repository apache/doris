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

package org.apache.doris.datasource.property.storage.auth;

import com.google.auth.oauth2.ComputeEngineCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ImpersonatedCredentials;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.http.SdkHttpRequest;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/** Creates GCP OAuth2 credentials and adapts them to the S3-compatible client. */
public final class GcpAuth {
    public static final String DEVSTORAGE_SCOPE =
            "https://www.googleapis.com/auth/devstorage.read_write";
    private static final String CLOUD_PLATFORM_SCOPE = "https://www.googleapis.com/auth/cloud-platform";
    private static final int IMPERSONATION_LIFETIME_SECONDS = 3600;

    private GcpAuth() {
    }

    public static GoogleCredentials createCredentials(GcpCredential credential) throws IOException {
        GoogleCredentials source;
        switch (credential.getCredentialProviderType()) {
            case DEFAULT:
                source = GoogleCredentials.getApplicationDefault();
                break;
            case COMPUTE_ENGINE:
                source = ComputeEngineCredentials.create();
                break;
            default:
                throw new IllegalArgumentException("Unsupported GCP credential provider: "
                        + credential.getCredentialProviderType());
        }
        if (credential.getImpersonationServiceAccount().isEmpty()) {
            return source.createScoped(Collections.singletonList(DEVSTORAGE_SCOPE));
        }
        // The source token calls the IAM Credentials API; the impersonated
        // target token itself only needs object read/write access.
        source = source.createScoped(Collections.singletonList(CLOUD_PLATFORM_SCOPE));
        return ImpersonatedCredentials.create(source, credential.getImpersonationServiceAccount(), null,
                Collections.singletonList(DEVSTORAGE_SCOPE), IMPERSONATION_LIFETIME_SECONDS);
    }

    public static ExecutionInterceptor bearerTokenInterceptor(GoogleCredentials credentials) {
        return new ExecutionInterceptor() {
            @Override
            public SdkHttpRequest modifyHttpRequest(Context.ModifyHttpRequest context,
                    ExecutionAttributes executionAttributes) {
                try {
                    Map<String, List<String>> metadata = credentials.getRequestMetadata();
                    List<String> authorization = metadata.get("Authorization");
                    if (authorization == null || authorization.isEmpty()
                            || authorization.get(0) == null || authorization.get(0).isEmpty()) {
                        throw new IOException("GCP credentials did not provide an Authorization header");
                    }
                    return context.httpRequest().toBuilder()
                            .putHeader("Authorization", authorization.get(0))
                            .build();
                } catch (IOException e) {
                    throw new UncheckedIOException("Failed to obtain a GCP access token", e);
                }
            }
        };
    }
}
