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

package org.apache.doris.datasource.property.common;

import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain;
import software.amazon.awssdk.auth.credentials.ContainerCredentialsProvider;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider;
import software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider;
import software.amazon.awssdk.utils.IoUtils;
import software.amazon.awssdk.utils.SdkAutoCloseable;

/**
 * DefaultDorisAwsCredentialsProviderChain
 *
 * <p>Default AWS credentials provider chain used across Doris (singleton).</p>
 *
 * <ul>
 *   <li>Attempts credential providers in the following order:
 *       WebIdentityTokenFile -> Container -> InstanceProfile -> SystemProperty -> Environment -> Profile.</li>
 *   <li>Implements {@code AwsCredentialsProvider} and delegates credential resolution to an internal
 *       {@code AwsCredentialsProviderChain} instance.</li>
 *   <li>Implements {@code SdkAutoCloseable}; call {@code close()} to release underlying resources when
 *       Doris shuts down or the provider is no longer needed.</li>
 *   <li>Prefer using the shared instance via {@code DefaultDorisAwsCredentialsProviderChain.getInstance()}
 *       to ensure consistent credential lookup behavior across the project. If a different lifecycle is
 *       required, a new instance can be created with {@code create()}.</li>
 * </ul>
 * <p>
 * Usage example:
 * {@code AwsCredentials creds = DefaultDorisAwsCredentialsProviderChain.create();}
 */
public final class DefaultDorisAwsCredentialsProviderChain implements AwsCredentialsProvider, SdkAutoCloseable {
    private static final DefaultDorisAwsCredentialsProviderChain INSTANCE =
            new DefaultDorisAwsCredentialsProviderChain();

    private final AwsCredentialsProvider delegate;

    private DefaultDorisAwsCredentialsProviderChain() {
        this.delegate = AwsCredentialsProviderChain.of(
                WebIdentityTokenFileCredentialsProvider.create(),
                ContainerCredentialsProvider.create(),
                InstanceProfileCredentialsProvider.create(),
                SystemPropertyCredentialsProvider.create(),
                EnvironmentVariableCredentialsProvider.create()
        );
    }

    public static DefaultDorisAwsCredentialsProviderChain create() {
        return new DefaultDorisAwsCredentialsProviderChain();
    }

    public static DefaultDorisAwsCredentialsProviderChain getInstance() {
        return INSTANCE;
    }

    @Override
    public AwsCredentials resolveCredentials() {
        return delegate.resolveCredentials();
    }

    @Override
    public void close() {
        IoUtils.closeIfCloseable(delegate, null);
    }

    @Override
    public String toString() {
        return "DefaultDorisAwsCredentialsProviderChain{delegate=" + delegate + "}";
    }
}
