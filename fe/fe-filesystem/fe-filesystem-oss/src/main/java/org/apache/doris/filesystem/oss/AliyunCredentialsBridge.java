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

package org.apache.doris.filesystem.oss;

import com.aliyun.oss.common.auth.Credentials;
import com.aliyun.oss.common.auth.CredentialsProvider;
import com.aliyun.oss.common.auth.DefaultCredentials;
import com.aliyuncs.auth.AlibabaCloudCredentials;
import com.aliyuncs.auth.AlibabaCloudCredentialsProvider;
import com.aliyuncs.auth.BasicSessionCredentials;

/**
 * Bridges {@link AlibabaCloudCredentialsProvider} (aliyuncs SDK) to
 * {@link CredentialsProvider} (OSS SDK) so SDK-managed providers such as
 * {@code OIDCCredentialsProvider} and {@code STSAssumeRoleSessionCredentialsProvider}
 * can be passed directly to {@code OSSClientBuilder.build()}.
 *
 * <p>The aliyuncs providers handle credential caching and refresh internally;
 * this class only bridges the credential types.
 */
class AliyunCredentialsBridge implements CredentialsProvider {

    private final AlibabaCloudCredentialsProvider delegate;

    AliyunCredentialsBridge(AlibabaCloudCredentialsProvider delegate) {
        this.delegate = delegate;
    }

    @Override
    public void setCredentials(Credentials credentials) {
    }

    @Override
    public Credentials getCredentials() {
        try {
            AlibabaCloudCredentials c = delegate.getCredentials();
            // Temporary credentials (STS/RRSA/ECS) return BasicSessionCredentials with a token
            if (c instanceof BasicSessionCredentials) {
                String token = ((BasicSessionCredentials) c).getSessionToken();
                return new DefaultCredentials(c.getAccessKeyId(), c.getAccessKeySecret(), token);
            }
            return new DefaultCredentials(c.getAccessKeyId(), c.getAccessKeySecret());
        } catch (Exception e) {
            throw new IllegalStateException("Failed to obtain credentials: " + e.getMessage(), e);
        }
    }
}
