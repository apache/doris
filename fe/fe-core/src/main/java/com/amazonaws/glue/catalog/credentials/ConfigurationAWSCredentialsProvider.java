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

package com.amazonaws.glue.catalog.credentials;

import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.glue.catalog.util.AWSGlueConfig;
import com.amazonaws.util.StringUtils;
import org.apache.doris.common.Config;
import org.apache.doris.datasource.property.common.AwsCredentialsProviderFactory;
import org.apache.doris.datasource.property.common.AwsCredentialsProviderMode;
import org.apache.hadoop.conf.Configuration;

public class ConfigurationAWSCredentialsProvider implements AWSCredentialsProvider {

    private final Configuration conf;

    // The SDK signer invokes getCredentials() on every request, so the underlying provider must
    // be built only once: providers like InstanceProfileCredentialsProvider and
    // STSAssumeRoleSessionCredentialsProvider cache their temporary credentials per instance,
    // and rebuilding them per call would hit IMDS/STS on every single Glue request.
    private volatile AWSCredentialsProvider delegate;

    public ConfigurationAWSCredentialsProvider(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public AWSCredentials getCredentials() {
        AWSCredentialsProvider provider = delegate;
        if (provider == null) {
            synchronized (this) {
                if (delegate == null) {
                    delegate = buildDelegate();
                }
                provider = delegate;
            }
        }
        return provider.getCredentials();
    }

    private AWSCredentialsProvider buildDelegate() {
        String accessKey = StringUtils.trim(conf.get(AWSGlueConfig.AWS_GLUE_ACCESS_KEY));
        String secretKey = StringUtils.trim(conf.get(AWSGlueConfig.AWS_GLUE_SECRET_KEY));
        String sessionToken = StringUtils.trim(conf.get(AWSGlueConfig.AWS_GLUE_SESSION_TOKEN));
        String roleArn = StringUtils.trim(conf.get(AWSGlueConfig.AWS_GLUE_ROLE_ARN));
        String externalId = StringUtils.trim(conf.get(AWSGlueConfig.AWS_GLUE_EXTERNAL_ID));
        if (!StringUtils.isNullOrEmpty(accessKey) && !StringUtils.isNullOrEmpty(secretKey)) {
            AWSCredentials credentials = StringUtils.isNullOrEmpty(sessionToken)
                    ? new BasicAWSCredentials(accessKey, secretKey)
                    : new BasicSessionCredentials(accessKey, secretKey, sessionToken);
            return new AWSStaticCredentialsProvider(credentials);
        }
        String credentialsProviderModeString =
                StringUtils.lowerCase(conf.get(AWSGlueConfig.AWS_CREDENTIALS_PROVIDER_MODE));
        AwsCredentialsProviderMode credentialsProviderMode =
                AwsCredentialsProviderMode.fromString(credentialsProviderModeString);
        AWSCredentialsProvider longLivedProvider = AwsCredentialsProviderFactory.createV1(credentialsProviderMode);
        if (!StringUtils.isNullOrEmpty(roleArn)) {
            STSAssumeRoleSessionCredentialsProvider.Builder builder =
                    new STSAssumeRoleSessionCredentialsProvider.Builder(roleArn, "local-session")
                            .withLongLivedCredentialsProvider(longLivedProvider);

            if (!StringUtils.isNullOrEmpty(externalId)) {
                builder.withExternalId(externalId);
            }
            return builder.build();
        }
        if (Config.aws_credentials_provider_version.equalsIgnoreCase("v2")) {
            return longLivedProvider;
        }
        throw new SdkClientException("Unable to load AWS credentials from any provider in the chain");
    }

    @Override
    public void refresh() {
        AWSCredentialsProvider provider = delegate;
        if (provider != null) {
            provider.refresh();
        }
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName();
    }
}
