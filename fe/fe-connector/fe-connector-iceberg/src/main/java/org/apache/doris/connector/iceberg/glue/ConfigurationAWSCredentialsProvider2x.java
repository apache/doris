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

package org.apache.doris.connector.iceberg.glue;

import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

import java.util.Map;

/**
 * Credentials provider for the glue flavor's static AK/SK, named by the {@code client.credentials-provider}
 * property {@link org.apache.doris.connector.iceberg.IcebergCatalogFactory} emits.
 *
 * <p>MUST live in this plugin module: iceberg's {@code AwsClientProperties} resolves the property's class name
 * through the plugin's child-first loader and gates it on {@code AwsCredentialsProvider.isAssignableFrom},
 * against the plugin's own copy of that interface. A copy loaded from any other loader implements a different
 * {@code AwsCredentialsProvider} and is rejected with "it does not implement ...".
 */
public class ConfigurationAWSCredentialsProvider2x implements AwsCredentialsProvider {

    private AwsCredentials credentials;

    private ConfigurationAWSCredentialsProvider2x(AwsCredentials credentials) {
        this.credentials = credentials;
    }

    @Override
    public AwsCredentials resolveCredentials() {
        return credentials;
    }

    /**
     * Keys here are the emitted {@code client.credentials-provider.glue.*} properties minus their prefix:
     * iceberg's {@code AwsClientProperties} strips it before reflecting into this method.
     */
    public static AwsCredentialsProvider create(Map<String, String> config) {
        String ak = config.get("glue.access_key");
        String sk = config.get("glue.secret_key");
        String sessionToken = config.get("glue.session_token");
        // Blank-check rather than null-check: AwsSessionCredentials.create accepts a blank token and only
        // fails later at AWS. The emitting side guards with putIfNotBlank, so keep the two halves symmetric.
        if (StringUtils.isBlank(sessionToken)) {
            return new ConfigurationAWSCredentialsProvider2x(AwsBasicCredentials.create(ak, sk));
        }
        return new ConfigurationAWSCredentialsProvider2x(AwsSessionCredentials.create(ak, sk, sessionToken));
    }
}
