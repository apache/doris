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

package org.apache.doris.regression.util

import com.amazonaws.auth.AWSCredentials
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.auth.BasicSessionCredentials

/** One credential snapshot for both SDK requests and explicitly constructed SQL properties. */
class S3TestCredentials implements AWSCredentialsProvider {
    private final Map config

    S3TestCredentials(Map config) {
        this.config = config
    }

    private Map<String, String> snapshot() {
        synchronized (config) {
            def values = [ak: config.get("ak")?.toString(), sk: config.get("sk")?.toString(),
                          sessionToken: config.get("sessionToken")?.toString()]
            if (values.ak == null || values.sk == null) {
                throw new IllegalArgumentException("S3 tests require both ak and sk")
            }
            SecretRedactor.register(values.values())
            return values
        }
    }

    @Override
    AWSCredentials getCredentials() {
        def values = snapshot()
        return values.sessionToken
                ? new BasicSessionCredentials(values.ak, values.sk, values.sessionToken)
                : new BasicAWSCredentials(values.ak, values.sk)
    }

    @Override
    void refresh() {
        // No credential is cached here. Each request reads the current configuration atomically.
        // Credential acquisition/renewal remains the responsibility of the test environment.
    }

    Map<String, String> properties(String namespace = "s3") {
        def values = snapshot()
        Map<String, String> names
        switch (namespace) {
            case "s3":
                names = [ak: "s3.access_key", sk: "s3.secret_key", sessionToken: "s3.session_token"]
                break
            case "AWS":
                names = [ak: "AWS_ACCESS_KEY", sk: "AWS_SECRET_KEY", sessionToken: "AWS_TOKEN"]
                break
            case "tvf":
                names = [ak: "ACCESS_KEY", sk: "SECRET_KEY", sessionToken: "session_token"]
                break
            default:
                throw new IllegalArgumentException("Unsupported S3 credential property namespace")
        }
        Map<String, String> properties = [(names.ak): values.ak, (names.sk): values.sk]
        if (values.sessionToken) {
            properties[names.sessionToken] = values.sessionToken
        }
        return properties
    }

    String sqlProperties(String namespace = "s3") {
        return properties(namespace).collect { key, value ->
            '"' + key + '" = "' + escapeSql(value) + '"'
        }.join(",\n")
    }

    private static String escapeSql(String value) {
        return value.replace("\\", "\\\\").replace('"', '\\"')
                .replace("\n", "\\n").replace("\r", "\\r")
    }

    @Override
    String toString() {
        return "S3TestCredentials([REDACTED])"
    }
}
