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

package org.apache.doris.filesystem.s3express;

import org.apache.doris.filesystem.s3.S3CompatSignals;
import org.apache.doris.filesystem.s3.S3CredentialsProviderType;
import org.apache.doris.filesystem.s3.S3FileSystemProperties;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** Provider-owned properties for Amazon S3 Express One Zone. */
public final class S3ExpressFileSystemProperties extends S3FileSystemProperties {

    private S3ExpressFileSystemProperties(Map<String, String> rawProperties) {
        super(rawProperties);
    }

    public static S3ExpressFileSystemProperties of(Map<String, String> properties) {
        S3ExpressFileSystemProperties props = new S3ExpressFileSystemProperties(properties);
        props.validate();
        return props;
    }

    @Override
    public void validate() {
        super.validate();
        if (!S3CompatSignals.isS3Express(rawProperties())) {
            throw new IllegalArgumentException("Properties do not select S3 Express");
        }
        if (isUsePathStyle()) {
            throw new IllegalArgumentException(
                    "S3 Express requires virtual-hosted-style access");
        }
        if (getCredentialsProviderType() == S3CredentialsProviderType.ANONYMOUS) {
            throw new IllegalArgumentException("S3 Express does not support anonymous access");
        }
    }

    @Override
    public String providerName() {
        return S3CompatSignals.S3_EXPRESS_PROVIDER;
    }

    @Override
    public Map<String, String> toFileSystemKv() {
        Map<String, String> kv = new HashMap<>(super.toFileSystemKv());
        kv.put(S3CompatSignals.PROVIDER_KEY, S3CompatSignals.S3_EXPRESS_PROVIDER);
        return Collections.unmodifiableMap(kv);
    }
}
