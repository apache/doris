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

import org.apache.doris.filesystem.gcs.auth.GcpCredential;

import com.google.cloud.hadoop.util.AccessTokenProvider;
import org.apache.hadoop.conf.Configured;

import java.io.IOException;
import java.time.Instant;

/** Adapts catalog-supplied tokens for Hadoop GCS access without consulting ADC. */
public class GcsVendedAccessTokenProvider extends Configured implements AccessTokenProvider {
    @Override
    public AccessToken getAccessToken() {
        long expiresAt = getConf().getLong(GcpCredential.OAUTH2_TOKEN_EXPIRES_AT, 0);
        return new AccessToken(getConf().get(GcpCredential.OAUTH2_TOKEN),
                Instant.ofEpochMilli(expiresAt == 0 ? Long.MAX_VALUE : expiresAt));
    }

    @Override
    public void refresh() throws IOException {
        // Only the catalog can renew a vended token; never substitute a machine credential.
        if (!getAccessToken().getExpirationTime().isAfter(Instant.now())) {
            throw new IOException("GCS access token has expired; reload catalog credentials");
        }
    }
}
