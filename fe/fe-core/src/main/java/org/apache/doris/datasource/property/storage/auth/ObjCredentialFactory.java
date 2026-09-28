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

import org.apache.doris.filesystem.auth.GcsAuth;
import org.apache.doris.filesystem.auth.GcsAuthResolver;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Selects exactly one provider-native credential from object-storage properties. */
public final class ObjCredentialFactory {
    private interface Parser {
        Optional<ObjCredential> parse(Map<String, String> properties, Optional<GcsAuth> gcsAuth);
    }

    // Add one parser here when introducing OSS, Azure, or another native credential.
    // Provider-specific resolvers select credentials; credential classes handle transport conversion.
    private static final List<Parser> PARSERS = Arrays.asList((properties, gcsAuth) ->
            gcsAuth.flatMap(GcsAuth::getNativeCredential).map(GcpCredentialAdapter::new));

    // TODO: Add AWS role-based authentication after its existing property and
    // transport fields can be migrated without breaking compatibility. Static
    // AK/SK credentials remain outside this provider-native credential model.

    private ObjCredentialFactory() {
    }

    public static Optional<ObjCredential> fromProperties(Map<String, String> properties) {
        return fromProperties(properties, GcsAuthResolver.resolve(properties));
    }

    /** Reuse the authentication choice already resolved by a protocol builder. */
    public static Optional<ObjCredential> fromProperties(Map<String, String> properties, Optional<GcsAuth> gcsAuth) {
        ObjCredential selected = null;
        for (Parser parser : PARSERS) {
            Optional<ObjCredential> parsed = parser.parse(properties, gcsAuth);
            if (!parsed.isPresent()) {
                continue;
            }
            if (selected != null) {
                throw new IllegalArgumentException(
                        "Only one provider-native object storage credential may be configured.");
            }
            selected = parsed.get();
        }
        return Optional.ofNullable(selected);
    }
}
