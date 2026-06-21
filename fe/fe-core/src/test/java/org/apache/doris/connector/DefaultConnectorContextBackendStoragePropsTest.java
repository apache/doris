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

package org.apache.doris.connector;

import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.kerberos.ExecutionAuthenticator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * FIX-STATIC-CREDS-BE (B-9) fe-core bridge test: pins that
 * {@link DefaultConnectorContext#getBackendStorageProperties} translates the catalog's parsed
 * {@code StorageProperties} map into the BE-canonical {@code AWS_*} keys (the same
 * {@code CredentialUtils.getBackendPropertiesFromStorageMap} legacy {@code PaimonScanNode} returns
 * from {@code getLocationProperties()}). The paimon connector cannot import that machinery, so this
 * hook is its only access; without it the connector ships raw {@code s3.access_key}/{@code oss.*}
 * aliases to BE, whose native (FILE_S3) reader understands only {@code AWS_*} -> no usable
 * credentials -> 403 on a private bucket. FAILS before the fix (the method is a no-op default
 * returning empty).
 */
public class DefaultConnectorContextBackendStoragePropsTest {

    private static final Supplier<ExecutionAuthenticator> NOOP_AUTH =
            () -> new ExecutionAuthenticator() {};

    /** A context whose storage-props supplier yields a real OSS storage-properties map, built with
     *  the same {@code StorageProperties.createAll} machinery a real OSS catalog uses. */
    private static DefaultConnectorContext ossContext() throws Exception {
        Map<String, String> oss = new HashMap<>();
        oss.put("oss.endpoint", "oss-cn-beijing.aliyuncs.com");
        oss.put("oss.access_key", "ak");
        oss.put("oss.secret_key", "sk");
        List<StorageProperties> all = StorageProperties.createAll(oss);
        Map<StorageProperties.Type, StorageProperties> map = all.stream()
                .collect(Collectors.toMap(StorageProperties::getType, Function.identity(), (a, b) -> a));
        return new DefaultConnectorContext("c", 1L, NOOP_AUTH, () -> map);
    }

    @Test
    public void normalizesStaticOssCredsToBackendAwsProps() throws Exception {
        // WHY (BLOCKER B-9): the BE native S3/object-store reader consumes ONLY canonical AWS_* keys;
        // the raw oss.access_key/oss.secret_key catalog aliases are unintelligible to it. The bridge
        // must run getBackendPropertiesFromStorageMap to produce them. MUTATION: returning the no-op
        // default (empty), or echoing the raw oss.* keys -> AWS_ACCESS_KEY absent -> red.
        Map<String, String> be = ossContext().getBackendStorageProperties();

        Assertions.assertEquals("ak", be.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("sk", be.get("AWS_SECRET_KEY"));
        Assertions.assertNotNull(be.get("AWS_ENDPOINT"), "endpoint must be emitted as canonical AWS_ENDPOINT");
        Assertions.assertFalse(be.containsKey("oss.access_key"),
                "the raw catalog alias must NOT survive to BE (that is the B-9 bug)");
    }

    @Test
    public void noStorageMapYieldsEmpty() {
        // WHY: a context with no storage map (non-plugin ctor, or a credential-less local-FS warehouse)
        // must short-circuit to empty -> no overlay, parity with legacy
        // getBackendPropertiesFromStorageMap({}). MUTATION: NPE, or fabricating props from nothing -> red.
        Assertions.assertTrue(new DefaultConnectorContext("c", 1L).getBackendStorageProperties().isEmpty());
    }
}
