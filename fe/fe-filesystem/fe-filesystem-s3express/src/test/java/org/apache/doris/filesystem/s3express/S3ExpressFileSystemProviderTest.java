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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class S3ExpressFileSystemProviderTest {

    private final S3ExpressFileSystemProvider provider = new S3ExpressFileSystemProvider();

    @Test
    void supports_routesS3ExpressEndpoint() {
        Map<String, String> properties = expressProperties();
        Map<String, String> explicit = new HashMap<>(properties);
        explicit.put("fs.s3.support", "true");

        Assertions.assertTrue(provider.supports(properties));
        Assertions.assertTrue(provider.supportsGuess(properties));
        Assertions.assertTrue(provider.supportsExplicit(explicit));
        Assertions.assertTrue(provider.supports(Map.of(
                "s3.endpoint", "https://s3express-control-dualstack.us-west-2.amazonaws.com",
                "s3.region", "us-west-2")));
        Assertions.assertFalse(provider.supports(Map.of(
                "s3.endpoint", "https://s3.us-west-2.amazonaws.com",
                "s3.region", "us-west-2")));
        Assertions.assertFalse(provider.supports(Map.of(
                "s3.endpoint", "https://my-s3express-archive.s3.us-west-2.amazonaws.com",
                "s3.region", "us-west-2")));
        Assertions.assertFalse(provider.supports(Map.of(
                "s3.endpoint", "https://s3express.internal.example",
                "s3.region", "us-west-2")));
    }

    @Test
    void bind_tagsProviderAndRejectsUnsupportedModes() {
        S3ExpressFileSystemProperties bound = provider.bind(expressProperties());

        Assertions.assertEquals("S3EXPRESS", bound.providerName());
        Assertions.assertEquals("S3EXPRESS", bound.toFileSystemKv().get("provider"));

        Map<String, String> pathStyle = new HashMap<>(expressProperties());
        pathStyle.put("use_path_style", "true");
        Assertions.assertThrows(IllegalArgumentException.class, () -> provider.bind(pathStyle));

        Map<String, String> anonymous = new HashMap<>(expressProperties());
        anonymous.put("s3.credentials_provider_type", "ANONYMOUS");
        Assertions.assertThrows(IllegalArgumentException.class, () -> provider.bind(anonymous));
    }

    private static Map<String, String> expressProperties() {
        return Map.of(
                "s3.endpoint", "https://s3express-usw2-az1.us-west-2.amazonaws.com",
                "s3.region", "us-west-2");
    }
}
