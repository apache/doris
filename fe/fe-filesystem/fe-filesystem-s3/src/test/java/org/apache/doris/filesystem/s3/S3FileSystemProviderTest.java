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

package org.apache.doris.filesystem.s3;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class S3FileSystemProviderTest {

    private final S3FileSystemProvider provider = new S3FileSystemProvider();

    @Test
    void supports_acceptsRoleBasedS3Configuration() {
        Map<String, String> props = new HashMap<>();
        props.put("AWS_ENDPOINT", "https://s3.us-west-2.amazonaws.com");
        props.put("AWS_REGION", "us-west-2");
        props.put("AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/snapshot-role");

        Assertions.assertTrue(provider.supports(props));
    }

    @Test
    void supports_rejectsConfigurationWithoutCredentialsOrRole() {
        Map<String, String> props = new HashMap<>();
        props.put("AWS_ENDPOINT", "https://s3.us-west-2.amazonaws.com");
        props.put("AWS_REGION", "us-west-2");

        Assertions.assertFalse(provider.supports(props));
    }
}
