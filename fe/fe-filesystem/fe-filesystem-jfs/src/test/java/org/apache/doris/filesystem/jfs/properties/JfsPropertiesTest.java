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

package org.apache.doris.filesystem.jfs.properties;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class JfsPropertiesTest {

    private Map<String, String> resolve(Map<String, String> raw) {
        JfsProperties props = new JfsProperties(raw);
        props.initNormalizeAndCheckProps();
        return props.getBackendConfigProperties();
    }

    @Test
    void jfsUriDerivesDefaultFs() {
        Map<String, String> raw = new HashMap<>();
        raw.put("uri", "jfs://myvol/path/to/file");

        Map<String, String> resolved = resolve(raw);

        Assertions.assertEquals("jfs://myvol", resolved.get("fs.defaultFS"));
    }

    @Test
    void juicefsKeysArePassedThrough() {
        Map<String, String> raw = new HashMap<>();
        raw.put("fs.defaultFS", "jfs://myvol");
        raw.put("juicefs.meta", "redis://localhost:6379/0");

        Map<String, String> resolved = resolve(raw);

        Assertions.assertEquals("redis://localhost:6379/0", resolved.get("juicefs.meta"));
    }
}
