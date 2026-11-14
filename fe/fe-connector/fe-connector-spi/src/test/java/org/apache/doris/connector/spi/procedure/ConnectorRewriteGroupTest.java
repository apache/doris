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

package org.apache.doris.connector.spi.procedure;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class ConnectorRewriteGroupTest {

    @Test
    void existingConstructorPreservesTheTaskSession() {
        ConnectorRewriteGroup group = new ConnectorRewriteGroup(Collections.singleton("file"), 1, 1024L, 0);
        Assertions.assertTrue(group.getSessionVariableOverrides().isEmpty());
    }

    @Test
    void writeSettingsAreAnImmutableSnapshot() {
        Map<String, String> settings = new HashMap<>();
        settings.put("write_option", "128");
        ConnectorRewriteGroup group = new ConnectorRewriteGroup(
                Collections.singleton("file"), 1, 1024L, 0, settings);

        settings.put("write_option", "256");

        Assertions.assertEquals(Collections.singletonMap("write_option", "128"),
                group.getSessionVariableOverrides());
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> group.getSessionVariableOverrides().put("write_option", "512"));
    }
}
