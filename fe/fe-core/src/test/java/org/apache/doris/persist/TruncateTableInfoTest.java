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

package org.apache.doris.persist;

import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TruncateTableInfoTest {
    @Test
    void testVersionSerialization() {
        TruncateTableInfo info = new TruncateTableInfo(1L, "db", 2L, "tbl", Lists.newArrayList(),
                true, "TRUNCATE TABLE tbl", Lists.newArrayList(), true, Maps.newHashMap(), 10L, 20L);

        TruncateTableInfo deserialized = GsonUtils.GSON.fromJson(info.toJson(), TruncateTableInfo.class);

        Assertions.assertEquals(10L, deserialized.getVersion());
        Assertions.assertEquals(20L, deserialized.getVersionTimeMs());
    }

    @Test
    void testLegacyVersionDefaults() {
        TruncateTableInfo deserialized = GsonUtils.GSON.fromJson("{}", TruncateTableInfo.class);

        Assertions.assertEquals(0L, deserialized.getVersion());
        Assertions.assertEquals(0L, deserialized.getVersionTimeMs());
    }
}
