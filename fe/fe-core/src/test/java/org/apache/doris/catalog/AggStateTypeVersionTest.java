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

package org.apache.doris.catalog;

import org.apache.doris.common.Config;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

public class AggStateTypeVersionTest {
    private int originalVersion;

    @BeforeEach
    public void saveVersion() {
        originalVersion = Config.be_exec_version;
    }

    @AfterEach
    public void restoreVersion() {
        Config.be_exec_version = originalVersion;
    }

    @Test
    public void testTypeVersionSurvivesReplayAndPlanning() throws Exception {
        // One bitmap argument is sufficient. No table or running cluster is needed.
        // Changing the query version must not change a previously created state's codec.
        for (int typeVersion : List.of(19, 20)) {
            Config.be_exec_version = typeVersion;
            AggStateType type = new AggStateType("bitmap_intersect", false,
                    List.of(Type.BITMAP), List.of(true));
            String persisted = GsonUtils.GSON.toJson(type, Type.class);
            Config.be_exec_version = typeVersion == 19 ? 20 : 19;
            checkVersion(type, typeVersion);
            Type replayed = GsonUtils.GSON.fromJson(persisted, Type.class);
            checkVersion(replayed, typeVersion);
            // Catalog -> Nereids -> converted Nereids -> catalog must retain the version.
            checkVersion(DataType.fromCatalogType(replayed).toCatalogDataType(), typeVersion);
            checkVersion(DataType.fromCatalogType(replayed).conversion().toCatalogDataType(), typeVersion);
        }
    }

    @Test
    public void testMissingVersionUsesTheLegacyCodec() throws Exception {
        Config.be_exec_version = 19;
        AggStateType type = new AggStateType("bitmap_intersect", false,
                List.of(Type.BITMAP), List.of(true));
        // Old catalog JSON has no version field. Remove only that field from a legal type.
        JsonObject legacy = JsonParser.parseString(GsonUtils.GSON.toJson(type, Type.class)).getAsJsonObject();
        legacy.remove("beExecVersion");
        Type replayed = GsonUtils.GSON.fromJson(legacy, Type.class);
        Config.be_exec_version = 20;
        checkVersion(replayed, 19);
        // Retain support for an operator who selects an older per-function codec.
        // An absent field must not force version 19 when the old query version is 16.
        Config.be_exec_version = 16;
        checkVersion(replayed, 16);
    }

    private void checkVersion(Type type, int expected) throws Exception {
        Assertions.assertEquals(expected, type.toThrift().getBeExecVersion());
        Column column = new Column("s", type, false, AggregateType.GENERIC, false, null, "");
        // Both shared-nothing and cloud tablet metadata must use the same codec version.
        Assertions.assertEquals(expected, ColumnToThrift.toThrift(column).getBeExecVersion());
        Assertions.assertEquals(expected, ColumnToProtobuf.toPb(column, null, null).getBeExecVersion());
    }
}
