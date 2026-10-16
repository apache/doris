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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.nereids.types.UuidType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;

public class ColumnDefinitionTest {
    @Test
    public void testAddColumnRejectsUuidDynamicDefaults() {
        for (String function : new String[] {"uuid_v4", "uuid_v7", "generateUUIDv4", "generate_uuid_v7"}) {
            ColumnDefinition column = new ColumnDefinition("u", UuidType.INSTANCE, false, null, false,
                    Optional.of(DefaultValue.uuidDefaultValue(function)), "");
            org.apache.doris.common.AnalysisException error = Assertions.assertThrows(
                    org.apache.doris.common.AnalysisException.class,
                    () -> AddColumnOp.validateColumnDef(null, column, null, null));
            Assertions.assertEquals("ADD COLUMN does not support UUID dynamic default values", error.getDetailMessage());
        }
        ColumnDefinition literal = new ColumnDefinition("u", UuidType.INSTANCE, false, null, false,
                Optional.of(new DefaultValue("00112233-4455-6677-8899-aabbccddeeff")), "");
        Assertions.assertFalse(literal.hasUuidDefaultValue());
    }
}
