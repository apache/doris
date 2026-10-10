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

import org.apache.doris.analysis.ColumnDef;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.UUID;

class DefaultValueTest {
    @Test
    void uuidGenerationAliasesAreCanonicalizedAndValidated() throws Exception {
        DefaultValue uuidV4 = DefaultValue.uuidDefaultValue("generateUUIDv4");
        DefaultValue uuidV7 = DefaultValue.uuidDefaultValue("generate_uuid_v7");

        Assertions.assertEquals("uuid_v4()", uuidV4.getValue());
        Assertions.assertEquals("uuid_v4()", uuidV4.getDefaultValueExprDef().getSql());
        Assertions.assertEquals(4, UUID.fromString(uuidV4.getRawValue()).version());
        Assertions.assertEquals("uuid_v7()", uuidV7.getValue());
        Assertions.assertEquals("uuid_v7()", uuidV7.getDefaultValueExprDef().getSql());
        Assertions.assertEquals(7, UUID.fromString(uuidV7.getRawValue()).version());

        ColumnDef.validateDefaultValue(Type.UUID, uuidV4.getValue(), uuidV4.getDefaultValueExprDef());
        Assertions.assertThrows(AnalysisException.class,
                () -> ColumnDef.validateDefaultValue(Type.INT, uuidV4.getValue(),
                        uuidV4.getDefaultValueExprDef()));
        Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class,
                () -> DefaultValue.uuidDefaultValue("unknown"));
    }
}
