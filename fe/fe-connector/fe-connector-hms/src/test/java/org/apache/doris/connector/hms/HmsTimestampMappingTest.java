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

package org.apache.doris.connector.hms;

import org.apache.doris.connector.spi.ConnectorType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class HmsTimestampMappingTest {
    @Test
    void legacyFlagsCannotChangeNestedInstantSemantics() {
        for (boolean flag : new boolean[] {false, true}) {
            HmsTypeMapping.Options options = new HmsTypeMapping.Options(6, flag, flag);
            Assertions.assertEquals("DATETIMEV2", HmsTypeMapping.toConnectorType("timestamp", options).getTypeName());
            ConnectorType nested = HmsTypeMapping.toConnectorType(
                    "struct<t:array<timestamp with local time zone>>", options);
            Assertions.assertEquals("TIMESTAMPTZ", nested.getChildren().get(0).getChildren().get(0).getTypeName());
            Assertions.assertEquals("VARBINARY", HmsTypeMapping.toConnectorType("binary", options).getTypeName());
        }
    }
}
