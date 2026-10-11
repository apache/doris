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

package org.apache.doris.connector.hudi;

import org.apache.doris.connector.spi.ConnectorType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class HudiTimestampMappingTest {
    @Test
    void distinguishInstantsFromWallClocks() {
        for (int precision : new int[] {3, 6}) {
            org.apache.avro.LogicalType instant = precision == 3 ? org.apache.avro.LogicalTypes.timestampMillis()
                    : org.apache.avro.LogicalTypes.timestampMicros();
            org.apache.avro.LogicalType local = precision == 3 ? org.apache.avro.LogicalTypes.localTimestampMillis()
                    : org.apache.avro.LogicalTypes.localTimestampMicros();
            Assertions.assertEquals(ConnectorType.of("TIMESTAMPTZ", precision, 0),
                    HudiTypeMapping.fromAvroSchema(instant.addToSchema(
                            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG))));
            Assertions.assertEquals(ConnectorType.of("DATETIMEV2", precision, 0),
                    HudiTypeMapping.fromAvroSchema(local.addToSchema(
                            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG))));
        }
    }
}
