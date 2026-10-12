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

package org.apache.doris.connector.paimon;

import org.apache.doris.connector.spi.ConnectorType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class PaimonTimestampMappingTest {
    @Test
    void legacyFlagsCannotChangeTimestampSemantics() {
        for (boolean flag : new boolean[] {false, true}) {
            for (int precision : new int[] {0, 3, 6, 9}) {
                PaimonTypeMapping.Options options = new PaimonTypeMapping.Options(flag, flag);
                Assertions.assertEquals(ConnectorType.of("DATETIMEV2", Math.min(precision, 6), 0),
                        PaimonTypeMapping.toConnectorType(new org.apache.paimon.types.TimestampType(precision), options));
                Assertions.assertEquals(ConnectorType.of("TIMESTAMPTZ", Math.min(precision, 6), 0),
                        PaimonTypeMapping.toConnectorType(
                                new org.apache.paimon.types.LocalZonedTimestampType(precision), options));
            }
        }
    }
}
