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

import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PaimonTimestampNsTest {
    @Test
    public void nanosecondTimestampRoundTripsWithoutTimezone() {
        ConnectorType mapped = PaimonTypeMapping.toConnectorType(
                new TimestampType(9), PaimonTypeMapping.Options.DEFAULT);
        Assertions.assertEquals("TIMESTAMP_NS", mapped.getTypeName());
        Assertions.assertEquals(new TimestampType(9),
                PaimonTypeMapping.toPaimonType(ConnectorType.of("TIMESTAMP_NS")));
        Assertions.assertEquals("DATETIMEV2", PaimonTypeMapping.toConnectorType(
                new TimestampType(6), PaimonTypeMapping.Options.DEFAULT).getTypeName());
        Assertions.assertEquals("TIMESTAMPTZ", PaimonTypeMapping.toConnectorType(
                new LocalZonedTimestampType(9), PaimonTypeMapping.Options.DEFAULT).getTypeName());
    }

    @Test
    public void nestedNanosecondsRequireTheLosslessReader() {
        Assertions.assertTrue(PaimonScanPlanProvider.containsNanosecondTimestamp(new TimestampType(9)));
        Assertions.assertTrue(PaimonScanPlanProvider.containsNanosecondTimestamp(
                new ArrayType(new TimestampType(8))));
        Assertions.assertTrue(PaimonScanPlanProvider.containsNanosecondTimestamp(
                new MapType(new IntType(), new TimestampType(7))));
        Assertions.assertTrue(PaimonScanPlanProvider.containsNanosecondTimestamp(
                RowType.of(new IntType(), new ArrayType(new TimestampType(9)))));
        Assertions.assertFalse(PaimonScanPlanProvider.containsNanosecondTimestamp(
                new ArrayType(new TimestampType(6))));
        Assertions.assertFalse(PaimonScanPlanProvider.containsNanosecondTimestamp(
                new LocalZonedTimestampType(9)));
    }
}
