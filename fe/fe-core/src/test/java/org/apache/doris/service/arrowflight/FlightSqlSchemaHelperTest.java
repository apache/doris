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

package org.apache.doris.service.arrowflight;

import org.apache.doris.catalog.PrimitiveType;

import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class FlightSqlSchemaHelperTest {
    @Test
    public void testDatetimeV2NanoArrowType() {
        String timeZone = "Asia/Shanghai";
        for (int scale = 7; scale <= 9; scale++) {
            ArrowType.Timestamp type = (ArrowType.Timestamp) FlightSqlSchemaHelper.getArrowType(
                    PrimitiveType.DATETIMEV2, 18, scale, timeZone);
            Assertions.assertEquals(TimeUnit.NANOSECOND, type.getUnit());
            Assertions.assertEquals(timeZone, type.getTimezone());
        }
    }
}
