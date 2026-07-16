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

package org.apache.doris.cdcclient.source.reader.mysql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

import java.sql.Date;
import java.time.Year;

class MySqlYearConverterTest {

    @Test
    void preservesSnapshotYearZero() {
        assertEquals(0, MySqlYearConverter.convertYear((short) 0));
    }

    @Test
    void restoresBinlogYearZero() {
        assertEquals(0, MySqlYearConverter.convertYear(Year.of(1900)));
    }

    @Test
    void preservesRegularYears() {
        assertEquals(2000, MySqlYearConverter.convertYear((short) 2000));
        assertEquals(2024, MySqlYearConverter.convertYear(Year.of(2024)));
    }

    @Test
    void acceptsDateFromYearIsDateType() {
        assertEquals(2024, MySqlYearConverter.convertYear(Date.valueOf("2024-01-01")));
    }

    @Test
    void preservesNull() {
        assertNull(MySqlYearConverter.convertYear(null));
    }
}
