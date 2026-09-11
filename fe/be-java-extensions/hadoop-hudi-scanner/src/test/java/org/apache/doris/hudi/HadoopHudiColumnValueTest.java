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

package org.apache.doris.hudi;

import org.apache.doris.common.jni.vec.ColumnType;

import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.LongWritable;
import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.ZoneId;

public class HadoopHudiColumnValueTest {
    @Test
    public void testInt64TimestampUsesSessionTimezone() {
        HadoopHudiColumnValue value = new HadoopHudiColumnValue(ZoneId.of("America/Los_Angeles"));
        value.setField(ColumnType.parseType("ts", "datetimev2(6)"), null);
        value.setRow(new LongWritable(0));

        Assert.assertEquals(LocalDateTime.of(1969, 12, 31, 16, 0), value.getDateTime());
    }

    @Test
    public void testInt96TimestampUsesSessionTimezone() {
        HadoopHudiColumnValue value = new HadoopHudiColumnValue(ZoneId.of("America/Los_Angeles"));
        value.setField(ColumnType.parseType("ts", "datetimev2(6)"),
                PrimitiveObjectInspectorFactory.writableTimestampObjectInspector);
        value.setRow(new TimestampWritableV2(Timestamp.ofEpochSecond(0)));

        Assert.assertEquals(LocalDateTime.of(1969, 12, 31, 16, 0), value.getDateTime());
    }
}
