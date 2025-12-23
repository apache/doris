/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.orc.tools.convert;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DateColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.UnionColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.TypeDescription;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.StringReader;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestJsonReader {
    @Test
    public void testCustomTimestampFormat() throws Exception {
        String tsFormat = "yyyy-MM-dd HH:mm:ss.SSSSSS";

        String s = "{\"a\":\"2018-03-21 12:23:34.123456\"}\n" +
                "{\"a\":\"2018-02-03 18:04:51.456789\"}\n";
        StringReader input = new StringReader(s);
        TypeDescription schema = TypeDescription.fromString(
                "struct<a:timestamp>");
        JsonReader reader = new JsonReader(input, null, 1, schema, tsFormat);
        VectorizedRowBatch batch = schema.createRowBatch(2);
        assertTrue(reader.nextBatch(batch));
        assertEquals(2, batch.size);
        TimestampColumnVector cv = (TimestampColumnVector) batch.cols[0];
        assertEquals("2018-03-21 12:23:34.123456", cv.asScratchTimestamp(0).toString());
        assertEquals("2018-02-03 18:04:51.456789", cv.asScratchTimestamp(1).toString());
    }

    @Test
    public void testTimestampOffByOne() throws Exception {
        String tsFormat = "yyyy-MM-dd HH:mm:ss.SSSS";

        String s = "{\"a\": \"1970-01-01 00:00:00.0001\"}\n" +
                "{\"a\": \"1970-01-01 00:00:00.0000\"}\n" +
                "{\"a\": \"1969-12-31 23:59:59.9999\"}\n" +
                "{\"a\": \"1969-12-31 23:59:59.0001\"}\n" +
                "{\"a\": \"1969-12-31 23:59:59.0000\"}\n" +
                "{\"a\": \"1969-12-31 23:59:58.9999\"}";
        StringReader input = new StringReader(s);
        TypeDescription schema = TypeDescription.fromString(
                "struct<a:timestamp>");
        JsonReader reader = new JsonReader(input, null, 1, schema, tsFormat);
        VectorizedRowBatch batch = schema.createRowBatch(6);
        assertTrue(reader.nextBatch(batch));
        assertEquals(6, batch.size);
        TimestampColumnVector cv = (TimestampColumnVector) batch.cols[0];
        assertEquals("1970-01-01 00:00:00.0001", cv.asScratchTimestamp(0).toString());
        assertEquals("1970-01-01 00:00:00.0", cv.asScratchTimestamp(1).toString());
        assertEquals("1969-12-31 23:59:59.9999", cv.asScratchTimestamp(2).toString());
        assertEquals("1969-12-31 23:59:59.0001", cv.asScratchTimestamp(3).toString());
        assertEquals("1969-12-31 23:59:59.0", cv.asScratchTimestamp(4).toString());
        assertEquals("1969-12-31 23:59:58.9999", cv.asScratchTimestamp(5).toString());
    }

    @Test
    public void testDateTypeSupport() throws IOException {
        LocalDate date1 = LocalDate.of(2021, 1, 18);
        LocalDate date2 = LocalDate.now();
        String inputString = "{\"dt\": \"" + date1 + "\"}\n" +
                             "{\"dt\": \"" + date2 + "\"}\n" +
                             "{\"dt\": \"" + date2 + "\"}\n" +
                             "{\"dt\": null}";


        StringReader input = new StringReader(inputString);

        TypeDescription schema = TypeDescription.fromString("struct<dt:date>");
        JsonReader reader = new JsonReader(input, null, 1, schema, "");
        VectorizedRowBatch batch = schema.createRowBatch(4);
        assertTrue(reader.nextBatch(batch));
        assertEquals(4, batch.size);
        DateColumnVector cv = (DateColumnVector) batch.cols[0];
        assertEquals(date1, LocalDate.ofEpochDay(cv.vector[0]));
        assertEquals(date2, LocalDate.ofEpochDay(cv.vector[1]));
        assertEquals(date2, LocalDate.ofEpochDay(cv.vector[2]));
        assertFalse(cv.isNull[2]);
        assertTrue(cv.isNull[3]);
    }

    @Test
    public void testDateTimeTypeSupport() throws IOException {
        String timestampFormat = "yyyy[[-][/]]MM[[-][/]]dd[['T'][ ]]HH:mm:ss[['.'][ ]][[SSSSSSSSS][SSSSSS][SSS]][[X][Z]['['VV']']]";
        LocalDateTime datetime1 = LocalDateTime.of(2021, 1, 18, 1, 2, 3, 4);
        LocalDateTime datetime2 = LocalDateTime.now();
        OffsetDateTime datetime3 = OffsetDateTime.of(datetime1, ZoneOffset.UTC);
        OffsetDateTime datetime4 = OffsetDateTime.of(datetime2, ZoneOffset.ofHours(-7));
        ZonedDateTime datetime5 = ZonedDateTime.of(datetime1, ZoneId.of("UTC"));
        ZonedDateTime datetime6 = ZonedDateTime.of(datetime2, ZoneId.of("America/New_York"));

        String datetime4Str = datetime4.toString();
        datetime4Str = datetime4Str.substring(0, datetime4Str.length() - 5) + "0700";

        String inputString = "{\"dt\": \"" + datetime1 + "\"}\n" +
                             "{\"dt\": \"" + datetime2 + "\"}\n" +
                             "{\"dt\": \"" + datetime3 + "\"}\n" +
                             "{\"dt\": \"" + datetime4Str + "\"}\n" +
                             "{\"dt\": \"" + datetime5.toLocalDateTime().toString() + "[" + datetime5.getZone() + "]\"}\n" +
                             "{\"dt\": \"" + datetime6.toLocalDateTime().toString() + "[" + datetime6.getZone() + "]\"}\n";

        StringReader input = new StringReader(inputString);

        TypeDescription schema = TypeDescription.fromString("struct<dt:timestamp>");
        JsonReader reader = new JsonReader(input, null, 1, schema, timestampFormat);
        VectorizedRowBatch batch = schema.createRowBatch(6);
        assertTrue(reader.nextBatch(batch));
        assertEquals(6, batch.size);
        TimestampColumnVector cv = (TimestampColumnVector) batch.cols[0];
        assertEquals(datetime1, LocalDateTime.from(cv.asScratchTimestamp(0).toLocalDateTime()));
        assertEquals(datetime2, LocalDateTime.from(cv.asScratchTimestamp(1).toLocalDateTime()));
        assertEquals(datetime3.toInstant(), cv.asScratchTimestamp(2).toInstant());
        assertEquals(datetime4.toInstant(), cv.asScratchTimestamp(3).toInstant());
        assertEquals(datetime5.toInstant(), cv.asScratchTimestamp(4).toInstant());
        assertEquals(datetime6.toInstant(), cv.asScratchTimestamp(5).toInstant());
    }

    @Test
    public void testUnionTypeSupport() throws IOException {
        String inputString = "{\"foo\": {\"tag\": 0, \"value\": 1}}\n" +
                "{\"foo\": {\"tag\": 1, \"value\": \"testing\"}}\n" +
                "{\"foo\": {\"tag\": 0, \"value\": 3}}";


        StringReader input = new StringReader(inputString);

        TypeDescription schema = TypeDescription.fromString("struct<foo:uniontype<int,string>>");
        JsonReader reader = new JsonReader(input, null, 1, schema, "", "tag", "value");
        VectorizedRowBatch batch = schema.createRowBatch(3);
        assertTrue(reader.nextBatch(batch));
        assertEquals(3, batch.size);
        UnionColumnVector union = (UnionColumnVector) batch.cols[0];
        LongColumnVector longs = (LongColumnVector) union.fields[0];
        BytesColumnVector strs = (BytesColumnVector) union.fields[1];
        assertTrue(union.noNulls);
        assertFalse(union.isNull[0]);
        assertEquals(0, union.tags[0]);
        assertEquals(1, longs.vector[0]);
        assertFalse(union.isNull[1]);
        assertEquals(1, union.tags[1]);
        assertEquals("testing", strs.toString(1));
        assertFalse(union.isNull[2]);
        assertEquals(0, union.tags[2]);
        assertEquals(3, longs.vector[2]);
    }
}
