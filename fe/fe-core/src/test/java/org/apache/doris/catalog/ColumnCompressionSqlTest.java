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

package org.apache.doris.catalog;

import org.apache.doris.proto.OlapFile;
import org.apache.doris.thrift.TColumn;
import org.apache.doris.thrift.TCompressionType;

import doris.segment_v2.SegmentV2;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

public class ColumnCompressionSqlTest {
    @Test
    public void testToSqlRendersCompression() {
        Column c = new Column("c1", Type.INT, true, null, false, "compressed column", true);
        c.setCompression(TCompressionType.ZSTD, 9);
        String sql = c.toSql();
        Assertions.assertTrue(sql.contains("COMPRESSION ZSTD(9)"));
        Assertions.assertTrue(sql.indexOf("COMPRESSION") < sql.indexOf("COMMENT"));
    }

    @Test
    public void testToSqlNoCompressionWhenUnset() {
        Column c = new Column("c1", Type.INT, true, null, false, "", true);
        Assertions.assertFalse(c.toSql().contains("COMPRESSION"));
    }

    @Test
    public void testToThriftSetsCompression() {
        Column c = new Column("c1", Type.INT, true, null, false, "", true);
        c.setCompression(TCompressionType.ZSTD, 9);
        TColumn t = ColumnToThrift.toThrift(c);
        Assertions.assertTrue(t.isSetCompressionType());
        Assertions.assertEquals(TCompressionType.ZSTD.getValue(), t.getCompressionType());
        Assertions.assertEquals(9, t.getCompressionLevel());
    }

    @Test
    public void testToThriftNoCompressionWhenUnset() {
        Column c = new Column("c1", Type.INT, true, null, false, "", true);
        TColumn t = ColumnToThrift.toThrift(c);
        Assertions.assertFalse(t.isSetCompressionType());
    }

    @Test
    public void testToProtobufSetsCompression() throws Exception {
        Column c = new Column("c1", Type.INT, true, null, false, "", true);
        c.setCompression(TCompressionType.ZSTD, 9);
        OlapFile.ColumnPB columnPb = ColumnToProtobuf.toPb(
                c, Collections.emptySet(), Collections.emptyList());
        Assertions.assertTrue(columnPb.hasCompressionType());
        Assertions.assertEquals(SegmentV2.CompressionTypePB.ZSTD, columnPb.getCompressionType());
        Assertions.assertEquals(9, columnPb.getCompressionLevel());
    }

    @Test
    public void testToProtobufNoCompressionWhenUnset() throws Exception {
        Column c = new Column("c1", Type.INT, true, null, false, "", true);
        OlapFile.ColumnPB columnPb = ColumnToProtobuf.toPb(
                c, Collections.emptySet(), Collections.emptyList());
        Assertions.assertFalse(columnPb.hasCompressionType());
        Assertions.assertFalse(columnPb.hasCompressionLevel());
    }

    @Test
    public void testEqualsIncludesCompression() {
        Column base = new Column("c1", Type.INT, true, null, false, "", true);
        base.setCompression(TCompressionType.ZSTD, 9);

        Column same = new Column("c1", Type.INT, true, null, false, "", true);
        same.setCompression(TCompressionType.ZSTD, 9);
        Assertions.assertEquals(base, same);
        Assertions.assertEquals(base.hashCode(), same.hashCode());

        Column differentType = new Column("c1", Type.INT, true, null, false, "", true);
        differentType.setCompression(TCompressionType.LZ4HC, 9);
        Assertions.assertNotEquals(base, differentType);

        Column differentLevel = new Column("c1", Type.INT, true, null, false, "", true);
        differentLevel.setCompression(TCompressionType.ZSTD, 10);
        Assertions.assertNotEquals(base, differentLevel);
    }
}
