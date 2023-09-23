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

package org.apache.doris.load.loadv2.etl;

import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.sparkdpp.EtlJobConfig;
import org.apache.doris.sparkdpp.EtlJobConfig.EtlColumn;
import org.apache.doris.sparkdpp.EtlJobConfig.EtlColumnMapping;
import org.apache.doris.sparkdpp.EtlJobConfig.EtlFileGroup;
import org.apache.doris.sparkdpp.EtlJobConfig.EtlIndex;
import org.apache.doris.sparkdpp.EtlJobConfig.EtlJobProperty;
import org.apache.doris.sparkdpp.EtlJobConfig.EtlPartition;
import org.apache.doris.sparkdpp.EtlJobConfig.EtlPartitionInfo;
import org.apache.doris.sparkdpp.EtlJobConfig.EtlTable;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import mockit.Expectations;
import mockit.Mocked;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SparkEtlJobTest {
    private long tableId;
    private long index1Id;
    private long index2Id;
    private long partition1Id;
    private long partition2Id;
    private EtlJobConfig etlJobConfig;

    @Before
    public void setUp() {
        tableId = 0L;
        index1Id = 1L;
        index2Id = 2L;
        partition1Id = 3L;
        partition2Id = 4L;

        // indexes
        EtlColumn k1 = new EtlColumn("k1", "INT", false, true, "NONE", "0", 0, 0, 0);
        EtlColumn k2 = new EtlColumn("k2", "VARCHAR", false, true, "NONE", "0", 10, 0, 0);
        EtlColumn v1 = new EtlColumn("v1", "BIGINT", false, false, "NONE", "0", 0, 0, 0);
        EtlIndex index1 = new EtlIndex(index1Id, Lists.newArrayList(k1, k2, v1), 666666, "DUPLICATE", true);
        v1 = new EtlColumn("v1", "BIGINT", false, false, "SUM", "0", 0, 0, 0);
        EtlIndex index2 = new EtlIndex(index2Id, Lists.newArrayList(k1, v1), 888888, "AGGREGATE", true);
        List<EtlIndex> indexes = Lists.newArrayList(index1, index2);
        // partition info
        List<EtlPartition> partitions = Lists.newArrayList();
        partitions.add(new EtlPartition(partition1Id, Lists.newArrayList(0), Lists.newArrayList(100), false, 2));
        partitions.add(new EtlPartition(partition2Id, Lists.newArrayList(100), Lists.newArrayList(), true, 3));
        EtlPartitionInfo partitionInfo = new EtlPartitionInfo("RANGE", Lists.newArrayList("k1"), Lists.newArrayList("k2"), partitions);
        EtlTable table = new EtlTable(indexes, partitionInfo);
        // file group
        Map<String, EtlColumnMapping> columnMappings = Maps.newHashMap();
        columnMappings.put("k1", new EtlColumnMapping("k1 + 1"));
        table.addFileGroup(new EtlFileGroup(EtlJobConfig.SourceType.FILE, Lists.newArrayList("hdfs://127.0.0.1:10000/file"),
                                            Lists.newArrayList(), Lists.newArrayList(), "\t", "\n", false, null,
                                            Maps.newHashMap(), "", Lists.newArrayList(partition1Id, partition2Id)));
        // tables
        Map<Long, EtlTable> tables = Maps.newHashMap();
        tables.put(tableId, table);
        // others
        String outputFilePattern = "V1.label0.%d.%d.%d.%d.%d.parquet";
        String label = "label0";
        EtlJobProperty properties = new EtlJobProperty();
        properties.strictMode = false;
        properties.timezone = "Asia/Shanghai";
        etlJobConfig = new EtlJobConfig(tables, outputFilePattern, label, properties);
    }

    @Test
    public void testInitConfig(@Mocked FileSystem fs) throws IOException {
        new Expectations() {
            {
                fs.open(new Path("hdfs://127.0.0.1:10000/jobconfig.json"));
                result = new FSDataInputStream(new SeekableByteArrayInputStream(etlJobConfig.configToJson().getBytes()));
            }
        };

        SparkEtlJob job = Deencapsulation.newInstance(SparkEtlJob.class, "hdfs://127.0.0.1:10000/jobconfig.json");
        Deencapsulation.invoke(job, "initConfig");
        EtlJobConfig parsedConfig = Deencapsulation.getField(job, "etlJobConfig");
        Assert.assertTrue(parsedConfig.tables.containsKey(tableId));
        EtlTable table = parsedConfig.tables.get(tableId);
        Assert.assertEquals(2, table.indexes.size());
        Assert.assertEquals(2, table.partitionInfo.partitions.size());
        Assert.assertEquals(false, parsedConfig.properties.strictMode);
        Assert.assertEquals("label0", parsedConfig.label);
    }

    @Test
    public void testCheckConfigWithoutBitmapDictColumns() {
        SparkEtlJob job = Deencapsulation.newInstance(SparkEtlJob.class, "hdfs://127.0.0.1:10000/jobconfig.json");
        Deencapsulation.setField(job, "etlJobConfig", etlJobConfig);
        Deencapsulation.invoke(job, "checkConfig");
        Map<Long, Set<String>> tableToBitmapDictColumns = Deencapsulation.getField(job, "tableToBitmapDictColumns");
        // check bitmap dict columns empty
        Assert.assertTrue(tableToBitmapDictColumns.isEmpty());
    }

    @Test
    public void testCheckConfigWithBitmapDictColumns() {
        SparkEtlJob job = Deencapsulation.newInstance(SparkEtlJob.class, "hdfs://127.0.0.1:10000/jobconfig.json");
        EtlTable table = etlJobConfig.tables.get(tableId);
        table.indexes.get(0).columns.add(
                new EtlColumn("v2", "BITMAP", false, false, "BITMAP_UNION", "0", 0, 0, 0)
        );
        EtlFileGroup fileGroup = table.fileGroups.get(0);
        fileGroup.sourceType = EtlJobConfig.SourceType.HIVE;
        fileGroup.columnMappings.put(
                "v2", new EtlColumnMapping("bitmap_dict", Lists.newArrayList("v2"))
        );
        Deencapsulation.setField(job, "etlJobConfig", etlJobConfig);
        Deencapsulation.invoke(job, "checkConfig");
        // check hive source
        Set<Long> hiveSourceTables = Deencapsulation.getField(job, "hiveSourceTables");
        Assert.assertTrue(hiveSourceTables.contains(tableId));
        // check bitmap dict columns has v2
        Map<Long, Set<String>> tableToBitmapDictColumns = Deencapsulation.getField(job, "tableToBitmapDictColumns");
        Assert.assertTrue(tableToBitmapDictColumns.containsKey(tableId));
        Assert.assertTrue(tableToBitmapDictColumns.get(tableId).contains("v2"));
        // check remove v2 bitmap_dict func mapping from file group column mappings
        Assert.assertFalse(table.fileGroups.get(0).columnMappings.containsKey("v2"));
    }

    private static class SeekableByteArrayInputStream extends ByteArrayInputStream implements Seekable, PositionedReadable {
        public SeekableByteArrayInputStream(byte[] buf) {
            super(buf);
        }

        public void seek(long position) {
            if (position < 0 || position >= buf.length) {
                throw new IllegalArgumentException("pos = " + position + " length = " + buf.length);
            }
            this.pos = (int) position;
        }

        public long getPos() {
            return this.pos;
        }

        @Override
        public boolean seekToNewSource(long targetPos) throws IOException {
            return false;
        }

        @Override
        public int read(long position, byte[] buffer, int offset, int length) throws IOException {
            this.seek(position);
            return this.read(buffer, offset, length);
        }

        @Override
        public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
            if (position + length > buf.length) {
                throw  new EOFException("End of file reached before reading fully.");
            }
            System.arraycopy(buf, (int) position, buffer, offset, length);
        }

        @Override
        public void readFully(long position, byte[] buffer) throws IOException {
            readFully(position, buffer, 0, buffer.length);
        }
    }
}
