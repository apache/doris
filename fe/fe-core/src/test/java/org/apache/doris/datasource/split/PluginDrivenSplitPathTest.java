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

package org.apache.doris.datasource.split;

import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.thrift.TFileType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

class PluginDrivenSplitPathTest {
    @Test
    void reuseFilePathWhilePreservingPerSplitOffsets() {
        PluginDrivenSplit.Factory factory = new PluginDrivenSplit.Factory();
        String path = "s3://bucket/a//p=中文%2F+ b/file.parquet";
        PluginDrivenSplit first = factory.create(range(path, 0));
        PluginDrivenSplit second = factory.create(range(new String(path), 100));
        Assertions.assertSame(first.getPath(), second.getPath());
        Assertions.assertEquals(0, first.getStart());
        Assertions.assertEquals(100, second.getStart());
        Assertions.assertEquals(path, second.getPathString());
        Assertions.assertEquals(TFileType.FILE_S3, second.getLocationType());
    }

    @Test
    void changedFileAndIndependentProducersDoNotShareMutableState() {
        PluginDrivenSplit.Factory factory = new PluginDrivenSplit.Factory();
        PluginDrivenSplit first = factory.create(range("s3://bucket/a", 0));
        PluginDrivenSplit other = factory.create(range("hdfs://namenode:8020/b", 0));
        Assertions.assertNotSame(first.getPath(), other.getPath());
        Assertions.assertEquals("s3://bucket/a", first.getPathString());
        Assertions.assertEquals("hdfs://namenode:8020", other.getPath().getFsIdentifier());
        Assertions.assertEquals(TFileType.FILE_HDFS, other.getLocationType());
        Assertions.assertNotSame(first.getPath(), factory.create(range("s3://bucket/a", 100)).getPath());
        Assertions.assertNotSame(other.getPath(), new PluginDrivenSplit.Factory()
                .create(range("hdfs://namenode:8020/b", 100)).getPath());
    }

    @Test
    void pathlessRangesKeepVirtualPath() {
        PluginDrivenSplit.Factory factory = new PluginDrivenSplit.Factory();
        PluginDrivenSplit first = factory.create(range(null, 0));
        PluginDrivenSplit second = factory.create(range(null, 100));
        Assertions.assertEquals("connector://virtual", first.getPathString());
        Assertions.assertSame(first.getPath(), second.getPath());
        Assertions.assertNotSame(first.getConnectorScanRange(), second.getConnectorScanRange());
    }

    private static ConnectorScanRange range(String path, long start) {
        return new ConnectorScanRange() {
            @Override
            public Map<String, String> getProperties() {
                return Collections.emptyMap();
            }

            @Override
            public Optional<String> getPath() {
                return Optional.ofNullable(path);
            }

            @Override
            public long getStart() {
                return start;
            }
        };
    }
}
