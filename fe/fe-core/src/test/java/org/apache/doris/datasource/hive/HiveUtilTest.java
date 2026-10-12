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

package org.apache.doris.datasource.hive;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

public class HiveUtilTest {
    @Test
    public void testExplicitParquetGzipCompression() {
        Map<String, String> properties = Maps.newHashMap();
        properties.put(HiveUtil.COMPRESSION_KEY, "gzip");
        HiveTableMetadata metadata = new HiveTableMetadata(
                "db", "table", Optional.empty(),
                ImmutableList.of(new Column("id", PrimitiveType.INT)),
                ImmutableList.of(), properties, "parquet", "");

        Table table = HiveUtil.toHiveTable(metadata);

        Assertions.assertEquals("gzip", table.getParameters().get("parquet.compression"));
        Assertions.assertFalse(table.getParameters().containsKey(HiveUtil.COMPRESSION_KEY));
    }
}
