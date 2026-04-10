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

package org.apache.doris.external.elasticsearch;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.datasource.es.EsExternalTable;
import org.apache.doris.datasource.es.EsShardPartitions;
import org.apache.doris.datasource.es.EsTablePartitions;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class EsShardPartitionsTest extends EsTestCase {

    @Test
    public void testPartition() throws Exception {
        List<Column> columns = new ArrayList<>();
        columns.add(new Column("userId", PrimitiveType.VARCHAR));
        columns.add(new Column("time", PrimitiveType.BIGINT));
        columns.add(new Column("type", PrimitiveType.VARCHAR));
        EsExternalTable esTable = fakeEsTable("doe", "doe", "doc", columns);
        EsShardPartitions esShardPartitions =
                EsShardPartitions.findShardPartitions("doe",
                        loadJsonFromFile("data/es/test_search_shards.json"));
        EsTablePartitions esTablePartitions =
                EsTablePartitions.fromShardPartitions(esTable, esShardPartitions);
        Assert.assertNotNull(esTablePartitions);
        Assert.assertEquals(1,
                esTablePartitions.getUnPartitionedIndexStates().size());
        Assert.assertEquals(5,
                esTablePartitions.getEsShardPartitions("doe")
                        .getShardRoutings().size());
    }
}
