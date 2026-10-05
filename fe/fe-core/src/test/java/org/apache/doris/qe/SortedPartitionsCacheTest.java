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

package org.apache.doris.qe;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.cache.NereidsSortedPartitionsCacheManager;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SortedPartitionsCacheTest extends TestWithFeService {

    @Test
    public void testPartitionCache() throws Exception {
        createDatabase("test");
        createTable("create table test.key_1_fixed_range_date_part (a int, dt datetime, c varchar(100)) duplicate key(a)\n"
                + "partition by range(dt) (\n"
                + "    PARTITION p_min VALUES LESS THAN (\"2023-01-01 00:00:00\"),\n"
                + "    PARTITION p_202301 VALUES [('2023-01-01 00:00:00'), ('2023-02-01 00:00:00')),\n"
                + "    PARTITION p_202302 VALUES [('2023-02-01 00:00:00'), ('2023-03-01 00:00:00')),\n"
                + "    PARTITION p_202303 VALUES [('2023-03-01 00:00:00'), ('2023-04-01 00:00:00')),\n"
                + "    PARTITION p_202304 VALUES [('2023-04-01 00:00:00'), ('2023-05-01 00:00:00')),\n"
                + "    PARTITION p_202305 VALUES [('2023-05-01 00:00:00'), ('2023-06-01 00:00:00')),\n"
                + "    PARTITION p_202306 VALUES [('2023-06-01 00:00:00'), ('2023-07-01 00:00:00')),\n"
                + "    PARTITION p_202307 VALUES [('2023-07-01 00:00:00'), ('2023-08-01 00:00:00')),\n"
                + "    PARTITION p_202308 VALUES [('2023-08-01 00:00:00'), ('2023-09-01 00:00:00')),\n"
                + "    PARTITION p_202309 VALUES [('2023-09-01 00:00:00'), ('2023-10-01 00:00:00')),\n"
                + "    PARTITION p_202310 VALUES [('2023-10-01 00:00:00'), ('2023-11-01 00:00:00')),\n"
                + "    PARTITION p_202311 VALUES [('2023-11-01 00:00:00'), ('2023-12-01 00:00:00')),\n"
                + "    PARTITION p_202312 VALUES [('2023-12-01 00:00:00'), ('2024-01-01 00:00:00')),\n"
                + "    PARTITION p_max VALUES [('2024-01-01 00:00:00'), ('9999-12-31 23:59:59'))\n"
                + ") distributed by hash(a) properties(\"replication_num\"=\"1\");");

        createTable("create table test.key_1_fixed_range_date_part2 (a int, dt datetime, c varchar(100)) duplicate key(a)\n"
                + "partition by range(dt) (\n"
                + "    PARTITION p_min VALUES LESS THAN (\"2023-01-01 00:00:00\"),\n"
                + "    PARTITION p_202301 VALUES [('2023-01-01 00:00:00'), ('2023-02-01 00:00:00')),\n"
                + "    PARTITION p_202302 VALUES [('2023-02-01 00:00:00'), ('2023-03-01 00:00:00')),\n"
                + "    PARTITION p_202303 VALUES [('2023-03-01 00:00:00'), ('2023-04-01 00:00:00')),\n"
                + "    PARTITION p_202304 VALUES [('2023-04-01 00:00:00'), ('2023-05-01 00:00:00')),\n"
                + "    PARTITION p_202305 VALUES [('2023-05-01 00:00:00'), ('2023-06-01 00:00:00')),\n"
                + "    PARTITION p_202306 VALUES [('2023-06-01 00:00:00'), ('2023-07-01 00:00:00')),\n"
                + "    PARTITION p_202307 VALUES [('2023-07-01 00:00:00'), ('2023-08-01 00:00:00')),\n"
                + "    PARTITION p_202308 VALUES [('2023-08-01 00:00:00'), ('2023-09-01 00:00:00')),\n"
                + "    PARTITION p_202309 VALUES [('2023-09-01 00:00:00'), ('2023-10-01 00:00:00')),\n"
                + "    PARTITION p_202310 VALUES [('2023-10-01 00:00:00'), ('2023-11-01 00:00:00')),\n"
                + "    PARTITION p_202311 VALUES [('2023-11-01 00:00:00'), ('2023-12-01 00:00:00')),\n"
                + "    PARTITION p_202312 VALUES [('2023-12-01 00:00:00'), ('2024-01-01 00:00:00')),\n"
                + "    PARTITION p_max VALUES [('2024-01-01 00:00:00'), ('9999-12-31 23:59:59'))\n"
                + ") distributed by hash(a) properties(\"replication_num\"=\"1\");");

        // wait cache enable
        Thread.sleep(10 * 1000);

        executeNereidsSql("select * from test.key_1_fixed_range_date_part where dt='2023-07-01 05:00:00'");
        executeNereidsSql("select * from test.key_1_fixed_range_date_part2 where dt='2023-07-01 05:00:00'");

        Env currentEnv = Env.getCurrentEnv();
        NereidsSortedPartitionsCacheManager sortedPartitionsCacheManager = currentEnv.getSortedPartitionsCacheManager();
        Assertions.assertEquals(2, sortedPartitionsCacheManager.getPartitionCaches().asMap().size());

        executeNereidsSql("admin set frontend config ('cache_partition_meta_table_manage_num'='1')");
        Assertions.assertEquals(1, sortedPartitionsCacheManager.getPartitionCaches().asMap().size());
    }
}
