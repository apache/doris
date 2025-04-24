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

package org.apache.doris.mtmv;

import org.apache.doris.nereids.sqltest.SqlTestBase;
import org.apache.doris.nereids.trees.plans.commands.info.ColumnDefinition;
import org.apache.doris.nereids.trees.plans.commands.info.SimpleColumnDefinition;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.VarcharType;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.util.List;

public class MTMVPlanUtilTest extends SqlTestBase {
    @Test
    public void testGenerateColumnsBySql() throws Exception {
        createTables(
                "CREATE TABLE IF NOT EXISTS MTMVPlanUtilTestT1 (\n"
                        + "    id varchar(10),\n"
                        + "    score String\n"
                        + ")\n"
                        + "DUPLICATE KEY(id)\n"
                        + "AUTO PARTITION BY LIST(`id`)\n"
                        + "(\n"
                        + ")\n"
                        + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                        + "PROPERTIES (\n"
                        + "  \"replication_num\" = \"1\"\n"
                        + ")\n"
        );

        String querySql = "select * from T1";
        List<ColumnDefinition> actual = MTMVPlanUtil.generateColumnsBySql(querySql, connectContext, null,
                Sets.newHashSet(), Lists.newArrayList(),
                Maps.newHashMap());
        List<ColumnDefinition> expect = Lists.newArrayList(new ColumnDefinition("id", BigIntType.INSTANCE, true),
                new ColumnDefinition("score", BigIntType.INSTANCE, true));
        checkRes(expect, actual);

        querySql = "select T1.id from T1 inner join T2 on T1.id = T2.id";
        actual = MTMVPlanUtil.generateColumnsBySql(querySql, connectContext, null,
                Sets.newHashSet(), Lists.newArrayList(),
                Maps.newHashMap());
        expect = Lists.newArrayList(new ColumnDefinition("id", BigIntType.INSTANCE, true));
        checkRes(expect, actual);

        querySql = "select id,sum(score) from T1 group by id";
        actual = MTMVPlanUtil.generateColumnsBySql(querySql, connectContext, null,
                Sets.newHashSet(), Lists.newArrayList(),
                Maps.newHashMap());
        expect = Lists.newArrayList(new ColumnDefinition("id", BigIntType.INSTANCE, true),
                new ColumnDefinition("__sum_1", BigIntType.INSTANCE, true));
        checkRes(expect, actual);

        querySql = "select id,sum(score) from T1 group by id";
        actual = MTMVPlanUtil.generateColumnsBySql(querySql, connectContext, null,
                Sets.newHashSet(), Lists.newArrayList(new SimpleColumnDefinition("id", null),
                        new SimpleColumnDefinition("sum_score", null)),
                Maps.newHashMap());
        expect = Lists.newArrayList(new ColumnDefinition("id", BigIntType.INSTANCE, true),
                new ColumnDefinition("sum_score", BigIntType.INSTANCE, true));
        checkRes(expect, actual);

        querySql = "select * from MTMVPlanUtilTestT1";
        actual = MTMVPlanUtil.generateColumnsBySql(querySql, connectContext, null,
                Sets.newHashSet(), Lists.newArrayList(),
                Maps.newHashMap());
        expect = Lists.newArrayList(new ColumnDefinition("id", new VarcharType(10), true),
                new ColumnDefinition("score", StringType.INSTANCE, true));
        checkRes(expect, actual);

        querySql = "select score from MTMVPlanUtilTestT1";
        actual = MTMVPlanUtil.generateColumnsBySql(querySql, connectContext, null,
                Sets.newHashSet(), Lists.newArrayList(),
                Maps.newHashMap());
        expect = Lists.newArrayList(
                new ColumnDefinition("score", VarcharType.MAX_VARCHAR_TYPE, true));
        checkRes(expect, actual);
    }

    private void checkRes(List<ColumnDefinition> expect, List<ColumnDefinition> actual) {
        Assert.assertEquals(expect.size(), actual.size());
        for (int i = 0; i < expect.size(); i++) {
            Assert.assertEquals(expect.get(i).getName(), actual.get(i).getName());
            Assert.assertEquals(expect.get(i).getType(), actual.get(i).getType());
        }
    }
}
