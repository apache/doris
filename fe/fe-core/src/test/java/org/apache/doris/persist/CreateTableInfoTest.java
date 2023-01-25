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

package org.apache.doris.persist;

import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FakeEnv;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.RandomDistributionInfo;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.SinglePartitionInfo;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.thrift.TStorageType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class CreateTableInfoTest {
    private Env env;

    private FakeEnv fakeEnv;

    @Before
    public void setUp() {
        fakeEnv = new FakeEnv();
        env = Deencapsulation.newInstance(Env.class);

        FakeEnv.setEnv(env);
        FakeEnv.setMetaVersion(FeConstants.meta_version);
    }

    @Test
    public void testSerialization() throws Exception {
        // 1. Write objects to file
        final Path path = Files.createTempFile("createTableInfo", "tmp");
        DataOutputStream dos = new DataOutputStream(Files.newOutputStream(path));

        Column column2 = new Column("column2",
                ScalarType.createType(PrimitiveType.TINYINT), false, AggregateType.MIN, "", "");
        ImmutableList<Column> columns = ImmutableList.<Column>builder()
                .add(column2)
                .add(new Column("column3", ScalarType.createType(PrimitiveType.SMALLINT), false, AggregateType.SUM, "", ""))
                .add(new Column("column4", ScalarType.createType(PrimitiveType.INT), false, AggregateType.REPLACE, "", ""))
                .add(new Column("column5", ScalarType.createType(PrimitiveType.BIGINT), false, AggregateType.REPLACE, "", ""))
                .add(new Column("column6", ScalarType.createType(PrimitiveType.FLOAT), false, AggregateType.REPLACE, "", ""))
                .add(new Column("column7", ScalarType.createType(PrimitiveType.DOUBLE), false, AggregateType.REPLACE, "", ""))
                .add(new Column("column8", ScalarType.createChar(10), true, null, "", ""))
                .add(new Column("column9", ScalarType.createVarchar(10), true, null, "", ""))
                .add(new Column("column10", ScalarType.createType(PrimitiveType.DATE), true, null, "", ""))
                .add(new Column("column11", ScalarType.createType(PrimitiveType.DATETIME), true, null, "", ""))
                .build();

        MaterializedIndex index = new MaterializedIndex(1, IndexState.NORMAL);
        RandomDistributionInfo distributionInfo = new RandomDistributionInfo(10);
        Partition partition = new Partition(20000L, "table", index, distributionInfo);
        OlapTable table = new OlapTable(1000L, "table", columns, KeysType.AGG_KEYS,
                                        new SinglePartitionInfo(), distributionInfo);
        short shortKeyColumnCount = 1;
        table.setIndexMeta(1000, "group1", columns, 1, 1, shortKeyColumnCount, TStorageType.COLUMN, KeysType.AGG_KEYS);

        List<Column> column = Lists.newArrayList();
        column.add(column2);
        table.setIndexMeta(1L, "test", column, 1, 1, shortKeyColumnCount,
                TStorageType.COLUMN, KeysType.AGG_KEYS);
        Deencapsulation.setField(table, "baseIndexId", 1000);
        table.addPartition(partition);
        CreateTableInfo info = new CreateTableInfo("db1", table);
        info.write(dos);

        dos.flush();
        dos.close();

        // 2. Read objects from file
        DataInputStream dis = new DataInputStream(Files.newInputStream(path));

        CreateTableInfo rInfo1 = CreateTableInfo.read(dis);
        Assert.assertEquals(rInfo1.getTable(), table);
        Assert.assertEquals(rInfo1, info);
        Assert.assertEquals(rInfo1.getDbName(), "db1");

        // 3. delete files
        dis.close();
        Files.delete(path);
    }
}
