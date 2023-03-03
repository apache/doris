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
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.jmockit.Deencapsulation;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;

public class RefreshExternalTableInfoTest {
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
        File file = new File("./RefreshExteranlTableInfo");
        file.createNewFile();
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));

        List<Column> columns = new ArrayList<Column>();
        Column column2 = new Column("column2",
                ScalarType.createType(PrimitiveType.TINYINT), false, AggregateType.MIN, "", "");
        columns.add(column2);
        columns.add(new Column("column3",
                ScalarType.createType(PrimitiveType.SMALLINT), false, AggregateType.SUM, "", ""));
        columns.add(new Column("column4",
                ScalarType.createType(PrimitiveType.INT), false, AggregateType.REPLACE, "", ""));
        columns.add(new Column("column5",
                ScalarType.createType(PrimitiveType.BIGINT), false, AggregateType.REPLACE, "", ""));
        columns.add(new Column("column6",
                ScalarType.createType(PrimitiveType.FLOAT), false, AggregateType.REPLACE, "", ""));
        columns.add(new Column("column7",
                ScalarType.createType(PrimitiveType.DOUBLE), false, AggregateType.REPLACE, "", ""));
        columns.add(new Column("column8", ScalarType.createChar(10), true, null, "", ""));
        columns.add(new Column("column9", ScalarType.createVarchar(10), true, null, "", ""));
        columns.add(new Column("column10", ScalarType.createType(PrimitiveType.DATE), true, null, "", ""));
        columns.add(new Column("column11", ScalarType.createType(PrimitiveType.DATETIME), true, null, "", ""));

        RefreshExternalTableInfo info = new RefreshExternalTableInfo("db1", "table1", columns);
        info.write(dos);

        dos.flush();
        dos.close();

        // 2. Read objects from file
        DataInputStream dis = new DataInputStream(new FileInputStream(file));

        RefreshExternalTableInfo rInfo1 = RefreshExternalTableInfo.read(dis);
        Assert.assertEquals(rInfo1.getDbName(), info.getDbName());
        Assert.assertEquals(rInfo1.getTableName(), info.getTableName());
        Assert.assertEquals(rInfo1.getNewSchema(), info.getNewSchema());

        // 3. delete files
        dis.close();
        file.delete();
    }
}
