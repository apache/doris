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

import org.apache.doris.thrift.TStorageType;

import com.google.common.collect.Lists;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

public class MaterializedIndexMetaTest {

    private static String fileName = "./MaterializedIndexMetaSerializeTest";

    @After
    public void tearDown() {
        File file = new File(fileName);
        file.delete();
    }

    @Test
    public void testSerializeMaterializedIndexMeta() throws IOException {
        // 1. Write objects to file
        File file = new File(fileName);
        file.createNewFile();
        DataOutputStream out = new DataOutputStream(new FileOutputStream(file));

        List<Column> schema = Lists.newArrayList();
        schema.add(new Column("k1", Type.TINYINT, true, null, true, "1", "abc"));
        schema.add(new Column("k2", Type.SMALLINT, true, null, true, "1", "debug"));
        schema.add(new Column("k3", Type.INT, true, null, true, "1", ""));
        schema.add(new Column("k4", Type.BIGINT, true, null, true, "1", "**"));
        schema.add(new Column("k5", Type.LARGEINT, true, null, true, null, ""));
        schema.add(new Column("k6", Type.DOUBLE, true, null, true, "1.1", ""));
        schema.add(new Column("k7", Type.FLOAT, true, null, true, "1", ""));
        schema.add(new Column("k8", Type.DATE, true, null, true, "1", ""));
        schema.add(new Column("k9", Type.DATETIME, true, null, true, "1", ""));
        schema.add(new Column("k10", Type.VARCHAR, true, null, true, "1", ""));
        schema.add(new Column("k11", Type.DECIMALV2, true, null, true, "1", ""));
        schema.add(new Column("k12", Type.INT, true, null, true, "1", ""));
        schema.add(new Column("v1", Type.INT, true, AggregateType.SUM, true, "1", ""));
        schema.add(new Column("v1", Type.VARCHAR, true, AggregateType.REPLACE, true, "1", ""));
        short shortKeyColumnCount = 1;
        MaterializedIndexMeta indexMeta = new MaterializedIndexMeta(1, schema, 1, 1, shortKeyColumnCount,
                TStorageType.COLUMN, KeysType.DUP_KEYS);
        indexMeta.write(out);
        out.flush();
        out.close();

        // 2. Read objects from file
        DataInputStream in = new DataInputStream(new FileInputStream(file));

        MaterializedIndexMeta readIndexMeta = MaterializedIndexMeta.read(in);
        Assert.assertEquals(indexMeta, readIndexMeta);
    }
}
