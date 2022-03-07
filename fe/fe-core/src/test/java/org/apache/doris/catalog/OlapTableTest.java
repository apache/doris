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

import mockit.Mock;
import mockit.MockUp;

import org.apache.doris.analysis.IndexDef;
import org.apache.doris.catalog.Table.TableType;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.io.FastByteArrayOutputStream;
import org.apache.doris.common.util.UnitTestUtil;

import com.google.common.collect.Lists;

import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

public class OlapTableTest {

    @Test
    public void test() throws IOException {
        
        new MockUp<Catalog>() {
            @Mock
            int getCurrentCatalogJournalVersion() {
                return FeConstants.meta_version;
            }
        };

        Database db = UnitTestUtil.createDb(1, 2, 3, 4, 5, 6, 7);
        List<Table> tables = db.getTables();
        
        for (Table table : tables) {
            if (table.getType() != TableType.OLAP) {
                continue;
            }
            OlapTable tbl = (OlapTable) table;
            tbl.setIndexes(Lists.newArrayList(new Index("index", Lists.newArrayList("col"), IndexDef.IndexType.BITMAP
                    , "xxxxxx")));
            System.out.println("orig table id: " + tbl.getId());

            FastByteArrayOutputStream byteArrayOutputStream = new FastByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(byteArrayOutputStream);
            tbl.write(out);

            out.flush();
            out.close();

            DataInputStream in = new DataInputStream(byteArrayOutputStream.getInputStream());
            Table copiedTbl = OlapTable.read(in);
            System.out.println("copied table id: " + copiedTbl.getId());
        }
        
    }

}
