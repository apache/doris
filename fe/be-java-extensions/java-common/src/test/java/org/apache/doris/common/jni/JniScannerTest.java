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

package org.apache.doris.common.jni;


import org.apache.doris.common.jni.utils.OffHeap;
import org.apache.doris.common.jni.vec.VectorTable;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;

public class JniScannerTest {
    @Test
    public void testMockJniScanner() throws IOException {
        OffHeap.setTesting();
        MockJniScanner scanner = new MockJniScanner(32, new HashMap<String, String>() {
            {
                put("mock_rows", "128");
                put("required_fields", "boolean,tinyint,smallint,int,bigint,largeint,float,double,"
                        + "date,timestamp,char,varchar,string,decimalv2,decimal64,array,map,struct");
                put("columns_types", "boolean#tinyint#smallint#int#bigint#largeint#float#double#"
                        + "date#timestamp#char(10)#varchar(10)#string#decimalv2(12,4)#decimal64(10,3)#"
                        + "array<array<string>>#map<string,array<int>>#struct<col1:timestamp(6),col2:array<char(10)>>");
            }
        });
        scanner.open();
        long metaAddress = 0;
        do {
            metaAddress = scanner.getNextBatchMeta();
            if (metaAddress != 0) {
                long rows = OffHeap.getLong(null, metaAddress);
                Assert.assertEquals(32, rows);

                VectorTable restoreTable = new VectorTable(scanner.getTable().getColumnTypes(),
                        scanner.getTable().getFields(), metaAddress);
                System.out.println(restoreTable.dump((int) rows).substring(0, 128));
                // Restored table is release by the origin table.
            }
            scanner.resetTable();
        } while (metaAddress != 0);
        scanner.releaseTable();
        scanner.close();
    }
}
