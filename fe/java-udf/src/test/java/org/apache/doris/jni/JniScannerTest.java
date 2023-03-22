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

package org.apache.doris.jni;

import org.apache.doris.jni.utils.OffHeap;

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
                put("required_fields", "boolean,tinyint,smallint,int,bigint,float,double,"
                        + "date,timestamp,char,varchar,string,decimalv2,decimal64");
                put("columns_types", "boolean#tinyint#smallint#int#bigint#float#double#"
                        + "date#timestamp#char(10)#varchar(10)#string#decimalv2(12,4)#decimal64(10,3)");
            }
        });
        StringBuilder result = new StringBuilder();
        scanner.open();
        long metaAddress = 0;
        do {
            metaAddress = scanner.getNextBatchMeta();
            if (metaAddress != 0) {
                result.append(scanner.getTable().dump(32));
                long rows = OffHeap.getLong(null, metaAddress);
                Assert.assertEquals(32, rows);
            }
            scanner.resetTable();
        } while (metaAddress != 0);
        scanner.releaseTable();
        scanner.close();
        System.out.print(result);
    }
}
