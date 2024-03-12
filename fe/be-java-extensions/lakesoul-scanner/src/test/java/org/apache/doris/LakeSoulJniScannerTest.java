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

package org.apache.doris;

import org.apache.doris.common.jni.utils.OffHeap;
import org.apache.doris.common.jni.vec.VectorTable;
import org.apache.doris.lakesoul.LakeSoulJniScanner;
import org.apache.doris.lakesoul.LakeSoulUtils;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

public class LakeSoulJniScannerTest {
    @Test
    public void testLakeSoulJniScanner() throws IOException {
        OffHeap.setTesting();
        HashMap<String, String> params = new HashMap<>();
        params.put(LakeSoulUtils.FILE_NAMES, String.join(LakeSoulUtils.LIST_DELIM, Arrays.asList(
            "/Users/ceng/Desktop/lakesoul_table/part-00000-320f2612-7f95-40f0-8f63-3f3a50b5d1a8-c000.parquet",
            "/Users/ceng/Desktop/lakesoul_table/part-00001-ef4578c0-37b3-483c-832a-72ff0a83acd7-c000.parquet"
        )));
        params.put(
            LakeSoulUtils.PRIMARY_KEYS,
            String.join(LakeSoulUtils.LIST_DELIM, Arrays.asList())
        );

        String schema = "{\n" +
            "  \"fields\" : [ {\n" +
            "    \"name\" : \"name\",\n" +
            "    \"nullable\" : true,\n" +
            "    \"type\" : {\n" +
            "      \"name\" : \"utf8\"\n" +
            "    },\n" +
            "    \"children\" : [ ]\n" +
            "  }, {\n" +
            "    \"name\" : \"age\",\n" +
            "    \"nullable\" : true,\n" +
            "    \"type\" : {\n" +
            "      \"name\" : \"int\",\n" +
            "      \"bitWidth\" : 32,\n" +
            "      \"isSigned\" : true\n" +
            "    },\n" +
            "    \"children\" : [ ]\n" +
            "  }, {\n" +
            "    \"name\" : \"birth\",\n" +
            "    \"nullable\" : true,\n" +
            "    \"type\" : {\n" +
            "      \"name\" : \"date\",\n" +
            "      \"unit\" : \"DAY\"\n" +
            "    },\n" +
            "    \"children\" : [ ]\n" +
            "  }, {\n" +
            "    \"name\" : \"grad\",\n" +
            "    \"nullable\" : true,\n" +
            "    \"type\" : {\n" +
            "      \"name\" : \"bool\"\n" +
            "    },\n" +
            "    \"children\" : [ ]\n" +
            "  }, {\n" +
            "    \"name\" : \"num\",\n" +
            "    \"nullable\" : true,\n" +
            "    \"type\" : {\n" +
            "      \"name\" : \"int\",\n" +
            "      \"bitWidth\" : 64,\n" +
            "      \"isSigned\" : true\n" +
            "    },\n" +
            "    \"children\" : [ ]\n" +
            "  }, {\n" +
            "    \"name\" : \"sal\",\n" +
            "    \"nullable\" : true,\n" +
            "    \"type\" : {\n" +
            "      \"name\" : \"floatingpoint\",\n" +
            "      \"precision\" : \"DOUBLE\"\n" +
            "    },\n" +
            "    \"children\" : [ ]\n" +
            "  }, {\n" +
            "    \"name\" : \"birthstamp\",\n" +
            "    \"nullable\" : true,\n" +
            "    \"type\" : {\n" +
            "      \"name\" : \"timestamp\",\n" +
            "      \"unit\" : \"MICROSECOND\",\n" +
            "      \"timezone\" : \"UTC\"\n" +
            "    },\n" +
            "    \"children\" : [ ]\n" +
            "  }, {\n" +
            "    \"name\" : \"decmal\",\n" +
            "    \"nullable\" : true,\n" +
            "    \"type\" : {\n" +
            "      \"name\" : \"decimal\",\n" +
            "      \"precision\" : 10,\n" +
            "      \"scale\" : 3,\n" +
            "      \"bitWidth\" : 128\n" +
            "    },\n" +
            "    \"children\" : [ ]\n" +
            "  }, {\n" +
            "    \"name\" : \"namevar\",\n" +
            "    \"nullable\" : true,\n" +
            "    \"type\" : {\n" +
            "      \"name\" : \"utf8\"\n" +
            "    },\n" +
            "    \"children\" : [ ]\n" +
            "  } ]\n" +
            "}";
        params.put(LakeSoulUtils.SCHEMA_JSON, schema);
//        params.put(LakeSoulUtils.PARTITION_DESC, "range=range");
        params.put(LakeSoulUtils.REQUIRED_FIELDS, String.join(LakeSoulUtils.LIST_DELIM, Arrays.asList("name", "age", "birth", "grad", "num", "sal", "birthstamp", "decmal", "namevar")));

        LakeSoulJniScanner scanner = new LakeSoulJniScanner(1024, params);
        scanner.open();
        long metaAddress = 0;
        do {
            metaAddress = scanner.getNextBatchMeta();
            if (metaAddress != 0) {
                long rows = OffHeap.getLong(null, metaAddress);

                VectorTable restoreTable = VectorTable.createReadableTable(scanner.getTable().getColumnTypes(),
                    scanner.getTable().getFields(), metaAddress);
                System.out.println(restoreTable.dump((int) rows));
                // Restored table is release by the origin table.
            }

        } while (metaAddress != 0);
        scanner.close();
    }
}
