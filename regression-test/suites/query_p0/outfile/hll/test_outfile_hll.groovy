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

import org.apache.doris.regression.util.ExportTestHelper

suite("test_outfile_hll") {
    sql "set return_object_data_as_binary=true"
    
    sql "DROP TABLE IF EXISTS h_table"
    sql """
    create table h_table(
        k1 int null,
        k2 hll hll_union
    )
    aggregate key (k1)
    distributed BY hash(k1) buckets 3
    properties("replication_num" = "1");
    """
    sql "insert into h_table values(1,hll_hash('a'));"
    sql "insert into h_table values(1,hll_hash('b'));"
    sql "insert into h_table values(1,hll_hash('b'));"
    sql "insert into h_table values(2,hll_hash('a'));"

    qt_test "select k1,hll_union_agg(k2) from h_table group by k1 order by k1;"

    sql "DROP TABLE IF EXISTS h_table2"
    sql """
    create table h_table2(
        k1 int null,
        k2 hll hll_union
    )
    aggregate key (k1)
    distributed BY hash(k1) buckets 3
    properties("replication_num" = "1");
    """

    ExportTestHelper.withParquetOutfile(delegate, "select k1, cast(hll_to_base64(k2) as string) as tmp from h_table") { filePath ->
        streamLoad {
            table "h_table2"
            set "format", "parquet"
            set "columns", "k1, tmp, k2=hll_from_base64(tmp)"
            file filePath
        }
    }
    qt_test "select k1,hll_union_agg(k2) from h_table2 group by k1 order by k1;"

}
