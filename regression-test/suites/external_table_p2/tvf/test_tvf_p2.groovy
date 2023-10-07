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

suite("test_tvf_p2", "p2") {
    String enabled = context.config.otherConfigs.get("enableExternalHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String nameNodeHost = context.config.otherConfigs.get("extHiveHmsHost")
        String hdfsPort = context.config.otherConfigs.get("extHdfsPort")

        qt_eof_check """select * from hdfs(
            "uri" = "hdfs://${nameNodeHost}:${hdfsPort}/catalog/tvf/parquet/bad_store_sales.parquet",
            "format" = "parquet")
            where ss_store_sk = 4 and ss_addr_sk is null order by ss_item_sk"""

        // array_ancestor_null.parquet is parquet file whose values in the array column are all nulls in a page
        qt_array_ancestor_null """select count(list_double_col) from hdfs(
            "uri" = "hdfs://${nameNodeHost}:${hdfsPort}/catalog/tvf/parquet/array_ancestor_null.parquet",
            "format" = "parquet");
        """

        // all_nested_types.parquet is parquet file that contains all complext types
        qt_nested_types_parquet """select count(array0), count(array1), count(array2), count(array3), count(struct0), count(struct1), count(map0)
            from hdfs(
            "uri" = "hdfs://${nameNodeHost}:${hdfsPort}/catalog/tvf/parquet/all_nested_types.parquet",
            "format" = "parquet");
        """

        // all_nested_types.orc is orc file that contains all complext types
        qt_nested_types_orc """select count(array0), count(array1), count(array2), count(array3), count(struct0), count(struct1), count(map0)
            from hdfs(
            "uri" = "hdfs://${nameNodeHost}:${hdfsPort}/catalog/tvf/orc/all_nested_types.orc",
            "format" = "orc");
        """

        // a row of complex type may be stored across more pages
        qt_row_cross_pages """select count(id), count(m1), count(m2)
            from hdfs(
            "uri" = "hdfs://${nameNodeHost}:${hdfsPort}/catalog/tvf/parquet/row_cross_pages.parquet",
            "format" = "parquet");
        """

        // viewfs
        qt_viewfs """select count(id), count(m1), count(m2)
            from hdfs(
            "uri" = "viewfs://my-cluster/ns1/catalog/tvf/parquet/row_cross_pages.parquet",
            "format" = "parquet",
            "fs.viewfs.mounttable.my-cluster.link./ns1" = "hdfs://${nameNodeHost}:${hdfsPort}/",
            "fs.viewfs.mounttable.my-cluster.homedir" = "/ns1")"""
    }
}
