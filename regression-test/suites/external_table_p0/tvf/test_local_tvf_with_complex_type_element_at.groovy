import org.junit.Assert

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

// This suit test the `backends` tvf
suite("test_local_tvf_with_complex_type_element_at","external,hive,tvf,external_docker") {
    List<List<Object>> backends =  sql """ show backends """
    assertTrue(backends.size() > 0)
    def be_id = backends[0][0]
    def dataFilePath = context.config.dataPath + "/external_table_p0/tvf/"


        // cluster mode need to make sure all be has this data
        def outFilePath="/"
        def transFile01="${dataFilePath}/t.orc"
        def transFile02="${dataFilePath}/t.parquet"
        for (List<Object> backend : backends) {
            def be_host = backend[1]
            scpFiles ("root", be_host, transFile01, outFilePath, false);
            scpFiles ("root", be_host, transFile02, outFilePath, false);
        }


    /**
     * here is file schema
     *        var schema = StructType(
                     StructField("id", IntegerType, true) ::
                     StructField("arr_arr", ArrayType(ArrayType(StringType), true), true)::
                     StructField("arr_map", ArrayType(MapType(StringType, DateType)), true) ::
                     StructField("arr_struct", ArrayType(StructType(StructField("vin", StringType, true)::StructField("charge_id", IntegerType, true)::Nil))) ::
                     StructField("map_map", MapType(StringType, MapType(StringType, DoubleType)), true)::
                     StructField("map_arr", MapType(IntegerType, ArrayType(DoubleType)), true)::
                     StructField("map_struct", MapType(TimestampType, StructType(StructField("vin", StringType, true)::StructField("charge_id", IntegerType, true)::StructField("start_time", DoubleType, true)::Nil), true))::
                     StructField("struct_arr_map", StructType(StructField("aa", ArrayType(StringType), true)::StructField("mm", MapType(DateType, StringType), true)::Nil))::
                     Nil
                     )
     */

    qt_sql """
        select * from local(
            "file_path" = "${outFilePath}/t.orc",
            "backend_id" = "${be_id}",
            "format" = "orc");"""

    qt_sql """
            select count(*) from local(
                "file_path" = "${outFilePath}/t.orc",
                "backend_id" = "${be_id}",
                "format" = "orc");"""

    qt_sql """ select arr_arr[1][1] from local (
                "file_path" = "${outFilePath}/t.orc",
                "backend_id" = "${be_id}",          
                "format" = "orc");"""

    qt_sql """ select arr_map[1] from local (
                "file_path" = "${outFilePath}/t.orc",
                "backend_id" = "${be_id}",          
                "format" = "orc");"""
    qt_sql """ select arr_map[1]["WdTnFb-LHW8Nel-laB-HCQA"] from local (
                "file_path" = "${outFilePath}/t.orc",
                "backend_id" = "${be_id}",          
                "format" = "orc");"""

    qt_sql """ select map_map["W1iF16-DE1gzJx-avC-Mrf6"]["HJVQSC-46l3xm7-J6c-moIH"] from local (
                "file_path" = "${outFilePath}/t.orc",
                "backend_id" = "${be_id}",          
                "format" = "orc");"""

    qt_sql """ select map_arr[1] from local (
                "file_path" = "${outFilePath}/t.orc",
                "backend_id" = "${be_id}",          
                "format" = "orc");"""
    qt_sql """ select map_arr[1][7] from local (
                "file_path" = "${outFilePath}/t.orc",
                "backend_id" = "${be_id}",          
                "format" = "orc");"""

    qt_sql """
        select * from local(
            "file_path" = "${outFilePath}/t.parquet",
            "backend_id" = "${be_id}",
            "format" = "parquet"); """

   qt_sql """
            select count(*) from local(
                "file_path" = "${outFilePath}/t.parquet",
                "backend_id" = "${be_id}",
                "format" = "parquet"); """


    qt_sql """ select arr_arr[1][1] from local (
                "file_path" = "${outFilePath}/t.parquet",
                "backend_id" = "${be_id}",          
                "format" = "parquet");"""

    qt_sql """ select arr_map[1] from local (
                "file_path" = "${outFilePath}/t.parquet",
                "backend_id" = "${be_id}",          
                "format" = "parquet");"""
    qt_sql """ select arr_map[1]["WdTnFb-LHW8Nel-laB-HCQA"] from local (
                "file_path" = "${outFilePath}/t.parquet",
                "backend_id" = "${be_id}",          
                "format" = "parquet");"""

    qt_sql """ select map_map["W1iF16-DE1gzJx-avC-Mrf6"]["HJVQSC-46l3xm7-J6c-moIH"] from local (
                "file_path" = "${outFilePath}/t.parquet",
                "backend_id" = "${be_id}",          
                "format" = "parquet");"""

    qt_sql """ select map_arr[1] from local (
                "file_path" = "${outFilePath}/t.parquet",
                "backend_id" = "${be_id}",          
                "format" = "parquet");"""
    qt_sql """ select map_arr[1][7] from local (
                "file_path" = "${outFilePath}/t.parquet",
                "backend_id" = "${be_id}",          
                "format" = "parquet");"""
}
