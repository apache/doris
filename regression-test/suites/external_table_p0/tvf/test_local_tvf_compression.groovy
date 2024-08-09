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

suite("test_local_tvf_compression", "p0,tvf") {
    List<List<Object>> backends =  sql """ show backends """
    assertTrue(backends.size() > 0)
    def be_id = backends[0][0]
    def dataFilePath = context.config.dataPath + "/external_table_p0/tvf/compress"

    def outFilePath="/compress"

    for (List<Object> backend : backends) {
         def be_host = backend[1]
         scpFiles ("root", be_host, dataFilePath, outFilePath, false);
    }

    String filename = "test_tvf.csv"

    sql """set enable_nereids_planner=true"""
    sql """set enable_fallback_to_original_planner=false"""

    String compress_type = "gz" 
    qt_gz_1 """
    select * from local(
        "file_path" = "${outFilePath}/${filename}.${compress_type}",
        "backend_id" = "${be_id}",
        "format" = "csv",
        "column_separator" = ",",
        "compress_type" ="${compress_type}") order by c1,c2,c3,c4,c5  limit 12;            
    """
    
    qt_gz_2 """
    select * from local(
        "file_path" = "${outFilePath}/${filename}.${compress_type}",
        "backend_id" = "${be_id}",
        "format" = "csv",
        "column_separator" = ",",
        "compress_type" ="${compress_type}") where c1="1" order by c1,c2,c3,c4,c5  limit 12;            
    """



    compress_type = "bz2" 
    qt_bz2_1 """
    select * from local(
        "file_path" = "${outFilePath}/${filename}.${compress_type}",
        "backend_id" = "${be_id}",
        "format" = "csv",
        "column_separator" = ",",
        "compress_type" ="${compress_type}") order by c1,c2,c3,c4,c5 limit 15;            
    """
    qt_bz2_2 """
    select c1,c4 from local(
        "file_path" = "${outFilePath}/${filename}.${compress_type}",
        "backend_id" = "${be_id}",
        "format" = "csv",
        "column_separator" = ",",
        "compress_type" ="${compress_type}") order by cast(c4 as date),c1 limit 15;            
    """




    compress_type = "lz4";
    
    qt_lz4_1 """
    select * from local(
        "file_path" = "${outFilePath}/${filename}.${compress_type}",
        "backend_id" = "${be_id}",
        "format" = "csv",
        "column_separator" = ",",
        "compress_type" ="${compress_type}FRAME") order by c1,c2,c3,c4,c5 limit 20;            
    """
    qt_lz4_2 """
    select c2,c3 from local(
        "file_path" = "${outFilePath}/${filename}.${compress_type}",
        "backend_id" = "${be_id}",
        "format" = "csv",
        "column_separator" = ",",
        "compress_type" ="${compress_type}FRAME")  where c2!="abcsdasdsadsad"  order by cast(c1 as int),c2,c3  limit 20;            
    """



    compress_type = "deflate";
    qt_deflate_1 """ 
        select * from local(
        "file_path" = "${outFilePath}/${filename}.${compress_type}",
        "backend_id" = "${be_id}",
        "format" = "csv",
        "column_separator" = ",",
        "compress_type" ="${compress_type}") order by c1,c2,c3,c4,c5 limit 12 ;            
    """
    qt_deflate_2 """ 
        select c4,count(*) from local(
        "file_path" = "${outFilePath}/${filename}.${compress_type}",
        "backend_id" = "${be_id}",
        "format" = "csv",
        "column_separator" = ",",
        "compress_type" ="${compress_type}") group by c4 order by c4 limit 12 ;            
    """ 
   

    
    compress_type = "snappy";
    qt_snappy_1 """ 
        select * from local(
        "file_path" = "${outFilePath}/${filename}.${compress_type}",
        "backend_id" = "${be_id}",
        "format" = "csv",
        "column_separator" = ",",
        "compress_type" ="${compress_type}block") order by c1,c2,c3,c4,c5  limit 22 ;            
    """    
    qt_snappy_2 """ 
        select c2,c3 from local(
        "file_path" = "${outFilePath}/${filename}.${compress_type}",
        "backend_id" = "${be_id}",
        "format" = "csv",
        "column_separator" = ",",
        "compress_type" ="${compress_type}block") where c2="abcd" order by c3 limit 22 ;            
    """

    // test empty snapppy file
    qt_snappy_empty """ 
        select * from local(
        "file_path" = "${outFilePath}/test_empty_snappy.${compress_type}",
        "backend_id" = "${be_id}",
        "format" = "csv",
        "column_separator" = ",",
        "compress_type" ="${compress_type}block");            
    """

    // test error case
    test {
        sql """
        select count(*) from local(
            "file_path" = "../be.out",
            "backend_id" = "${be_id}",
            "column_separator" = ",",
            "format" = "csv")
        where c1 like "%FE type%";
        """
        // check exception message contains
        exception "can not contain '..' in path"
    }

    test {
        sql """
        select count(*) from local(
            "file_path" = "./xx.out",
            "backend_id" = "${be_id}",
            "column_separator" = ",",
            "format" = "csv")
        where c1 like "%FE type%";
        """
        // check exception message contains
        exception "No matches found"
    }
    
}
