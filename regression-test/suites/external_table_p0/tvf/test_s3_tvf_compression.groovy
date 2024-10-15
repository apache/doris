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

suite("test_s3_tvf_compression", "p0") {
    
    String ak = getS3AK()
    String sk = getS3SK()
    String s3_endpoint = getS3Endpoint()
    String region = getS3Region()
    String bucket = context.config.otherConfigs.get("s3BucketName");
    
    sql """ set query_timeout=3600; """ 

    String compress_type = "gz" 
    qt_gz_1 """ 
    select * from 
    s3(     
        "URI" = "https://${bucket}.${s3_endpoint}/regression/tvf/compression/test_tvf.csv.${compress_type}",    
        "s3.access_key" = "${ak}",     
        "s3.secret_key" = "${sk}",     
        "REGION" = "${region}",    
        "FORMAT" = "csv",
        "column_separator" = ",",
        "use_path_style" = "false", -- aliyun does not support path_style
        "compress_type" ="${compress_type}") order by c1,c2,c3,c4,c5 limit 20;
    """


    qt_gz_2 """ 
    select c1,c4 from 
    s3(     
        "URI" = "https://${bucket}.${s3_endpoint}/regression/tvf/compression/test_tvf.csv.${compress_type}",    
        "s3.access_key" = "${ak}",     
        "s3.secret_key" = "${sk}",     
        "REGION" = "${region}",    
        "FORMAT" = "csv",
        "column_separator" = ",",
        "use_path_style" = "false", -- aliyun does not support path_style
        "compress_type" ="${compress_type}") order by cast(c1 as int),c4 limit 20;
    """



    compress_type = "bz2";
    qt_bz2_1 """ 
    select * from 
    s3(     
        "URI" = "https://${bucket}.${s3_endpoint}/regression/tvf/compression/test_tvf.csv.${compress_type}",    
        "s3.access_key" = "${ak}",     
        "s3.secret_key" = "${sk}",
        "REGION" = "${region}",    
        "FORMAT" = "csv",
        "column_separator" = ",",
        "use_path_style" = "false", -- aliyun does not support path_style
        "compress_type" ="${compress_type}") order by c1,c2,c3,c4,c5 limit 15;
    """


    qt_bz2_2 """ 
    select c1,c4 from 
    s3(     
        "URI" = "https://${bucket}.${s3_endpoint}/regression/tvf/compression/test_tvf.csv.${compress_type}",    
        "s3.access_key" = "${ak}",     
        "s3.secret_key" = "${sk}",
        "REGION" = "${region}",    
        "FORMAT" = "csv",
        "column_separator" = ",",
        "use_path_style" = "false", -- aliyun does not support path_style
        "compress_type" ="${compress_type}")  where c1!="100"  order by cast(c4 as date),c1 limit 13;
    """



    compress_type = "lz4";
    qt_lz4_1 """ 
    select * from 
    s3(     
        "URI" = "https://${bucket}.${s3_endpoint}/regression/tvf/compression/test_tvf.csv.${compress_type}",    
        "s3.access_key" = "${ak}",     
        "s3.secret_key" = "${sk}",     
        "REGION" = "${region}",    
        "FORMAT" = "csv",
        "column_separator" = ",",
        "use_path_style" = "false", -- aliyun does not support path_style
        "compress_type" ="${compress_type}FRAME") order by c1,c2,c3,c4,c5  limit 14;
    """
    

    qt_lz4_2 """ 
    select c1,c3 from 
    s3(     
        "URI" = "https://${bucket}.${s3_endpoint}/regression/tvf/compression/test_tvf.csv.${compress_type}",    
        "s3.access_key" = "${ak}",     
        "s3.secret_key" = "${sk}",     
        "REGION" = "${region}",    
        "FORMAT" = "csv",
        "column_separator" = ",",
        "use_path_style" = "false", -- aliyun does not support path_style
        "compress_type" ="${compress_type}FRAME")  where c3="buHDwfGeNHfpRFdNaogneddi" order by c3,c1  limit 14;
    """


    String select_field = "c1,c12,c23,c40";
    String orderBy_limit = "order by c1,c12,c23,c40  limit 17 ";

    compress_type = "deflate";
    qt_deflate_1 """ 
    select ${select_field} from 
    s3(     
        "URI" = "https://${bucket}.${s3_endpoint}/regression/tvf/compression/000000_0.${compress_type}",    
        "s3.access_key" = "${ak}",     
        "s3.secret_key" = "${sk}",     
        "REGION" = "${region}",    
        "FORMAT" = "csv",
        "use_path_style" = "false", -- aliyun does not support path_style
        "column_separator" = '\001',
        "compress_type" ="${compress_type}") ${orderBy_limit};
    """

    qt_deflate_2 """ 
    select c1,c2 from 
    s3(     
        "URI" = "https://${bucket}.${s3_endpoint}/regression/tvf/compression/000000_0.${compress_type}",    
        "s3.access_key" = "${ak}",     
        "s3.secret_key" = "${sk}",     
        "REGION" = "${region}",    
        "FORMAT" = "csv",
        "column_separator" = '\001',
        "use_path_style" = "false", -- aliyun does not support path_style
        "compress_type" ="${compress_type}") group by c1,c2  order by c1,c2 limit 5;
    """



   
    compress_type = "snappy";
    qt_snappy_1 """ 
    select ${select_field} from 
    s3(     
        "URI" = "https://${bucket}.${s3_endpoint}/regression/tvf/compression/000000_0.${compress_type}",    
        "s3.access_key" = "${ak}",     
        "s3.secret_key" = "${sk}",     
        "REGION" = "${region}",    
        "FORMAT" = "csv",
        "use_path_style" = "false", -- aliyun does not support path_style
        "column_separator" = '\001',
        "compress_type" ="${compress_type}block") ${orderBy_limit};
    """


    qt_snappy_2 """ 
    select count(*) from 
    s3(     
        "URI" = "https://${bucket}.${s3_endpoint}/regression/tvf/compression/000000_0.${compress_type}",    
        "s3.access_key" = "${ak}",     
        "s3.secret_key" = "${sk}",     
        "REGION" = "${region}",    
        "FORMAT" = "csv",
        "use_path_style" = "false", -- aliyun does not support path_style
        "column_separator" = '\001',
        "compress_type" ="${compress_type}block") where c2 ="abccc";
    """
}
