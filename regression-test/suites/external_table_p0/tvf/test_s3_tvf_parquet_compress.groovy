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

suite("test_s3_tvf_parquet_compress", "p0") {
//parquet data page v2 test 


    String ak = getS3AK()
    String sk = getS3SK()
    

    String s3_endpoint = getS3Endpoint()
    String region = getS3Region()
    String bucket = context.config.otherConfigs.get("s3BucketName");

    order_qt_gzip_1 """
    select * from 
            s3(     
                "URI" = "https://${bucket}.${s3_endpoint}/regression/datalake/pipeline_data/data_page_v2_gzip.parquet", 
                "s3.access_key" = "${ak}",     
                "s3.secret_key" = "${sk}",     
                "REGION" = "${region}",    
                "FORMAT" = "parquet"
                )where col_bigint > 607 and col_string = 'CyGmiNdrY' order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_decimal,col_date,col_timestamp limit 10;
    """
    order_qt_gzip_2 """
    select * from 
            s3(     
                "URI" = "https://${bucket}.${s3_endpoint}/regression/datalake/pipeline_data/data_page_v2_gzip.parquet", 
                "s3.access_key" = "${ak}",     
                "s3.secret_key" = "${sk}",     
                "REGION" = "${region}",    
                "FORMAT" = "parquet"
                )where col_bigint > 752 and  col_float = 0.3 order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_decimal,col_date,col_timestamp limit 10;
    """
    order_qt_gzip_3 """
    select * from 
            s3(     
                "URI" = "https://${bucket}.${s3_endpoint}/regression/datalake/pipeline_data/data_page_v2_gzip.parquet", 
                "s3.access_key" = "${ak}",     
                "s3.secret_key" = "${sk}",     
                "REGION" = "${region}",    
                "FORMAT" = "parquet"
                )where col_char = 'TaOCwaPcNfGeNxMqi' and col_boolean = 680 and col_int = 1953 order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_decimal,col_date,col_timestamp limit 10;
    """
    order_qt_gzip_4 """
    select * from 
            s3(     
                "URI" = "https://${bucket}.${s3_endpoint}/regression/datalake/pipeline_data/data_page_v2_gzip.parquet", 
                "s3.access_key" = "${ak}",     
                "s3.secret_key" = "${sk}",     
                "REGION" = "${region}",    
                "FORMAT" = "parquet"
                )where col_tinyint < 1423 order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_decimal,col_date,col_timestamp limit 10;
    """
    order_qt_gzip_5 """
    select * from 
            s3(     
                "URI" = "https://${bucket}.${s3_endpoint}/regression/datalake/pipeline_data/data_page_v2_gzip.parquet", 
                "s3.access_key" = "${ak}",     
                "s3.secret_key" = "${sk}",     
                "REGION" = "${region}",    
                "FORMAT" = "parquet"
                )where  col_float = 2.5 order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_decimal,col_date,col_timestamp limit 10;
    """
    order_qt_lz42_1 """
    select * from 
            s3(     
                "URI" = "https://${bucket}.${s3_endpoint}/regression/datalake/pipeline_data/data_page_v2_lz42.parquet", 
                "s3.access_key" = "${ak}",     
                "s3.secret_key" = "${sk}",     
                "REGION" = "${region}",    
                "FORMAT" = "parquet"
                )where  col_decimal = 0.8 order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_decimal,col_date,col_timestamp limit 10;
    """
    order_qt_lz42_2 """
    select * from 
            s3(     
                "URI" = "https://${bucket}.${s3_endpoint}/regression/datalake/pipeline_data/data_page_v2_lz42.parquet", 
                "s3.access_key" = "${ak}",     
                "s3.secret_key" = "${sk}",     
                "REGION" = "${region}",    
                "FORMAT" = "parquet"
                )where col_char = 'MVAhYjsZf' and  col_decimal < 2.3 and  col_float = 2.7 order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_decimal,col_date,col_timestamp limit 10;
    """
    order_qt_lz42_3 """
    select * from 
            s3(     
                "URI" = "https://${bucket}.${s3_endpoint}/regression/datalake/pipeline_data/data_page_v2_lz42.parquet", 
                "s3.access_key" = "${ak}",     
                "s3.secret_key" = "${sk}",     
                "REGION" = "${region}",    
                "FORMAT" = "parquet"
                )where col_tinyint < 32 and col_char = 'Ja' order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_decimal,col_date,col_timestamp limit 10;
    """
    order_qt_lz42_4 """
    select * from 
            s3(     
                "URI" = "https://${bucket}.${s3_endpoint}/regression/datalake/pipeline_data/data_page_v2_lz42.parquet", 
                "s3.access_key" = "${ak}",     
                "s3.secret_key" = "${sk}",     
                "REGION" = "${region}",    
                "FORMAT" = "parquet"
                )where  col_decimal > 1.6 order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_decimal,col_date,col_timestamp limit 10;
    """
    order_qt_no_null_none_1 """
    select * from 
            s3(     
                "URI" = "https://${bucket}.${s3_endpoint}/regression/datalake/pipeline_data/data_page_v2_no_null_none.parquet", 
                "s3.access_key" = "${ak}",     
                "s3.secret_key" = "${sk}",     
                "REGION" = "${region}",    
                "FORMAT" = "parquet"
                )where col_varchar = 'gLmEXgvqlm' and col_smallint < 1271 order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_decimal,col_date,col_timestamp limit 10;
    """
    order_qt_no_null_none_2 """
    select * from 
            s3(     
                "URI" = "https://${bucket}.${s3_endpoint}/regression/datalake/pipeline_data/data_page_v2_no_null_none.parquet", 
                "s3.access_key" = "${ak}",     
                "s3.secret_key" = "${sk}",     
                "REGION" = "${region}",    
                "FORMAT" = "parquet"
                )where col_tinyint > 1288 order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_decimal,col_date,col_timestamp limit 10;
    """
    order_qt_no_null_none_3 """
    select * from 
            s3(     
                "URI" = "https://${bucket}.${s3_endpoint}/regression/datalake/pipeline_data/data_page_v2_no_null_none.parquet", 
                "s3.access_key" = "${ak}",     
                "s3.secret_key" = "${sk}",     
                "REGION" = "${region}",    
                "FORMAT" = "parquet"
                )where col_string = 'jTFdDLHZK' and year(col_date)=2016 order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_decimal,col_date,col_timestamp limit 10;
    """
    order_qt_no_null_none_4 """
    select * from 
            s3(     
                "URI" = "https://${bucket}.${s3_endpoint}/regression/datalake/pipeline_data/data_page_v2_no_null_none.parquet", 
                "s3.access_key" = "${ak}",     
                "s3.secret_key" = "${sk}",     
                "REGION" = "${region}",    
                "FORMAT" = "parquet"
                )where year(col_timestamp)=2016 order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_decimal,col_date,col_timestamp limit 10;
    """
    order_qt_no_null_none_5 """
    select * from 
            s3(     
                "URI" = "https://${bucket}.${s3_endpoint}/regression/datalake/pipeline_data/data_page_v2_no_null_none.parquet", 
                "s3.access_key" = "${ak}",     
                "s3.secret_key" = "${sk}",     
                "REGION" = "${region}",    
                "FORMAT" = "parquet"
                )where col_char = 'qHqL' order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_decimal,col_date,col_timestamp limit 10;
    """
    order_qt_none_1 """
    select * from 
            s3(     
                "URI" = "https://${bucket}.${s3_endpoint}/regression/datalake/pipeline_data/data_page_v2_none.parquet", 
                "s3.access_key" = "${ak}",     
                "s3.secret_key" = "${sk}",     
                "REGION" = "${region}",    
                "FORMAT" = "parquet"
                )where  col_float = 0.3 order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_decimal,col_date,col_timestamp limit 10;
    """
    order_qt_none_2 """
    select * from 
            s3(     
                "URI" = "https://${bucket}.${s3_endpoint}/regression/datalake/pipeline_data/data_page_v2_none.parquet", 
                "s3.access_key" = "${ak}",     
                "s3.secret_key" = "${sk}",     
                "REGION" = "${region}",    
                "FORMAT" = "parquet"
                )where col_bigint < 50 and  col_float < 1.6 order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_decimal,col_date,col_timestamp limit 10;
    """
    order_qt_none_3 """
    select * from 
            s3(     
                "URI" = "https://${bucket}.${s3_endpoint}/regression/datalake/pipeline_data/data_page_v2_none.parquet", 
                "s3.access_key" = "${ak}",     
                "s3.secret_key" = "${sk}",     
                "REGION" = "${region}",    
                "FORMAT" = "parquet"
                )where col_boolean > 314 order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_decimal,col_date,col_timestamp limit 10;
    """
    order_qt_none_4 """
    select * from 
            s3(     
                "URI" = "https://${bucket}.${s3_endpoint}/regression/datalake/pipeline_data/data_page_v2_none.parquet", 
                "s3.access_key" = "${ak}",     
                "s3.secret_key" = "${sk}",     
                "REGION" = "${region}",    
                "FORMAT" = "parquet"
                )where col_boolean = 1240 and  col_decimal < 0.8 and  col_float < 1.4 order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_decimal,col_date,col_timestamp limit 10;
    """
    order_qt_none_5 """
    select * from 
            s3(     
                "URI" = "https://${bucket}.${s3_endpoint}/regression/datalake/pipeline_data/data_page_v2_none.parquet", 
                "s3.access_key" = "${ak}",     
                "s3.secret_key" = "${sk}",     
                "REGION" = "${region}",    
                "FORMAT" = "parquet"
                )where col_char = 'doB' order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_decimal,col_date,col_timestamp limit 10;
    """
    order_qt_none_6 """
    select * from 
            s3(     
                "URI" = "https://${bucket}.${s3_endpoint}/regression/datalake/pipeline_data/data_page_v2_none.parquet", 
                "s3.access_key" = "${ak}",     
                "s3.secret_key" = "${sk}",     
                "REGION" = "${region}",    
                "FORMAT" = "parquet"
                )where  col_double = 2.4 and col_varchar = 'OlykmDCvDpdh' and col_boolean > 1449 order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_decimal,col_date,col_timestamp limit 10;
    """
    order_qt_snappy2_1 """
    select * from 
            s3(     
                "URI" = "https://${bucket}.${s3_endpoint}/regression/datalake/pipeline_data/data_page_v2_snappy2.parquet", 
                "s3.access_key" = "${ak}",     
                "s3.secret_key" = "${sk}",     
                "REGION" = "${region}",    
                "FORMAT" = "parquet"
                )where year(col_timestamp)=2016 and col_boolean > 1011 and col_smallint < 780 order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_decimal,col_date,col_timestamp limit 10;
    """
    order_qt_snappy2_2 """
    select * from 
            s3(     
                "URI" = "https://${bucket}.${s3_endpoint}/regression/datalake/pipeline_data/data_page_v2_snappy2.parquet", 
                "s3.access_key" = "${ak}",     
                "s3.secret_key" = "${sk}",     
                "REGION" = "${region}",    
                "FORMAT" = "parquet"
                )where  col_decimal = 1.4 order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_decimal,col_date,col_timestamp limit 10;
    """
    order_qt_snappy2_3 """
    select * from 
            s3(     
                "URI" = "https://${bucket}.${s3_endpoint}/regression/datalake/pipeline_data/data_page_v2_snappy2.parquet", 
                "s3.access_key" = "${ak}",     
                "s3.secret_key" = "${sk}",     
                "REGION" = "${region}",    
                "FORMAT" = "parquet"
                )where  col_float > 1.8 and  col_double < 2.0 order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_decimal,col_date,col_timestamp limit 10;
    """
    order_qt_snappy2_4 """
    select * from 
            s3(     
                "URI" = "https://${bucket}.${s3_endpoint}/regression/datalake/pipeline_data/data_page_v2_snappy2.parquet", 
                "s3.access_key" = "${ak}",     
                "s3.secret_key" = "${sk}",     
                "REGION" = "${region}",    
                "FORMAT" = "parquet"
                )where  col_double = 1.0 and col_varchar = 'YpajmCZhNSTaHwOriViw' and col_char = 'Syavtn' order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_decimal,col_date,col_timestamp limit 10;
    """
    order_qt_gzip2_1 """
    select * from 
            s3(     
                "URI" = "https://${bucket}.${s3_endpoint}/regression/datalake/pipeline_data/data_page_v2_gzip2.parquet", 
                "s3.access_key" = "${ak}",     
                "s3.secret_key" = "${sk}",     
                "REGION" = "${region}",    
                "FORMAT" = "parquet"
                )where col_int = 1795 and col_smallint > 1239 order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_decimal,col_date,col_timestamp limit 10;
    """
    order_qt_gzip2_2 """
    select * from 
            s3(     
                "URI" = "https://${bucket}.${s3_endpoint}/regression/datalake/pipeline_data/data_page_v2_gzip2.parquet", 
                "s3.access_key" = "${ak}",     
                "s3.secret_key" = "${sk}",     
                "REGION" = "${region}",    
                "FORMAT" = "parquet"
                )where col_int = 1812 and col_bigint < 1976 and  col_float < 2.2 order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_decimal,col_date,col_timestamp limit 10;
    """
    order_qt_gzip2_3 """
    select * from 
            s3(     
                "URI" = "https://${bucket}.${s3_endpoint}/regression/datalake/pipeline_data/data_page_v2_gzip2.parquet", 
                "s3.access_key" = "${ak}",     
                "s3.secret_key" = "${sk}",     
                "REGION" = "${region}",    
                "FORMAT" = "parquet"
                )where  col_float > 1.4 and col_tinyint > 1799 and  col_decimal > 0.3 order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_decimal,col_date,col_timestamp limit 10;
    """
    order_qt_gzip2_4 """
    select * from 
            s3(     
                "URI" = "https://${bucket}.${s3_endpoint}/regression/datalake/pipeline_data/data_page_v2_gzip2.parquet", 
                "s3.access_key" = "${ak}",     
                "s3.secret_key" = "${sk}",     
                "REGION" = "${region}",    
                "FORMAT" = "parquet"
                )where year(col_timestamp)=2016 and col_bigint > 1938 order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_decimal,col_date,col_timestamp limit 10;
    """
    order_qt_no_null_gzip_1 """
    select * from 
            s3(     
                "URI" = "https://${bucket}.${s3_endpoint}/regression/datalake/pipeline_data/data_page_v2_no_null_gzip.parquet", 
                "s3.access_key" = "${ak}",     
                "s3.secret_key" = "${sk}",     
                "REGION" = "${region}",    
                "FORMAT" = "parquet"
                )where col_bigint = 1426 and col_varchar = 'NcXlcfnRpO' order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_decimal,col_date,col_timestamp limit 10;
    """
    order_qt_no_null_gzip_2 """
    select * from 
            s3(     
                "URI" = "https://${bucket}.${s3_endpoint}/regression/datalake/pipeline_data/data_page_v2_no_null_gzip.parquet", 
                "s3.access_key" = "${ak}",     
                "s3.secret_key" = "${sk}",     
                "REGION" = "${region}",    
                "FORMAT" = "parquet"
                )where  col_float = 1.8 and col_string = 'khbrpZBfNbFP' order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_decimal,col_date,col_timestamp limit 10;
    """
    order_qt_no_null_gzip_3 """
    select * from 
            s3(     
                "URI" = "https://${bucket}.${s3_endpoint}/regression/datalake/pipeline_data/data_page_v2_no_null_gzip.parquet", 
                "s3.access_key" = "${ak}",     
                "s3.secret_key" = "${sk}",     
                "REGION" = "${region}",    
                "FORMAT" = "parquet"
                )where  col_double < 0.3 and  col_float < 0.3 and col_bigint < 1642 order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_decimal,col_date,col_timestamp limit 10;
    """
    order_qt_no_null_snappy_1 """
    select * from 
            s3(     
                "URI" = "https://${bucket}.${s3_endpoint}/regression/datalake/pipeline_data/data_page_v2_no_null_snappy.parquet", 
                "s3.access_key" = "${ak}",     
                "s3.secret_key" = "${sk}",     
                "REGION" = "${region}",    
                "FORMAT" = "parquet"
                )where col_varchar = 'DdYENcTybQV' and col_boolean < 1004 order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_decimal,col_date,col_timestamp limit 10;
    """
    order_qt_no_null_snappy_2 """
    select * from 
            s3(     
                "URI" = "https://${bucket}.${s3_endpoint}/regression/datalake/pipeline_data/data_page_v2_no_null_snappy.parquet", 
                "s3.access_key" = "${ak}",     
                "s3.secret_key" = "${sk}",     
                "REGION" = "${region}",    
                "FORMAT" = "parquet"
                )where col_char = 'bcuSohgIFYKTMVv' and year(col_date)=2016 order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_decimal,col_date,col_timestamp limit 10;
    """
    order_qt_no_null_snappy_3 """
    select * from 
            s3(     
                "URI" = "https://${bucket}.${s3_endpoint}/regression/datalake/pipeline_data/data_page_v2_no_null_snappy.parquet", 
                "s3.access_key" = "${ak}",     
                "s3.secret_key" = "${sk}",     
                "REGION" = "${region}",    
                "FORMAT" = "parquet"
                )where col_tinyint = 693 and col_varchar = 'qHnjmUWAaKcpAn' and year(col_timestamp)=2016 order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_decimal,col_date,col_timestamp limit 10;
    """
    order_qt_no_null_snappy_4 """
    select * from 
            s3(     
                "URI" = "https://${bucket}.${s3_endpoint}/regression/datalake/pipeline_data/data_page_v2_no_null_snappy.parquet", 
                "s3.access_key" = "${ak}",     
                "s3.secret_key" = "${sk}",     
                "REGION" = "${region}",    
                "FORMAT" = "parquet"
                )where  col_double > 2.3 order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_decimal,col_date,col_timestamp limit 10;
    """
    order_qt_none2_1 """
    select * from 
            s3(     
                "URI" = "https://${bucket}.${s3_endpoint}/regression/datalake/pipeline_data/data_page_v2_none2.parquet", 
                "s3.access_key" = "${ak}",     
                "s3.secret_key" = "${sk}",     
                "REGION" = "${region}",    
                "FORMAT" = "parquet"
                )where  col_decimal < 0.2 order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_decimal,col_date,col_timestamp limit 10;
    """
    order_qt_none2_2 """
    select * from 
            s3(     
                "URI" = "https://${bucket}.${s3_endpoint}/regression/datalake/pipeline_data/data_page_v2_none2.parquet", 
                "s3.access_key" = "${ak}",     
                "s3.secret_key" = "${sk}",     
                "REGION" = "${region}",    
                "FORMAT" = "parquet"
                )where col_boolean > 1693 and col_varchar = 'YowzLFscqYmYB' and col_char = 'CJ' order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_decimal,col_date,col_timestamp limit 10;
    """
    order_qt_none2_3 """
    select * from 
            s3(     
                "URI" = "https://${bucket}.${s3_endpoint}/regression/datalake/pipeline_data/data_page_v2_none2.parquet", 
                "s3.access_key" = "${ak}",     
                "s3.secret_key" = "${sk}",     
                "REGION" = "${region}",    
                "FORMAT" = "parquet"
                )where col_char = 'WGSpFnLYSPVTlyl' and year(col_date)=2016 and col_string = 'fmhUKNb' order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_decimal,col_date,col_timestamp limit 10;
    """
    order_qt_none2_4 """
    select * from 
            s3(     
                "URI" = "https://${bucket}.${s3_endpoint}/regression/datalake/pipeline_data/data_page_v2_none2.parquet", 
                "s3.access_key" = "${ak}",     
                "s3.secret_key" = "${sk}",     
                "REGION" = "${region}",    
                "FORMAT" = "parquet"
                )where col_bigint > 1841 order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_decimal,col_date,col_timestamp limit 10;
    """
    order_qt_none2_5 """
    select * from 
            s3(     
                "URI" = "https://${bucket}.${s3_endpoint}/regression/datalake/pipeline_data/data_page_v2_none2.parquet", 
                "s3.access_key" = "${ak}",     
                "s3.secret_key" = "${sk}",     
                "REGION" = "${region}",    
                "FORMAT" = "parquet"
                )where year(col_date)=2016 order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_decimal,col_date,col_timestamp limit 10;
    """
    order_qt_zstd2_1 """
    select * from 
            s3(     
                "URI" = "https://${bucket}.${s3_endpoint}/regression/datalake/pipeline_data/data_page_v2_zstd2.parquet", 
                "s3.access_key" = "${ak}",     
                "s3.secret_key" = "${sk}",     
                "REGION" = "${region}",    
                "FORMAT" = "parquet"
                )where year(col_timestamp)=2016 and col_smallint = 130 order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_decimal,col_date,col_timestamp limit 10;
    """
    order_qt_zstd2_2 """
    select * from 
            s3(     
                "URI" = "https://${bucket}.${s3_endpoint}/regression/datalake/pipeline_data/data_page_v2_zstd2.parquet", 
                "s3.access_key" = "${ak}",     
                "s3.secret_key" = "${sk}",     
                "REGION" = "${region}",    
                "FORMAT" = "parquet"
                )where  col_decimal > 2.0 and col_tinyint = 1662 and col_bigint < 1224 order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_decimal,col_date,col_timestamp limit 10;
    """
    order_qt_zstd2_3 """
    select * from 
            s3(     
                "URI" = "https://${bucket}.${s3_endpoint}/regression/datalake/pipeline_data/data_page_v2_zstd2.parquet", 
                "s3.access_key" = "${ak}",     
                "s3.secret_key" = "${sk}",     
                "REGION" = "${region}",    
                "FORMAT" = "parquet"
                )where col_smallint > 615 and col_int > 1410 order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_decimal,col_date,col_timestamp limit 10;
    """
    order_qt_zstd2_4 """
    select * from 
            s3(     
                "URI" = "https://${bucket}.${s3_endpoint}/regression/datalake/pipeline_data/data_page_v2_zstd2.parquet", 
                "s3.access_key" = "${ak}",     
                "s3.secret_key" = "${sk}",     
                "REGION" = "${region}",    
                "FORMAT" = "parquet"
                )where col_boolean < 128 order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_decimal,col_date,col_timestamp limit 10;
    """
    order_qt_lz4_1 """
    select * from 
            s3(     
                "URI" = "https://${bucket}.${s3_endpoint}/regression/datalake/pipeline_data/data_page_v2_lz4.parquet", 
                "s3.access_key" = "${ak}",     
                "s3.secret_key" = "${sk}",     
                "REGION" = "${region}",    
                "FORMAT" = "parquet"
                )where col_varchar = 'qirvWjuTAgAPrzjlcU' and  col_float < 0.9 order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_decimal,col_date,col_timestamp limit 10;
    """
    order_qt_lz4_2 """
    select * from 
            s3(     
                "URI" = "https://${bucket}.${s3_endpoint}/regression/datalake/pipeline_data/data_page_v2_lz4.parquet", 
                "s3.access_key" = "${ak}",     
                "s3.secret_key" = "${sk}",     
                "REGION" = "${region}",    
                "FORMAT" = "parquet"
                )where col_char = 'YNT' and col_string = 'iLZNgqK' order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_decimal,col_date,col_timestamp limit 10;
    """
    order_qt_lz4_3 """
    select * from 
            s3(     
                "URI" = "https://${bucket}.${s3_endpoint}/regression/datalake/pipeline_data/data_page_v2_lz4.parquet", 
                "s3.access_key" = "${ak}",     
                "s3.secret_key" = "${sk}",     
                "REGION" = "${region}",    
                "FORMAT" = "parquet"
                )where year(col_date)=2016 and col_tinyint > 1491 and col_int > 342 order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_decimal,col_date,col_timestamp limit 10;
    """
    order_qt_no_null_lz4_1 """
    select * from 
            s3(     
                "URI" = "https://${bucket}.${s3_endpoint}/regression/datalake/pipeline_data/data_page_v2_no_null_lz4.parquet", 
                "s3.access_key" = "${ak}",     
                "s3.secret_key" = "${sk}",     
                "REGION" = "${region}",    
                "FORMAT" = "parquet"
                )where  col_decimal > 2.5 and year(col_timestamp)=2016 order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_decimal,col_date,col_timestamp limit 10;
    """
    order_qt_no_null_lz4_2 """
    select * from 
            s3(     
                "URI" = "https://${bucket}.${s3_endpoint}/regression/datalake/pipeline_data/data_page_v2_no_null_lz4.parquet", 
                "s3.access_key" = "${ak}",     
                "s3.secret_key" = "${sk}",     
                "REGION" = "${region}",    
                "FORMAT" = "parquet"
                )where year(col_date)=2016 and col_int > 166 order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_decimal,col_date,col_timestamp limit 10;
    """
    order_qt_no_null_lz4_3 """
    select * from 
            s3(     
                "URI" = "https://${bucket}.${s3_endpoint}/regression/datalake/pipeline_data/data_page_v2_no_null_lz4.parquet", 
                "s3.access_key" = "${ak}",     
                "s3.secret_key" = "${sk}",     
                "REGION" = "${region}",    
                "FORMAT" = "parquet"
                )where col_char = 'YN' and col_string = 'XBnUNfbPCijQSaxwKLh' order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_decimal,col_date,col_timestamp limit 10;
    """
    order_qt_no_null_lz4_4 """
    select * from 
            s3(     
                "URI" = "https://${bucket}.${s3_endpoint}/regression/datalake/pipeline_data/data_page_v2_no_null_lz4.parquet", 
                "s3.access_key" = "${ak}",     
                "s3.secret_key" = "${sk}",     
                "REGION" = "${region}",    
                "FORMAT" = "parquet"
                )where col_boolean = 27 and col_string = 'VYDMZZ' and year(col_date)=2016 order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_decimal,col_date,col_timestamp limit 10;
    """
    order_qt_no_null_zstd_1 """
    select * from 
            s3(     
                "URI" = "https://${bucket}.${s3_endpoint}/regression/datalake/pipeline_data/data_page_v2_no_null_zstd.parquet", 
                "s3.access_key" = "${ak}",     
                "s3.secret_key" = "${sk}",     
                "REGION" = "${region}",    
                "FORMAT" = "parquet"
                )where col_int = 1361 order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_decimal,col_date,col_timestamp limit 10;
    """
    order_qt_no_null_zstd_2 """
    select * from 
            s3(     
                "URI" = "https://${bucket}.${s3_endpoint}/regression/datalake/pipeline_data/data_page_v2_no_null_zstd.parquet", 
                "s3.access_key" = "${ak}",     
                "s3.secret_key" = "${sk}",     
                "REGION" = "${region}",    
                "FORMAT" = "parquet"
                )where col_bigint = 516 and col_varchar = 'rxqicLIhJEykBOEmr' and col_char = 'sioJZDxWfjFkTsO' order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_decimal,col_date,col_timestamp limit 10;
    """
    order_qt_no_null_zstd_3 """
    select * from 
            s3(     
                "URI" = "https://${bucket}.${s3_endpoint}/regression/datalake/pipeline_data/data_page_v2_no_null_zstd.parquet", 
                "s3.access_key" = "${ak}",     
                "s3.secret_key" = "${sk}",     
                "REGION" = "${region}",    
                "FORMAT" = "parquet"
                )where year(col_timestamp)=2016 and col_char = 'NSKJPkGHUSXVDD' order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_decimal,col_date,col_timestamp limit 10;
    """
    order_qt_no_null_zstd_4 """
    select * from 
            s3(     
                "URI" = "https://${bucket}.${s3_endpoint}/regression/datalake/pipeline_data/data_page_v2_no_null_zstd.parquet", 
                "s3.access_key" = "${ak}",     
                "s3.secret_key" = "${sk}",     
                "REGION" = "${region}",    
                "FORMAT" = "parquet"
                )where col_bigint < 886 and year(col_timestamp)=2016 order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_decimal,col_date,col_timestamp limit 10;
    """
    order_qt_no_null_zstd_5 """
    select * from 
            s3(     
                "URI" = "https://${bucket}.${s3_endpoint}/regression/datalake/pipeline_data/data_page_v2_no_null_zstd.parquet", 
                "s3.access_key" = "${ak}",     
                "s3.secret_key" = "${sk}",     
                "REGION" = "${region}",    
                "FORMAT" = "parquet"
                )where  col_float > 0.5 order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_decimal,col_date,col_timestamp limit 10;
    """
    order_qt_no_null_zstd_6 """
    select * from 
            s3(     
                "URI" = "https://${bucket}.${s3_endpoint}/regression/datalake/pipeline_data/data_page_v2_no_null_zstd.parquet", 
                "s3.access_key" = "${ak}",     
                "s3.secret_key" = "${sk}",     
                "REGION" = "${region}",    
                "FORMAT" = "parquet"
                )where col_char = 'VyUYzxhFtq' and col_varchar = 'OJfigYX' and col_string = 'EzFjFJEWXuOdG' order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_decimal,col_date,col_timestamp limit 10;
    """
    order_qt_snappy_1 """
    select * from 
            s3(     
                "URI" = "https://${bucket}.${s3_endpoint}/regression/datalake/pipeline_data/data_page_v2_snappy.parquet", 
                "s3.access_key" = "${ak}",     
                "s3.secret_key" = "${sk}",     
                "REGION" = "${region}",    
                "FORMAT" = "parquet"
                )where  col_float = 1.0 and year(col_timestamp)=2016 and col_boolean < 1328 order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_decimal,col_date,col_timestamp limit 10;
    """
    order_qt_snappy_2 """
    select * from 
            s3(     
                "URI" = "https://${bucket}.${s3_endpoint}/regression/datalake/pipeline_data/data_page_v2_snappy.parquet", 
                "s3.access_key" = "${ak}",     
                "s3.secret_key" = "${sk}",     
                "REGION" = "${region}",    
                "FORMAT" = "parquet"
                )where  col_float < 1.8 and year(col_date)=2016 order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_decimal,col_date,col_timestamp limit 10;
    """
    order_qt_snappy_3 """
    select * from 
            s3(     
                "URI" = "https://${bucket}.${s3_endpoint}/regression/datalake/pipeline_data/data_page_v2_snappy.parquet", 
                "s3.access_key" = "${ak}",     
                "s3.secret_key" = "${sk}",     
                "REGION" = "${region}",    
                "FORMAT" = "parquet"
                )where col_bigint = 949 order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_decimal,col_date,col_timestamp limit 10;
    """
    order_qt_snappy_4 """
    select * from 
            s3(     
                "URI" = "https://${bucket}.${s3_endpoint}/regression/datalake/pipeline_data/data_page_v2_snappy.parquet", 
                "s3.access_key" = "${ak}",     
                "s3.secret_key" = "${sk}",     
                "REGION" = "${region}",    
                "FORMAT" = "parquet"
                )where col_string = 'rq' and col_varchar = 'pinxLaVk' and  col_decimal < 2.3 order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_decimal,col_date,col_timestamp limit 10;
    """
    order_qt_snappy_5 """
    select * from 
            s3(     
                "URI" = "https://${bucket}.${s3_endpoint}/regression/datalake/pipeline_data/data_page_v2_snappy.parquet", 
                "s3.access_key" = "${ak}",     
                "s3.secret_key" = "${sk}",     
                "REGION" = "${region}",    
                "FORMAT" = "parquet"
                )where col_tinyint > 628 and col_varchar = 'xQWHzZPhLlCM' order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_decimal,col_date,col_timestamp limit 10;
    """

}