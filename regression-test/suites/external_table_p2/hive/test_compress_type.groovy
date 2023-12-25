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

suite("test_compress_type", "p2,external,hive,external_remote,external_remote_hive") {
    String enabled = context.config.otherConfigs.get("enableExternalHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String extHiveHmsHost = context.config.otherConfigs.get("extHiveHmsHost")
        String extHiveHmsPort = context.config.otherConfigs.get("extHiveHmsPort")
        String catalog_name = "test_compress_type"
        sql """drop catalog if exists ${catalog_name};"""
        sql """
            create catalog if not exists ${catalog_name} properties (
                'type'='hms',
                'hadoop.username' = 'hadoop',
                'hive.metastore.uris' = 'thrift://${extHiveHmsHost}:${extHiveHmsPort}'
            );
        """
        logger.info("catalog " + catalog_name + " created")
        sql """switch ${catalog_name};"""
        logger.info("switched to catalog " + catalog_name)
        
        sql """ use multi_catalog """

        // table test_compress_partitioned has 6 partitions with different compressed file: plain, gzip, bzip2, deflate
        sql """set file_split_size=0"""
        explain {
            sql("select count(*) from test_compress_partitioned")
            contains "inputSplitNum=16, totalFileSize=734675596, scanRanges=16"
            contains "partition=8/8"
        }
        qt_q21 """select count(*) from test_compress_partitioned where dt="gzip" or dt="mix""""
        qt_q22 """select count(*) from test_compress_partitioned"""
        order_qt_q23 """select * from test_compress_partitioned where watchid=4611870011201662970"""

        sql """set file_split_size=8388608"""
        explain {
            sql("select count(*) from test_compress_partitioned")
            contains "inputSplitNum=82, totalFileSize=734675596, scanRanges=82"
            contains "partition=8/8"
        }

        qt_q31 """select count(*) from test_compress_partitioned where dt="gzip" or dt="mix""""
        qt_q32 """select count(*) from test_compress_partitioned"""
        order_qt_q33 """select * from test_compress_partitioned where watchid=4611870011201662970"""
        sql """set file_split_size=0"""


        order_qt_q42 """ select count(*) from parquet_lz4_compression ;       """
        order_qt_q43 """ select * from parquet_lz4_compression 
            order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_date,col_timestamp,col_decimal
        """
        
        order_qt_q44 """ select * from parquet_lz4_compression where col_int = 17 
            order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_date,col_timestamp,col_decimal        
            """

        order_qt_q45 """ select * from parquet_lz4_compression where col_bigint >= 10738473173
            order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_date,col_timestamp,col_decimal        
            """
        
        order_qt_q46 """ select * from parquet_lz4_compression  where col_boolean = 1 and col_char='C'
            order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_date,col_timestamp,col_decimal        
             """

        order_qt_q47 """ select * from parquet_lz4_compression  where col_decimal >= 1000
            order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_date,col_timestamp,col_decimal        
            """
        
        order_qt_q48 """ select * from parquet_lz4_compression where col_string != "Random"
            order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_date,col_timestamp,col_decimal        
             """
        
        order_qt_lzo_1 """ select * from parquet_lzo_compression 
        order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_date,col_timestamp,col_decimal
        limit 20; 
        """

        order_qt_lzo_2 """ select * from parquet_lzo_compression where col_int > 1000 
        order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_date,col_timestamp,col_decimal
        limit 10; 
        """


        order_qt_lzo_3 """ select * from parquet_lzo_compression where col_float > 5.1 and col_boolean = 1  
        order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_date,col_timestamp,col_decimal
        limit 10; 
        """

        order_qt_lzo_4 """ select * from parquet_lzo_compression where col_float > 1000 and col_boolean != 1  
        order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_date,col_timestamp,col_decimal
        limit 10; 
        """


        order_qt_lzo_5 """ select * from parquet_lzo_compression where col_double < 17672101476 and col_char !='ft'  
        order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_date,col_timestamp,col_decimal
        limit 10; 
        """


        order_qt_lzo_6 """ select * from parquet_lzo_compression where col_string='nuXBDInOfoaWz'
        order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_date,col_timestamp,col_decimal
        limit 10; 
        """


        order_qt_lzo_7 """ select * from parquet_lzo_compression where col_decimal > 86208 and year(col_timestamp) = 2023
        order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_date,col_timestamp,col_decimal
        limit 10; 
        """


        order_qt_lzo_8 """ select * from parquet_lzo_compression where year(col_date)!=2023 and year(col_timestamp) = 2023
        order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_date,col_timestamp,col_decimal
        limit 10; 
        """


    }
}
