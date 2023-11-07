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

suite("test_hive_parquet_alter_column", "p2,external,hive,external_remote,external_remote_hive") {
    String enabled = context.config.otherConfigs.get("enableExternalHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String extHiveHmsHost = context.config.otherConfigs.get("extHiveHmsHost")
        String extHiveHmsPort = context.config.otherConfigs.get("extHiveHmsPort")
        String hms_port = context.config.otherConfigs.get("hms_port")

        String catalog_name = "test_hive_parquet_alter_column"
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
        String Orderby = """ order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_decimal,col_date,col_timestamp limit 7 """

        sql """ use multi_catalog """


        
        types = ["int","smallint","tinyint","bigint","float","double","boolean","string","char","varchar","date","timestamp","decimal"]
        
        for( String type1 in types) {
            qt_desc """ desc parquet_alter_column_to_${type1} ; """

            qt_show """ select * from parquet_alter_column_to_${type1} ${Orderby} """
            
            for( String type2 in types) {

                qt_order """ select col_${type2} from  parquet_alter_column_to_${type1} order by col_${type2} limit 3 """
            
            }
        }
        
        order_qt_int_int """ select col_int from  parquet_alter_column_to_int  where col_int>=2 order by col_int limit 3""" 
        order_qt_int_smallint """ select col_smallint from  parquet_alter_column_to_int  where col_smallint>=3 order by col_smallint limit 3""" 
        order_qt_int_tinyint """ select col_tinyint from  parquet_alter_column_to_int  where col_tinyint>=3 order by col_tinyint limit 3""" 
        order_qt_int_bigint """ select col_bigint from  parquet_alter_column_to_int  where col_bigint>=3 order by col_bigint limit 3""" 
        order_qt_int_float """ select col_float from  parquet_alter_column_to_int  where col_float=2.6 order by col_float limit 3""" 
        order_qt_int_double """ select col_double from  parquet_alter_column_to_int  where col_double=0.8 order by col_double limit 3""" 
        order_qt_int_boolean """ select col_boolean from  parquet_alter_column_to_int  where year(col_boolean)=2023 order by col_boolean limit 3""" 
        order_qt_int_string """ select col_string from  parquet_alter_column_to_int  where col_string="B" order by col_string limit 3""" 
        order_qt_int_char """ select col_char from  parquet_alter_column_to_int  where col_char="B" order by col_char limit 3""" 
        order_qt_int_varchar """ select col_varchar from  parquet_alter_column_to_int  where col_varchar="C" order by col_varchar limit 3""" 
        order_qt_int_date """ select col_date from  parquet_alter_column_to_int  where year(col_date)=2023 order by col_date limit 3""" 
        order_qt_int_timestamp """ select col_timestamp from  parquet_alter_column_to_int  where year(col_timestamp)=2023 order by col_timestamp limit 3""" 
        order_qt_int_decimal """ select col_decimal from  parquet_alter_column_to_int  where col_decimal=1.1 order by col_decimal limit 3""" 
        order_qt_smallint_int """ select col_int from  parquet_alter_column_to_smallint  where col_int>=1 order by col_int limit 3""" 
        order_qt_smallint_smallint """ select col_smallint from  parquet_alter_column_to_smallint  where col_smallint>=3 order by col_smallint limit 3""" 
        order_qt_smallint_tinyint """ select col_tinyint from  parquet_alter_column_to_smallint  where col_tinyint>=2 order by col_tinyint limit 3""" 
        order_qt_smallint_bigint """ select col_bigint from  parquet_alter_column_to_smallint  where col_bigint>=2 order by col_bigint limit 3""" 
        order_qt_smallint_float """ select col_float from  parquet_alter_column_to_smallint  where col_float=3.0 order by col_float limit 3""" 
        order_qt_smallint_double """ select col_double from  parquet_alter_column_to_smallint  where col_double=0.5 order by col_double limit 3""" 
        order_qt_smallint_boolean """ select col_boolean from  parquet_alter_column_to_smallint  where year(col_boolean)=2023 order by col_boolean limit 3""" 
        order_qt_smallint_string """ select col_string from  parquet_alter_column_to_smallint  where col_string="helloworld" order by col_string limit 3""" 
        order_qt_smallint_char """ select col_char from  parquet_alter_column_to_smallint  where col_char="C" order by col_char limit 3""" 
        order_qt_smallint_varchar """ select col_varchar from  parquet_alter_column_to_smallint  where col_varchar="A" order by col_varchar limit 3""" 
        order_qt_smallint_date """ select col_date from  parquet_alter_column_to_smallint  where year(col_date)=2023 order by col_date limit 3""" 
        order_qt_smallint_timestamp """ select col_timestamp from  parquet_alter_column_to_smallint  where year(col_timestamp)=2023 order by col_timestamp limit 3""" 
        order_qt_smallint_decimal """ select col_decimal from  parquet_alter_column_to_smallint  where col_decimal=2.5 order by col_decimal limit 3""" 
        order_qt_tinyint_int """ select col_int from  parquet_alter_column_to_tinyint  where col_int>=3 order by col_int limit 3""" 
        order_qt_tinyint_smallint """ select col_smallint from  parquet_alter_column_to_tinyint  where col_smallint>=3 order by col_smallint limit 3""" 
        order_qt_tinyint_tinyint """ select col_tinyint from  parquet_alter_column_to_tinyint  where col_tinyint>=3 order by col_tinyint limit 3""" 
        order_qt_tinyint_bigint """ select col_bigint from  parquet_alter_column_to_tinyint  where col_bigint>=1 order by col_bigint limit 3""" 
        order_qt_tinyint_float """ select col_float from  parquet_alter_column_to_tinyint  where col_float=0.6 order by col_float limit 3""" 
        order_qt_tinyint_double """ select col_double from  parquet_alter_column_to_tinyint  where col_double=1.1 order by col_double limit 3""" 
        order_qt_tinyint_boolean """ select col_boolean from  parquet_alter_column_to_tinyint  where year(col_boolean)=2023 order by col_boolean limit 3""" 
        order_qt_tinyint_string """ select col_string from  parquet_alter_column_to_tinyint  where col_string="helloworld" order by col_string limit 3""" 
        order_qt_tinyint_char """ select col_char from  parquet_alter_column_to_tinyint  where col_char="A" order by col_char limit 3""" 
        order_qt_tinyint_varchar """ select col_varchar from  parquet_alter_column_to_tinyint  where col_varchar="C" order by col_varchar limit 3""" 
        order_qt_tinyint_date """ select col_date from  parquet_alter_column_to_tinyint  where year(col_date)=2023 order by col_date limit 3""" 
        order_qt_tinyint_timestamp """ select col_timestamp from  parquet_alter_column_to_tinyint  where year(col_timestamp)=2023 order by col_timestamp limit 3""" 
        order_qt_tinyint_decimal """ select col_decimal from  parquet_alter_column_to_tinyint  where col_decimal=1.4 order by col_decimal limit 3""" 
        order_qt_bigint_int """ select col_int from  parquet_alter_column_to_bigint  where col_int>=3 order by col_int limit 3""" 
        order_qt_bigint_smallint """ select col_smallint from  parquet_alter_column_to_bigint  where col_smallint>=2 order by col_smallint limit 3""" 
        order_qt_bigint_tinyint """ select col_tinyint from  parquet_alter_column_to_bigint  where col_tinyint>=2 order by col_tinyint limit 3""" 
        order_qt_bigint_bigint """ select col_bigint from  parquet_alter_column_to_bigint  where col_bigint>=1 order by col_bigint limit 3""" 
        order_qt_bigint_float """ select col_float from  parquet_alter_column_to_bigint  where col_float=2.5 order by col_float limit 3""" 
        order_qt_bigint_double """ select col_double from  parquet_alter_column_to_bigint  where col_double=0.2 order by col_double limit 3""" 
        order_qt_bigint_boolean """ select col_boolean from  parquet_alter_column_to_bigint  where year(col_boolean)=2023 order by col_boolean limit 3""" 
        order_qt_bigint_string """ select col_string from  parquet_alter_column_to_bigint  where col_string="A" order by col_string limit 3""" 
        order_qt_bigint_char """ select col_char from  parquet_alter_column_to_bigint  where col_char="A" order by col_char limit 3""" 
        order_qt_bigint_varchar """ select col_varchar from  parquet_alter_column_to_bigint  where col_varchar="A" order by col_varchar limit 3""" 
        order_qt_bigint_date """ select col_date from  parquet_alter_column_to_bigint  where year(col_date)=2023 order by col_date limit 3""" 
        order_qt_bigint_timestamp """ select col_timestamp from  parquet_alter_column_to_bigint  where year(col_timestamp)=2023 order by col_timestamp limit 3""" 
        order_qt_bigint_decimal """ select col_decimal from  parquet_alter_column_to_bigint  where col_decimal=0.8 order by col_decimal limit 3""" 
        order_qt_float_int """ select col_int from  parquet_alter_column_to_float  where col_int=1.4 order by col_int limit 3""" 
        order_qt_float_smallint """ select col_smallint from  parquet_alter_column_to_float  where col_smallint=0.3 order by col_smallint limit 3""" 
        order_qt_float_tinyint """ select col_tinyint from  parquet_alter_column_to_float  where col_tinyint=0.2 order by col_tinyint limit 3""" 
        order_qt_float_bigint """ select col_bigint from  parquet_alter_column_to_float  where col_bigint=2.2 order by col_bigint limit 3""" 
        order_qt_float_float """ select col_float from  parquet_alter_column_to_float  where col_float=1.2 order by col_float limit 3""" 
        order_qt_float_double """ select col_double from  parquet_alter_column_to_float  where col_double=1.5 order by col_double limit 3""" 
        order_qt_float_boolean """ select col_boolean from  parquet_alter_column_to_float  where year(col_boolean)=2023 order by col_boolean limit 3""" 
        order_qt_float_string """ select col_string from  parquet_alter_column_to_float  where col_string="A" order by col_string limit 3""" 
        order_qt_float_char """ select col_char from  parquet_alter_column_to_float  where col_char="helloworld" order by col_char limit 3""" 
        order_qt_float_varchar """ select col_varchar from  parquet_alter_column_to_float  where col_varchar="1" order by col_varchar limit 3""" 
        order_qt_float_date """ select col_date from  parquet_alter_column_to_float  where year(col_date)=2023 order by col_date limit 3""" 
        order_qt_float_timestamp """ select col_timestamp from  parquet_alter_column_to_float  where year(col_timestamp)=2023 order by col_timestamp limit 3""" 
        order_qt_float_decimal """ select col_decimal from  parquet_alter_column_to_float  where col_decimal=0.8 order by col_decimal limit 3""" 
        order_qt_double_int """ select col_int from  parquet_alter_column_to_double  where col_int=2.0 order by col_int limit 3""" 
        order_qt_double_smallint """ select col_smallint from  parquet_alter_column_to_double  where col_smallint=2.0 order by col_smallint limit 3""" 
        order_qt_double_tinyint """ select col_tinyint from  parquet_alter_column_to_double  where col_tinyint=1.4 order by col_tinyint limit 3""" 
        order_qt_double_bigint """ select col_bigint from  parquet_alter_column_to_double  where col_bigint=1.5 order by col_bigint limit 3""" 
        order_qt_double_float """ select col_float from  parquet_alter_column_to_double  where col_float=2.2 order by col_float limit 3""" 
        order_qt_double_double """ select col_double from  parquet_alter_column_to_double  where col_double=0.6 order by col_double limit 3""" 
        order_qt_double_boolean """ select col_boolean from  parquet_alter_column_to_double  where year(col_boolean)=2023 order by col_boolean limit 3""" 
        order_qt_double_string """ select col_string from  parquet_alter_column_to_double  where col_string="B" order by col_string limit 3""" 
        order_qt_double_char """ select col_char from  parquet_alter_column_to_double  where col_char="A" order by col_char limit 3""" 
        order_qt_double_varchar """ select col_varchar from  parquet_alter_column_to_double  where col_varchar="C" order by col_varchar limit 3""" 
        order_qt_double_date """ select col_date from  parquet_alter_column_to_double  where year(col_date)=2023 order by col_date limit 3""" 
        order_qt_double_timestamp """ select col_timestamp from  parquet_alter_column_to_double  where year(col_timestamp)=2023 order by col_timestamp limit 3""" 
        order_qt_double_decimal """ select col_decimal from  parquet_alter_column_to_double  where col_decimal=0.3 order by col_decimal limit 3""" 
        order_qt_boolean_int """ select col_int from  parquet_alter_column_to_boolean  where col_int>=3 order by col_int limit 3""" 
        order_qt_boolean_smallint """ select col_smallint from  parquet_alter_column_to_boolean  where col_smallint>=2 order by col_smallint limit 3""" 
        order_qt_boolean_tinyint """ select col_tinyint from  parquet_alter_column_to_boolean  where col_tinyint>=1 order by col_tinyint limit 3""" 
        order_qt_boolean_bigint """ select col_bigint from  parquet_alter_column_to_boolean  where col_bigint>=3 order by col_bigint limit 3""" 
        order_qt_boolean_float """ select col_float from  parquet_alter_column_to_boolean  where col_float=1.1 order by col_float limit 3""" 
        order_qt_boolean_double """ select col_double from  parquet_alter_column_to_boolean  where col_double=0.5 order by col_double limit 3""" 
        order_qt_boolean_boolean """ select col_boolean from  parquet_alter_column_to_boolean  where year(col_boolean)=2023 order by col_boolean limit 3""" 
        order_qt_boolean_string """ select col_string from  parquet_alter_column_to_boolean  where col_string="1" order by col_string limit 3""" 
        order_qt_boolean_char """ select col_char from  parquet_alter_column_to_boolean  where col_char="A" order by col_char limit 3""" 
        order_qt_boolean_varchar """ select col_varchar from  parquet_alter_column_to_boolean  where col_varchar="B" order by col_varchar limit 3""" 
        order_qt_boolean_date """ select col_date from  parquet_alter_column_to_boolean  where year(col_date)=2023 order by col_date limit 3""" 
        order_qt_boolean_timestamp """ select col_timestamp from  parquet_alter_column_to_boolean  where year(col_timestamp)=2023 order by col_timestamp limit 3""" 
        order_qt_boolean_decimal """ select col_decimal from  parquet_alter_column_to_boolean  where col_decimal=2.8 order by col_decimal limit 3""" 
        order_qt_string_int """ select col_int from  parquet_alter_column_to_string  where col_int="C" order by col_int limit 3""" 
        order_qt_string_smallint """ select col_smallint from  parquet_alter_column_to_string  where col_smallint="C" order by col_smallint limit 3""" 
        order_qt_string_tinyint """ select col_tinyint from  parquet_alter_column_to_string  where col_tinyint="B" order by col_tinyint limit 3""" 
        order_qt_string_bigint """ select col_bigint from  parquet_alter_column_to_string  where col_bigint="helloworld" order by col_bigint limit 3""" 
        order_qt_string_float """ select col_float from  parquet_alter_column_to_string  where col_float="1" order by col_float limit 3""" 
        order_qt_string_double """ select col_double from  parquet_alter_column_to_string  where col_double="C" order by col_double limit 3""" 
        order_qt_string_boolean """ select col_boolean from  parquet_alter_column_to_string  where year(col_boolean)=2023 order by col_boolean limit 3""" 
        order_qt_string_string """ select col_string from  parquet_alter_column_to_string  where col_string="B" order by col_string limit 3""" 
        order_qt_string_char """ select col_char from  parquet_alter_column_to_string  where col_char="A" order by col_char limit 3""" 
        order_qt_string_varchar """ select col_varchar from  parquet_alter_column_to_string  where col_varchar="B" order by col_varchar limit 3""" 
        order_qt_string_date """ select col_date from  parquet_alter_column_to_string  where col_date="helloworld" order by col_date limit 3""" 
        order_qt_string_timestamp """ select col_timestamp from  parquet_alter_column_to_string  where col_timestamp="B" order by col_timestamp limit 3""" 
        order_qt_string_decimal """ select col_decimal from  parquet_alter_column_to_string  where col_decimal="1" order by col_decimal limit 3""" 
        order_qt_char_int """ select col_int from  parquet_alter_column_to_char  where col_int="B" order by col_int limit 3""" 
        order_qt_char_smallint """ select col_smallint from  parquet_alter_column_to_char  where col_smallint="A" order by col_smallint limit 3""" 
        order_qt_char_tinyint """ select col_tinyint from  parquet_alter_column_to_char  where col_tinyint="A" order by col_tinyint limit 3""" 
        order_qt_char_bigint """ select col_bigint from  parquet_alter_column_to_char  where col_bigint="B" order by col_bigint limit 3""" 
        order_qt_char_float """ select col_float from  parquet_alter_column_to_char  where col_float="C" order by col_float limit 3""" 
        order_qt_char_double """ select col_double from  parquet_alter_column_to_char  where col_double="A" order by col_double limit 3""" 
        order_qt_char_boolean """ select col_boolean from  parquet_alter_column_to_char  where year(col_boolean)=2023 order by col_boolean limit 3""" 
        order_qt_char_string """ select col_string from  parquet_alter_column_to_char  where col_string="C" order by col_string limit 3""" 
        order_qt_char_char """ select col_char from  parquet_alter_column_to_char  where col_char="A" order by col_char limit 3""" 
        order_qt_char_varchar """ select col_varchar from  parquet_alter_column_to_char  where col_varchar="B" order by col_varchar limit 3""" 
        order_qt_char_date """ select col_date from  parquet_alter_column_to_char  where col_date="B" order by col_date limit 3""" 
        order_qt_char_timestamp """ select col_timestamp from  parquet_alter_column_to_char  where col_timestamp="A" order by col_timestamp limit 3""" 
        order_qt_char_decimal """ select col_decimal from  parquet_alter_column_to_char  where col_decimal="C" order by col_decimal limit 3""" 
        order_qt_varchar_int """ select col_int from  parquet_alter_column_to_varchar  where col_int="B" order by col_int limit 3""" 
        order_qt_varchar_smallint """ select col_smallint from  parquet_alter_column_to_varchar  where col_smallint="helloworld" order by col_smallint limit 3""" 
        order_qt_varchar_tinyint """ select col_tinyint from  parquet_alter_column_to_varchar  where col_tinyint="A" order by col_tinyint limit 3""" 
        order_qt_varchar_bigint """ select col_bigint from  parquet_alter_column_to_varchar  where col_bigint="helloworld" order by col_bigint limit 3""" 
        order_qt_varchar_float """ select col_float from  parquet_alter_column_to_varchar  where col_float="1" order by col_float limit 3""" 
        order_qt_varchar_double """ select col_double from  parquet_alter_column_to_varchar  where col_double="B" order by col_double limit 3""" 
        order_qt_varchar_boolean """ select col_boolean from  parquet_alter_column_to_varchar  where year(col_boolean)=2023 order by col_boolean limit 3""" 
        order_qt_varchar_string """ select col_string from  parquet_alter_column_to_varchar  where col_string="A" order by col_string limit 3""" 
        order_qt_varchar_char """ select col_char from  parquet_alter_column_to_varchar  where col_char="B" order by col_char limit 3""" 
        order_qt_varchar_varchar """ select col_varchar from  parquet_alter_column_to_varchar  where col_varchar="B" order by col_varchar limit 3""" 
        order_qt_varchar_date """ select col_date from  parquet_alter_column_to_varchar  where col_date="C" order by col_date limit 3""" 
        order_qt_varchar_timestamp """ select col_timestamp from  parquet_alter_column_to_varchar  where col_timestamp="C" order by col_timestamp limit 3""" 
        order_qt_varchar_decimal """ select col_decimal from  parquet_alter_column_to_varchar  where col_decimal="helloworld" order by col_decimal limit 3""" 
        order_qt_date_int """ select col_int from  parquet_alter_column_to_date  where col_int>=3 order by col_int limit 3""" 
        order_qt_date_smallint """ select col_smallint from  parquet_alter_column_to_date  where col_smallint>=1 order by col_smallint limit 3""" 
        order_qt_date_tinyint """ select col_tinyint from  parquet_alter_column_to_date  where col_tinyint>=3 order by col_tinyint limit 3""" 
        order_qt_date_bigint """ select col_bigint from  parquet_alter_column_to_date  where col_bigint>=1 order by col_bigint limit 3""" 
        order_qt_date_float """ select col_float from  parquet_alter_column_to_date  where col_float=2.8 order by col_float limit 3""" 
        order_qt_date_double """ select col_double from  parquet_alter_column_to_date  where col_double=2.5 order by col_double limit 3""" 
        order_qt_date_boolean """ select col_boolean from  parquet_alter_column_to_date  where year(col_boolean)=2023 order by col_boolean limit 3""" 
        order_qt_date_string """ select col_string from  parquet_alter_column_to_date  where col_string="helloworld" order by col_string limit 3""" 
        order_qt_date_char """ select col_char from  parquet_alter_column_to_date  where col_char="A" order by col_char limit 3""" 
        order_qt_date_varchar """ select col_varchar from  parquet_alter_column_to_date  where col_varchar="1" order by col_varchar limit 3""" 
        order_qt_date_date """ select col_date from  parquet_alter_column_to_date  where year(col_date)=2023 order by col_date limit 3""" 
        order_qt_date_timestamp """ select col_timestamp from  parquet_alter_column_to_date  where year(col_timestamp)=2023 order by col_timestamp limit 3""" 
        order_qt_date_decimal """ select col_decimal from  parquet_alter_column_to_date  where col_decimal=0.3 order by col_decimal limit 3""" 
        order_qt_timestamp_int """ select col_int from  parquet_alter_column_to_timestamp  where col_int>=3 order by col_int limit 3""" 
        order_qt_timestamp_smallint """ select col_smallint from  parquet_alter_column_to_timestamp  where col_smallint>=3 order by col_smallint limit 3""" 
        order_qt_timestamp_tinyint """ select col_tinyint from  parquet_alter_column_to_timestamp  where col_tinyint>=1 order by col_tinyint limit 3""" 
        order_qt_timestamp_bigint """ select col_bigint from  parquet_alter_column_to_timestamp  where col_bigint>=3 order by col_bigint limit 3""" 
        order_qt_timestamp_float """ select col_float from  parquet_alter_column_to_timestamp  where col_float=2.4 order by col_float limit 3""" 
        order_qt_timestamp_double """ select col_double from  parquet_alter_column_to_timestamp  where col_double=1.3 order by col_double limit 3""" 
        order_qt_timestamp_boolean """ select col_boolean from  parquet_alter_column_to_timestamp  where year(col_boolean)=2023 order by col_boolean limit 3""" 
        order_qt_timestamp_string """ select col_string from  parquet_alter_column_to_timestamp  where col_string="C" order by col_string limit 3""" 
        order_qt_timestamp_char """ select col_char from  parquet_alter_column_to_timestamp  where col_char="B" order by col_char limit 3""" 
        order_qt_timestamp_varchar """ select col_varchar from  parquet_alter_column_to_timestamp  where col_varchar="C" order by col_varchar limit 3""" 
        order_qt_timestamp_date """ select col_date from  parquet_alter_column_to_timestamp  where year(col_date)=2023 order by col_date limit 3""" 
        order_qt_timestamp_timestamp """ select col_timestamp from  parquet_alter_column_to_timestamp  where year(col_timestamp)=2023 order by col_timestamp limit 3""" 
        order_qt_timestamp_decimal """ select col_decimal from  parquet_alter_column_to_timestamp  where col_decimal=1.3 order by col_decimal limit 3""" 
        order_qt_decimal_int """ select col_int from  parquet_alter_column_to_decimal  where col_int=2.8 order by col_int limit 3""" 
        order_qt_decimal_smallint """ select col_smallint from  parquet_alter_column_to_decimal  where col_smallint=0.1 order by col_smallint limit 3""" 
        order_qt_decimal_tinyint """ select col_tinyint from  parquet_alter_column_to_decimal  where col_tinyint=2.9 order by col_tinyint limit 3""" 
        order_qt_decimal_bigint """ select col_bigint from  parquet_alter_column_to_decimal  where col_bigint=2.3 order by col_bigint limit 3""" 
        order_qt_decimal_float """ select col_float from  parquet_alter_column_to_decimal  where col_float=2.5 order by col_float limit 3""" 
        order_qt_decimal_double """ select col_double from  parquet_alter_column_to_decimal  where col_double=1.7 order by col_double limit 3""" 
        order_qt_decimal_boolean """ select col_boolean from  parquet_alter_column_to_decimal  where year(col_boolean)=2023 order by col_boolean limit 3""" 
        order_qt_decimal_string """ select col_string from  parquet_alter_column_to_decimal  where col_string="helloworld" order by col_string limit 3""" 
        order_qt_decimal_char """ select col_char from  parquet_alter_column_to_decimal  where col_char="helloworld" order by col_char limit 3""" 
        order_qt_decimal_varchar """ select col_varchar from  parquet_alter_column_to_decimal  where col_varchar="helloworld" order by col_varchar limit 3""" 
        order_qt_decimal_date """ select col_date from  parquet_alter_column_to_decimal  where year(col_date)=2023 order by col_date limit 3""" 
        order_qt_decimal_timestamp """ select col_timestamp from  parquet_alter_column_to_decimal  where year(col_timestamp)=2023 order by col_timestamp limit 3""" 
        order_qt_decimal_decimal """ select col_decimal from  parquet_alter_column_to_decimal  where col_decimal=1.5 order by col_decimal limit 3""" 

    }
}
