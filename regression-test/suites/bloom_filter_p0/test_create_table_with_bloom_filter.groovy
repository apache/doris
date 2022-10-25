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

suite("test_create_table_with_bloom_filter") {
    sql """DROP TABLE IF EXISTS test_bloom_filter"""
    sql """
        CREATE TABLE IF NOT EXISTS test_bloom_filter( 
            tinyint_key TINYINT NOT NULL, 
            smallint_key SMALLINT NOT NULL, 
            int_key INT NOT NULL, 
            bigint_key BIGINT NOT NULL, 
            char_50_key CHAR(50) NOT NULL, 
            character_key VARCHAR(500) NOT NULL, 
            char_key CHAR NOT NULL, 
            character_most_key VARCHAR(65533) NOT NULL, 
            decimal_key DECIMAL(20, 6) NOT NULL, 
            decimal_most_key DECIMAL(27, 9) NOT NULL, 
            date_key DATE NOT NULL, 
            datetime_key DATETIME NOT NULL,
            datev2_key DATEV2 NOT NULL,
            datetimev2_key_1 DATETIMEV2 NOT NULL,
            datetimev2_key_2 DATETIMEV2(3) NOT NULL,
            datetimev2_key_3 DATETIMEV2(6) NOT NULL,
            tinyint_value TINYINT SUM NOT NULL, 
            smallint_value SMALLINT SUM NOT NULL, 
            int_value int SUM NOT NULL, 
            bigint_value BIGINT SUM NOT NULL, 
            char_50_value CHAR(50) REPLACE NOT NULL, 
            character_value VARCHAR(500) REPLACE NOT NULL, 
            char_value CHAR REPLACE NOT NULL, 
            character_most_value VARCHAR(65533) REPLACE NOT NULL, 
            decimal_value DECIMAL(20, 6) SUM NOT NULL, 
            decimal_most_value DECIMAL(27, 9) SUM NOT NULL, 
            date_value_max DATE MAX NOT NULL, 
            date_value_replace DATE REPLACE NOT NULL, 
            date_value_min DATE MIN NOT NULL, 
            datetime_value_max DATETIME MAX NOT NULL, 
            datetime_value_replace DATETIME REPLACE NOT NULL, 
            datetime_value_min DATETIME MIN NOT NULL,
            datev2_value_max DATEV2 MAX NOT NULL,
            datev2_value_replace DATEV2 REPLACE NOT NULL,
            datev2_value_min DATEV2 MIN NOT NULL,
            datetimev2_value_1_max DATETIMEV2 MAX NOT NULL,
            datetimev2_value_1_replace DATETIMEV2 REPLACE NOT NULL,
            datetimev2_value_1_min DATETIMEV2 MIN NOT NULL,
            datetimev2_value_2_max DATETIMEV2(3) MAX NOT NULL,
            datetimev2_value_2_replace DATETIMEV2(3) REPLACE NOT NULL,
            datetimev2_value_2_min DATETIMEV2(3) MIN NOT NULL,
            datetimev2_value_3_max DATETIMEV2(6) MAX NOT NULL,
            datetimev2_value_3_replace DATETIMEV2(6) REPLACE NOT NULL,
            datetimev2_value_3_min DATETIMEV2(6) MIN NOT NULL,
            float_value FLOAT SUM NOT NULL, 
            double_value DOUBLE SUM NOT NULL ) 
        AGGREGATE KEY(
            tinyint_key,
            smallint_key,
            int_key,
            bigint_key,
            char_50_key,
            character_key,
            char_key,
            character_most_key,
            decimal_key,
            decimal_most_key,
            date_key,
            datetime_key,
            datev2_key,
            datetimev2_key_1,
            datetimev2_key_2,
            datetimev2_key_3)
        DISTRIBUTED BY HASH(tinyint_key) BUCKETS 5 
        PROPERTIES ( 
            "bloom_filter_columns"="smallint_key,int_key,bigint_key,char_50_key,character_key,
                                    char_key,character_most_key,decimal_key,decimal_most_key,
                                    date_key,datetime_key,datev2_key, datetimev2_key_1, datetimev2_key_2, datetimev2_key_3",
            "replication_num" = "1"
        )
        """
    sql """
            INSERT INTO test_bloom_filter VALUES 
                ('1', '2', '4', '8', '50string', '500varchar', 'c', '65535varchar', 
                 '0', '123456789012345678.123456789', '2013-12-01', '1900-01-01 00:00:00',
                 '2013-12-01', '1900-01-01 00:00:00.111111',
                 '1900-01-01 00:00:00.111111', '1900-01-01 00:00:00.111111',
                 '1', '2', '4', '8', '50string', '500varchar_replace',
                 'c', '65535varchar', '12345678901234.123456', '123456789012345678.123456789',
                 '1900-01-01', '1900-01-01', '1900-01-01', '1900-01-01 00:00:00', '1900-01-01 00:00:00',
                 '1900-01-01 00:00:00', '2013-12-01', '2013-12-01', '2013-12-01', '1900-01-01 00:00:00.111111',
                 '1900-01-01 00:00:00.111111', '1900-01-01 00:00:00.111111', '1900-01-01 00:00:00.111111',
                 '1900-01-01 00:00:00.111111', '1900-01-01 00:00:00.111111', '1900-01-01 00:00:00.111111',
                 '1900-01-01 00:00:00.111111', '1900-01-01 00:00:00.111111', '0.4', '0.8')
        """
    qt_select_tb "SELECT * FROM test_bloom_filter"
    qt_desc_tb "DESC test_bloom_filter"
    sql "DROP TABLE test_bloom_filter"
}

