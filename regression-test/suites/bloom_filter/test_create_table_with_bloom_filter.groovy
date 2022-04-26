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

suite("test_create_table_with_bloom_filter", "bloom_filter") {
    sql """DROP TABLE IF EXISTS test_bloom_filter"""
    sql """
        CREATE TABLE test_bloom_filter( 
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
            datetime_key) 
        DISTRIBUTED BY HASH(tinyint_key) BUCKETS 5 
        PROPERTIES ( 
            "bloom_filter_columns"="smallint_key,int_key,bigint_key,char_50_key,character_key,
                                    char_key,character_most_key,decimal_key,decimal_most_key,
                                    date_key,datetime_key",
            "replication_num" = "1"
        )
        """
    sql """
            INSERT INTO test_bloom_filter VALUES 
                ('1', '2', '4', '8', '50string', '500varchar', 'c', '65535varchar', 
                 '0', '123456789012345678.123456789', '2013-12-01', '1900-01-01 00:00:00', 
                 '1', '2', '4', '8', '50string', '500varchar_replace', 'c', '65535varchar', 
                 '12345678901234.123456', '123456789012345678.123456789', '1900-01-01', 
                 '1900-01-01', '1900-01-01', '1900-01-01 00:00:00', '1900-01-01 00:00:00', 
                 '1900-01-01 00:00:00', '0.4', '0.8')
        """
    qt_select_tb "SELECT * FROM test_bloom_filter"
    qt_desc_tb "DESC test_bloom_filter"
    sql "DROP TABLE test_bloom_filter"
}

