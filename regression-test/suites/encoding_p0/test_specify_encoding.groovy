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

suite("test_specify_encoding")  {
    sql """
        admin set frontend config("enable_specify_column_encoding" = "true");
    """

    def checkScalar = { java.util.Map column, String encoding->
        def columnName = column.name
        var exceptedEncoding = encoding
        if (encoding == null) {
            def encodingStr = columnName.split("_")[1]
            switch (encodingStr) {
                case "bs": exceptedEncoding = "BIT_SHUFFLE"; break
                case "for": exceptedEncoding = "FOR_ENCODING"; break
                case "plain": exceptedEncoding = "PLAIN_ENCODING"; break
                case "dict": exceptedEncoding = "DICT_ENCODING"; break
                case "prefix": exceptedEncoding = "PREFIX_ENCODING"; break
                case "rle": exceptedEncoding = "RLE"; break
                case "default": exceptedEncoding = "DEFAULT_ENCODING"; break
                default: throw new Exception("unexpected encoding: " + encodingStr)
            }
        }

        def realEncoding = column.encoding
        logger.info("realEncoding=" + realEncoding + ", exceptedEncoding=" + exceptedEncoding + ", name=" + columnName)
        assertEquals(exceptedEncoding, realEncoding)
    }

    def checkColumn = {Map column, String complexEncoding ->
        if ("ARRAY".equalsIgnoreCase(column.type)) {
            checkScalar(column.children_columns[0], complexEncoding)
        } else if ("MAP".equalsIgnoreCase(column.type)) {
            checkScalar(column.children_columns[0], complexEncoding)
            checkScalar(column.children_columns[1], complexEncoding)
        } else if ("STRUCT".equalsIgnoreCase(column.type)) {
            for (int i = 0; i < column.children_columns.size(); i++) {
                checkScalar(column.children_columns[i], null)
            }
        } else {
            checkScalar(column, null)
        }
    }

    def assertEncoding = { String tableName, String complexEncoding ->
        def tablets = sql_return_maparray """ show tablets from ${tableName}; """
        def metaUrl = tablets[0].MetaUrl
        def (code, out, err) = http_client("GET", metaUrl)
        logger.info("get tablet meta: code=" + code + ", err=" + err)
        assertEquals(code, 0)
        def tabletJson = parseJson(out.trim())
        assert tabletJson.schema.column instanceof List
        for (Map column in (List) tabletJson.schema.column) {
            checkColumn.call(column, complexEncoding)
        }
    }

    def tableName = "test_specify_encoding"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `tiny_bs` tinyint encoding "bit_shuffle" NULL,
            -- `tiny_for` tinyint encoding "for" NULL,
            `tiny_plain` tinyint encoding "plain" NULL,
            `small_bs` smallint encoding "bit_shuffle" NULL,
            -- `small_for` smallint encoding "for" NULL,
            `small_plain` smallint encoding "plain" NULL,
            `int_bs` int encoding "bit_shuffle" NULL,
            -- `int_for` int encoding "for" NULL,
            `int_plain` int encoding "plain" NULL,
            `bigint_bs` bigint encoding "bit_shuffle" NULL,
            -- `bigint_for` bigint encoding "for" NULL,
            `bigint_plain` bigint encoding "plain" NULL,
            `largeint_bs` largeint encoding "bit_shuffle" NULL,
            -- `largeint_for` largeint encoding "for" NULL,
            `largeint_plain` largeint encoding "plain" NULL,

            `float_bs` float encoding "bit_shuffle" NULL,
            `float_plain` float encoding "plain" NULL,
            `double_bs` double encoding "bit_shuffle" NULL,
            `double_plain` double encoding "plain" NULL,

            `char_dict` char(10) encoding "dict" NULL,
            `char_plain` char(10) encoding "plain" NULL,
            -- `char_prefix` char(10) encoding "prefix" NULL,
            `varchar_dict` varchar(10) encoding "dict" NULL,
            `varchar_plain` varchar(10) encoding "plain" NULL,
            -- `varchar_prefix` varchar(10) encoding "prefix" NULL,
            `string_dict` string encoding "dict" NULL,
            `string_plain` string encoding "plain" NULL,
            -- `string_prefix` string encoding "prefix" NULL,
            `json_dict` json encoding "dict" NULL,
            `json_plain` json encoding "plain" NULL,
            -- `json_prefix` json encoding "prefix" NULL,
            `variant_dict` variant encoding "dict" NULL,
            `variant_plain` variant encoding "plain" NULL,
            -- `variant_prefix` variant encoding "prefix" NULL,

            `boolean_rle` boolean encoding "rle" NULL,
            `boolean_bs` boolean encoding "bit_shuffle" NULL,
            `boolean_plain` boolean encoding "plain" NULL,
            
            `date_bs` date encoding "bit_shuffle" NULL,
            `date_plain` date encoding "plain" NULL,
            -- `date_for` date encoding "for" NULL,
            `datetime_bs` datetime encoding "bit_shuffle" NULL,
            `datetime_plain` datetime encoding "plain" NULL,
            -- `datetime_for` datetime encoding "for" NULL,
            
            `decimal_bs` decimal encoding "bit_shuffle" NULL,
            `decimal_plain` decimal encoding "plain" NULL,
            
            `ipv4_bs` ipv4 encoding "bit_shuffle" NULL,
            `ipv4_plain` ipv4 encoding "plain" NULL,
            `ipv6_bs` ipv6 encoding "bit_shuffle" NULL,
            `ipv6_plain` ipv6 encoding "plain" NULL,

            `array_test` array<int encoding "bit_shuffle">  NULL,
            `map_test` map<int encoding "bit_shuffle", int encoding "bit_shuffle"> NULL,
            `struct_test` struct<`a_bs`: int encoding "bit_shuffle", `b_bs`: int encoding "bit_shuffle"> NULL
       ) DUPLICATE KEY(`tiny_bs`, `tiny_plain`)
         DISTRIBUTED BY RANDOM BUCKETS 1
         PROPERTIES (
           "replication_allocation" = "tag.location.default: 1"
       )
    """
    assertEncoding(tableName, 'BIT_SHUFFLE')

    sql """
        INSERT INTO ${tableName} VALUES 
        (
            1,            
            -- 1,            
            -- 1,            
            -- 1,            
            -- 1,            
            -- 1,            
            1,            
            1,            
            1,            
            1,            
            1,            
            1,            
            1,            
            1,            
            1,            
            
            1.1,          
            1.1,          
            1.1,          
            1.1,          
            
            'test_str',   
            'test_str',   
            'test_str',   
            'test_str',   
            'test_str',   
            'test_str',   
            -- 'test_str',   
            -- 'test_str',   
            -- 'test_str',   
            '{"test_j"}', 
            '{"test_j"}', 
            '{"test_j"}', 
            '{"test_j"}', 
            -- '{"test_j"}', 
            -- '{"test_j"}', 
            
            true,         
            false,        
            true,         
                        
            '2025-01-01',  
            '2025-01-01',  
            -- '2025-01-01',  
            -- '2025-12-31 10:10:10',   
            '2025-12-31 10:10:10', 
            '2025-12-31 10:10:10',   
                        
            12.23,       
            12.23,       
                        
            to_ipv4('127.0.0.1'),  
            to_ipv4('127.0.0.1'),  
            to_ipv6('::1'),        
            to_ipv6('::1'),        
            
            [1,2,3],               
            map(1, 100, 2, 200),   
            struct(1, 2)         
        )  
    """
    qt_select_encoding "select * from ${tableName}"


    def hllTableName = "test_hll_encoding";
    sql """ DROP TABLE IF EXISTS ${hllTableName} """
    sql """
         CREATE TABLE IF NOT EXISTS ${hllTableName} (
          `tiny_default` tinyint encoding "default" NULL,
          `hll_plain` hll encoding "plain" hll_union NOT NULL COMMENT "hll"
        )ENGINE=OLAP
        AGGREGATE KEY(`tiny_default`)
        DISTRIBUTED BY RANDOM BUCKETS 1
         PROPERTIES (
           "replication_allocation" = "tag.location.default: 1"
        )
   """
    assertEncoding(hllTableName, null)
    sql """
        INSERT INTO ${hllTableName} VALUES (1, hll_hash('some value'))
    """
    qt_select_hll "select tiny_default, HLL_CARDINALITY(hll_plain) from ${hllTableName}"

    def defaultTableName = "test_default_encoding"
    sql """ DROP TABLE IF EXISTS ${defaultTableName} """
    sql """
         CREATE TABLE IF NOT EXISTS ${defaultTableName} (
           `tiny_default` tinyint NULL,
           `small_default` smallint NULL,
           `int_default` int NULL,
           `bigint_default` bigint NULL,
           `largeint_default` largeint NULL,
           `float_default` float NULL,
           `double_default` double NULL,
           `char_default` char(10) NULL,
           `varchar_default` varchar(10) NULL,
           `string_default` text NULL,
           `json_default` json NULL,
           `variant_default` variant NULL,
           `boolean_default` boolean NULL,
           `date_default` date NULL,
           `datetime_default` datetime NULL,
           `decimal_default` decimal(38,9) NULL,
           `ipv4_default` ipv4 NULL,
           `ipv6_default` ipv6 NULL,
           `array_test` array<int> NULL,
           `map_test` map<int,int> NULL,
           `struct_test` struct<a_default:int> NULL
        )ENGINE=OLAP
         DUPLICATE KEY(`tiny_default`, `small_default`, `int_default`)
         DISTRIBUTED BY RANDOM BUCKETS 1
         PROPERTIES (
           "replication_allocation" = "tag.location.default: 1"
        )
    """
    assertEncoding(defaultTableName, 'DEFAULT_ENCODING')
    sql """
        INSERT INTO ${defaultTableName} VALUES 
        (
            1,           
            1,           
            1,           
            1,           
            1,           
            1.1,         
            1.1,         
            'str',       
            'str',       
            'str',       
            '{"test_j"}',
            '{"test_j"}',
            true,         
            '2025-01-01',
            '2025-12-31 10:10:10', 
            12.23,          
            to_ipv4('127.0.0.1'),
            to_ipv6('::1'),      
            [1,2,3],             
            map(1, 100, 2, 200), 
            struct(1)         
        ) 
"""
    qt_select_default "SELECT * FROM ${defaultTableName}"
}