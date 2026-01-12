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

suite("test_array_map") {
    sql """
        drop table if exists mock_table;
    """

    sql """ DROP FUNCTION IF EXISTS clean_html_entity_test(string) """
    sql """
        CREATE ALIAS FUNCTION clean_html_entity_test(string) WITH PARAMETER(html) AS
        REPLACE(
        REPLACE(
        REPLACE(
        REPLACE(
        REPLACE(
                REPLACE(
                    REPLACE(
                        REPLACE(
                            REPLACE(
                                REPLACE(
                                    REPLACE(html, '&amp;', '&'),
                                    '&lt;', '<'
                                ),
                                '&gt;', '>'
                            ),
                            '&quot;', '"'
                        ),
                        '&apos;', '\\\''
                    ),'&euro;', '€'
                ),
                '&nbsp;', ' '
            ), "Ⅰ", "I"), "Ⅱ", "II"), "Ⅲ", "III"),".", ". ");
    """
    sql """ CREATE ALIAS FUNCTION clean_html_tag_test(string) WITH  PARAMETER(html) AS REGEXP_REPLACE(html, '</?[^>]+>', ''); """
    sql """
    CREATE TABLE `mock_table` (
          `aa` varchar(255) NULL,
          `ab` varchar(255) NULL,
          `ac` varchar(255) NULL,
          `ad` text NULL,
          `ae` text NULL,
          `af` text NULL,
          `ag` text NULL,
          `ah` text NULL,
          `ai` text NULL,
          `aj` varchar(255) NULL,
          `ak` text NULL,
          `al` text NULL,
          `am` text NULL,
          `an` text NULL,
          `ao` text NULL,
          `ap` text NULL,
          `aq` text NULL,
          `ar` text NULL,
          `as` text NULL,
          `at` text NULL,
          `au` text NULL,
          `av` bigint NULL,
          `aw` text NULL,
          `ax` varchar(255) NULL,
          `ay` text NULL,
          `az` varchar(255) NULL,
          `ba` varchar(255) NULL,
          `bb` varchar(255) NULL,
          `bc` int NULL,
          `bd` int NULL,
          `be` varchar(255) NULL,
          `bf` varchar(255) NULL,
          `bg` array<varchar(255)> NULL,
          `bh` json NULL,
          `bi` varchar(255) NULL,
          `bj` varchar(255) NULL,
          `bk` array<varchar(255)> NULL,
          `bl` boolean NULL,
          INDEX idx_ag (`ag`) USING INVERTED PROPERTIES("support_phrase" = "true", "parser" = "unicode", "lower_case" = "true"),
          INDEX idx_ad (`ad`) USING INVERTED PROPERTIES("support_phrase" = "true", "parser" = "unicode", "lower_case" = "true"),
          INDEX idx_ah (`ah`) USING INVERTED PROPERTIES("support_phrase" = "true", "parser" = "unicode", "lower_case" = "true"),
          INDEX idx_ac (`ac`) USING INVERTED PROPERTIES("support_phrase" = "true", "parser" = "unicode", "lower_case" = "true"),
          INDEX idx_ak (`ak`) USING INVERTED PROPERTIES("support_phrase" = "true", "parser" = "unicode", "lower_case" = "true"),
          INDEX idx_al (`al`) USING INVERTED PROPERTIES("support_phrase" = "true", "parser" = "unicode", "lower_case" = "true"),
          INDEX idx_am (`am`) USING INVERTED PROPERTIES("support_phrase" = "true", "parser" = "unicode", "lower_case" = "true"),
          INDEX idx_ag_ngrambf (`ag`) USING NGRAM_BF PROPERTIES("bf_size" = "256", "gram_size" = "2"),
          INDEX idx_ad_ngrambf (`ad`) USING NGRAM_BF PROPERTIES("bf_size" = "256", "gram_size" = "2"),
          INDEX idx_ac_ngrambf (`ac`) USING NGRAM_BF PROPERTIES("bf_size" = "256", "gram_size" = "2"),
          INDEX idx_ah_ngrambf (`ah`) USING NGRAM_BF PROPERTIES("bf_size" = "256", "gram_size" = "2"),
          INDEX idx_bi (`bi`) USING INVERTED,
          INDEX idx_ar (`ar`) USING INVERTED PROPERTIES("support_phrase" = "true", "parser" = "unicode", "lower_case" = "true"),
          INDEX idx_ar_ngrambf (`ar`) USING NGRAM_BF PROPERTIES("bf_size" = "256", "gram_size" = "2"),
          INDEX idx_bl (`bl`) USING INVERTED
        ) ENGINE=OLAP
        UNIQUE KEY(`aa`)
        DISTRIBUTED BY HASH(`aa`) BUCKETS 16
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "min_load_replica_num" = "-1",
        "is_being_synced" = "false",
        "storage_medium" = "hdd",
        "storage_format" = "V2",
        "inverted_index_storage_format" = "V1",
        "enable_unique_key_merge_on_write" = "true",
        "light_schema_change" = "true",
        "disable_auto_compaction" = "false",
        "enable_single_replica_compaction" = "false",
        "group_commit_interval_ms" = "10000",
        "group_commit_data_bytes" = "134217728",
        "enable_mow_light_delete" = "false"
        );
    """
    sql """
     CREATE VIEW `mock_view` AS
         WITH
             bm AS (SELECT
                 `aa`, `ab`, `ac`, `ad`, `ae`, `af`, `ag`, `ah`, `ai`, `aj`, `ak`, `al`, `am`, `an`, `ao`, `ap`, `aq`, `ar`, `as`, `at`, `au`, `av`, `aw`, `ax`, `ay`, `az`, `ba`, `bb`, `bc`, `bd`, `be`, `bf`, `bg`, `bh`, `bi`, `bj`, `bk`, `bl`,
                 CASE WHEN YEAR(`as`) >= 1970 THEN `as` ELSE NULL END as `bn`,
                 CASE WHEN YEAR(`au`) >= 1970 THEN `au` ELSE NULL END as `bo`,
                 CASE WHEN YEAR(`at`) >= 1970 THEN `at` ELSE NULL END as `bp`,
                 LENGTH(`aw`) as `bq`,
                 TRIM(`clean_html_entity_test`(`clean_html_tag_test`(`ah`))) as `br`,
                 TRIM(`clean_html_entity_test`(`clean_html_tag_test`(`ad`))) as `bs`,
                 ARRAY_MAP(x-> if(least((left(x, 5) = '6841-'), (length(x) = 10)), concat_ws('-', substring(x, 1, 7), substring(x, 8)), if(least((left(x, 5) = '6841-'), (length(x) = 9)), concat_ws('-', substring(x, 1, 6), substring(x, 7)), x)), `bk`) as `bt`,
                 ARRAY_JOIN(`bg`, " ") as `bu`,
                 ARRAY_JOIN(`bk`, " ") as `bv`
             FROM mock_table),
             bw AS (SELECT
                 `aa`, `ab`, `ac`, `ad`, `ae`, `af`, `ag`, `ah`, `ai`, `aj`, `ak`, `al`, `am`, `an`, `ao`, `ap`, `aq`, `ar`, `as`, `at`, `au`, `av`, `aw`, `ax`, `ay`, `az`, `ba`, `bb`, `bc`, `bd`, `be`, `bf`, `bg`, `bh`, `bi`, `bj`, `bk`, `bl`, `bn`, `bo`, `bp`, `bq`, `br`, `bs`, `bt`, `bu`, `bv`,
                 CASE
                     WHEN LENGTH(`bn`) = 10 THEN STR_TO_DATE(`bn`, 'yyyy-MM-dd')
                     WHEN LENGTH(`bn`) = 19 THEN STR_TO_DATE(`bn`, 'yyyy-MM-dd HH:mm:ss')
                     WHEN LENGTH(`bn`) = 26 THEN STR_TO_DATE(`bn`, 'yyyy-MM-dd HH:mm:ss.SSSSSS')
                     ELSE NULL
                 END AS `bx`,
                 CASE
                     WHEN LENGTH(`bo`) = 10 THEN STR_TO_DATE(`bo`, 'yyyy-MM-dd')
                     WHEN LENGTH(`bo`) = 19 THEN STR_TO_DATE(`bo`, 'yyyy-MM-dd HH:mm:ss')
                     WHEN LENGTH(`bo`) = 26 THEN STR_TO_DATE(`bo`, 'yyyy-MM-dd HH:mm:ss.SSSSSS')
                     ELSE NULL
                 END AS `by`,
                 CASE
                     WHEN LENGTH(`bp`) = 10 THEN STR_TO_DATE(`bp`, 'yyyy-MM-dd')
                     WHEN LENGTH(`bp`) = 19 THEN STR_TO_DATE(`bp`, 'yyyy-MM-dd HH:mm:ss')
                     WHEN LENGTH(`bp`) = 26 THEN STR_TO_DATE(`bp`, 'yyyy-MM-dd HH:mm:ss.SSSSSS')
                     ELSE NULL
                 END AS `bz`,
                 ARRAY_REMOVE(
                 ARRAY_COMPACT(
                 ARRAY_UNION(
                     ARRAY_MAP(x-> ARRAY_JOIN(ARRAY_SLICE(split_by_string(x, '-'), 1, size(split_by_string(x, '-')) -1), '-'), `bt`),
                     ARRAY_MAP(x-> ARRAY_JOIN(ARRAY_SLICE(split_by_string(x, '-'), 1, size(split_by_string(x, '-')) -2), '-'), `bt`),
                     ARRAY_MAP(x-> ARRAY_JOIN(ARRAY_SLICE(split_by_string(x, '-'), 1, size(split_by_string(x, '-')) -3), '-'), `bt`))), '') as `ca`,
                 SPLIT_BY_STRING(MASK(`bu`, '*', '*', '*'), ' ') as `cb`,
                 SPLIT_BY_STRING(`bu`, ' ') as `cc`,
                 array_first_index(x-> locate('*', x ) = 0, SPLIT_BY_STRING(MASK(`bu`, '*', '*', '*'), ' ')) as `cd`,
                 array_last_index(x-> locate('*', x ) = 0, SPLIT_BY_STRING(MASK(`bu`, '*', '*', '*'), ' ')) as `ce`,
                 SPLIT_BY_STRING(MASK(`bv`, '*', '*', '*'), ' ') as `cf`,
                 SPLIT_BY_STRING(`bv`, ' ') as `cg`,
                 array_first_index(x-> locate('*', x ) = 0, SPLIT_BY_STRING(MASK(`bv`, '*', '*', '*'), ' ')) as `ch`,
                 array_last_index(x-> locate('*', x ) = 0, SPLIT_BY_STRING(MASK(`bv`, '*', '*', '*'), ' ')) as `ci`
             FROM bm),
             cj AS (SELECT
                 `aa`, `ab`, `ac`, `ad`, `ae`, `af`, `ag`, `ah`, `ai`, `aj`, `ak`, `al`, `am`, `an`, `ao`, `ap`, `aq`, `ar`, `as`, `at`, `au`, `av`, `aw`, `ax`, `ay`, `az`, `ba`, `bb`, `bc`, `bd`, `be`, `bf`, `bg`, `bh`, `bi`, `bj`, `bk`, `bl`, `bn`, `bo`, `bp`, `bq`, `br`, `bs`, `bt`, `bu`, `bv`, `bx`, `by`, `bz`, `ca`, `cb`, `cc`, `cd`, `ce`, `cf`, `cg`, `ch`, `ci`,
                 ARRAY_COMPACT(ARRAY_EXCEPT(`bt`, `ca`)) as `ck`,
                 ARRAY_COMPACT(ARRAY_UNION(`bt`, `ca`)) as `cl`,
                 CASE
                     WHEN SIZE(`cc`) = 0 THEN `bs`
                     WHEN `cd`=1 AND `ce` < size(`cb`) and `ce` - `cd` > 1 THEN ARRAY_JOIN(ARRAY_SLICE(`cc`, 1, `ce`), " ")
                     WHEN `cd`=2 AND `ce` < size(`cb`) and `ce` - `cd` > 1 THEN ARRAY_JOIN(ARRAY_SLICE(`cc`, 1, `ce`), " ")
                     WHEN `cd` >2 AND `ce` = size(`cb`) and `ce` - `cd` > 1 THEN
                     CASE
                         WHEN element_at(`cc`, 1) = element_at(`cc`, `cd`-1) THEN ARRAY_JOIN(ARRAY_SLICE(`cc`, `cd`-1), "")
                         ELSE ARRAY_JOIN(ARRAY_SLICE(`cc`, `cd`), " ")
                     END
                     ELSE ARRAY_JOIN(`cc`, " ")
                 END AS `cm`,
                 CASE
                     WHEN size(`cc`) = 0 THEN "tokenize_failed"
                     WHEN `cd` = 0 THEN "en"
                     WHEN `cd`=1 AND `ce` = size(`cb`) THEN "zh"
                     WHEN `cd`=1 AND `ce` < size(`cb`) THEN "zh_en"
                     WHEN `cd`=2 AND `ce` < size(`cb`) THEN "zh_en"
                     WHEN `cd` >2 AND `ce` = size(`cb`) THEN "en_zh"
                     ELSE "mixed"
                 END AS `cn`,
                 CASE
                     WHEN SIZE(`cg`) = 0 THEN `br`
                     WHEN `ch`=1 AND `ci` < size(`cf`) and `ci` - `ch` > 1 THEN ARRAY_JOIN(ARRAY_SLICE(`cg`, 1, `ci`), " ")
                     WHEN `ch`=2 AND `ci` < size(`cf`) and `ci` - `ch` > 1 THEN ARRAY_JOIN(ARRAY_SLICE(`cg`, 1, `ci`), " ")
                     WHEN `ch` >2 AND `ci` = size(`cf`) and `ci` - `ch` > 1 THEN
                     CASE
                         WHEN element_at(`cg`, 1) = element_at(`cg`, `ch`-1) THEN ARRAY_JOIN(ARRAY_SLICE(`cg`, `ch`-1), "")
                         ELSE ARRAY_JOIN(ARRAY_SLICE(`cg`, `ch`), " ")
                     END
                     ELSE ARRAY_JOIN(`cg`, " ")
                 END AS `co`,
                 CASE
                     WHEN size(`cg`) = 0 THEN "tokenize_failed"
                     WHEN `ch` = 0 THEN "en"
                     WHEN `ch`=1 AND `ci` = size(`cf`) THEN "zh"
                     WHEN `ch`=1 AND `ci` < size(`cf`) THEN "zh_en"
                     WHEN `ch`=2 AND `ci` < size(`cf`) THEN "zh_en"
                     WHEN `ch` >2 AND `ci` = size(`cf`) THEN "en_zh"
                     ELSE "mixed"
                 END AS `cp`
             FROM bw)
         SELECT * FROM cj;
     """
    sql """
        insert into mock_table(aa, ab,ac,ad) values('1','2','3','4');
    """


/*
FIXME
qt_sql """
        SELECT * FROM mock_view LIMIT 530000,1000;
    """
*/

}
