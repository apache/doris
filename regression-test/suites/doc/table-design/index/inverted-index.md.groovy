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

import org.junit.jupiter.api.Assertions;

suite("docs/table-design/index/inverted-index.md.groovy") {
    def waitUntilSchemaChangeDone = { tbl ->
        waitForSchemaChangeDone({
            sql " SHOW ALTER TABLE COLUMN FROM test_inverted_index WHERE TableName='${tbl}' ORDER BY createtime DESC LIMIT 1 "
        })
    }
    try {
        sql """ SELECT TOKENIZE('武汉长江大桥','"parser"="chinese","parser_mode"="fine_grained"') """
        sql """ SELECT TOKENIZE('武汉市长江大桥','"parser"="chinese","parser_mode"="fine_grained"') """
        sql """ SELECT TOKENIZE('武汉市长江大桥','"parser"="chinese","parser_mode"="coarse_grained"') """
        sql """ SELECT TOKENIZE('I love CHINA','"parser"="english"') """
        sql """ SELECT TOKENIZE('I love CHINA 我爱我的祖国','"parser"="unicode"') """

        sql "DROP DATABASE IF EXISTS test_inverted_index;"
        multi_sql """
        CREATE DATABASE test_inverted_index;
        
        USE test_inverted_index;
        
        -- 创建表的同时创建了 comment 的倒排索引 idx_comment
        --   USING INVERTED 指定索引类型是倒排索引
        --   PROPERTIES("parser" = "english") 指定采用 "english" 分词，还支持 "chinese" 中文分词和 "unicode" 中英文多语言混合分词，如果不指定 "parser" 参数表示不分词
        
        CREATE TABLE hackernews_1m
        (
            `id` BIGINT,
            `deleted` TINYINT,
            `type` String,
            `author` String,
            `timestamp` DateTimeV2,
            `comment` String,
            `dead` TINYINT,
            `parent` BIGINT,
            `poll` BIGINT,
            `children` Array<BIGINT>,
            `url` String,
            `score` INT,
            `title` String,
            `parts` Array<INT>,
            `descendants` INT,
            INDEX idx_comment (`comment`) USING INVERTED PROPERTIES("parser" = "english") COMMENT 'inverted index for comment'
        )
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 10
        PROPERTIES ("replication_num" = "1");       
        """

        sql """ SELECT count() FROM hackernews_1m """
        sql """ SELECT count() FROM hackernews_1m WHERE comment LIKE '%OLAP%' """
        sql """ SELECT count() FROM hackernews_1m WHERE comment MATCH_ANY 'OLAP' """
        multi_sql """
            SELECT count() FROM hackernews_1m WHERE comment LIKE '%OLTP%';
            SELECT count() FROM hackernews_1m WHERE comment MATCH_ANY 'OLTP';
        """
        multi_sql """
            SELECT count() FROM hackernews_1m WHERE comment LIKE '%OLAP%' AND comment LIKE '%OLTP%';
            SELECT count() FROM hackernews_1m WHERE comment MATCH_ALL 'OLAP OLTP';
        """
        multi_sql """ 
            SELECT count() FROM hackernews_1m WHERE comment LIKE '%OLAP%' OR comment LIKE '%OLTP%';
            SELECT count() FROM hackernews_1m WHERE comment MATCH_ANY 'OLAP OLTP';
        """
        sql """ SELECT count() FROM hackernews_1m WHERE timestamp > '2007-08-23 04:17:00' """
        sql """ CREATE INDEX idx_timestamp ON hackernews_1m(timestamp) USING INVERTED """
        waitUntilSchemaChangeDone("hackernews_1m")
        if (!isCloudMode()) {
            sql """ BUILD INDEX idx_timestamp ON hackernews_1m """
        }
        sql """ SHOW ALTER TABLE COLUMN """
        sql """ SHOW BUILD INDEX """
        sql """ SELECT count() FROM hackernews_1m WHERE timestamp > '2007-08-23 04:17:00' """

        multi_sql """
            SELECT count() FROM hackernews_1m WHERE parent = 11189;
            ALTER TABLE hackernews_1m ADD INDEX idx_parent(parent) USING INVERTED;
        """

        waitUntilSchemaChangeDone("hackernews_1m")
        if (!isCloudMode()) {
            sql "BUILD INDEX idx_parent ON hackernews_1m;"
        }
        multi_sql """
            SHOW ALTER TABLE COLUMN;
            SHOW BUILD INDEX;
            SELECT count() FROM hackernews_1m WHERE parent = 11189;
        """
        multi_sql """
            SELECT count() FROM hackernews_1m WHERE author = 'faster';
            ALTER TABLE hackernews_1m ADD INDEX idx_author(author) USING INVERTED;
        """
        waitUntilSchemaChangeDone("hackernews_1m")
        if (!isCloudMode()) {
            sql "BUILD INDEX idx_author ON hackernews_1m"
        }
        multi_sql """
            SHOW ALTER TABLE COLUMN;
            SHOW BUILD INDEX order by CreateTime desc limit 1;
            SELECT count() FROM hackernews_1m WHERE author = 'faster';
        """
    } catch (Throwable t) {
        Assertions.fail("examples in docs/table-design/index/inverted-index.md failed to exec, please fix it", t)
    }
}
