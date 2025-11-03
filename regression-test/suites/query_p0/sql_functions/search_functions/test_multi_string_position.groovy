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

suite("test_multi_string_position") {
    def table_name = "test_multi_string_position_strings"

    sql """ DROP TABLE IF EXISTS ${table_name} """
    sql """ CREATE TABLE IF NOT EXISTS ${table_name}
            (
                `col1`      INT NOT NULL,
                `content`   TEXT NULL,
                `mode`      ARRAY<TEXT> NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`col1`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`col1`) BUCKETS 3
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "in_memory" = "false",
            "storage_format" = "V2"
            );
        """

    sql """ INSERT INTO ${table_name} (col1, content, mode) VALUES
            (1, 'Hello, World!', ['hello', 'world'] ),
            (2, 'Hello, World!', ['hello', 'world', 'Hello', '!'] ),
            (3, 'hello, world!', ['Hello', 'world'] ),
            (4, 'hello, world!', ['hello', 'world', 'Hello', '!'] ),
            (5, 'HHHHW!', ['H', 'HHHH', 'HW', 'WH'] ),
            (6, 'abc', null),
            (7, null, null),
            (8, null, ['a', 'b', 'c']);
        """

    qt_table_select1 "select multi_search_all_positions(content, ['hello', '!', 'world', 'Hello', 'World']) from ${table_name} order by col1"
    qt_table_select2 "select multi_search_all_positions(content, mode) from ${table_name} order by col1"

    qt_select1 "select multi_search_all_positions('jmdqwjbrxlbatqeixknricfk', ['qwjbrxlba', 'jmd', '', 'mdqwjbrxlbatqe', 'jbrxlbatqeixknric', 'jmdqwjbrxlbatqeixknri', '', 'fdtmnwtts', 'qwjbrxlba', '', 'qeixknricfk', 'hzjjgrnoilfkvzxaemzhf', 'lb', 'kamz', 'ixknr', 'jbrxlbatq'])"
    qt_select2 "select multi_search_all_positions('coxcctuehmzkbrsmodfvx', ['bkhnp', 'nlypjvriuk', 'rkslxwfqjjivcwdexrdtvjdtvuu', 'oxcctuehm', 'xcctuehmzkbrsm', 'kfrieuocovykjmkwxbdlkgwctwvcuh', 'coxc', 'lbwvetgxyndxjqqwthtkgasbafii', 'ctuehmzkbrsmodfvx', 'obzldxjldxowk', 'ngfikgigeyll', 'wdaejjukowgvzijnw', 'zkbr', 'mzkb', 'tuehm', 'ue'])"
    qt_select3 "select multi_search_all_positions('mpswgtljbbrmivkcglamemayfn', ['', 'm', 'saejhpnfgfq', 'rzanrkdssmmkanqjpfi', 'oputeneprgoowg', 'mp', '', '', 'wgtljbbrmivkcglamemay', 'cbpthtrgrmgfypizi', 'tl', 'tlj', 'xuhs', 'brmivkcglamemayfn', '', 'gtljb'])"
    qt_select4 "select multi_search_all_positions('arbphzbbecypbzsqsljurtddve', ['arbphzb', 'mnrboimjfijnti', 'cikcrd', 'becypbz', 'z', 'uocmqgnczhdcrvtqrnaxdxjjlhakoszuwc', 'bbe', '', 'bp', 'yhltnexlpdijkdzt', 'jkwjmrckvgmccmmrolqvy', 'vdxmicjmfbtsbqqmqcgtnrvdgaucsgspwg', 'witlfqwvhmmyjrnrzttrikhhsrd', 'pbzsqsljurt'])"
    qt_select5 "select multi_search_all_positions('aizovxqpzcbbxuhwtiaaqhdqjdei', ['qpzcbbxuhw', 'jugrpglqbm', 'dspwhzpyjohhtizegrnswhjfpdz', 'pzcbbxuh', 'vayzeszlycke', 'i', 'gvrontcpqavsjxtjwzgwxugiyhkhmhq', 'gyzmeroxztgaurmrqwtmsxcqnxaezuoapatvu', 'xqpzc', 'mjiswsvlvlpqrhhptqq', 'iz', 'hmzjxxfjsvcvdpqwtrdrp', 'zovxqpzcbbxuhwtia', 'ai'])"

    try {
        sql "select multi_search_all_positions(content, 'hello') from ${table_name} order by col1"
    } catch (Exception ex) {
        assert("${ex}".contains("multi_search_all_positions"))
    }

    try {
        sql "select multi_search_all_positions(content, 'hello, !, world, Hello, World') from ${table_name} order by col1"
    } catch (Exception ex) {
        assert("${ex}".contains("multi_search_all_positions"))
    }

    try {
        sql "select multi_search_all_positions(content, '[hello]') from ${table_name} order by col1"
    } catch (Exception ex) {
        assert("${ex}".contains("multi_search_all_positions"))
    }

    try {
        sql "select multi_search_all_positions(content, '[hello, !, world, Hello, World]') from ${table_name} order by col1"
    } catch (Exception ex) {
        assert("${ex}".contains("multi_search_all_positions"))
    }
}
