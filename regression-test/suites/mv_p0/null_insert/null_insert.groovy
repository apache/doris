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

import org.codehaus.groovy.runtime.IOGroovyMethods

suite ("null_insert") {

    sql """ DROP TABLE IF EXISTS test; """

    sql """
        CREATE TABLE `test` (
        `date` date NOT NULL,
        `vid` bigint(20),
        `uid` bigint(20),
        `str_uid` varchar(300),
        `os` varchar(65533),
        `net` varchar(300),
        `ver` varchar(65533),
        `os_ver` varchar(300),
        `channel_profile` varchar(300),
        `channel_mode` varchar(300),
        `product_line` varchar(300),
        `product_type` varchar(300),
        `ip` varchar(300),
        `ip_domain` varchar(65533),
        `ip_country` varchar(65533)
        ) ENGINE=OLAP
        DUPLICATE KEY(`date`)
        DISTRIBUTED BY HASH(`date`)
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
        """

    sql """INSERT INTO `test` (`date`) VALUES ('2023-07-19');"""

    createMV("""CREATE materialized view mv_test AS
                SELECT date, vid, os, ver, ip_country, hll_union(hll_hash(uid))
                FROM test
                GROUP BY date,vid,os,ver,ip_country;""")

    sql """INSERT INTO `test` (`date`) VALUES ('2023-07-19');"""

    streamLoad {
        table "test"

        set 'columns', 'date'

        file './test'
        time 10000 // limit inflight 10s
    }

    explain {
        sql("""SELECT date, vid, os, ver, ip_country, hll_union(hll_hash(uid))
                FROM test
                GROUP BY date,vid,os,ver,ip_country;""")
        contains "(mv_test)"
    }

    qt_select_mv """SELECT date, vid, os, ver, ip_country, hll_union(hll_hash(uid))
                    FROM test
                    GROUP BY date,vid,os,ver,ip_country;"""
}
