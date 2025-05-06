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

suite("test_remote_doris_predict", "p0,external,doris,external_docker,external_docker_doris") {
    String remote_doris_host = context.config.otherConfigs.get("extArrowFlightSqlHost")
    String remote_doris_arrow_port = context.config.otherConfigs.get("extArrowFlightSqlPort")
    String remote_doris_http_port = context.config.otherConfigs.get("extArrowFlightHttpPort")
    String remote_doris_user = context.config.otherConfigs.get("extArrowFlightSqlUser")
    String remote_doris_psw = context.config.otherConfigs.get("extArrowFlightSqlPassword")
    sql """DROP DATABASE IF EXISTS test_remote_doris_predict_db"""

    sql """CREATE DATABASE IF NOT EXISTS test_remote_doris_predict_db"""

    sql """
        CREATE TABLE `test_remote_doris_predict_db`.`test_remote_doris_predict_t` (
          `id` int NOT NULL,
          `c_int` int NULL,
          `c_string` text NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        INSERT INTO `test_remote_doris_predict_db`.`test_remote_doris_predict_t` VALUES
        (1,1,'abc'),
        (2,2,'bcd'),
        (3,3,'cde'),
        (4,4,'def'),
        (5,5,'efg');
    """

    sql """
        DROP CATALOG IF EXISTS `test_remote_doris_predict_catalog`
    """

    sql """
       CREATE CATALOG `test_remote_doris_all_types_select_catalog` PROPERTIES (
                'type' = 'doris',
                'fe_http_hosts' = 'http://${remote_doris_host}:${remote_doris_http_port}',
                'fe_arrow_hosts' = '${remote_doris_host}:${remote_doris_arrow_port}',
                'user' = '${remote_doris_user}',
                'password' = '${remote_doris_psw}'
        );
    """

    sql """use test_remote_doris_predict_catalog.test_remote_doris_predict_db"""

    explain {
        sql("select * from test_remote_doris_predict_t where c_int < 3")
        contains("c_int < 3")
    }

    explain {
        sql("select * from test_remote_doris_predict_t where c_int <= 3")
        contains("c_int <= 3")
    }

    explain {
        sql("select * from test_remote_doris_predict_t where c_int > 3")
        contains("c_int > 3")
    }

    explain {
        sql("select * from test_remote_doris_predict_t where c_int >= 3")
        contains("c_int >= 3")
    }

    explain {
        sql("select * from test_remote_doris_predict_t where c_int = 3")
        contains("c_int = 3")
    }

    explain {
        sql("select * from test_remote_doris_predict_t where c_int != 3")
        contains("c_int != 3")
    }

    explain {
        sql("select * from test_remote_doris_predict_t where c_int > 3 AND c_string = 'cde'")
        contains("((c_int > 3)) AND ((c_string = 'cde'))")
    }

    explain {
        sql("select * from test_remote_doris_predict_t where c_int > 3 OR c_string = 'cde'")
        contains("(c_int > 3) OR (c_string = 'cde')")
    }

    explain {
        sql("select * from test_remote_doris_predict_t where NOT c_int > 3")
        contains("c_int <= 3")
    }

    explain {
        sql("select * from test_remote_doris_predict_t where c_string LIKE '%d%'")
        contains("c_string like '%d%'")
    }

    explain {
        sql("select * from test_remote_doris_predict_t where NOT c_string LIKE '%d%'")
        contains("NOT c_string like '%d%'")
    }

    explain {
        sql("select * from test_remote_doris_predict_t where c_int IN(2,3,4)")
        contains("c_int IN (2, 3, 4)")
    }

    explain {
        sql("select * from test_remote_doris_predict_t where c_string IS NULL")
        contains("c_string IS NULL")
    }

    explain {
        sql("select * from test_remote_doris_predict_t where c_string IS NOT NULL")
        contains("c_string IS NOT NULL")
    }

    explain {
        sql("select * from test_remote_doris_predict_t where trim_in(c_string,'a') = 'bc';")
        contains("(trim_in(c_string, 'a') = 'bc'")
    }

    sql """DROP DATABASE IF EXISTS test_remote_doris_predict_db"""
    sql """
        DROP CATALOG IF EXISTS `test_remote_doris_predict_catalog`
    """
}

