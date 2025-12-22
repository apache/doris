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

suite("test_arrow_flight_predict", "p0,external,doris,external_docker,external_docker_doris") {
    String arrow_flight_host = context.config.otherConfigs.get("extArrowFlightSqlHost")
    String arrow_flight_arrow_port = context.config.otherConfigs.get("extArrowFlightSqlPort")
    String arrow_flight_http_port = context.config.otherConfigs.get("extArrowFlightHttpPort")
    String arrow_flight_user = context.config.otherConfigs.get("extArrowFlightSqlUser")
    String arrow_flight_psw = context.config.otherConfigs.get("extArrowFlightSqlPassword")
    String arrow_flight_thrift_port = context.config.otherConfigs.get("extFeThriftPort")

    def showres = sql "show frontends";
    arrow_flight_arrow_port = showres[0][6]
    arrow_flight_http_port = showres[0][3]
    arrow_flight_thrift_port = showres[0][5]
    log.info("show frontends log = ${showres}, arrow: ${arrow_flight_arrow_port}, http: ${arrow_flight_http_port}, thrift: ${arrow_flight_thrift_port}")

    def showres2 = sql "show backends";
    log.info("show backends log = ${showres2}")

    def db_name = "test_arrow_flight_predict_db"
    def catalog_name = "test_arrow_flight_predict_catalog"

    sql """DROP DATABASE IF EXISTS ${db_name}"""

    sql """CREATE DATABASE IF NOT EXISTS ${db_name}"""

    sql """
        CREATE TABLE `${db_name}`.`test_arrow_flight_predict_t` (
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
        INSERT INTO `${db_name}`.`test_arrow_flight_predict_t` VALUES
        (1,1,'abc'),
        (2,2,'bcd'),
        (3,3,'cde'),
        (4,4,'def'),
        (5,5,'efg');
    """

    sql """
        DROP CATALOG IF EXISTS `${catalog_name}`
    """

    sql """
     CREATE CATALOG `${catalog_name}` PROPERTIES (
                'type' = 'arrow',
                'hosts' = '${arrow_flight_host}:${arrow_flight_arrow_port}',
                'user' = '${arrow_flight_user}',
                'password' = '${arrow_flight_psw}',
                'flight_sql_catalog_name' = 'internal'
        );
    """

    sql """use ${catalog_name}.${db_name}"""

    explain {
        sql("select * from test_arrow_flight_predict_t where c_int < 3")
        contains("c_int < 3")
    }

    explain {
        sql("select * from test_arrow_flight_predict_t where c_int <= 3")
        contains("c_int <= 3")
    }

    explain {
        sql("select * from test_arrow_flight_predict_t where c_int > 3")
        contains("c_int > 3")
    }

    explain {
        sql("select * from test_arrow_flight_predict_t where c_int >= 3")
        contains("c_int >= 3")
    }

    explain {
        sql("select * from test_arrow_flight_predict_t where c_int = 3")
        contains("c_int = 3")
    }

    explain {
        sql("select * from test_arrow_flight_predict_t where c_int != 3")
        contains("c_int != 3")
    }

    explain {
        sql("select * from test_arrow_flight_predict_t where c_int > 3 AND c_string = 'cde'")
        contains("((c_int > 3)) AND ((c_string = 'cde'))")
    }

    explain {
        sql("select * from test_arrow_flight_predict_t where c_int > 3 OR c_string = 'cde'")
        contains("(c_int > 3) OR (c_string = 'cde')")
    }

    explain {
        sql("select * from test_arrow_flight_predict_t where NOT c_int > 3")
        contains("c_int <= 3")
    }

    explain {
        sql("select * from test_arrow_flight_predict_t where c_string LIKE '%d%'")
        contains("c_string like '%d%'")
    }

    explain {
        sql("select * from test_arrow_flight_predict_t where NOT c_string LIKE '%d%'")
        contains("NOT c_string like '%d%'")
    }

    explain {
        sql("select * from test_arrow_flight_predict_t where c_int IN(2,3,4)")
        contains("c_int IN (2, 3, 4)")
    }

    explain {
        sql("select * from test_arrow_flight_predict_t where c_string IS NULL")
        contains("c_string IS NULL")
    }

    explain {
        sql("select * from test_arrow_flight_predict_t where c_string IS NOT NULL")
        contains("c_string IS NOT NULL")
    }

    explain {
        sql("select * from test_arrow_flight_predict_t where trim_in(c_string,'a') = 'bc';")
        contains("(trim_in(c_string, 'a') = 'bc'")
    }
}

