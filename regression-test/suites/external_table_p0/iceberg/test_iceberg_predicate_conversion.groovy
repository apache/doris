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

suite("test_iceberg_predicate_conversion", "p0,external,doris,external_docker,external_docker_doris") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable iceberg test.")
        return
    }

    String catalog_name = "test_iceberg_predicate_conversion"
    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")


    sql """drop catalog if exists ${catalog_name}"""
    sql """
    CREATE CATALOG ${catalog_name} PROPERTIES (
        'type'='iceberg',
        'iceberg.catalog.type'='rest',
        'uri' = 'http://${externalEnvIp}:${rest_port}',
        "s3.access_key" = "admin",
        "s3.secret_key" = "password",
        "s3.endpoint" = "http://${externalEnvIp}:${minio_port}",
        "s3.region" = "us-east-1"
    );"""

        sql """switch ${catalog_name};"""
        sql """ use `multi_catalog`; """

        def sqlstr = """select glue_varchar from tb_predict where glue_varchar > date '2023-03-07' """
        order_qt_q01 """${sqlstr}""" 
        explain {
            sql("""${sqlstr}""")
            contains """ref(name="glue_varchar") > "2023-03-07 00:00:00"""
        }

        sqlstr = """select l_shipdate from tb_predict where l_shipdate in ("1997-05-18", "1996-05-06"); """
        order_qt_q02 """${sqlstr}""" 
        explain {
            sql("""${sqlstr}""")
            contains """ref(name="l_shipdate") in"""
            contains """1997-05-18"""
            contains """1996-05-06"""
        }

        sqlstr = """select l_shipdate, l_shipmode from tb_predict where l_shipdate in ("1997-05-18", "1996-05-06") and l_shipmode = "MAIL";"""
        order_qt_q03 """${sqlstr}""" 
        explain {
            sql("""${sqlstr}""")
            contains """ref(name="l_shipdate") in"""
            contains """1997-05-18"""
            contains """1996-05-06"""
            contains """ref(name="l_shipmode") == "MAIL"""
        }

        sqlstr = """select l_shipdate, l_shipmode from tb_predict where l_shipdate in ("1997-05-18", "1996-05-06") or NOT(l_shipmode = "MAIL") order by l_shipdate, l_shipmode limit 10"""
        plan = """(ref(name="l_shipdate") in ("1997-05-18", "1996-05-06") or not(ref(name="l_shipmode") == "MAIL"))"""
        order_qt_q04 """${sqlstr}""" 
        explain {
            sql("""${sqlstr}""")
            contains """or not(ref(name="l_shipmode") == "MAIL"))"""
            contains """ref(name="l_shipdate")"""
            contains """1997-05-18"""
            contains """1996-05-06"""
        }

        sqlstr = """select glue_timstamp from tb_predict where glue_timstamp > '2023-03-07 20:35:59' order by glue_timstamp limit 5"""
        order_qt_q05 """${sqlstr}""" 
        explain {
            sql("""${sqlstr}""")
            contains """ref(name="glue_timstamp") > 1678192559000000"""
        }
}

/*

create table tb_predict (
    glue_varchar string,
    glue_timstamp timestamp,
    l_shipdate date,
    l_shipmode string
) using iceberg;

insert into tb_predict values ('2023-03-08', timestamp '2023-03-07 20:35:59.123456', date "1997-05-19", "MAIL");
insert into tb_predict values ('2023-03-06', timestamp '2023-03-07 20:35:58', date "1997-05-19", "MAI");
insert into tb_predict values ('2023-03-07', timestamp '2023-03-07 20:35:59.123456', date "1997-05-18", "MAIL");
insert into tb_predict values ('2023-03-07', timestamp '2023-03-07 20:35:59', date "1997-05-18", "MAI");
insert into tb_predict values ('2023-03-07', timestamp '2023-03-07 20:35:58', date "1996-05-06", "MAIL");
insert into tb_predict values ('2023-03-04', timestamp '2023-03-07 20:36:00', date "1996-05-06", "MAI");
insert into tb_predict values ('2023-03-07', timestamp '2023-03-07 20:34:59', date "1996-05-01", "MAIL");
insert into tb_predict values ('2023-03-09', timestamp '2023-03-07 20:37:59', date "1996-05-01", "MAI");

*/
