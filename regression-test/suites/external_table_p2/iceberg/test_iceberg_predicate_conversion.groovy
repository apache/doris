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

suite("test_iceberg_predicate_conversion", "p2,external,hive,external_remote,external_remote_hive") {
    String enabled = context.config.otherConfigs.get("enableExternalHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String extHiveHmsHost = context.config.otherConfigs.get("extHiveHmsHost")
        String extHiveHmsPort = context.config.otherConfigs.get("extHiveHmsPort")

        sql """drop catalog if exists test_iceberg_predicate_conversion;"""
        sql """
            create catalog if not exists test_iceberg_predicate_conversion properties (
                'type'='hms',
                'hive.metastore.uris' = 'thrift://${extHiveHmsHost}:${extHiveHmsPort}'
            );
        """

        sql """switch test_iceberg_predicate_conversion;"""
        sql """ use `iceberg_catalog`; """

        def sqlstr = """select glue_int, glue_varchar from iceberg_glue_types where glue_varchar > date '2023-03-07' """
        order_qt_q01 """${sqlstr}""" 
        explain {
            sql("""${sqlstr}""")
            contains """ref(name="glue_varchar") > "2023-03-07 00:00:00"""
        }

        sqlstr = """select l_shipdate from lineitem where l_shipdate in ("1997-05-18", "1996-05-06"); """
        order_qt_q02 """${sqlstr}""" 
        explain {
            sql("""${sqlstr}""")
            contains """ref(name="l_shipdate") in"""
            contains """"1997-05-18""""
            contains """"1996-05-06""""
        }

        sqlstr = """select l_shipdate, l_shipmode from lineitem where l_shipdate in ("1997-05-18", "1996-05-06") and l_shipmode = "MAIL";"""
        order_qt_q03 """${sqlstr}""" 
        explain {
            sql("""${sqlstr}""")
            contains """ref(name="l_shipdate") in"""
            contains """"1997-05-18""""
            contains """"1996-05-06""""
            contains """ref(name="l_shipmode") == "MAIL""""
        }

        sqlstr = """select l_shipdate, l_shipmode from lineitem where l_shipdate in ("1997-05-18", "1996-05-06") or NOT(l_shipmode = "MAIL") order by l_shipdate, l_shipmode limit 10"""
        plan = """(ref(name="l_shipdate") in ("1997-05-18", "1996-05-06") or not(ref(name="l_shipmode") == "MAIL"))"""
        order_qt_q04 """${sqlstr}""" 
        explain {
            sql("""${sqlstr}""")
            contains """or not(ref(name="l_shipmode") == "MAIL"))"""
            contains """ref(name="l_shipdate")"""
            contains """"1997-05-18""""
            contains """"1996-05-06""""
        }

        sqlstr = """select glue_timstamp from iceberg_glue_types where glue_timstamp > '2023-03-07 20:35:59' order by glue_timstamp limit 5"""
        order_qt_q04 """${sqlstr}""" 
        explain {
            sql("""${sqlstr}""")
            contains """ref(name="glue_timstamp") > 1678192559000000"""
        }
    }
}
