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

suite("test_iceberg_optimize_min_max", "p0,external,doris,external_docker,external_docker_doris") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        return
    }

    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalog_name = "test_iceberg_optimize_min_max_catalog"

    try {

        sql """drop catalog if exists ${catalog_name}"""
        sql """CREATE CATALOG ${catalog_name} PROPERTIES (
                'type'='iceberg',
                'iceberg.catalog.type'='rest',
                'uri' = 'http://${externalEnvIp}:${rest_port}',
                "s3.access_key" = "admin",
                "s3.secret_key" = "password",
                "s3.endpoint" = "http://${externalEnvIp}:${minio_port}",
                "s3.region" = "us-east-1"
            );"""

        sql """ switch ${catalog_name} """
        sql """ use format_v2 """

        String SQLSTR = """
                MIN(id) AS id_min,
                MAX(id) AS id_max,
                MIN(col_boolean) AS col_boolean_min,
                MAX(col_boolean) AS col_boolean_max,
                MIN(col_short) AS col_short_min,
                MAX(col_short) AS col_short_max,
                MIN(col_byte) AS col_byte_min,
                MAX(col_byte) AS col_byte_max,
                MIN(col_integer) AS col_integer_min,
                MAX(col_integer) AS col_integer_max,
                MIN(col_long) AS col_long_min,
                MAX(col_long) AS col_long_max,
                MIN(col_float) AS col_float_min,
                MAX(col_float) AS col_float_max,
                MIN(col_double) AS col_double_min,
                MAX(col_double) AS col_double_max,
                MIN(col_date) AS col_date_min,
                MAX(col_date) AS col_date_max
        """;
        def sqlstr1 = """ SELECT ${SQLSTR} FROM sample_mor_parquet; """
        def sqlstr2 = """ select ${SQLSTR} from sample_cow_parquet; """
        def sqlstr3 = """ select ${SQLSTR} from sample_mor_orc; """
        def sqlstr4 = """ select ${SQLSTR} from sample_mor_parquet; """

        // don't use push down count mix/max
        sql """ set enable_iceberg_min_max_optimization=false; """
        qt_false01 """${sqlstr1}""" 
        qt_false02 """${sqlstr2}""" 
        qt_false03 """${sqlstr3}""" 
        qt_false04 """${sqlstr4}""" 

        // use push down count min/max
        sql """ set enable_iceberg_min_max_optimization=true; """
        for (String val: ["1K", "0"]) {
            sql "set file_split_size=${val}"
            qt_q01 """${sqlstr1}""" 
            qt_q02 """${sqlstr2}""" 
            qt_q03 """${sqlstr3}""" 
            qt_q04 """${sqlstr4}""" 
        }
        sql "unset variable file_split_size;"

        // traditional mode
        sql """set num_files_in_batch_mode=100000"""
        explain {
            sql("""select * from sample_cow_orc""")
            notContains "approximate"
        }
        explain {
            sql("""${sqlstr1}""")
            notContains "approximate"
            contains """pushdown agg=MINMAX"""
            contains """opt/total_splits(32/41)"""
        }
        explain {
            sql("""select * from sample_cow_parquet""")
            notContains "approximate"
        }
        explain {
            sql("""${sqlstr2}""")
            notContains "approximate"
            contains """pushdown agg=MINMAX"""
            contains """opt/total_splits(32/32)"""
        }
        explain {
            sql("""select * from sample_mor_orc""")
            notContains "approximate"
        }
        explain {
            sql("""${sqlstr3}""")
            notContains "approximate"
            contains """pushdown agg=MINMAX"""
            contains """opt/total_splits(32/32)"""
        }
        // because it has dangling delete
        explain {
            sql("""${sqlstr4}""")
            notContains "approximate"
            contains """pushdown agg=MINMAX"""
            contains """opt/total_splits(32/41)"""
        }

        // batch mode
        sql """set num_files_in_batch_mode=1"""
        explain {
            sql("""select * from sample_cow_orc""")
            contains "approximate"
        }
        explain {
            sql("""${sqlstr1}""")
            contains "approximate"
            contains """pushdown agg=MINMAX"""
            contains """opt/total_splits""" //total is not accurate in batch mode
        }
        explain {
            sql("""select * from sample_cow_parquet""")
            contains "approximate"
        }
        explain {
            sql("""${sqlstr2}""")
            contains "approximate"
            contains """pushdown agg=MINMAX"""
            contains """opt/total_splits""" //total is not accurate in batch mode
        }
        explain {
            sql("""select * from sample_mor_orc""")
            contains "approximate"
        }
        explain {
            sql("""${sqlstr3}""")
            contains "approximate"
            contains """pushdown agg=MINMAX"""
            contains """opt/total_splits""" //total is not accurate in batch mode
        }
        explain {
            sql("""select * from sample_mor_parquet""")
            contains "approximate"
        }
        // because it has dangling delete
        explain {
            sql("""${sqlstr4}""")
            contains "approximate"
            contains """pushdown agg=MINMAX"""
            contains """opt/total_splits""" //total is not accurate in batch mode
        }

        // don't use push down min/max
        sql """ set enable_iceberg_min_max_optimization=false; """

        explain {
            sql("""${sqlstr1}""")
            contains """pushdown agg=NONE"""
        }
        explain {
            sql("""${sqlstr2}""")
            contains """pushdown agg=NONE"""
        }
        explain {
            sql("""${sqlstr3}""")
            contains """pushdown agg=NONE"""
        }
        explain {
            sql("""${sqlstr4}""")
            contains """pushdown agg=NONE"""
        }

        // There has `dangling delete` after rewrite
        sql """ set enable_iceberg_min_max_optimization=true; """
        def sqlstr5 = """ select min(id),max(id) from ${catalog_name}.test_db.dangling_delete_after_write; """
        explain {
            sql("""${sqlstr5}""")
            contains """pushdown agg=MINMAX"""
        }
        qt_q09 """${sqlstr5}""" 

        sql """ set enable_iceberg_min_max_optimization=false"""
        explain {
            sql("""${sqlstr5}""")
            contains """pushdown agg=NONE"""
        }
        qt_q10 """${sqlstr5}""" 

        sql """ set enable_iceberg_min_max_optimization=true; """
        // "col_timestamp", "col_timestamp_ntz","col_decimal" is not supported, but show pushdown agg=MINMAX
        // need to fix it later in explain
        for (String val: [ "col_char", "col_varchar", "col_string", "col_binary","city"]) {
            explain {
                sql """select min(${val}),max(${val}) from ${catalog_name}.format_v2.sample_cow_orc;"""
                contains """pushdown agg=NONE"""
            }
            explain {
                sql """select min(${val}),max(${val}) from ${catalog_name}.format_v2.sample_cow_parquet;"""
                contains """pushdown agg=NONE"""
            }
            explain {
                sql """select min(${val}),max(${val}) from ${catalog_name}.format_v2.sample_mor_orc;"""
                contains """pushdown agg=NONE"""
            }
            explain {
                sql """select min(${val}),max(${val}) from ${catalog_name}.format_v2.sample_mor_parquet;"""
                contains """pushdown agg=NONE"""
            }
        }


    } finally {
        // sql """drop catalog if exists ${catalog_name}"""
    }
}

