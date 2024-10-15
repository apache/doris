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

suite("test_iceberg_optimize_count", "p0,external,doris,external_docker,external_docker_doris") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        return
    }

    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalog_name = "test_iceberg_optimize_count"

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

        sqlstr1 = """ select count(*) from sample_cow_orc; """
        sqlstr2 = """ select count(*) from sample_cow_parquet; """
        sqlstr3 = """ select count(*) from sample_mor_orc; """
        sqlstr4 = """ select count(*) from sample_mor_parquet; """

        // use push down count
        sql """ set enable_count_push_down_for_external_table=true; """

        qt_q01 """${sqlstr1}""" 
        qt_q02 """${sqlstr2}""" 
        qt_q03 """${sqlstr3}""" 
        qt_q04 """${sqlstr4}""" 

        explain {
            sql("""${sqlstr1}""")
            contains """pushdown agg=COUNT (1000)"""
        }
        explain {
            sql("""${sqlstr2}""")
            contains """pushdown agg=COUNT (1000)"""
        }
        explain {
            sql("""${sqlstr3}""")
            contains """pushdown agg=COUNT (1000)"""
        }
        explain {
            sql("""${sqlstr4}""")
            contains """pushdown agg=COUNT (1000)"""
        }

        // don't use push down count
        sql """ set enable_count_push_down_for_external_table=false; """

        qt_q05 """${sqlstr1}""" 
        qt_q06 """${sqlstr2}""" 
        qt_q07 """${sqlstr3}""" 
        qt_q08 """${sqlstr4}""" 

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

    } finally {
        sql """ set enable_count_push_down_for_external_table=true; """
        sql """drop catalog if exists ${catalog_name}"""
    }
}

