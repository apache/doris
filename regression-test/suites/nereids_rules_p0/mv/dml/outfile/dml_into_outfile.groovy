package mv.dml.outfile
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

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths

suite("dml_into_outfile", "p0") {
    // open nereids
    sql """ set enable_nereids_planner=true """
    sql """ set enable_fallback_to_original_planner=false """

    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "set runtime_filter_mode=OFF";
    sql "SET ignore_shape_nodes='PhysicalDistribute,PhysicalProject'"

    String ak = getS3AK()
    String sk = getS3SK()
    String s3_endpoint = getS3Endpoint()
    String region = getS3Region()
    String bucket = context.config.otherConfigs.get("s3BucketName");

    def outFilePath = "${bucket}/outfile/parquet/dml_mv_rewrite/rewritten_"
    def outfile_format = "parquet"


    sql """
    drop table if exists partsupp
    """

    sql"""
    CREATE TABLE IF NOT EXISTS partsupp (
      ps_partkey     INTEGER NOT NULL,
      ps_suppkey     INTEGER NOT NULL,
      ps_availqty    INTEGER NOT NULL,
      ps_supplycost  DECIMALV3(15,2)  NOT NULL,
      ps_comment     VARCHAR(199) NOT NULL 
    )
    DUPLICATE KEY(ps_partkey, ps_suppkey)
    DISTRIBUTED BY HASH(ps_partkey) BUCKETS 3
    PROPERTIES (
      "replication_num" = "1"
    )
    """

    sql """
    insert into partsupp values
    (2, 3, 9, 10.01, 'supply1'),
    (2, 3, 10, 11.01, 'supply2');
    """

    sql """analyze table partsupp with sync;"""

    def create_async_mv = { mv_name, mv_sql ->
        sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name}"""
        sql"""
        CREATE MATERIALIZED VIEW ${mv_name} 
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1') 
        AS ${mv_sql}
        """
        waitingMTMVTaskFinished(getJobName(db, mv_name))
    }

    def outfile_to_S3 = {query_sql ->
        // select ... into outfile ...
        def res = sql """
            ${query_sql};
        """
        return res[0][3]
    }


    // 1. test into outfile when async mv
    def into_outfile_async_mv_name = 'partsupp_agg'
    def into_outfile_async_query = """
        select
        ps_partkey,
        ps_suppkey,
        ps_availqty,
        ps_supplycost,
        ps_comment
        from
        partsupp
        group by
        ps_partkey,
        ps_suppkey,
        ps_availqty,
        ps_supplycost,
        ps_comment
        INTO OUTFILE "s3://${outFilePath}"
        FORMAT AS ${outfile_format}
        PROPERTIES (
                "s3.endpoint" = "${s3_endpoint}",
                "s3.region" = "${region}",
                "s3.secret_key"="${sk}",
                "s3.access_key" = "${ak}"
            );
    """

    create_async_mv(into_outfile_async_mv_name,
            """select
        ps_partkey,
        ps_suppkey,
        ps_availqty,
        ps_supplycost,
        ps_comment
        from
        partsupp
        group by
        ps_partkey,
        ps_suppkey,
        ps_availqty,
        ps_supplycost,
        ps_comment;""")

    // disable query rewrite by mv
    sql "set enable_materialized_view_rewrite=false";
    // enable dml rewrite by mv
    sql "set enable_dml_materialized_view_rewrite=true";

    explain {
        sql """${into_outfile_async_query}"""
        check {result ->
            def splitResult = result.split("MaterializedViewRewriteFail")
            splitResult.length == 2 ? splitResult[0].contains(into_outfile_async_mv_name) : false
        }
    }

    def outfile_url = outfile_to_S3(into_outfile_async_query)
    order_qt_query_into_outfile_async_mv_after """ SELECT * FROM S3 (
                "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.${outfile_format}",
                "ACCESS_KEY"= "${ak}",
                "SECRET_KEY" = "${sk}",
                "format" = "${outfile_format}",
                "region" = "${region}"
            );
            """
    sql """DROP MATERIALIZED VIEW IF EXISTS ${into_outfile_async_mv_name}"""


    // 2. test into outfile when sync mv
    def into_outfile_sync_mv_name = 'group_by_each_column_sync_mv'
    def into_outfile_sync_query = """
        select
        ps_partkey,
        ps_suppkey,
        ps_availqty,
        ps_supplycost,
        ps_comment
        from
        partsupp
        group by
        ps_partkey,
        ps_suppkey,
        ps_availqty,
        ps_supplycost,
        ps_comment
        INTO OUTFILE "s3://${outFilePath}"
        FORMAT AS ${outfile_format}
        PROPERTIES (
                "s3.endpoint" = "${s3_endpoint}",
                "s3.region" = "${region}",
                "s3.secret_key"="${sk}",
                "s3.access_key" = "${ak}"
            );
    """
    createMV(""" create materialized view ${into_outfile_sync_mv_name}
        as select
        ps_partkey,
        ps_suppkey,
        ps_availqty,
        ps_supplycost,
        ps_comment,
        count(*)
        from
        partsupp
        group by
        ps_partkey,
        ps_suppkey,
        ps_availqty,
        ps_supplycost,
        ps_comment;""")

    // disable query rewrite by mv
    sql "set enable_materialized_view_rewrite=false";
    // enable dml rewrite by mv
    sql "set enable_dml_materialized_view_rewrite=true";

    explain {
        sql """${into_outfile_sync_query}"""
        check {result ->
            def splitResult = result.split("MaterializedViewRewriteFail")
            splitResult.length == 2 ? splitResult[0].contains(into_outfile_sync_mv_name) : false
        }
    }

    def sync_outfile_url = outfile_to_S3(into_outfile_sync_query)
    order_qt_query_into_outfile_sync_mv_after """ SELECT * FROM S3 (
                "uri" = "http://${bucket}.${s3_endpoint}${sync_outfile_url.substring(5 + bucket.length(), sync_outfile_url.length() - 1)}0.${outfile_format}",
                "ACCESS_KEY"= "${ak}",
                "SECRET_KEY" = "${sk}",
                "format" = "${outfile_format}",
                "region" = "${region}"
            );
            """
    sql """drop materialized view if exists ${into_outfile_sync_mv_name} on partsupp;"""
}
