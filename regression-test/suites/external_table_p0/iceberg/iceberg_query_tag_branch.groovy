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

suite("iceberg_query_tag_branch", "p0,external,doris,external_docker,external_docker_doris,branch_tag") {

    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalog_name = "iceberg_query_tag_branch"

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


    logger.info("catalog " + catalog_name + " created")
    sql """switch ${catalog_name};"""
    logger.info("switched to catalog " + catalog_name)
    sql """ use test_db;""" 

    def query_tag_branch_only = {

        qt_branch_1  """ select * from tag_branch_table@branch(b1) order by c1;""" 
        qt_branch_2  """ select c1 from tag_branch_table@branch(b1) order by c1;""" 
        qt_branch_3  """ select c1,c2,c3 from tag_branch_table@branch(b1) order by c1;""" 
        qt_branch_4  """ select * from tag_branch_table@branch(b2) order by c1 ;""" 
        qt_branch_5  """ select c1 from tag_branch_table@branch(b2) order by c1;""" 
        qt_branch_6  """ select c1,c2,c3 from tag_branch_table@branch(b2) order by c1;""" 
        qt_branch_6  """ select * from tag_branch_table@branch(b3) order by c1 ;""" 
        qt_branch_7  """ select c1 from tag_branch_table@branch(b3) order by c1;""" 
        qt_branch_9  """ select c1,c2,c3 from tag_branch_table@branch(b3) order by c1;""" 

        qt_branch_10  """ select * from tag_branch_table@branch('name'='b1') order by c1 ;""" 
        qt_branch_11  """ select c1 from tag_branch_table@branch('name'='b1') order by c1 ;""" 
        qt_branch_12  """ select c1,c2,c3 from tag_branch_table@branch(b1) order by c1;""" 
        qt_branch_13  """ select * from tag_branch_table@branch('name'='b2') order by c1 ;""" 
        qt_branch_14  """ select c1,c2 from tag_branch_table@branch('name'='b2') order by c1 ;""" 
        qt_branch_15  """ select c1,c2,c3 from tag_branch_table@branch(b2) order by c1;""" 
        qt_branch_16  """ select * from tag_branch_table@branch('name'='b3') order by c1 ;""" 
        qt_branch_17  """ select c1,c2 from tag_branch_table@branch('name'='b3') order by c1 ;""" 
        qt_branch_18  """ select c1,c2,c3 from tag_branch_table@branch(b3) order by c1;""" 

        qt_tag_1  """ select * from tag_branch_table@tag(t1) order by c1 ;""" 
        qt_tag_2  """ select c1 from tag_branch_table@tag(t1) order by c1 ;""" 
        qt_tag_3  """ select * from tag_branch_table@tag(t2) order by c1 ;""" 
        qt_tag_4  """ select c1 from tag_branch_table@tag(t2) order by c1 ;""" 
        qt_tag_5  """ select * from tag_branch_table@tag(t3) order by c1 ;""" 
        qt_tag_6  """ select c1,c2 from tag_branch_table@tag(t3) order by c1 ;""" 

        qt_tag_7  """ select * from tag_branch_table@tag('name'='t1') order by c1 ;""" 
        qt_tag_8  """ select c1 from tag_branch_table@tag('name'='t1') order by c1 """
        qt_tag_9  """ select * from tag_branch_table@tag('name'='t2') order by c1 ;""" 
        qt_tag_10  """ select c1 from tag_branch_table@tag('name'='t2') order by c1 """
        qt_tag_11  """ select * from tag_branch_table@tag('name'='t3') order by c1 ;""" 
        qt_tag_12  """ select c1 from tag_branch_table@tag('name'='t3') order by c1 """
        qt_tag_13  """ select c1,c2 from tag_branch_table@tag('name'='t3') order by c1 """

        qt_version_1  """ select * from tag_branch_table for version as of 'b1' order by c1 ;""" 
        qt_version_2  """ select c1,c2,c3 from tag_branch_table for version as of 'b1' order by c1 ;""" 
        qt_version_3  """ select * from tag_branch_table for version as of 'b2' order by c1 ;""" 
        qt_version_4  """ select c1,c2,c3 from tag_branch_table for version as of 'b2' order by c1 ;""" 
        qt_version_5  """ select * from tag_branch_table for version as of 'b3' order by c1 ;""" 
        qt_version_6  """ select c1,c2,c3 from tag_branch_table for version as of 'b3' order by c1 ;""" 

        qt_version_7  """ select * from tag_branch_table for version as of 't1' order by c1 ;""" 
        qt_version_8  """ select c1 from tag_branch_table for version as of 't1' order by c1 ;""" 
        qt_version_9  """ select * from tag_branch_table for version as of 't2' order by c1;""" 
        qt_version_10  """ select c1 from tag_branch_table for version as of 't2' order by c1 ;""" 
        qt_version_11  """ select * from tag_branch_table for version as of 't3' order by c1 ;""" 
        qt_version_12  """ select c1 from tag_branch_table for version as of 't3' order by c1 ;""" 
        qt_version_13  """ select c1,c2 from tag_branch_table for version as of 't3' order by c1 ;""" 

        qt_count_branch_1 """ select count(*) from tag_branch_table@branch(b1);""" 
        qt_count_branch_2 """ select count(*) from tag_branch_table@branch(b2);""" 
        qt_count_branch_3 """ select count(*) from tag_branch_table@branch(b3);""" 

        qt_count_tag_1  """ select count(*) from tag_branch_table@tag(t1);""" 
        qt_count_tag_2  """ select count(*) from tag_branch_table@tag(t2);""" 
        qt_count_tag_3  """ select count(*) from tag_branch_table@tag(t3);""" 
    }

    def query_tag_branch_in_subquery = {
        qt_sub_join_branch_with_branch_1 """ SELECT t1.c1, t1.c2, t1.c3, t2.c1, t2.c2, t2.c3
                        FROM tag_branch_table@branch(b1) t1
                        JOIN tag_branch_table@branch(b2) t2
                        ON t1.c1 = t2.c1 order by t1.c1;  """
        qt_sub_join_branch_with_branch_2 """ SELECT t1.c1, t1.c2, t1.c3, t2.c1, t2.c2, t2.c3
                        FROM tag_branch_table@branch(b1) t1
                        JOIN tag_branch_table@branch(b3) t2
                        ON t1.c1 = t2.c1 order by t1.c1;  """
        qt_sub_join_branch_with_branch_3 """ SELECT t1.c1, t1.c2, t1.c3, t2.c1, t2.c2, t2.c3
                        FROM tag_branch_table@branch(b2) t1
                        JOIN tag_branch_table@branch(b3) t2
                        ON t1.c1 = t2.c1 order by t1.c1;  """
        qt_sub_join_branch_with_branch_4 """ SELECT t1.c1, t1.c2, t1.c3, t2.c1, t2.c2, t2.c3
                        FROM tag_branch_table@branch(b1) t1
                        JOIN tag_branch_table@branch(b1) t2
                        ON t1.c1 = t2.c1 order by t1.c1;  """
        qt_sub_join_branch_with_branch_5 """ SELECT t1.c1, t1.c2, t1.c3, t2.c1, t2.c2, t2.c3
                        FROM tag_branch_table@branch(b3) t1
                        JOIN tag_branch_table@branch(b3) t2
                        ON t1.c1 = t2.c1 order by t1.c1;  """

        qt_sub_join_tag_with_tag_1 """ SELECT t1.c1, t2.c1
                        FROM tag_branch_table@tag(t1) t1
                        JOIN tag_branch_table@tag(t2) t2
                        ON t1.c1 = t2.c1 order by t1.c1;  """

        qt_sub_join_tag_with_tag_2 """ SELECT t1.c1, t2.c1
                        FROM tag_branch_table@tag(t1) t1
                        JOIN tag_branch_table@tag(t3) t2
                        ON t1.c1 = t2.c1 order by t1.c1;  """

        qt_sub_join_tag_with_tag_3 """ SELECT t1.c1, t2.c1
                        FROM tag_branch_table@tag(t2) t1
                        JOIN tag_branch_table@tag(t3) t2
                        ON t1.c1 = t2.c1 order by t1.c1;  """

        qt_sub_join_tag_with_tag_4 """ SELECT t1.c1, t2.c1
                        FROM tag_branch_table@tag(t1) t1
                        JOIN tag_branch_table@tag(t1) t2
                        ON t1.c1 = t2.c1 order by t1.c1;  """
    
        qt_sub_join_tag_with_branch_1 """ SELECT t1.c1, t2.c1
                        FROM tag_branch_table@tag(t1) t1
                        JOIN tag_branch_table@branch(b1) t2
                        ON t1.c1 = t2.c1 order by t1.c1;  """
        qt_sub_join_tag_with_branch_2 """ SELECT t1.c1, t2.c1
                        FROM tag_branch_table@tag(t3) t1
                        JOIN tag_branch_table@branch(b3) t2
                        ON t1.c1 = t2.c1 order by t1.c1;  """
        qt_sub_join_tag_with_branch_3 """ SELECT t1.c1, t1.c2, t2.c1, t2.c2
                        FROM tag_branch_table@tag(t3) t1
                        JOIN tag_branch_table@branch(b3) t2
                        ON t1.c1 = t2.c1
                        WHERE t1.c1 > 1
                        order by t1.c1;  """

        qt_sub_with_branch_1 """ WITH t1 AS ( SELECT c1,c2 FROM tag_branch_table@branch(b1) WHERE c1 > 0) SELECT * FROM t1 order by c1; """
        qt_sub_with_branch_2 """ WITH t1 AS ( SELECT c1,c2 FROM tag_branch_table@branch(b2) WHERE c1 > 1) SELECT * FROM t1 order by c1; """
        qt_sub_with_branch_3 """ WITH t1 AS ( SELECT c1,c2,c3 FROM tag_branch_table@branch(b3) WHERE c2 IS NOT NULL) SELECT * FROM t1 order by c1; """
        qt_sub_with_branch_4 """ WITH t1 AS ( SELECT c1,c2,c3 FROM tag_branch_table@branch(b3) WHERE c1 > 1) SELECT * FROM t1 order by c1; """

        qt_sub_with_tag_1 """ WITH t1 AS ( SELECT c1 FROM tag_branch_table@tag(t1) WHERE c1 > 0) SELECT * FROM t1 order by c1; """
        qt_sub_with_tag_2 """ WITH t1 AS ( SELECT c1 FROM tag_branch_table@tag(t2) WHERE c1 > 1) SELECT * FROM t1 order by c1; """
        qt_sub_with_tag_3 """ WITH t1 AS ( SELECT c1,c2 FROM tag_branch_table@tag(t3) WHERE c2 IS NOT NULL) SELECT * FROM t1 order by c1; """
        qt_sub_with_tag_4 """ WITH t1 AS ( SELECT c1,c2 FROM tag_branch_table@tag(t3) WHERE c1 > 1) SELECT * FROM t1 order by c1; """
    }

    def query_exception = {
        test {
            sql """ select * from tag_branch_table@branch('name'='not_exists_branch'); """
            exception "UserException"
        }
        test {
            sql """ select * from tag_branch_table@branch('nme'='not_exists_branch'); """
            exception "must contain key 'name' in params"
        }
        test {
            sql """ select * from tag_branch_table@branch(not_exists_branch); """
            exception "UserException"
        }
        test {
            sql """ select * from tag_branch_table for version as of 'not_exists_branch'; """
            exception "UserException"
        }

        test {
            sql """ select * from tag_branch_table@tag('name'='not_exists_tag'); """
            exception "UserException"
        }
        test {
            sql """ select * from tag_branch_table@tag('na'='not_exists_tag'); """
            exception "must contain key 'name' in params"
        }
        test {
            sql """ select * from tag_branch_table@tag(not_exists_tag); """
            exception "UserException"
        }
        test {
            sql """ select * from tag_branch_table for version as of 'not_exists_tag'; """
            exception "UserException"
        }

        // Use branch function to query tags
        test {
            sql """ select * from tag_branch_table@branch(t1) ; """
            exception "does not have branch named t1"
        }
        // Use tag function to query branch
        test {
            sql """ select * from tag_branch_table@tag(b1) ; """
            exception "does not have tag named b1"
        }

        test {
            sql """ select * from tag_branch_table@brand(b1) ; """
            exception "Invalid param type: brand"
        }
    }

    try {
        // use batch mode
        sql """  set num_files_in_batch_mode=1; """ 
        query_tag_branch_only()
        query_tag_branch_in_subquery()
        query_exception()

        // use none-batch mode
        sql """  set num_files_in_batch_mode=1024; """ 
        query_tag_branch_only()
        query_tag_branch_in_subquery()
        query_exception()

        // test sql. no result check because snapshot_id is changed every time
        sql """
            SELECT
              refs_data.snapshot_id,
              snapshots.committed_at,
              snapshots.operation,
              ARRAY_SORT(refs_data.refs)
            FROM (
              SELECT
                snapshot_id,
                ARRAY_AGG(CONCAT(type, ':', name)) AS refs
              FROM
                tag_branch_table\$refs
              GROUP BY
                snapshot_id
            ) AS refs_data
            JOIN (
              SELECT
                snapshot_id,
                committed_at,
                operation
              FROM
                tag_branch_table\$snapshots
            ) AS snapshots
            ON refs_data.snapshot_id = snapshots.snapshot_id
            ORDER BY
              snapshots.committed_at;
        """

    } finally {
        // Restore default values
        sql """  set num_files_in_batch_mode=1024; """ 
    }
}
